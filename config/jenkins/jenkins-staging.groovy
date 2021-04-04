/*  Description for job build-publish-deploy-test-staging
builds, packages, publishes - images, deploys to staging and runs newman tests for Agave core services in the staging environment .

**Stages of pipeline and their function**

From master node

+ stage ('Start with clean workspace') **default false**
      * clean the workspace 'stage'....
+ stage ('Checkout Agave source') **default true**
      * code checkout 'stage'....
+ stage ('Build jars') **default true**
      * execute a maven build to create the java jar artifacts
+ stage('Run Unit tests') **default true**
      *  calls out to job "develop_unit_tests" to run unit tests based on unit grouping for TestNG.
+ stage ('Build dev images') **default true**
      *  build the images for services.
+ stage ('Publish dev images') **default true**
      *  publish the images to private registry locally
+ stage ('Publish to dockerhub') **default false**
      *  publish the images to dockerhub registry
+ stage ('Deploy to staging') **default true**
      *  pulling latest deployer image
      *  login to the private registry
      *  run deployer with update_core playbook
+ stage ('Newman integration tests') **default true**


Each stage may be turned on or off based on a parameter resembling the stage name.
The default is on
 */

node('master') {
   
    echo "Execute stage \"Build images\" : ${env.run_build_images}"
    echo "Execute stage \"Publish images\" : ${env.run_publish_images}"
    echo "Execute stage \"Deploy to staging\" : ${env.run_deploy}"
    echo "Execute stage \"Newman integration tests\" : ${env.run_newman_tests}"
    echo "Execute Setting: \"Publish to dockerhub\" : ${env.run_publish_to_dockerhub}"
    def mvnHome = tool 'maven-3.5.0'
    def jdk = tool 'openjdk-9'
    echo "${env.PATH}"
    env.PATH = "${mvnHome}/bin:${jdk}/bin:${env.PATH}"
    echo "java version ${jdk}"

    slackSend color: "good", message: "Jenkins-3 Build Started: ${env.JOB_NAME} ${env.BUILD_NUMBER} Science API images. "

    // Mark the code checkout 'stage'....
    stage('Start with clean workspace'){
            echo "Cleaning the workspace"
            echo "workspace ${env.WORKSPACE}"
            sh " rm -rf ${env.WORKSPACE}/*"
            sh "ls -la"
    }

    stage('Checkout Agave source') {
            sh 'git version'
            sh 'git config --global user.email "deardooley@gmail.com"'
            sh 'git config --global user.name "deardooley"'

            checkout([$class                           : 'GitSCM',
                      branches                         : [[name: '*/staging']],
                      doGenerateSubmoduleConfigurations: false,
                      extensions                       : [],
                      submoduleCfg                     : [],
                      userRemoteConfigs                : [
                              [credentialsId: 'github_token',
                               refspec      : '+refs/heads/staging:refs/remotes/origin/staging',
                               url          : 'https://deardooley@github.com/agaveplatform/science-apis.git'
                              ]
                      ]
            ])
            sh 'git checkout staging'
            sh 'git pull origin staging'
            sh 'git branch -v'
    }
    
    AGAVE_VERSION = sh (
        script: 'mvn -s config/maven/settings-SAMPLE.xml org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep -v Downloading | grep -v Downloaded   | grep -e \'^[^\\[]\'',
        returnStdout: true
    ).trim()
    echo "Agave version : ${AGAVE_VERSION}"
    def AGAVE_BUILD_TAG = AGAVE_VERSION + "-dev"

    def DOCKER_REGISTRY_USER=new String(env.DOCKER_REGISTRY_USER)
    def DOCKER_REGISTRY_PASS=new String(env.DOCKER_REGISTRY_PASS)
    def DOCKER_REGISTRY_URL=new String(env.DOCKER_REGISTRY_URL)
    def DOCKER_REGISTRY_ORG=new String(env.DOCKER_REGISTRY_ORG)

    stage('Build jars') {
        def dobuild=new Boolean(env.run_build_images)
        def dopublish=new Boolean(env.run_publish_images)
        if (dobuild || dopublish) {
            echo "Building jars...."
            try {
                sh "mvn -s config/maven/settings-SAMPLE.xml -Pagave,plain -B install -DskipDocker=true"
            }
            catch (err) {
                slackSend color: "red", message: "Jenkins-3 Failed to compile the core services. Failed build is on display at <${env.BUILD_URL}|here>."
            }
        } else {
            echo "Skipping the \"Build jars\" stage...."
        }
    }

    stage('Run Unit tests') {
        def dobuild=new Boolean(env.run_build_images)
        def dopublish=new Boolean(env.run_publish_images)
        if (dobuild || dopublish) {
            echo "Running the  Unit tests...."
            try {
                build 'Agave/Core/Staging/staging_unit_tests'
                // sh "mvn -s config/maven/settings-SAMPLE.xml -Pagave,utest test -Dgroups=unit -DskipDocker=true -Dskip.migrations=true"
            }
            catch (err) {
                slackSend color: "red", message: "Jenkins-3 Failed to compile the core services. Failed build is on display at <${env.BUILD_URL}|here>."
            }
        } else {
            echo "Skipping the \"Run Unit tests\" stage...."
        }
    }

    stage('Build images') {
        def dobuild=new Boolean(env.run_build_images)
        def dopublish=new Boolean(env.run_publish_images)
        if (dobuild || dopublish) {
            echo "Building images stage...."

            try {
                // create a copy of the maven settings file to use for deployment.
                sh "cp config/maven/settings-SAMPLE.xml config/maven/settings.xml"

                // inject the docker registry credentials into the maven settings file
                sh "sed -i -e 's/%%DOCKER_USERNAME%%/${DOCKER_REGISTRY_USER}/' config/maven/settings.xml"
                sh "sed -i -e 's/%%DOCKER_PASSWORD%%/${DOCKER_REGISTRY_PASS}/' config/maven/settings.xml"
                sh "sed -i -e 's/%%DOCKER_EMAIL%%/devops@agaveplatform.org/' config/maven/settings.xml"

                try {
                    sh "docker login -u ${DOCKER_REGISTRY_USER} -p ${DOCKER_REGISTRY_PASS} ${DOCKER_REGISTRY_URL}"
                } catch (dlerr) {
                    // retry since it usually passes the second tiem
                    sh "echo Retrying login"
                    sh "sleep 10"
                    sh "docker login -u ${DOCKER_REGISTRY_USER} -p ${DOCKER_REGISTRY_PASS} ${DOCKER_REGISTRY_URL}"
                }
                // update base images explicitly

                sh "docker pull agaveplatform/tomcat:8.5-ubuntu"
                sh "docker pull agaveplatform/maven:3.6-proto"
                sh "docker pull agaveplatform/php-api-base:alpine"

                try {
                    sh "./dockerbuild.sh -o -b -t ${DOCKER_REGISTRY_URL}/${DOCKER_REGISTRY_ORG}  -v ${AGAVE_BUILD_TAG}"
                    def dockerhub=new Boolean(env.run_publish_to_dockerhub)
                    if(dockerhub) {
                       sh "./dockerbuild.sh -r -s ${DOCKER_REGISTRY_URL}/${DOCKER_REGISTRY_ORG}  -w ${AGAVE_VERSION} -t ${DOCKER_REGISTRY_ORG} -v ${AGAVE_VERSION}"
                       //sh "./dockerbuild.sh -r -s ${DOCKER_REGISTRY_URL}/${DOCKER_REGISTRY_ORG}  -w ${AGAVE_VERSION} -t tacc -v ${AGAVE_VERSION}"
                    }
                } catch (dperr) {
                    slackSend color: "red", message: "Jenkins-3 Failed to build Science API Docker images. Failed build is on display at <${env.BUILD_URL}|here>."
                    throw err
                }

            }
            catch (err) {
                slackSend color: "red", message: "Jenkins-3 Failed to build Science API Docker images. Failed build is on display at <${env.BUILD_URL}|here>."

            }
        } else {
            echo "Skipping the \"Build images\" stage...."
        }
    }

    stage('Publish images') {
        def dorun=new Boolean(env.run_publish_images)
        if (dorun) {
            try {
                // publish to private registry
                try {
                    sh "docker login -u ${DOCKER_REGISTRY_USER} -p ${DOCKER_REGISTRY_PASS} ${DOCKER_REGISTRY_URL}"
                    sh "./dockerbuild.sh -p -t ${DOCKER_REGISTRY_URL}/${DOCKER_REGISTRY_ORG}  -v ${AGAVE_BUILD_TAG}"
                } catch (dderr) {
                	slackSend color: "red", message: "Jenkins-3 Failed to publish the Science API dev images to the private registry. Failed publication is on display at <${env.BUILD_URL}|here>."
                	throw err
                }
                // cleanup
                 sh "./dockerbuild.sh -c -t ${DOCKER_REGISTRY_URL}/${DOCKER_REGISTRY_ORG}  -v ${AGAVE_VERSION}"
                slackSend color: "green", message: "Jenkins-3 A new batch of Science API dev images are available in your <${DOCKER_REGISTRY_URL}/${DOCKER_REGISTRY_ORG}|private registry>."
            }
            catch (err) {
                slackSend color: "red", message: "Jenkins-3 Failed to publish the Science API dev images to the private registry. Failed publication is on display at <${env.BUILD_URL}|here>."
                throw err
            }
        } else {
            echo "Skipping the \"Publish images\" stage...."
        }
    }
    
    stage('Publish images to dockerhub') {
        def dorun=new Boolean(env.run_publish_to_dockerhub)
        if (dorun) {
            echo "Publishing images to dockerhub...."
                // publish to public registry
            try {
                    //First clean all the local images
                    sh "./dockerbuild.sh -c -t ${DOCKER_REGISTRY_URL}/${DOCKER_REGISTRY_ORG}  -v ${AGAVE_VERSION}"

                    //Then login to the private docker repository
                    sh "docker login -u ${DOCKER_REGISTRY_USER} -p ${DOCKER_REGISTRY_PASS} ${DOCKER_REGISTRY_URL}"
                    
                    //Then pull the images pulled from private registry
                    sh "./dockerbuild.sh -g -t ${DOCKER_REGISTRY_URL}/${DOCKER_REGISTRY_ORG} -v ${AGAVE_VERSION}"
                    
                    //Then retag the images to push
                    sh "./dockerbuild.sh -r -s ${DOCKER_REGISTRY_URL}/${DOCKER_REGISTRY_ORG} -w ${AGAVE_VERSION} -t agaveapidev -v ${AGAVE_VERSION}"
                    sh "./dockerbuild.sh -r -s ${DOCKER_REGISTRY_URL}/${DOCKER_REGISTRY_ORG} -w ${AGAVE_VERSION} -t tacc -v ${AGAVE_VERSION}"

                    //Push the images to public  registry
//                    sh "./dockerbuild.sh -p -t agaveapidev -v ${AGAVE_VERSION}"
//                    sh "./dockerbuild.sh -p -t tacc -v ${AGAVE_VERSION}"

                    //Finally clean up
                    sh "./dockerbuild.sh -c -t ${DOCKER_REGISTRY_URL}/${DOCKER_REGISTRY_ORG}  -v ${AGAVE_VERSION}"
            } catch (dderr) {
                	slackSend color: "red", message: "Jenkins-3 Failed to publish the Science API dev images to the private registry. Failed publication is on display at <${env.BUILD_URL}|here>."
                	throw err
            }
                // cleanup
            slackSend color: "green", message: "Jenkins-3 A new batch of Science API dev images are available in your <${DOCKER_REGISTRY_URL}/${DOCKER_REGISTRY_ORG}|private registry>."
        } else {
            echo "Skipping the \"Publish images to dockerhub\" stage...."
        }
    }


    stage('Deploy to staging') {
        // deploy to staging
        def dorun=new Boolean(env.run_deploy)
        if (dorun) {
            // finally found the right combination of """ triple quotes and single '' quotes
            // that allow me to chain commands to remote host, probably should have used sshoogr
            //def keyfile="${env.deployKey}"
            def repoDir = "/home/apim/repos/core-compose"
            def deployDir = "/home/apim/deploy"
            def core1Dir = "${repoDir}/staging/core-apis/agave-core-dev"
            def core2Dir = "${repoDir}/staging/core-apis/agave-core-staging2"
            def ssh_cmd = "ssh -o StrictHostKeyChecking=no -i /home/jenkins/.ssh/jenkins-prod "
            def core_host1 = "root@agave-core-staging.tacc.utexas.edu"
            def core_host2 = "root@araport-api-prod.tacc.utexas.edu"
            def gitPull = "cd ${repoDir}; git pull"
            def copy_to_deploy = "cp -rf ${repoDir}/${core1Dir}/*.* ${deployDir}"
            def pull = "cd ${deployDir}; export AGAVE_VERSION=${AGAVE_VERSION}; docker-compose -f a.yml -p a pull"
            def turnDown = "cd ${deployDir}; export AGAVE_VERSION=${AGAVE_VERSION}; docker-compose -f a.yml -p a down; docker ps"
            def turnUP = "cd ${deployDir}; export AGAVE_VERSION=${AGAVE_VERSION}; docker-compose -f a.yml -p a up -d; docker ps"

            // for both core1 and core2 hosts do the following
            // pull fresh images
            echo "Pull fresh images"
            sh "${ssh_cmd} ${core_host1} '${pull}'"
            // 1. turn down current services
            echo "Core APIs down"
            //echo "${ssh_cmd} ${core_host1} '${turnDown}'"
            sh "${ssh_cmd} ${core_host1} '${turnDown}'"
            // 2. pull the latest git repo commit for docker compose files
            // sh """${ssh_cmd} $jenkins_key ${core_host1} '${gitPull}' """
            // 3. the repo has been updated so copy the compose files to deploy
            // sh """${ssh_cmd} $jenkins_key ${core_host1} '${copy_to_deploy}' """
            // 4. startup the a.yml compose files
            echo "Core APIs up"
            //echo "${ssh_cmd} ${core_host1} '${turnUP}'"
            sh "${ssh_cmd} ${core_host1} '${turnUP}'"
            //
            echo " "
            echo "    ***********   "
            echo " "
            // pull fresh images
            echo "Pull fresh images"
            sh "${ssh_cmd} ${core_host2} '${pull}'"
            echo "Core workers down"
            //echo "${ssh_cmd} ${core_host2} '${turnDown}'"

            sh "${ssh_cmd} ${core_host2} '${turnDown}'"
            echo "Core workers up"
            //echo "${ssh_cmd} ${core_host2} '${turnUP}'"
            sh "${ssh_cmd} ${core_host2} '${turnUP}'"
        } else {
            echo "Skipping the \"Deploy to staging\" stage...."
        }
    }

    stage('Newman integration tests') {
        // run the newman tests for staging
        def dorun=new Boolean(env.run_deploy)
        if (dorun) {
            try {
                build 'Agave/Core/Staging/newman-staging-tests'

                slackSend message: "Jenkins-3 Newman integration tests successfully passed against the new Science API dev images in staging."

            }
            catch (err) {
                slackSend color: "red", message: "Jenkins-3 Science APIs successfully deployed to staging, but the latest Newman integration tests failed with the current deployment. Failed integration tests are on display at <${env.BUILD_URL}|here>."
            }
        }else{
            echo "Skipping the \"Newman integration tests\" stage...."
        }
    }
}
