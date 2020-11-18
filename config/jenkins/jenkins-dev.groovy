/*   Description for job  build-publish-deploy-test-develop
builds, packages, publishes - images, deploys to staging and runs newman tests for Agave core services in the develop environment .

**Stages of pipeline and their function**

From master node

+ stage ('Start with clean workspace')    **default false**
      * clean the workspace 'stage'....
+ stage ('Checkout Agave source')         **default true**
      * code checkout 'stage'....
+ stage ('Build jars')                    **default true**
      * execute a maven build to create the java jar artifacts
+ stage('Run Unit tests')                 **default true**
      *  calls out to job "develop_unit_tests" to run unit tests based on unit grouping for TestNG.
+ stage ('Build dev images')              **default true**
      *  build the images for services.
+ stage ('Publish dev images')            **default true**
      *  publish the images to private registry locally
+ stage ('Deploy to develop')             **default true**
      *  pulling latest deployer image
      *  login to the private registry
      *  run deployer with update_core playbook
+ stage ('Newman integration tests')      **default true**


Each stage may be turned on or off based on a parameter resembling the stage name.
The default is on
 */

node('master') {
    echo "Agave version : ${env.AGAVE_VERSION}-dev"
    echo "Execute stage \"Start with clean workspace\" : ${env.clean_workspace}"
    echo "Execute stage \"Checkout Agave source\" : ${env.run_checkout}"
    echo "Execute stage \"Build jars\" : ${env.run_build_jars}"
    echo "Execute stage \"Run Unit tests\" : ${env.run_unit_tests}"

    def mvnHome = tool 'maven-3.5.0'
    def jdk = tool 'openjdk-9'
    echo "${env.PATH}"
    env.PATH = "${mvnHome}/bin:${jdk}/bin:${env.PATH}"
    echo "java version ${jdk}"

    // slackSend color: "good", message: "Jenkins-3 Build Started: ${env.JOB_NAME} ${env.BUILD_NUMBER} Science API images. "

    // Mark the code checkout 'stage'....
    stage('Start with clean workspace'){
        def dorun=new Boolean(env.clean_workspace)
        if (dorun) {
            echo "workspace ${env.WORKSPACE}"
            sh " rm -rf ${env.WORKSPACE}/*"
            sh "ls -la"
        } else {
            echo "Skipping the \"Start with clean workspace\" stage...."
        }
    }

    stage('Checkout Agave source') {
        def dorun=new Boolean(env.run_checkout)
        if (dorun) {
            sh 'git version'
            sh 'git config --global user.email "deardooley@gmail.com"'
            sh 'git config --global user.name "deardooley"'

            checkout([$class                           : 'GitSCM',
                      branches                         : [[name: '*/develop']],
                      doGenerateSubmoduleConfigurations: false,
                      extensions                       : [],
                      submoduleCfg                     : [],
                      userRemoteConfigs                : [
                              [credentialsId: 'github_token',
                               refspec      : '+refs/heads/develop:refs/remotes/origin/develop',
                               url          : 'https://deardooley@github.com/agaveplatform/science-apis.git'
                              ]
                      ]
            ])
            sh 'git checkout develop'
            sh 'git pull origin develop'
            sh 'git branch -v'
        } else {
            echo "Skipping the \"Checkout Agave source\" stage...."
        }
    }

    stage('Build jars') {
        def dorun=new Boolean(env.run_build_jars)
        if (dorun) {
            try {
                sh "mvn -P agave,dev versions:set -DgenerateBackupPoms=false -DnewVersion=${env.AGAVE_VERSION}-dev"
                sh "mvn -P agave,dev -B install -DskipDocker=true"
            }
            catch (err) {
                slackSend color: "red", message: "Jenkins-3 Failed to compile the core services. Failed build is on display at <${env.BUILD_URL}|here>."
            }
        } else {
            echo "Skipping the \"Build jars\" stage...."
        }
    }

    stage('Run Unit tests') {
        def dorun=new Boolean(env.run_unit_tests)
        if (dorun) {
            try {
                build 'Agave/Core/Develop/develop_unit_tests'
                // sh "mvn -s config/maven/settings-SAMPLE.xml -Pagave,utest test -Dgroups=unit -DskipDocker=true -Dskip.migrations=true"
            }
            catch (err) {
                slackSend color: "red", message: "Jenkins-3 Failed to compile the core services. Failed build is on display at <${env.BUILD_URL}|here>."
            }
        } else {
            echo "Skipping the \"Run Unit tests\" stage...."
        }
    }
}
