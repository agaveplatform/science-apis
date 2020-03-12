#!/bin/bash
#######################################################################################################################
# This script is the script that builds the dokcer images for agave modules.                                          #
# The script is invoked as follows                                                                                    #
# 1. To build all images use switch -b which stands for build: dockerbuild.sh -b -t <tagename> -v <version>           #
#    e.g. ./dockerbuild.sh  -b -t "jenkins2.tacc.utexas.edu:5000/agaveapidev" -v 2.10.                                #
#    If an image with the same tag and version is already created, then ti script won't recreate(overwrite) it.       #
#    To force the recreation use -o (overwrite) switch: dockerbuild.sh -o -b -t <tagename> -v <version>               #
#    e.g. ./dockerbuild.sh  -o -b -t "jenkins2.tacc.utexas.edu:5000/agaveapidev" -v 2.10.                             #
# 2. To delete all images locally use switch -c which stands for clean: dockerbuild.sh -c -t <tagename> -v <version>  #
#    e.g. ./dockerbuild.sh  -c -t "jenkins2.tacc.utexas.edu:5000/agaveapidev" -v 2.10.                                #
# 3. To publish an image use -p switch:  dockerbuild.sh -p -t <tagename> -v <version>                                 #
#    e.g. ./dockerbuild.sh  -p -t "jenkins2.tacc.utexas.edu:5000/agaveapidev" -v 2.10.                                #
#    This will try to push the docker image into the repository indicated in the tag. Please note that an imagea      #
#    must be created before it can be pushed. Also, as per docker convention, the tag should have the URL required    #
#    of the docker registry.                                                                                          #
# 4. To retag use: ./dockerbuild.sh -r -s jenkins2.tacc.utexas.edu:5000/agaveapidev -w latest -t newtag -v newversion #
# 5. To pull images ./dockerbuild.sh -g -t tag -v version                                                             #
#######################################################################################################################



set -e
set -u
set -o pipefail

export javamodules="apps files jobs metadata monitors notifications profiles systems tags uuids"
export phpmodules="postits logging tenants usage apidocs"
export gomodules="sftp-relay"

delete_docker_image_if_exists()
{
  image=$1
  if [[ "$(docker images -q ${image} 2> /dev/null)" != "" ]]; then
    echo "Removing ${image}"
    docker rmi ${image}
  else
    echo "${image} does not exist"
  fi
}

clean()
{

 for javamodule in ${javamodules}; do
  rm -f docker/${javamodule}/*.war
  delete_docker_image_if_exists $1/${javamodule}-api:$2 
 done

 for phpmodule in ${phpmodules}; do
  rm -rf docker/${phpmodule}/html
  delete_docker_image_if_exists  $1/${phpmodule}-api:$2 
 done 


 rm -f docker/migrations/conf/flyway.conf
 rm -f docker/migrations/lib/*
 rm -f docker/migrations/sql/*
 rm -f docker/migrations/drivers/*
 rm -f docker/migrations/docker-entrypoint.sh
 rm -f docker/migrations/Dockerfile
 delete_docker_image_if_exists  $1/agave-migrations:$2 
 echo "Cleaned all the modules..."

}

create_agave_migrations_image() {
  
 image=$1/agave-migrations:$2

 if [[ "$(docker images -q ${image} 2> /dev/null)" == "" || "$(docker images -q "$1/agave-mariadb:$2" 2> /dev/null)" == "" ||  "${overwrite}" = true ]]; then

  echo "Building image for migrations"
  mvn -P agave,dev,integration-test -Dskip.migrations=false verify -pl agave-migrations

  docker tag agave-migrations:$2 $image
  docker tag agave-mariadb:$2 $1/agave-mariadb:$2

#
#  cp -f agave-migrations/target/classes/flyway.conf docker/migrations/conf
#  rsync -av --exclude='mysql*.jar' --exclude='flyway*.jar' agave-migrations/target/dependency/* docker/migrations/lib
#  cp -rf agave-migrations/target/classes/db/migration/*.sql  docker/migrations/sql
#  cp -rf agave-migrations/target/dependency/mysql*.jar docker/migrations/drivers
#  cp -rf agave-migrations/target/classes/docker-entrypoint.sh  docker/migrations/
#  cp -rf agave-migrations/target/agave-migrations*.jar  docker/migrations/lib
#  cp -f agave-migrations/target/classes/Dockerfile docker/migrations/
#
#  echo "Building image for migrations"
#  docker build docker/migrations -t ${image}
 else
  echo "Already ${image} exists not rebuilding"
 fi

}

build()
{
 echo "Recreating the docker in workspace"

 for javamodule in ${javamodules}; do

  image=$1/${javamodule}-api:$2

  if [[ "$(docker images -q ${image} 2> /dev/null)" == "" || "${overwrite}" = true ]]; then
   echo "Building image for ${javamodule}"
   cp agave-${javamodule}/${javamodule}-api/target/*.war docker/${javamodule}
   docker build docker/${javamodule} -t ${image}
  else
   echo "Already ${image} exists not rebuilding"
  fi

 done

 for phpmodule in ${phpmodules}; do

  image=$1/${phpmodule}-api:$2

  if [[ "$(docker images -q ${image} 2> /dev/null)" == "" || "${overwrite}" = true ]]; then
   echo "Building image for ${phpmodule}"
   cp -rf agave-${phpmodule}/${phpmodule}-api/target/html docker/${phpmodule}
   docker build docker/${phpmodule} -t ${image}
  else
   echo "Already ${image} exists not rebuilding"
  fi

 done

 for gomodule in ${gomodules}; do

  image=$1/${gomodule}:$2

  if [[ "$(docker images -q ${image} 2> /dev/null)" == "" || "${overwrite}" = true ]]; then
   echo "Building image for ${gomodule}"
   pushd agave-transfers/${gomodule} >> /dev/null
   make image
   docker tag ${gomodule}:develop ${image}
   popd >> /dev/null
  else
   echo "Already ${image} exists not rebuilding"
  fi

 done

 #finally agave migrations
 create_agave_migrations_image $1 $2

}

push_if_exists()
{
 image=$1

 if [[ "$(docker images -q ${image} 2> /dev/null)" == "" ]]; then
  echo "${image} does not exists."
  exit 1
 else
  echo "Pushing image ${image}"
  docker push ${image}
 fi
}

pull_image()
{
 image=$1
 echo "Pulling image ${image}"
 docker pull ${image}
}

publish()
{
 echo "Recreating the docker in workspace"

 for javamodule in ${javamodules}; do
  push_if_exists $1/${javamodule}-api:$2
 done

 for phpmodule in ${phpmodules}; do
  push_if_exists $1/${phpmodule}-api:$2
 done

 for gomodule in ${gomodules}; do
  push_if_exists $1/${gomodule}:$2
 done

 #finally agave migrations
 push_if_exists $1/agave-migrations:$2

}

retag()
{
 echo "Retagging the images.."

 for javamodule in ${javamodules}; do
   docker tag $1/${javamodule}-api:$2 $3/${javamodule}-api:$4
 done

 for phpmodule in ${phpmodules}; do
   docker tag $1/${phpmodule}-api:$2 $3/${phpmodule}-api:$4
 done

 for gomodule in ${gomodules}; do
   docker tag $1/${gomodule}:$2 $3/${gomodule}:$4
 done

 #finally agave migrations
 docker tag $1/agave-migrations:$2 $3/agave-migrations:$4
}

pull()
{
 echo "Pulling the images.."

 for javamodule in ${javamodules}; do
  pull_image $1/${javamodule}-api:$2
 done

 for phpmodule in ${phpmodules}; do
  pull_image $1/${phpmodule}-api:$2
 done

 for gomodule in ${gomodules}; do
  pull_image $1/${gomodule}-api:$2
 done

 #finally agave migrations
 pull_image $1/agave-migrations:$2
}

overwrite=false
clean=false
bld=false
publish=false
retag=false
sourcetag=""
sourceversion=""
tag=""
version=""
pull=false

while getopts 'cbporgs:w:t:v:' OPTION; do
  case "$OPTION" in
    c)
      clean=true
      ;;

    o)
      overwrite=true
      ;;
    t)
      tag="$OPTARG"
      ;;
    s)
      sourcetag="$OPTARG"
      ;;
    w)
      sourceversion="$OPTARG"
      ;;
    v)
      version="$OPTARG"
      ;;
    b)
      bld=true
      ;;
    p)
      publish=true
      ;;
    r)
      retag=true
      ;;
    g)
      pull=true
      ;;
    ?)
      echo "script usage: $(basename $0) [-c] [-o] -t imagetag -v imageversion" >&2
      exit 1
      ;;
  esac
done
shift "$(($OPTIND -1))"

if [[ -z "${tag}" || -z "${version}" ]]; then
  echo "script usage: $(basename $0) [-c] [-o] -t imagetag -v imageversion" >&2
  exit 1
fi

if [ "${retag}" = true ]; then
 if [[ -z "${sourcetag}" || -z "${sourceversion}" ]]; then
  echo "script usage: $(basename $0) [-r] -s sourcetag -w sourceversion  -t imagetag -v imageversion" >&2
  exit 1
 fi
 retag ${sourcetag} ${sourceversion} ${tag} ${version}
fi

if [ "${clean}" = true ]; then
 clean ${tag} ${version}
fi

if [ "${bld}" = true ]; then
 build  ${tag} ${version} ${overwrite}
fi

if [ "${publish}" = true ]; then
 publish  ${tag} ${version}
fi

if [ "${pull}" = true ]; then
 pull  ${tag} ${version}
fi

