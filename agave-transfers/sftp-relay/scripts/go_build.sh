#!/usr/bin/env bash
echo Start building go
DIR=$( cd "$( dirname "$0" )" && pwd )

PROJECT_ROOT=$(dirname $DIR)
GOBIN=$PROJECT_ROOT/release

pushd $PROJECT_ROOT/pkg
echo $PROJECT_ROOT/pkg
go build ./sftp/fileServer.go
pwd
#mv ./release/fileServer ../release/fileServer
# go install cmd/Server/fileServer.go

echo Finished

