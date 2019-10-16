#!/usr/bin/env bash
echo Start building go

PROJECT_ROOT=$(git rev-parse --show-toplevel)/agave-goSFTP
GOBIN=$PROJECT_ROOT/release

cd $PROJECT_ROOT/pkg
echo $PROJECT_ROOT/pkg
go build ./sftp/fileServer.go
pwd
mv ./fileServer ../release/fileServer
# go install cmd/Server/fileServer.go

echo Finished