#!/usr/bin/env bash
echo Start building go

PROJECT_ROOT=$(git rev-parse --show-toplevel)/agave-goSFTP
GOBIN=$PROJECT_ROOT/release

cd $PROJECT_ROOT/cmd
echo $PROJECT_ROOT/cmd
go build ./Server/fileServer.go
pwd
mv ./fileServer ../release/fileServer
# go install cmd/Server/fileServer.go

echo Finished