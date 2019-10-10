#!/usr/bin/env bash
echo Start building go

PROJECT_ROOT=$(git rev-parse --show-toplevel)/agave-goSFTP
GOBIN=$PROJECT_ROOT/release

cd $PROJECT_ROOT
echo $PROJECT_ROOT
go build ./cmd/Server/fileServer.go
mv ./fileServer ./release/fileServer
# go install cmd/Server/fileServer.go

echo Finished