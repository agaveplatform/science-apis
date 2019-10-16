#!/usr/bin/env bash
echo Starting test script

PROJECT_ROOT=$(git rev-parse --show-toplevel)/agave-goSFTP

pushd "${PROJECT_ROOT}" > /dev/null
echo "${PROJECT_ROOT}"
cd $PROJECT_ROOT/pkg/sftp
go test