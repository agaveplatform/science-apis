#!/usr/bin/env bash
echo Starting test script

PROJECT_ROOT=$(dirname $( cd "$( dirname "$0" )" && pwd ))

pushd "${PROJECT_ROOT}" > /dev/null
echo "${PROJECT_ROOT}"
cd $PROJECT_ROOT/pkg/sftprelay
go test