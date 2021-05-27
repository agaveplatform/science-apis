#!/usr/bin/env bash
echo Starting test script

# ensure ssh agent is running when we invoke these
# tests so the tests will not fail when invoked
# outside the docker entrypoint script.
if [[ -z "$SSH_AUTH_SOCK" ]]; then
  eval $(ssh-agent)
fi

PROJECT_ROOT=$(dirname $( cd "$( dirname "$0" )" && pwd ))

pushd "${PROJECT_ROOT}" > /dev/null
echo "${PROJECT_ROOT}"
cd $PROJECT_ROOT/pkg/sftprelay
go test