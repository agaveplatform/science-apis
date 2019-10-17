#!/usr/bin/env bash
# Copyright 2017 The Go Authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.
#
# This script will build dep and calculate hash for each
# (BUILD_PLATFORMS, BUILD_ARCHS) pair.
# BUILD_PLATFORMS="linux" BUILD_ARCHS="amd64" ./scripts/build.sh
# can be called to build only for linux-amd64
echo Starting script
DIR=$( cd "$( dirname "$0" )" && pwd )

PROJECT_ROOT=$(dirname $DIR)
BASE_CMD_NAME=${BASE_CMD_NAME:-"proto"}
VERSION=develop
COMMIT_HASH=123456
DATE=$(date "+%Y-%m-%d")
DEFAULT_BUILD_PLATFORM=$(uname -a | awk '{print tolower($1);}')
BUILD_PLATFORMS=${BUILD_PLATFORMS:-"linux darwin"}
BUILD_ARCHS=${BUILD_ARCHS:-"amd64"}
IMPORT_DURING_SOLVE=${IMPORT_DURING_SOLVE:-false}

#echo "${PROJECT_ROOT} Pjt_Root from build.sh"
#echo "${BASE_CMD_NAME}  from build.sh"
#echo "${VERSION} from build.sh"
#echo "${DATE} from build.sh"

GO_BUILD_CMD="go build -mod vendor -a -installsuffix cgo"
#echo ${GO_BUILD_CMD}
GO_BUILD_LDFLAGS="-s -w -X main.commitHash=${COMMIT_HASH} -X main.buildDate=${DATE} -X main.version=${VERSION} -X main.flagImportDuringSolve=${IMPORT_DURING_SOLVE}"
#echo ${GO_BUILD_LDFLAGS}

mkdir -p "${PROJECT_ROOT}/release"

pushd "${MEDIFOR_WORKER_ROOT}/cmd/${BASE_CMD_NAME}" > /dev/null
for OS in ${BUILD_PLATFORMS[@]}; do
  for ARCH in ${BUILD_ARCHS[@]}; do
    NAME="${BASE_CMD_NAME}-${OS}-${ARCH}"
    if [[ "${OS}" == "windows" ]]; then
      NAME="${NAME}.exe"
    fi

    # Enable CGO if building for OS X on OS X; see
    # https://github.com/golang/dep/issues/1838 for details.
    if [[ "${OS}" == "darwin" && "${BUILD_PLATFORM}" == "darwin" ]]; then
      CGO_ENABLED=1
    else
      CGO_ENABLED=0
    fi
    if [[ "${ARCH}" == "ppc64" || "${ARCH}" == "ppc64le" ]] && [[ "${OS}" != "linux" ]]; then
        # ppc64 and ppc64le are only supported on Linux.
        echo "Building ${BASE_CMD_NAME} for ${OS}/${ARCH} not supported."
    else
        echo "Building ${BASE_CMD_NAME} for ${OS}/${ARCH} with CGO_ENABLED=${CGO_ENABLED}"

        GOARCH=${ARCH} GOOS=${OS} CGO_ENABLED=${CGO_ENABLED} ${GO_BUILD_CMD} \
            -ldflags "${GO_BUILD_LDFLAGS}"\
            -pkgdir "${PROJECT_ROOT}/pkg" \
            -o "${PROJECT_ROOT}/release/${NAME}" ./
        if [[ $? -eq 0 ]]; then
          pushd "${PROJECT_ROOT}/release" > /dev/null
          shasum -a 256 "${NAME}" > "${NAME}.sha256"
          popd > /dev/null
        fi
    fi
  done
done
popd > /dev/null
