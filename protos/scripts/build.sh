#!/usr/bin/env bash
# Copyright 2017 The Go Authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.
#
# This script will build dep and calculate hash for each
# (BUILD_PLATFORMS, BUILD_ARCHS) pair.
# BUILD_PLATFORMS="linux" BUILD_ARCHS="amd64" ./scripts/build.sh
# can be called to build only for linux-amd64

set -e
PROJECT_ROOT=$(git rev-parse --show-toplevel)/protos
BASE_CMD_NAME=${BASE_CMD_NAME:-"proto"}
echo $BASE_CMD_NAME
VERSION=$(git describe --tags --dirty 2>/dev/null || echo "head")
COMMIT_HASH=$(git rev-parse --short HEAD 2>/dev/null)
DATE=$(date "+%Y-%m-%d")
DEFAULT_BUILD_PLATFORM=$(uname -a | awk '{print tolower($1);}')
BUILD_PLATFORMS=${BUILD_PLATFORMS:-"linux darwin"}
BUILD_ARCHS=${BUILD_ARCHS:-"amd64"}
IMPORT_DURING_SOLVE=${IMPORT_DURING_SOLVE:-false}

GO_BUILD_CMD="go build -a -installsuffix cgo"
GO_BUILD_LDFLAGS="-s -w -X main.commitHash=${COMMIT_HASH} -X main.buildDate=${DATE} -X main.version=${VERSION} -X main.flagImportDuringSolve=${IMPORT_DURING_SOLVE}"

mkdir -p "${PROJECT_ROOT}/release"

pushd "${PROJECT_ROOT}/src/proto/" > /dev/null
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

        echo ${ARCH} GOOS=${OS} CGO_ENABLED=${CGO_ENABLED} ${GO_BUILD_CMD} \
            -ldflags "${GO_BUILD_LDFLAGS}"\
            -pkgdir "${PROJECT_ROOT}/pkg" \
            -o "${PROJECT_ROOT}/release/${NAME}"

        GOARCH=${ARCH} GOOS=${OS} CGO_ENABLED=${CGO_ENABLED} ${GO_BUILD_CMD} \
            -ldflags "${GO_BUILD_LDFLAGS}"\
            -pkgdir "${PROJECT_ROOT}/pkg" \
            -o "${PROJECT_ROOT}/release/${NAME}" ./
        pushd "${PROJECT_ROOT}/release" > /dev/null
        shasum -a 256 "${NAME}" > "${NAME}.sha256"
        popd > /dev/null
    fi
  done
done
popd > /dev/null
