#!/usr/bin/env bash
# This script will validate code with various linters
set -e

PKGS=$(go list ./... | grep -vF /vendor/)
go vet $PKGS
golint $PKGS
