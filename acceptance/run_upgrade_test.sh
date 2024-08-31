#!/bin/bash
set -e
set -x

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <branch1> <branch2>"
    exit 1
fi

if [ -n "$BRANCH_SPECIFIER" ]; then
  git config --global user.email "dummy@rubrik.com"
  git config --global user.name "jenkins"
  git add .
  git commit -m "Dummy Commit for test"
fi

B1=$1
B2=$(git rev-parse --abbrev-ref HEAD)

if [ -z "$B2" ]; then
  echo "Error: Not on any branch currently."
  exit 1
fi

git checkout "$B1"
go build -o kronos-1 ./cmd/kronos
git checkout "$B2"
go build -o kronos-2 ./cmd/kronos
PATH=$PATH:$(pwd):$(go env GOPATH)/bin go test ./acceptance/... --tags=acceptance,upgrade -v -run TestClusterRollingUpgrade
PATH=$PATH:$(pwd):$(go env GOPATH)/bin go test ./acceptance/... --tags=acceptance,upgrade -v -run TestClusterDisruptiveUpgrade