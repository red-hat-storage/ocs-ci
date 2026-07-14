#!/bin/bash

set -e
source Docker_files/ocsci_container/scripts/common.sh

PWD=$(pwd)

$ENGINE_ARG build -t $IMAGE_NAME_ARG -f \
${PWD}/Docker_files/ocsci_container/Containerfile.ci .\

#make build-image IMAGE_NAME=quay.io/ocsci/ocs-ci-container:stable ENGINE=podman
