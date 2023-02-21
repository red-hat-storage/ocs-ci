#!/bin/bash

set -e
source Docker_files/ocsci_container/scripts/common.sh

PWD=$(pwd)

$ENGINE_CMD build -t $IMAGE_NAME_ARG -f ${PWD}/Docker_files/ocsci_container/Dockerfile_ocsci ${PWD}/Docker_files/ocsci_container

# make build-image
