#!/bin/bash

set -e
source Docker_files/ocsci_container/scripts/common.sh

PWD=$(pwd)

if [ "$IMAGE_NAME" == "" ]; then
   IMAGE_NAME=ocs-ci-container
fi

$PLATFORM_CMD build -t $IMAGE_NAME -f ${PWD}/Docker_files/ocsci_container/Dockerfile_ocsci ${PWD}/Docker_files/ocsci_container

# make build-image
