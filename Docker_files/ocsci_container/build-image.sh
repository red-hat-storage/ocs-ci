#!/bin/bash

set -e

PWD=$(pwd)

if [ "$IMAGE_NAME" == "" ]; then
   IMAGE_NAME=ocs-ci-container
fi

docker build -t $IMAGE_NAME -f ${PWD}/Docker_files/ocsci_container/Dockerfile_ocsci ${PWD}/Docker_files/ocsci_container

# make build-image
