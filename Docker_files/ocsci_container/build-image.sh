#!/bin/bash

set -e
source Docker_files/ocsci_container/scripts/common.sh

PWD=$(pwd)

$ENGINE_ARG build -t $IMAGE_NAME_ARG -f \
${PWD}/Docker_files/ocsci_container/Dockerfile_ocsci \
${PWD}/Docker_files/ocsci_container \
--build-arg BRANCH_ID_ARG=$BRANCH_ARG \
--build-arg PIP_INSTALLATION_ARG=$PIP_VERSION_ARG \

# make build-image
