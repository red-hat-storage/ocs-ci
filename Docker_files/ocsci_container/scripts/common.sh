#!/usr/bin/env bash

if [ "$PLATFORM_CMD" == "" ]
then
      PLATFORM_CMD=docker
      IMAGE_NAME=ocs-ci-container:latest
else
      PLATFORM_CMD=podman
      IMAGE_NAME=localhost/ocs-ci-container:latest
fi

#Pull image from Registry
if [ "$PULL_IMAGE" != "" ]
then
      $PLATFORM_CMD image pull quay.io/ocsci/ocs-ci-container:latest
fi
