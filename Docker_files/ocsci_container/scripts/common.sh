#!/usr/bin/env bash

ENGINE_ARG="${ENGINE:-podman}"

IMAGE_NAME_ARG="${IMAGE_NAME:-"quay.io/ocsci/ocs-ci-container:stable"}"

if [ "$PULL_IMAGE" != "" ]
then
      $ENGINE_ARG image pull $PULL_IMAGE
fi
