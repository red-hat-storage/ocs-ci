#!/usr/bin/env bash


ENGINE_CMD="${ENGINE:-docker}"

#Pull image from Registry
IMAGE_NAME_ARG="${IMAGE_NAME:-"quay.io/ocsci/ocs-ci-container:stable"}"


if [ "$PULL_IMAGE" != "" ]
then
      $ENGINE_CMD image pull $PULL_IMAGE
fi
