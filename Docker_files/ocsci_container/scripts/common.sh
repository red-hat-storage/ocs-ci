#!/usr/bin/env bash


ENGINE_ARG="${ENGINE:-docker}"

IMAGE_NAME_ARG="${IMAGE_NAME:-"quay.io/ocsci/ocs-ci-container:stable"}"

PYTHON_VERSION_ARG="${PYTHON_VERSION:-"python3.8"}"

PIP_VERSION_ARG="${PIP_VERSION:-"pip3.8"}"

BRANCH_ARG="${BRANCH:-"stable"}"

if [ "$PULL_IMAGE" != "" ]
then
      $ENGINE_ARG image pull $PULL_IMAGE
fi
