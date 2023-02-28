#!/bin/bash

set -x

OCSCI_INSTALL_DIR="${OCSCI_INSTALL_DIR:=/opt/ocs-ci}"
OCSCI_REPO_URL="https://github.com/red-hat-storage/ocs-ci"

# Clone OCS-CI Project
git clone "$OCSCI_REPO_URL" "$OCSCI_INSTALL_DIR"
pushd "$OCSCI_INSTALL_DIR"

# Checkout to relevant branch [the default is stable]
if [ "$BRANCH" == "" ]
then
      echo checkout to stable branch
      git checkout stable
else
      echo Checkout to branch $BRANCH
      git checkout $BRANCH
fi

#Install dependencies
$PIP_VERSION install --upgrade pip setuptools
$PIP_VERSION install setuptools==65.5.0
$PIP_VERSION install -r requirements.txt
popd

which run-ci
