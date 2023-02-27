#!/bin/bash

set -x

OCSCI_INSTALL_DIR="${OCSCI_INSTALL_DIR:=/opt/ocs-ci}"
OCSCI_REPO_URL="https://github.com/red-hat-storage/ocs-ci"

# Clone OCS-CI Project
git clone "$OCSCI_REPO_URL" "$OCSCI_INSTALL_DIR"
pushd "$OCSCI_INSTALL_DIR"

# Checkout to relevant branch [the default is stable]
if [ "$BRANCH_ID" == "stable" ]
then
      echo checkout to stable branch
      git checkout stable
elif [[ $BRANCH_ID == "master" ]]
then
      echo Working on master branch
else
      echo Checkout to PR $BRANCH_ID
      git fetch origin pull/$BRANCH_ID/head:$BRANCH_ID
      git checkout $BRANCH_ID
fi

#Install dependencies
$PIP_INSTALLATION install --upgrade pip setuptools
$PIP_INSTALLATION install setuptools==65.5.0
$PIP_INSTALLATION install -r requirements.txt
popd

which run-ci
