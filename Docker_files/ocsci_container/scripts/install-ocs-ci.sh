#!/bin/bash

set -x

OCSCI_INSTALL_DIR="${OCSCI_INSTALL_DIR:=/opt/ocs-ci}"
OCSCI_REPO_URL="https://github.com/red-hat-storage/ocs-ci"

git clone "$OCSCI_REPO_URL" "$OCSCI_INSTALL_DIR"

pushd "$OCSCI_INSTALL_DIR"
python3.8 -m venv venv
source venv/bin/activate
pip install --upgrade pip setuptools
pip install setuptools==65.5.0
pip install -r requirements.txt
popd

which run-ci
