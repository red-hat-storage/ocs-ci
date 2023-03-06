#!/bin/bash
set -e

mkdir -p /opt/cluster/logs_ocsci_container

# Execute run-ci
if [[ "$@" == *"--debug--"* ]]; then
  cd /opt/ocs-ci-debug
  $PIP_VERSION install --upgrade pip setuptools
  $PIP_VERSION install setuptools==65.5.0
  $PIP_VERSION install -r requirements.txt
  STR_TMP="$@"
  CMD=${STR_TMP:10:5000000}
  exec $CMD | tee /opt/cluster/logs_ocsci_container/output-$(date +"%m_%d_%y-%H_%M").txt
else
  cd /opt/ocs-ci
  exec "$@" | tee /opt/cluster/logs_ocsci_container/output-$(date +"%m_%d_%y-%H_%M").txt
fi
