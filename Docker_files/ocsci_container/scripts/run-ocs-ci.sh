#!/bin/bash
set -e

mkdir -p /opt/cluster/logs_ocsci_container

# Execute run-ci
if [[ "$@" == *"--debug--"* ]]; then
  cd /opt/ocs-ci-debug
  pip3.8 install --upgrade pip setuptools
  pip3.8 install setuptools==65.5.0
  pip3.8 install -r requirements.txt
  STR_TMP="$@"
  CMD=${STR_TMP:10:5000000}
  exec $CMD > /opt/cluster/logs_ocsci_container/output_$RANDOM.txt
else
  cd /opt/ocs-ci
  exec "$@" | tee /opt/cluster/logs_ocsci_container/output_$RANDOM.txt
fi
