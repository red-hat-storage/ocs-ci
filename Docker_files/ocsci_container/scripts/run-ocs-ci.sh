#!/bin/bash
set -e

mkdir -p /opt/cluster/logs_ocsci_container

# Execute run-ci
cd /opt/ocs-ci
exec "$@" > /opt/cluster/logs_ocsci_container/output_"$@".txt

if [[ $@ == *"provider"* ]]; then
  python3.8 /opt/edit_yaml.py $PROVIDER_NAME
fi
