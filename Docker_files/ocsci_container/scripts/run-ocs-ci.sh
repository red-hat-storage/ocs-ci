#!/bin/bash
set -e

mkdir -p /opt/cluster/logs_ocsci_container

# Execute run-ci
cd /opt/ocs-ci
run-ci --cluster-path /opt/cluster "$@" > /opt/cluster/logs_ocsci_container/output.txt
