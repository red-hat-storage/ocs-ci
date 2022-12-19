#!/bin/bash
set -e

mkdir -p /opt/cluster/logs_ocsci_container

# Execute run-ci
cd /opt/ocs-ci
exec "$@" > /opt/cluster/logs_ocsci_container/output.txt
