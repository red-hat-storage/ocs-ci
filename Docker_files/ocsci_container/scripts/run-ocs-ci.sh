#!/bin/bash
set -e

mkdir -p /opt/cluster/logs
mkdir -p /opt/cluster_path/auth
cp -R /opt/cluster/* /opt/cluster_path/auth

# Execute run-ci
cd /opt/ocs-ci
exec "$@" > /opt/cluster/logs/output.txt
