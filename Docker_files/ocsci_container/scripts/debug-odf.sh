#!/bin/bash

set -e
PWD=$(pwd)

source Docker_files/ocsci_container/scripts/common.sh

$ENGINE_ARG run -v $CLUSTER_PATH:/opt/cluster:Z -v $PWD:/opt/ocs-ci-debug:Z \
 $IMAGE_NAME_ARG $RUN_CI

#make debug-odf CLUSTER_PATH=~/ClusterPath RUN_CI="run-ci --cluster-path /opt/cluster --ocp-version 4.12 \
# --ocs-version 4.12 --cluster-name oviner6-jun tests/e2e/workloads/app/jenkins/test_oded.py"
