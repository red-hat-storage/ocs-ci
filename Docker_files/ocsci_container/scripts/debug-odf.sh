#!/bin/bash

set -e
PWD=$(pwd)

source Docker_files/ocsci_container/scripts/common.sh

$PLATFORM_CMD run -v $CLUSTER_PATH:/opt/cluster -v $PWD:/opt/ocs-ci-debug $IMAGE_NAME --debug-- $RUN_CI

#make debug-odf CLUSTER_PATH=~/ClusterPath RUN_CI="run-ci --cluster-path /opt/cluster --ocp-version 4.12 --ocs-version 4.12 --cluster-name oviner6-jun tests/e2e/workloads/app/jenkins/test_oded.py"
