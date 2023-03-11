#!/bin/bash

set -e
PWD=$(pwd)

source Docker_files/ocsci_container/scripts/common.sh

if [ "$DEBUG_NEW_REQUIRES_ARG" == "" ];
then
  $ENGINE_ARG run -v $CLUSTER_PATH:/opt/cluster -v $PWD:/opt/ocs-ci-debug \
   $IMAGE_NAME_ARG $RUN_CI
else
  $ENGINE_ARG run -v $CLUSTER_PATH:/opt/cluster -v $PWD:/opt/ocs-ci-debug \
   $IMAGE_NAME_ARG --debug-- $RUN_CI
fi


#make debug-odf CLUSTER_PATH=~/ClusterPath RUN_CI="run-ci --cluster-path /opt/cluster --ocp-version 4.12 --ocs-version 4.12 --cluster-name oviner6-jun tests/e2e/workloads/app/jenkins/test_oded.py"
