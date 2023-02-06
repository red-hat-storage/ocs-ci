#!/bin/bash

set -e
PWD=$(pwd)
source Docker_files/ocsci_container/scripts/common.sh

if [ "$AWS_PATH" == "" ];
then
  $PLATFORM_CMD run -v $CLUSTER_PATH:/opt/cluster -v $PWD/data:/opt/ocs-ci/data $IMAGE_NAME $RUN_CI
else
  $PLATFORM_CMD run -v $CLUSTER_PATH:/opt/cluster -v $PWD/data:/opt/ocs-ci/data -v $AWS_PATH:/root/.aws $IMAGE_NAME $RUN_CI
fi

#make run-odf CLUSTER_PATH=~/ClusterPath RUN_CI="run-ci --cluster-path /opt/cluster --ocp-version 4.12 --ocs-version 4.12 --cluster-name oviner6-jun tests/e2e/workloads/app/jenkins/test_oded.py"
