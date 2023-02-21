#!/bin/bash

set -e
PWD=$(pwd)
source Docker_files/ocsci_container/scripts/common.sh

if [ "$AWS_PATH_ARG" == "" ];
then
  $ENGINE_CMD run -v $CLUSTER_PATH:/opt/cluster -v $PWD/data:/opt/ocs-ci/data $IMAGE_NAME_ARG $RUN_CI
else
  $ENGINE_CMD run -v $CLUSTER_PATH:/opt/cluster -v $PWD/data:/opt/ocs-ci/data -v $AWS_PATH_ARG:/root/.aws $IMAGE_NAME_ARG $RUN_CI
fi

#make run-odf CLUSTER_PATH=~/ClusterPath RUN_CI="run-ci --cluster-path /opt/cluster --ocp-version 4.12 --ocs-version 4.12 --cluster-name oviner6-jun tests/e2e/workloads/app/jenkins/test_oded.py"
