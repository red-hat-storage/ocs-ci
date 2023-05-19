#!/bin/bash

set -e
PWD=$(pwd)
source Docker_files/ocsci_container/scripts/common.sh

if [ "$AWS_PATH_ARG" == "" ];
then
  $ENGINE_ARG run -v $CLUSTER_PATH:/opt/cluster:Z \
  -v $PWD/data:/opt/ocs-ci/data:Z $IMAGE_NAME_ARG $RUN_CI
else
  $ENGINE_ARG run -v $CLUSTER_PATH:/opt/cluster:Z -v $PWD/data:/opt/ocs-ci/data:Z \
   -v $AWS_PATH_ARG:/root/.aws:Z $IMAGE_NAME_ARG $RUN_CI
fi

#make run-odf CLUSTER_PATH=~/ClusterPath RUN_CI="run-ci --cluster-path /opt/cluster --ocp-version 4.12\
# --ocs-version 4.12 --cluster-name oviner-test tests/manage/z_cluster/test_osd_heap_profile.py"
