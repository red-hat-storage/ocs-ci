#!/bin/bash

set -e
pwd=$(pwd)

# CLUSTER_PATH - cluster path on local machine
# CMD_RUN_CI -RUN-CI command
# EXTERNAL_CONF_FILES


#make test-odf CLUSTER_PATH=~/ClusterPath \
#  CMD_RUN_CI="--ocp-version 4.12 --ocs-version 4.12 --cluster-name cluster tests/manage/z_cluster/test_must_gather.py" \
#  EXTERNAL_CONF_FILES="vSphere7-DC-CP_VC1.yaml"


IFS=',' read -ra CONF_FILES <<< "$EXTERNAL_CONF_FILES"
for CONF_FILE in "${CONF_FILES[@]}"; do
  EX_CONF+="--ocsci-conf /opt/cluster/${CONF_FILE} "
done

docker image pull quay.io/ocsci/ocs-ci-container:latest
echo docker run -v $CLUSTER_PATH:/opt/cluster -v $pwd/data:/opt/ocs-ci/data \
 odf_build \
 run-ci --cluster-path /opt/cluster $EX_CONF $CMD_RUN_CI
