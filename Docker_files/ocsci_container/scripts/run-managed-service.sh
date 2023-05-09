#!/bin/bash

set -e
PWD=$(pwd)
source Docker_files/ocsci_container/scripts/common.sh

AWS_PATH_ARG=${AWS_PATH:-"~/.aws"}
OCM_CONFIG_ARG=${OCM_CONFIG:-"~/.config/ocm"}

#run-ci
$ENGINE_ARG run -v $OCM_CONFIG_ARG:/root/.config/ocm -v \
$CLUSTER_PATH:/opt/cluster -v $PWD/data:/opt/ocs-ci/data -v \
$AWS_PATH_ARG:/root/.aws $IMAGE_NAME_ARG $RUN_CI

if [ "$PROVIDER_NAME" != "" ]; then
   $ENGINE_ARG run -v $CLUSTER_PATH:/opt/cluster $IMAGE_NAME_ARG \
   python3 /opt/ocs-ci/Docker_files/ocsci_container/scripts/edit_yaml.py $PROVIDER_NAME
fi

#provider:
#make run-managed-service CLUSTER_PATH=~/ClusterPath PROVIDER_NAME=oviner-provider RUN_CI="run-ci --color=yes ./tests/ -m deployment --ocs-version 4.10 --ocp-version 4.10 --ocsci-conf=conf/deployment/rosa/managed_3az_provider_qe_3m_3w_m54x.yaml --ocsci-conf=/opt/cluster/ocm-credentials-stage --cluster-name oviner-pr --cluster-path /opt/cluster/p1 --deploy"

#Consumer:
#make run-managed-service CLUSTER_PATH=~/ClusterPath RUN_CI="run-ci --color=yes ./tests/ -m deployment --ocs-version 4.10 --ocp-version 4.10 --ocsci-conf=conf/deployment/rosa/managed_3az_consumer_qe_3m_3w_m52x.yaml --ocsci-conf=/opt/cluster/ocm-credentials-stage --ocsci-conf /opt/cluster/build_config.yaml --cluster-name oviner-c1 --cluster-path /opt/cluster/c1 --deploy"
