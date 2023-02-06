#!/bin/bash

set -e
PWD=$(pwd)
source Docker_files/ocsci_container/scripts/common.sh

if [ "$AWS_PATH" == "" ]; then
   AWS_PATH=~/.aws
fi

if [ "$OCM_CONFIG" == "" ]; then
   OCM_CONFIG=~/.config/ocm
fi

#run-ci
docker run -v $OCM_CONFIG:/root/.config/ocm -v $CLUSTER_PATH:/opt/cluster -v $PWD/data:/opt/ocs-ci/data -v $AWS_PATH:/root/.aws ocs-ci-container:latest $RUN_CI

#provider:
#make run-ocs-ci CLUSTER_PATH=~/ClusterPath RUN_CI="run-ci --color=yes ./tests/ -m deployment --ocs-version 4.10 --ocp-version 4.10 --ocsci-conf=conf/deployment/rosa/managed_3az_provider_qe_3m_3w_m54x.yaml --ocsci-conf=/opt/cluster/ocm-credentials-stage --cluster-name oviner-pr --cluster-path /opt/cluster/p1 --deploy"

#Consumer:
#make run-ocs-ci CLUSTER_PATH=~/ClusterPath RUN_CI="run-ci --color=yes ./tests/ -m deployment --ocs-version 4.10 --ocp-version 4.10 --ocsci-conf=conf/deployment/rosa/managed_3az_consumer_qe_3m_3w_m52x.yaml --ocsci-conf=/opt/cluster/ocm-credentials-stage --ocsci-conf /opt/cluster/build_config.yaml --cluster-name oviner-c1 --cluster-path /opt/cluster/c1 --deploy"
