#!/bin/bash

set -e
PWD=$(pwd)

#Pull image from Registry
docker image pull quay.io/ocsci/ocs-ci-container:latest

if [ "$AWS_PATH" == "" ]; then
   AWS_PATH=~/.aws
fi

#run-ci
docker run -v $CLUSTER_PATH:/opt/cluster -v $PWD/data:/opt/ocs-ci/data -v $AWS_PATH:/root/.aws ocs-ci-container:latest $RUN_CI

#provider:
#make run-ocs-ci CLUSTER_PATH=~/ClusterPath RUN_CI="run-ci --color=yes ./tests/ -m deployment --ocs-version 4.10 --ocp-version 4.10 --ocsci-conf=/opt/cluster/ocm-credentials-stage --ocsci-conf=conf/deployment/rosa/managed_3az_provider_qe_3m_3w_m54x.yaml --cluster-name oviner-pr --cluster-path /opt/cluster/p1 --deploy"
#Consumer:
#make run-ocs-ci CLUSTER_PATH=~/ClusterPath RUN_CI="run-ci --color=yes ./tests/ -m deployment --ocs-version 4.10 --ocp-version 4.10 --ocsci-conf=/opt/cluster/ocm-credentials-stage --ocsci-conf=conf/deployment/rosa/managed_3az_consumer_qe_3m_3w_m52x.yaml --cluster-name oviner-c1 --cluster-path /opt/cluster/c1 --ocsci-conf /opt/cluster/build_config.yaml --deploy"
