#!/bin/bash

set -e
pwd=$(pwd)

# CLUSTER_PATH - cluster path on local machine [~/ClusterPath]
# DATA_PATH - /home/odedviner/OCS-AP/ocs-ci/data
# RUN_CI_PROVIDER - run-ci --color=yes ./tests/ -m deployment --ocs-version 4.10 --ocp-version 4.10 --ocsci-conf=/opt/cluster/ocm-credentials-stage
# --ocsci-conf=/opt/cluster/ocsci_conf.yaml --ocsci-conf=conf/deployment/rosa/managed_3az_provider_qe_3m_3w_m54x.yaml
# --cluster-name oviner-pr --cluster-path /opt/cluster/p1 --deploy

#Pull image from Registry
#docker image pull quay.io/ocsci/ocs-ci-container:latest

#Deploy Provider cluster
echo docker run -v $CLUSTER_PATH:/opt/cluster -v $DATA_PATH:/opt/ocs-ci/data -v ~/.aws:/root/.aws $RUN_CI_PROVIDER



#RUN_CI_CONSUMER - run-ci --color=yes ./tests/ -m deployment --ocs-version 4.10 --ocp-version 4.10
# --ocsci-conf=/opt/cluster/ocm-credentials-stage --ocsci-conf=conf/deployment/rosa/managed_3az_consumer_qe_3m_3w_m52x.yaml
# --cluster-name oviner55-c1 --cluster-path /opt/cluster/c1 --ocsci-conf /opt/cluster/build_config.yaml  --deploy

#Deploy Consumer cluster
echo docker run -v $CLUSTER_PATH:/opt/cluster -v $DATA_PATH:/opt/ocs-ci/data -v ~/.aws:/root/.aws $RUN_CI_CONSUMER
