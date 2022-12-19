#!/bin/bash
set -e
pwd=$(pwd)

docker image pull quay.io/ocsci/ocs-ci-container:latest

if [ "$DEPLOYMENT_PROVIDER" != "" ]; then
   echo run -v $pwd/data:/opt/ocs-ci/data quay.io/ocsci/ocs-ci-container:latest $DEPLOYMENT_PROVIDER
fi

i=1
declare "DEPLOYMENT_CONSUMER$i"
var="DEPLOYMENT_CONSUMER$i"
while [ "${!var}" != "" ]
do
  echo "run -v $pwd/data:/opt/ocs-ci/data quay.io/ocsci/ocs-ci-container:latest ${!var} &"
  i=$(( $i + 1 ))
  declare "DEPLOYMENT_CONSUMER$i"
  var="DEPLOYMENT_CONSUMER$i"
done

#docker image pull quay.io/ocsci/ocs-ci-container:latest
#echo run -v $CLUSTER_PATH:/opt/cluster -v $pwd/data:/opt/ocs-ci/data quay.io/ocsci/ocs-ci-container:latest run-ci --cluster-path /opt/cluster $ocp_version $ocs_version --cluster-name cluster tests/manage/z_cluster/test_must_gather.py

#
# Add marker to run-ci command
#if [ "$m" != "" ]; then
#   marker="-m ${m} "
#fi
#echo $m
## Add ocp version to run-ci command
#if [ "$ocp_version" != "" ]; then
#   ocp_version="--ocp-version ${ocp_version} "
#fi
#echo $ocp_version
#
## Add ocs version to run-ci command
#if [ "$ocs_version" != "" ]; then
#   ocs_version="--ocs-version ${ocs_version} "
#fi
#
## Add internal config-file to run-ci command [for example conf/ocsci/encryption_at_rest.yaml]
#if [ "$CONF_FILE_INTERNAL" != "" ]; then
#   conf="--ocsci-conf  ${CONF_FILE_INTERNAL} "
#fi
#
## Add cluster name to run-ci command
#if [ "$CLUSTER_NAME" == "" ]
#then
#      run_ci_cmd+="--cluster-name cluster-name "
#else
#      run_ci_cmd+="--cluster-name ${CLUSTER_NAME} "
#fi
#
## Add the test path to run-ci command
#if [ "$TEST_PATH" == "" ]
#then
#      run_ci_cmd+="tests/"
#else
#      run_ci_cmd+="${TEST_PATH}"
#fi

#docker image pull quay.io/ocsci/ocs-ci-container:latest
#echo run -v $CLUSTER_PATH:/opt/cluster -v $pwd/data:/opt/ocs-ci/data quay.io/ocsci/ocs-ci-container:latest run-ci --cluster-path /opt/cluster $ocp_version $ocs_version --cluster-name cluster tests/manage/z_cluster/test_must_gather.py
#

#docker run -v $cluster_path:/opt/cluster -v $pwd/data:/opt/ocs-ci/data -it quay.io/ocsci/ocs-ci-container:latest


#docker login -u <username> quay.io
#docker build -t ocsci_container -f Dockerfile_ocsci .
#docker image tag ocsci_container:latest quay.io/ocsci/ocs-ci-container:latest
#docker push quay.io/ocsci/ocs-ci-container:latest

#docker build -t ocsci_container -f Dockerfile_ocsci .
#
#
# docker run --entrypoint /bin/bash
#   -v ~/ClusterPath:/opt/cluster
#   -v /home/odedviner/OCS-AP/ocs-ci/data:/opt/ocs-ci/data
#   -v ~/.aws:~./aws
#   -v /usr/local/bin/rosa:/usr/local/bin/rosa
#   -v /usr/bin/ocm:/usr/bin/ocm
#   -it ocsci_container


#docker run --entrypoint /bin/bash -v ~/ClusterPath:/opt/cluster -v /home/odedviner/OCS-AP/ocs-ci:/opt/ocs-ci-local -v ~/.aws:/root/.aws -v /usr/local/bin/rosa:/usr/local/bin/rosa -v /usr/bin/ocm:/usr/bin/ocm -it ocsci_container


# run-ci --cluster-path /opt/cluster --ocp-version 4.12 --ocs-version 4.12 --cluster-name cluster tests/manage/z_cluster/test_must_gather.py
