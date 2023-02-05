#!/bin/bash

set -e
PWD=$(pwd)

#Pull image from Registry
docker image pull quay.io/ocsci/ocs-ci-container:latest

if [ "$AWS_PATH" == "" ];
then
   docker run -v $CLUSTER_PATH:/opt/cluster -v $PWD/data:/opt/ocs-ci/data ocs-ci-container:latest $RUN_CI
else
  docker run -v $CLUSTER_PATH:/opt/cluster -v $PWD/data:/opt/ocs-ci/data -v $AWS_PATH:/root/.aws ocs-ci-container:latest $RUN_CI
fi

#make run-odf CLUSTER_PATH=~/ClusterPath RUN_CI="run-ci --cluster-path /opt/cluster --ocp-version 4.12 --ocs-version 4.12 --cluster-name oviner6-jun tests/e2e/workloads/app/jenkins/test_oded.py"
