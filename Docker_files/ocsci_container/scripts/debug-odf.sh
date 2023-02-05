#!/bin/bash

set -e
PWD=$(pwd)

#Pull image from Registry
docker image pull quay.io/ocsci/ocs-ci-container:latest

docker run -v $CLUSTER_PATH:/opt/cluster -v $PWD:/opt/ocs-ci-debug ocs-ci-container:latest --debug-- $RUN_CI

#make debug-odf CLUSTER_PATH=~/ClusterPath RUN_CI="run-ci --cluster-path /opt/cluster --ocp-version 4.12 --ocs-version 4.12 --cluster-name oviner6-jun tests/e2e/workloads/app/jenkins/test_oded.py"
