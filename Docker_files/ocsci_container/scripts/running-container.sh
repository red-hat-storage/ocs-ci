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
