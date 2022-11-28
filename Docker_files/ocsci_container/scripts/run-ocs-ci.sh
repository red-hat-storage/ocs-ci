#!/bin/bash

run_ci_cmd="run-ci --cluster-path /opt/cluster "

# Add marker to run-ci command
if [ "$MARKER_PYTEST" != "" ]; then
   run_ci_cmd+="-m ${MARKER_PYTEST} "
fi

# Add ocp version to run-ci command
if [ "$OCP_VERSION" != "" ]; then
   run_ci_cmd+="--ocp-version ${OCP_VERSION} "
fi

# Add ocs version to run-ci command
if [ "$OCS_VERSION" != "" ]; then
   run_ci_cmd+="--ocs-version ${OCS_VERSION} "
fi

# Add internal config-file to run-ci command [for example conf/ocsci/encryption_at_rest.yaml]
if [ "$CONF_FILE_INTERNAL" != "" ]; then
   run_ci_cmd+="--ocsci-conf  ${CONF_FILE_INTERNAL} "
fi

# Add exteranl config-file to run-ci command [the new config file locate on same dir like kubeconfig and the file name is "config_file.yaml"]
if [ "$CONF_FILE_EXTERNAL" != "" ]; then
   run_ci_cmd+="--ocsci-conf  /opt/cluster/auth/config_file.yaml "
fi

# Add cluster name to run-ci command
if [ "$CLUSTER_NAME" == "" ]
then
      run_ci_cmd+="--cluster-name cluster-name "
else
      run_ci_cmd+="--cluster-name ${CLUSTER_NAME} "
fi

# Add the test path to run-ci command
if [ "$TEST_PATH" == "" ]
then
      run_ci_cmd+="tests/"
else
      run_ci_cmd+="${TEST_PATH}"
fi

# Execute run-ci on relevant env
cd /opt/ocs-ci
source venv/bin/activate
$run_ci_cmd > /opt/cluster/auth/output.txt
