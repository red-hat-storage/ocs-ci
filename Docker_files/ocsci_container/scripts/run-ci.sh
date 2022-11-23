#!/bin/bash

set -x

TEST_PATH_SCRIPT=${TEST_PATH}
OCP_VERSION_SCRIPT=${OCP_VERSION}
OCS_VERSION_SCRIPT=${OCS_VERSION}
MARKER_PYTEST_SCRIPT=${MARKER_PYTEST}

cd /opt/ocs-ci
source venv/bin/activate
run-ci --cluster-path /opt/cluster -m "$MARKER_PYTEST_SCRIPT" --ocp-version "$OCP_VERSION_SCRIPT" --ocs-version "$OCS_VERSION_SCRIPT" --cluster-name cluster-name "$TEST_PATH_SCRIPT"
