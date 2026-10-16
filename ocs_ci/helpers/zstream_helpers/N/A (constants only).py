"""
Helper function extracted by z-stream test generator.

Target: ocs_ci/ocs/constants.py
Insertion point: end of file
Description: Add constants for ClusterResourceQuota API group, resource name, and ocs-client-operator SA

Review and merge this into ocs_ci/ocs/constants.py before running the test.
"""

CLUSTER_RESOURCE_QUOTA_API_GROUP = "quota.openshift.io"
CLUSTER_RESOURCE_QUOTA_RESOURCE = "clusterresourcequotas"
OCS_CLIENT_OPERATOR_SA = "ocs-client-operator-controller-manager"
