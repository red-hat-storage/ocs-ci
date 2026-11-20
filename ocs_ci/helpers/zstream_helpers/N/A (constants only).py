"""
Helper function extracted by z-stream test generator.

Target: ocs_ci/ocs/constants.py
Insertion point: end of file
Description: Add constants for Ramen configmap names and DRPolicy validation condition strings

Review and merge this into ocs_ci/ocs/constants.py before running the test.
"""

# Ramen DR operator configmap names
RAMEN_HUB_OPERATOR_CONFIG = "ramen-hub-operator-config"
RAMEN_DR_CLUSTER_OPERATOR_CONFIG = "ramen-dr-cluster-operator-config"
RAMEN_MANAGER_CONFIG_KEY = "ramen_manager_config.yaml"

# DRPolicy condition types and reasons
DRPOLICY_CONDITION_VALIDATED = "Validated"
DRPOLICY_S3_BUCKET_NOT_FOUND = "s3BucketNotFound"
DRPOLICY_S3_CONNECTION_FAILED = "s3ConnectionFailed"
DRPOLICY_S3_LIST_FAILED = "s3ListFailed"
