"""
Helper function extracted by z-stream test generator.

Target: ocs_ci/helpers/helpers.py
Insertion point: after function get_storage_consumer_quota
Description: Set the storageQuotaInGiB for a StorageConsumer on the provider cluster

Review and merge this into ocs_ci/helpers/helpers.py before running the test.
"""

def set_storage_consumer_quota(provider_index, consumer_name, quota_value):
    """
    Set the storage quota for a StorageConsumer on the provider cluster.

    Args:
        provider_index (int): The config index for the provider cluster.
        consumer_name (str): Name of the StorageConsumer resource.
        quota_value (int): Quota value in GiB. 0 means unlimited.
    """
    from ocs_ci.framework import config
    from ocs_ci.ocs import ocp

    with config.RunWithConfigContext(provider_index):
        storage_consumer_ocp = ocp.OCP(
            kind="StorageConsumer",
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        patch_data = [
            {
                "op": "replace",
                "path": "/spec/storageQuotaInGiB",
                "value": quota_value,
            }
        ]
        storage_consumer_ocp.patch(
            resource_name=consumer_name,
            params=patch_data,
            format_type="json",
        )
        logger.info(f"Set quota to {quota_value} for {consumer_name}")
