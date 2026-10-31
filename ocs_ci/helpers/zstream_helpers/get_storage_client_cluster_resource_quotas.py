"""
Helper function extracted by z-stream test generator.

Target: ocs_ci/helpers/helpers.py
Insertion point: after function set_storage_consumer_quota
Description: Get storage-client related ClusterResourceQuota objects on a given cluster

Review and merge this into ocs_ci/helpers/helpers.py before running the test.
"""

def get_storage_client_cluster_resource_quotas(cluster_index):
    """
    Get all ClusterResourceQuota objects on the specified cluster that are
    related to the storage client (name contains 'storage-client' and 'resourceqouta').

    Args:
        cluster_index (int): The config index for the target cluster.

    Returns:
        list: List of ClusterResourceQuota resource dicts matching the filter.
    """
    from ocs_ci.framework import config
    from ocs_ci.ocs import ocp

    with config.RunWithConfigContext(cluster_index):
        crq_ocp = ocp.OCP(kind="ClusterResourceQuota")
        try:
            crqs = crq_ocp.get().get("items", [])
        except Exception:
            crqs = []
        storage_crqs = [
            crq for crq in crqs
            if "storage-client" in crq.get("metadata", {}).get("name", "")
            and "resourceqouta" in crq.get("metadata", {}).get("name", "")
        ]
        logger.info(
            f"Found {len(storage_crqs)} storage-client ClusterResourceQuota(s) on cluster"
        )
        return storage_crqs
