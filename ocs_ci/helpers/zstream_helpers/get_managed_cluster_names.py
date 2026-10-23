"""
Helper function extracted by z-stream test generator.

Target: ocs_ci/helpers/dr_helpers.py
Insertion point: end of file
Description: Retrieve non-local managed cluster names from the hub cluster

Review and merge this into ocs_ci/helpers/dr_helpers.py before running the test.
"""

def get_managed_cluster_names(exclude_local=True):
    """
    Retrieve the names of ManagedCluster resources from the hub cluster.

    Args:
        exclude_local (bool): If True, exclude the 'local-cluster' entry.
            Defaults to True.

    Returns:
        list: List of managed cluster name strings
    """
    from ocs_ci.ocs import ocp

    managed_clusters_obj = ocp.OCP(kind="ManagedCluster")
    managed_clusters = managed_clusters_obj.get().get("items", [])

    cluster_names = []
    for mc in managed_clusters:
        mc_name = mc["metadata"]["name"]
        if exclude_local and mc_name == "local-cluster":
            continue
        cluster_names.append(mc_name)

    return cluster_names
