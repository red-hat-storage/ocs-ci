"""
Helper function extracted by z-stream test generator.

Target: ocs_ci/helpers/dr_helpers.py
Insertion point: end of file
Description: Get list of VolumeSnapshot names in a given namespace, returning names and count.

Review and merge this into ocs_ci/helpers/dr_helpers.py before running the test.
"""

def get_volumesnapshots_in_namespace(namespace):
    """
    Get all VolumeSnapshots in a given namespace on the current cluster context.

    Args:
        namespace (str): The namespace to query for VolumeSnapshots

    Returns:
        list: List of VolumeSnapshot names found in the namespace
    """
    from ocs_ci.ocs import ocp

    vs_ocp = ocp.OCP(kind="VolumeSnapshot", namespace=namespace)
    vs_result = vs_ocp.get(dont_raise=True)
    if not vs_result or not vs_result.get("items"):
        return []
    return [item["metadata"]["name"] for item in vs_result["items"]]
