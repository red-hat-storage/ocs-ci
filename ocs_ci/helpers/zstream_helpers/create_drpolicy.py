"""
Helper function extracted by z-stream test generator.

Target: ocs_ci/helpers/dr_helpers.py
Insertion point: after function get_managed_cluster_names
Description: Create a DRPolicy custom resource with the given spec

Review and merge this into ocs_ci/helpers/dr_helpers.py before running the test.
"""

def create_drpolicy(drpolicy_name, cluster_names, scheduling_interval="5m"):
    """
    Create a DRPolicy custom resource.

    Args:
        drpolicy_name (str): Name of the DRPolicy resource
        cluster_names (list): List of DR cluster names to include in the policy
        scheduling_interval (str): Scheduling interval for the DR policy.
            Defaults to "5m".

    Returns:
        OCP: The OCP object for the created DRPolicy
    """
    from ocs_ci.ocs import ocp

    drpolicy_body = {
        "apiVersion": "ramendr.openshift.io/v1alpha1",
        "kind": "DRPolicy",
        "metadata": {
            "name": drpolicy_name,
        },
        "spec": {
            "schedulingInterval": scheduling_interval,
            "drClusters": cluster_names,
        },
    }

    drpolicy_obj = ocp.OCP(kind="DRPolicy")
    drpolicy_obj.create(body=drpolicy_body)
    return drpolicy_obj
