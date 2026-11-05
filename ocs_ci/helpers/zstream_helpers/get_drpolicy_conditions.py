"""
Helper function extracted by z-stream test generator.

Target: ocs_ci/helpers/dr_helpers.py
Insertion point: after function create_drpolicy
Description: Get the status conditions list from a DRPolicy resource

Review and merge this into ocs_ci/helpers/dr_helpers.py before running the test.
"""

def get_drpolicy_conditions(drpolicy_name):
    """
    Retrieve the status conditions from a DRPolicy resource.

    Args:
        drpolicy_name (str): Name of the DRPolicy resource

    Returns:
        list: List of condition dicts from the DRPolicy status.
            Each dict has keys: type, status, reason, message.
            Returns empty list if no conditions are found.
    """
    from ocs_ci.ocs import ocp

    drpolicy_obj = ocp.OCP(kind="DRPolicy", resource_name=drpolicy_name)
    resource = drpolicy_obj.get(resource_name=drpolicy_name)
    return resource.get("status", {}).get("conditions", [])
