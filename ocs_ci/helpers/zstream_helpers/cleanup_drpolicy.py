"""
Helper function extracted by z-stream test generator.

Target: ocs_ci/helpers/dr_helpers.py
Insertion point: after function get_ramen_hub_config
Description: Delete a DRPolicy resource if it exists, suppressing errors

Review and merge this into ocs_ci/helpers/dr_helpers.py before running the test.
"""

def cleanup_drpolicy(drpolicy_name):
    """
    Delete a DRPolicy resource if it exists. Logs a warning on failure.

    Args:
        drpolicy_name (str): Name of the DRPolicy to delete
    """
    import logging
    from ocs_ci.ocs import ocp

    logger = logging.getLogger(__name__)

    try:
        drpolicy_obj = ocp.OCP(
            kind="DRPolicy",
            resource_name=drpolicy_name,
        )
        if drpolicy_obj.check_resource_existence(
            resource_name=drpolicy_name,
            should_exist=True,
        ):
            logger.info(f"Cleaning up DRPolicy: {drpolicy_name}")
            drpolicy_obj.delete(resource_name=drpolicy_name, wait=True)
    except Exception as e:
        logger.warning(f"Failed to clean up DRPolicy {drpolicy_name}: {e}")
