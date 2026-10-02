"""
Helper function extracted by z-stream test generator.

Target: ocs_ci/helpers/dr_helpers.py
Insertion point: after function wait_for_drpc_phase
Description: Wait for a DRPC to complete its first kube object protection cycle.

Review and merge this into ocs_ci/helpers/dr_helpers.py before running the test.
"""

def wait_for_first_kube_object_protection(drpc_obj, timeout=600, sleep=20):
    """
    Wait for a DRPC resource to complete its first kube object protection cycle.

    Args:
        drpc_obj: The DRPC resource object (OCP or DRPC class instance).
        timeout (int): Maximum time to wait in seconds. Default 600.
        sleep (int): Polling interval in seconds. Default 20.

    Returns:
        str: The lastKubeObjectProtectionTime value.

    Raises:
        TimeoutExpiredError: If kube object protection is not completed within timeout.
    """
    logger = logging.getLogger(__name__)
    for sample in TimeoutSampler(
        timeout=timeout,
        sleep=sleep,
        func=lambda: drpc_obj.get().get("status", {}).get("lastKubeObjectProtectionTime"),
    ):
        if sample:
            logger.info(
                f"DRPC {drpc_obj.resource_name} completed first kube object "
                f"protection at {sample}"
            )
            return sample
