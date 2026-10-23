"""
Helper function extracted by z-stream test generator.

Target: ocs_ci/helpers/dr_helpers.py
Insertion point: after function get_drpc_condition
Description: Wait for a VRG resource to reach the expected spec and status state on the current cluster

Review and merge this into ocs_ci/helpers/dr_helpers.py before running the test.
"""

def wait_for_vrg_state(
    resource_name, namespace, expected_spec_state, expected_status_state, timeout=300, interval=10
):
    """
    Wait for a VolumeReplicationGroup (VRG) to reach the expected
    spec.replicationState and status.state on the current cluster context.

    Args:
        resource_name (str): The VRG resource name.
        namespace (str): The namespace of the VRG.
        expected_spec_state (str): Expected spec.replicationState (e.g., "secondary").
        expected_status_state (str): Expected status.state (e.g., "Secondary").
        timeout (int): Maximum time in seconds to wait. Default 300.
        interval (int): Polling interval in seconds. Default 10.

    Raises:
        TimeoutExpiredError: If the VRG does not reach the expected state within timeout.
    """
    logger = logging.getLogger(__name__)
    from ocs_ci.ocs import ocp
    from ocs_ci.utility.utils import TimeoutSampler

    vrg_ocp = ocp.OCP(
        kind="VolumeReplicationGroup",
        namespace=namespace,
    )
    for sample in TimeoutSampler(
        timeout=timeout,
        sleep=interval,
        func=vrg_ocp.get,
        resource_name=resource_name,
        dont_raise=True,
    ):
        if sample and isinstance(sample, dict):
            vrg_status_state = sample.get("status", {}).get("state", "")
            vrg_spec_state = sample.get("spec", {}).get("replicationState", "")
            logger.debug(
                f"VRG spec.replicationState={vrg_spec_state}, status.state={vrg_status_state}"
            )
            if vrg_spec_state == expected_spec_state and vrg_status_state == expected_status_state:
                logger.info(
                    f"VRG {resource_name} reached expected state: "
                    f"spec={expected_spec_state}, status={expected_status_state}"
                )
                return
