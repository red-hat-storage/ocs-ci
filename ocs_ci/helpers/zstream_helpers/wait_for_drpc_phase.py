"""
Helper function extracted by z-stream test generator.

Target: ocs_ci/helpers/dr_helpers.py
Insertion point: after function get_drpc_resource_condition
Description: Wait for a DRPC resource to reach a specific phase (e.g., Deployed, Relocated).

Review and merge this into ocs_ci/helpers/dr_helpers.py before running the test.
"""

import logging

from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


def wait_for_drpc_phase(drpc_obj, phase, timeout=300, sleep=15):
    """
    Wait for a DRPC resource to reach a specific phase.

    Args:
        drpc_obj: The DRPC resource object (OCP or DRPC class instance).
        phase (str): The expected phase (e.g., "Deployed", "Relocated", "FailedOver").
        timeout (int): Maximum time to wait in seconds. Default 300.
        sleep (int): Polling interval in seconds. Default 15.

    Raises:
        TimeoutExpiredError: If the DRPC does not reach the expected phase within timeout.
    """
    for sample in TimeoutSampler(
        timeout=timeout,
        sleep=sleep,
        func=lambda: drpc_obj.get().get("status", {}).get("phase", ""),
    ):
        if sample == phase:
            logger.info(f"DRPC {drpc_obj.resource_name} reached phase '{phase}'")
            return
