"""
Helper function extracted by z-stream test generator.

Target: ocs_ci/helpers/dr_helpers.py
Insertion point: end of file
Description: Wait for all DRPlacementControl resources to be deleted in a namespace on the hub cluster.

Review and merge this into ocs_ci/helpers/dr_helpers.py before running the test.
"""

def wait_for_drpc_deletion(namespace, timeout=300, sleep=15):
    """
    Wait for all DRPlacementControl resources to be deleted in a namespace.
    Assumes the current context is already set to the ACM/hub cluster.

    Args:
        namespace (str): The namespace to check for DRPC resources
        timeout (int): Timeout in seconds to wait for deletion
        sleep (int): Sleep interval in seconds between checks

    Raises:
        TimeoutExpiredError: If DRPC resources are not deleted within the timeout
    """
    import logging
    from ocs_ci.ocs import ocp
    from ocs_ci.ocs.exceptions import TimeoutExpiredError
    from ocs_ci.utility.utils import TimeoutSampler

    logger = logging.getLogger(__name__)
    drpc_ocp = ocp.OCP(
        kind="DRPlacementControl",
        namespace=namespace,
    )
    try:
        for sample in TimeoutSampler(
            timeout=timeout,
            sleep=sleep,
            func=drpc_ocp.get,
            dont_raise=True,
        ):
            if not sample or not sample.get("items"):
                logger.info(f"DRPC is fully deleted from hub cluster in namespace {namespace}")
                return
            remaining = [item["metadata"]["name"] for item in sample.get("items", [])]
            logger.info(f"Waiting for DRPC deletion, remaining: {remaining}")
    except TimeoutExpiredError:
        raise TimeoutExpiredError(
            f"DRPC was not deleted within {timeout}s in namespace {namespace}"
        )
