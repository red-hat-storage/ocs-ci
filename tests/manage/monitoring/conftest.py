import pytest
import logging
import time

from ocs_ci.ocs import constants, defaults, ocp


logger = logging.getLogger(__name__)

def measure_operation(
        operation, minimal_time=None, metadata=None, measure_after=False):
    """
    Get dictionary with keys 'start', 'stop', 'metadata' and 'result' that
    contain information about start and stop time of given function and its
    result.

    Args:
        operation (function): Function to be performed.
        minimal_time (int): Minimal number of seconds to run, it can be more
            based on given operation.
        metadata (dict): This can contain dictionary object with information
            relevant to test (e.g. volume name, operating host, ...).
        measure_after (bool): Determine if time measurement is done before or
            after the operation returns its state. This can be useful e.g.
            for capacity utilization testing where operation fills capacity
            and utilized data are measured after the utilization is completed.

    Returns:
        dict: contains information about `start` and `stop` time of given
            function and its `result` and provided `metadata`.
    """
    if not measure_after:
        start_time = time.time()
    result = operation()
    if measure_after:
        start_time = time.time()
    passed_time = time.time() - start_time
    if minimal_time:
        additional_time = minimal_time - passed_time
        if additional_time > 0:
            time.sleep(additional_time)
    stop_time = time.time()
    return {
        "start": start_time,
        "stop": stop_time,
        "result": result,
        "metadata": metadata
    }


@pytest.fixture(scope="session")
def workload_stop_ceph_mgr():
    """
    Returns:
        dict: Contains information about `start` and `stop` time for stopping
            Ceph manager node.
    """
    oc = ocp.OCP(
        kind=constants.DEPLOYMENT,
        namespace=defaults.ROOK_CLUSTER_NAMESPACE
    )
    mgr = 'rook-ceph-mgr-a'

    def stop_mgr():
        """
        Downscale Ceph Manager deployment for 11 minutes.
        """
        # run_time of operation
        run_time = 60 * 2
        nonlocal oc
        nonlocal mgr
        logger.info(f"Downscaling deployment {mgr} to 0")
        oc.exec_oc_cmd(f"scale --replicas=0 deployment/{mgr}")
        logger.info(f"Waiting for {run_time} seconds")
        time.sleep(run_time)
        return oc.get(mgr)

    measured_op = measure_operation(stop_mgr)
    logger.info(f"Upscaling deployment {mgr} to 1")
    oc.exec_oc_cmd(f"scale --replicas=1 deployment/{mgr}")
    return measured_op
