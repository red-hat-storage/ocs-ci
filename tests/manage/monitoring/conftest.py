import logging
import pytest
import threading
import time

from ocs_ci.ocs import constants, defaults, ocp
from ocs_ci.utility.prometheus import PrometheusAPI


logger = logging.getLogger(__name__)


def measure_operation(
    operation, minimal_time=None, metadata=None, measure_after=False
):
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
    def prometheus_log(info, alert_list):
        """
        Log all alerts from Prometheus API every 10 seconds.

        Args:
            run (bool): When this var turns into False the thread stops.
            alert_list (list): List to be populated with alerts
        """
        prometheus = PrometheusAPI()
        while info.get('run'):
            alerts_response = prometheus.get(
                'alerts',
                payload={
                    'silenced': False,
                    'inhibited': False
                }
            )
            assert alerts_response.ok, 'Prometheus API request failed'
            for alert in alerts_response.json().get('data').get('alerts'):
                if alert not in alert_list:
                    logger.info(f"Adding {alert} to alert list")
                    alert_list.append(alert)
            time.sleep(10)

    if not measure_after:
        start_time = time.time()

    # init logging thread that checks for Prometheus alerts
    # while workload is running
    # based on https://docs.python.org/3/howto/logging-cookbook.html#logging-from-multiple-threads
    info = {'run': True}
    alert_list = []

    logging_thread = threading.Thread(
        target=prometheus_log,
        args=(info, alert_list)
    )
    logging_thread.start()

    try:
        result = operation()
        if measure_after:
            start_time = time.time()
        passed_time = time.time() - start_time
        if minimal_time:
            additional_time = minimal_time - passed_time
            if additional_time > 0:
                time.sleep(additional_time)
        stop_time = time.time()
    except KeyboardInterrupt:
        # Thread should be correctly terminated on next few lines
        # this is done in case of user interuption to make sure that thread
        # is terminated correctly
        pass
    info['run'] = False
    logging_thread.join()
    logger.info(f"Alerts found during measurement: {alert_list}")
    return {
        'start': start_time,
        'stop': stop_time,
        'result': result,
        'metadata': metadata,
        'prometheus_alerts': alert_list
    }


@pytest.fixture(scope="session")
def workload_stop_ceph_mgr():
    """
    Downscales Ceph Manager deployment, measures the time when it was downscaled
    and monitors alerts that were triggered during this event.

    Returns:
        dict: Contains information about `start` and `stop` time for stopping
            Ceph Manager pod.
    """
    oc = ocp.OCP(
        kind=constants.DEPLOYMENT,
        namespace=defaults.ROOK_CLUSTER_NAMESPACE
    )
    mgr_deployments = oc.get(selector=constants.MGR_APP_LABEL)['items']
    mgr = mgr_deployments[0]['metadata']['name']

    def stop_mgr():
        """
        Downscale Ceph Manager deployment for 6 minutes. First 5 minutes
        the alert should be in 'Pending'.
        After 5 minutes it should be 'Firing'.

        Returns:
            str: Name of downscaled deployment.
        """
        # run_time of operation
        run_time = 60 * 6
        nonlocal oc
        nonlocal mgr
        logger.info(f"Downscaling deployment {mgr} to 0")
        oc.exec_oc_cmd(f"scale --replicas=0 deployment/{mgr}")
        logger.info(f"Waiting for {run_time} seconds")
        time.sleep(run_time)
        return oc.get(mgr)

    measured_op = measure_operation(stop_mgr)
    logger.info(f"Upscaling deployment {mgr} back to 1")
    oc.exec_oc_cmd(f"scale --replicas=1 deployment/{mgr}")
    return measured_op
