import logging
import random

from ocs_ci.ocs.resources.pod import (
    delete_pods,
    get_osd_pods,
    get_mgr_pods,
    get_mds_pods,
    get_mon_pods,
    get_rgw_pods,
)

from ocs_ci.resiliency.resiliency_tools import CephStatusTool

logger = logging.getLogger(__name__)


class ApplicationFailures:
    """
    Simulates failures of key Ceph component pods by deleting them.

    Useful for resiliency testing and validating recovery behavior
    of OpenShift Data Foundation.
    """

    SCENARIO_NAME = "APPLICATION_FAILURES"

    FAILURE_METHODS = {
        "OSD_POD_FAILURES": "restart_osd_pods",
        "MGR_POD_FAILURES": "restart_mgr_pods",
        "MDS_POD_FAILURES": "restart_mds_pods",
        "MON_POD_FAILURES": "restart_mon_pods",
        "RGW_POD_FAILURES": "restart_rgw_pods",
    }

    def __init__(self, failure_data):
        """
        Initialize the ApplicationFailures class.
        """
        self.failure_data = failure_data
        logger.info("Initializing ApplicationFailures class.")

    def _restart_pods(self, get_pods_func, pod_type: str, wait: bool = True):
        """
        Generic pod restart handler.

        Args:
            get_pods_func (function): Function to fetch specific Ceph component pods.
            pod_type (str): Name of the pod/component being restarted.
            wait (bool): Whether to wait for pods to be recreated and running.
        """
        logger.info(f"Fetching {pod_type} pods for deletion...")
        pods = get_pods_func()
        logger.info(f"Deleting {len(pods)} {pod_type} pod(s)...")
        delete_pods(pods, wait=wait)
        logger.info(f"{pod_type} pod(s) deleted. Recovery wait: {wait}.")

    def restart_osd_pods(self, wait_till_pods_running: bool = True):
        """
        Restart OSD pods.

        Args:
            wait_till_pods_running (bool): Wait until OSD pods are recreated and running.
        """
        self._restart_pods(get_osd_pods, "OSD", wait_till_pods_running)

    def restart_mgr_pods(self, wait_till_pods_running: bool = True):
        """
        Restart MGR pods.

        Args:
            wait_till_pods_running (bool): Wait until MGR pods are recreated and running.
        """
        self._restart_pods(get_mgr_pods, "MGR", wait_till_pods_running)

    def restart_mds_pods(self, wait_till_pods_running: bool = True):
        """
        Restart MDS pods.

        Args:
            wait_till_pods_running (bool): Wait until MDS pods are recreated and running.
        """
        self._restart_pods(get_mds_pods, "MDS", wait_till_pods_running)

    def restart_mon_pods(self, wait_till_pods_running: bool = True):
        """
        Restart MON pods.

        Args:
            wait_till_pods_running (bool): Wait until MON pods are recreated and running.
        """
        self._restart_pods(get_mon_pods, "MON", wait_till_pods_running)

    def restart_rgw_pods(self, wait_till_pods_running: bool = True):
        """
        Restart RGW pods.

        Args:
            wait_till_pods_running (bool): Wait until RGW pods are recreated and running.
        """
        self._restart_pods(get_rgw_pods, "RGW", wait_till_pods_running)

    def pre_failure_checks(self):
        """
        Perform pre-failure checks to ensure the system is in a healthy state.
        This can include checking the status of pods, OSDs, and other components.
        """
        logger.info("Performing pre-failure checks...")
        # Implement any necessary pre-failure checks here
        # For example, check if all pods are running, OSDs are up, etc.
        # This is a placeholder for actual implementation
        pass
        logger.info("Pre-failure checks completed.")

    def post_failure_checks(self):
        """
        Perform post-failure checks to ensure the system has recovered properly.
        This can include checking the status of pods, OSDs, and other components.
        """
        logger.info("Performing post-failure checks...")
        # Implement any necessary post-failure checks here
        # For example, check if all pods are running, OSDs are up, etc.
        # This is a placeholder for actual implementation
        pass
        logger.info("Post-failure checks completed.")
        ceph_tool = CephStatusTool()
        ceph_tool.wait_till_ceph_status_became_healthy()
        logger.info("Ceph status is healthy after failure injection.")

    def run(self, wait_till_pods_running=True, iteration=20):
        """
        Run random application failure scenarios for a number of iterations.

        Args:
            wait_till_pods_running (bool): Wait until pods are recreated and running.
            iteration (int): Number of iterations to run.
        """
        logger.info(
            f"Running application failure scenarios for {iteration} iterations."
        )
        for i in range(iteration):
            logger.info(f"Running iteration {i + 1} of {iteration}.")
            action_name, method_name = random.choice(list(self.FAILURE_METHODS.items()))
            logger.info(f"Selected failure scenario: {action_name}")
            method = getattr(self, method_name)
            self.pre_failure_checks()
            logger.info(f"Executing action: {method_name}")
            method(wait_till_pods_running)
            self.post_failure_checks()
            logger.info(f"Iteration {i + 1} completed.")
            logger.info(f"Completed action: {method_name}")
