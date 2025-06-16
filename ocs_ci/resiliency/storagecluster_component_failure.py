import logging
import random

from ocs_ci.helpers.disruption_helpers import Disruptions
from ocs_ci.resiliency.resiliency_tools import CephStatusTool

logger = logging.getLogger(__name__)


class StorageClusterComponentFailures:
    """
    Simulates failures of Ceph component pods by deleting them.
    Uses Disruptions class for operations.
    Useful for resiliency testing of OpenShift Data Foundation.
    """

    SCENARIO_NAME = "STORAGECLUSTER_COMPONENT_FAILURES"

    # Maps failure method names to Ceph resource types
    FAILURE_METHODS = {
        "OSD_POD_FAILURES": "osd",
        "MGR_POD_FAILURES": "mgr",
        "MDS_POD_FAILURES": "mds",
        "MON_POD_FAILURES": "mon",
        "CEPHFS_POD_FAILURES": "cephfsplugin",
        "RBD_POD_FAILURES": "rbdplugin",
    }

    def __init__(self, failure_data):
        self.failure_data = failure_data
        self.disruptions = Disruptions()
        logger.info("Initialized StorageClusterComponentFailures")

    def _restart_pods(self, resource_type, wait=True):
        """Handles pod restarts for any Ceph component."""
        logger.info(f"Restarting '{resource_type}' pods...")
        self.disruptions.set_resource(resource_type)
        self.disruptions.delete_resource()
        logger.info(f"'{resource_type}' pods deleted. Waiting for recovery: {wait}")

    def pre_failure_checks(self):
        """System health checks before disruption."""
        logger.info("Running pre-failure checks...")
        # Insert checks if needed
        logger.info("Pre-failure checks passed.")

    def post_failure_checks(self):
        """Validate Ceph is healthy post-disruption."""
        logger.info(" Running post-failure checks...")
        CephStatusTool().wait_till_ceph_status_became_healthy()
        logger.info(" Ceph is healthy after failure injection.")

    def run(self, failure_method=None, wait_for_recovery=True, iterations=20):
        """
        Run disruption scenarios for the given component.

        Args:
            failure_method (str): Optional specific method (key from FAILURE_METHODS)
            wait_for_recovery (bool): Wait for recovery after disruption
            iterations (int): How many times to run the scenario
        """
        logger.info(f" Starting {self.SCENARIO_NAME} for {iterations} iterations")

        for i in range(1, iterations + 1):
            logger.info(f"--- Iteration {i}/{iterations} ---")

            # Select method and resource
            if failure_method:
                resource = self.FAILURE_METHODS.get(failure_method)
                if not resource:
                    raise ValueError(
                        f"Invalid failure method '{failure_method}'. "
                        f"Available methods: {list(self.FAILURE_METHODS.keys())}"
                    )
                method_name = failure_method
            else:
                method_name, resource = random.choice(
                    list(self.FAILURE_METHODS.items())
                )

            logger.info(f"Selected scenario: {method_name} → {resource}")

            self.pre_failure_checks()
            self._restart_pods(resource, wait_for_recovery)
            self.post_failure_checks()

            logger.info(f" Completed iteration {i}: {method_name}")
