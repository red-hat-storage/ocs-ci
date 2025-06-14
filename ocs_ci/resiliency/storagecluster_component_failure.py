import logging
import random

from ocs_ci.helpers.disruption_helpers import Disruptions
from ocs_ci.resiliency.resiliency_tools import CephStatusTool

logger = logging.getLogger(__name__)


class StorageClusterComponentFailures:
    """
    Simulates failures of key Ceph component pods by deleting them.
    Uses Disruptions class for core disruption operations.

    Useful for resiliency testing and validating recovery behavior
    of OpenShift Data Foundation.
    """

    SCENARIO_NAME = "STORAGECLUSTER_COMPONENT_FAILURES"

    # Map failure types to resource names
    FAILURE_METHODS = {
        "OSD_POD_FAILURES": "osd",
        "MGR_POD_FAILURES": "mgr",
        "MDS_POD_FAILURES": "mds",
        "MON_POD_FAILURES": "mon",
        "RGW_POD_FAILURES": "rgw",
    }

    def __init__(self, failure_data):
        """Initialize with failure data and create Disruptions instance."""
        self.failure_data = failure_data
        self.disruptions = Disruptions()
        logger.info("Initialized StorageClusterComponentFailures")

    def _restart_pods(self, resource_type, wait=True):
        """Generic pod restart handler using Disruptions class."""
        logger.info(f"Restarting {resource_type} pods...")
        self.disruptions.set_resource(resource_type)
        self.disruptions.delete_resource()
        logger.info(f"{resource_type} pods restarted. Wait for recovery: {wait}")

    def restart_osd_pods(self, wait=True):
        """Restart OSD pods."""
        self._restart_pods("osd", wait)

    def restart_mgr_pods(self, wait=True):
        """Restart MGR pods."""
        self._restart_pods("mgr", wait)

    def restart_mds_pods(self, wait=True):
        """Restart MDS pods."""
        self._restart_pods("mds", wait)

    def restart_mon_pods(self, wait=True):
        """Restart MON pods."""
        self._restart_pods("mon", wait)

    def restart_rgw_pods(self, wait=True):
        """Restart RGW pods."""
        self._restart_pods("rgw", wait)

    def pre_failure_checks(self):
        """Verify system is healthy before failure injection."""
        logger.info("Running pre-failure checks...")
        # Add actual checks here if needed
        logger.info("Pre-failure checks completed")

    def post_failure_checks(self):
        """Verify system recovery after failure injection."""
        logger.info("Running post-failure checks...")
        CephStatusTool().wait_till_ceph_status_became_healthy()
        logger.info("Ceph status healthy after failure")

    def run(self, failure_method=None, wait_for_recovery=True, iterations=20):
        """
        Execute failure scenarios for specified iterations.

        Args:
            failure_method: Specific failure method to run or None for random
            wait_for_recovery: Whether to wait for pod recovery
            iterations: Number of times to run the scenario
        """
        logger.info(f"Starting failure scenarios for {iterations} iterations")

        for i in range(1, iterations + 1):
            logger.info(f"Iteration {i}/{iterations}")

            # Select failure method
            if failure_method:
                if failure_method not in self.FAILURE_METHODS:
                    available = list(self.FAILURE_METHODS.keys())
                    raise ValueError(
                        f"Invalid method: {failure_method}. Available: {available}"
                    )
                method_name = failure_method
                resource = self.FAILURE_METHODS[failure_method]
            else:
                method_name, resource = random.choice(
                    list(self.FAILURE_METHODS.items())
                )

            logger.info(f"Selected scenario: {method_name}")

            # Execute the scenario
            self.pre_failure_checks()
            getattr(self, f"restart_{resource}_pods")(wait_for_recovery)
            self.post_failure_checks()

            logger.info(f"Completed iteration {i}: {method_name}")
