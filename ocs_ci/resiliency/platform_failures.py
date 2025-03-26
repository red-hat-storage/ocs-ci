"""
Platform Failure Injection Module

This module defines a set of failure scenarios related to the platform layer.
These failures simulate physical or infrastructure-level disruptions across
supported platforms such as vSphere, AWS, IBM Cloud, and Bare Metal.

Supported Failure Types:
- Instance Failure (node shutdown and restart)
- Zone Failure (placeholder)
- Network Failure (NIC disable/enable)
- Disk Failure (placeholder)

Each failure type is designed to evaluate the resiliency of the OpenShift
cluster and its workloads under real-world conditions.
"""

import logging
import random

from ocs_ci.ocs.platform_nodes import PlatformNodesFactory
from ocs_ci.ocs.node import get_nodes

log = logging.getLogger(__name__)


class PlatformFailures(PlatformNodesFactory):
    """Simulates various platform-level failure scenarios for resiliency testing."""

    SCENARIO_NAME = "PLATFORM_FAILURES"

    FAILURE_METHODS = {
        "PLATFORM_INSTANCE_FAILURES": "_run_platform_instance_failure",
        "PLATFORM_ZONE_FAILURES": "_run_platform_zone_failure",
        "PLATFORM_NETWORK_FAILURES": "_run_platform_network_failure",
        "PLATFORM_DISK_FAILURES": "_run_platform_disk_failure",
    }

    def __init__(self, failure_data):
        """
        Initialize the PlatformFailures class.

        Args:
            failure_data (dict): Configuration containing scenario parameters.
        """
        super().__init__()
        self.platform_node_obj = self.get_nodes_platform()
        self.failure_data = failure_data
        self.nodes = get_nodes()

    def _run_platform_instance_failure(self):
        """
        Simulates platform instance failures by restarting random nodes.

        Iterates over a copy of the node list and restarts each node
        one-by-one using a stop-and-start mechanism.
        """
        log.info("Running Failure Case: PLATFORM_INSTANCE_FAILURES.")
        available_nodes = self.nodes.copy()
        random.shuffle(available_nodes)

        for i, node in enumerate(available_nodes, start=1):
            log.info(f"Iteration {i}: Restarting node {node.name}")
            self.platform_node_obj.restart_nodes_by_stop_and_start([node])

        log.info("Platform instance failure scenario completed.")

    def _run_platform_zone_failure(self):
        """
        Placeholder for simulating platform zone-level failures.

        This could include taking down a full failure domain (e.g., availability zone).
        """
        log.warning("PLATFORM_ZONE_FAILURES is not yet implemented.")

    def _run_platform_network_failure(self):
        """
        Simulates network failure by disabling/enabling network interfaces
        on all cluster nodes temporarily.
        """
        log.info("Running Failure Case: PLATFORM_NETWORK_FAILURES.")
        self.platform_node_obj.disable_nodes_network_temporarily(
            self.nodes, duration=20
        )
        log.info("Completed simulation of network interface failure.")

    def _run_platform_disk_failure(self):
        """
        Placeholder for simulating platform disk-level failures.

        Example implementations might detach disks or simulate I/O errors.
        """
        log.warning("PLATFORM_DISK_FAILURES is not yet implemented.")

    def run(self, failure_case):
        """
        Executes the selected failure scenario.

        Args:
            failure_case (str): Key of the failure method to run.

        Raises:
            NotImplementedError: If the method is missing or unimplemented.
        """
        method_name = self.FAILURE_METHODS.get(failure_case)
        if method_name and hasattr(self, method_name):
            log.info(f"Executing failure case: {failure_case}")
            getattr(self, method_name)()
        else:
            raise NotImplementedError(
                f"Failure method for '{failure_case}' is not implemented."
            )
