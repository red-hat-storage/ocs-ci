import logging
import time

from ocs_ci.resiliency.cluster_failures import get_cluster_object

log = logging.getLogger(__name__)


class NetworkFailures:
    SCENARIO_NAME = "NETWORK_FAILURES"
    FAILURE_METHODS = {
        "POD_NETWORK_FAILURE": "_run_pod_network_failures",
        "NODE_NETWORK_DOWN": "_run_node_network_failure",
    }

    def __init__(self, failure_data):
        self.scenario_name = self.SCENARIO_NAME
        self.failure_data = failure_data
        self.cluster_obj = get_cluster_object()

    def failure_case(self):
        """Get the first failure case key from failure_data."""
        if not self.failure_data:
            raise ValueError("No failure case provided in failure_data.")
        return next(iter(self.failure_data))

    def run(self):
        """Dynamically call the appropriate method based on the failure case."""
        case = self.failure_case()
        method_name = self.FAILURE_METHODS.get(case)
        if method_name and hasattr(self, method_name):
            method = getattr(self, method_name)
            method()
        else:
            raise NotImplementedError(
                f"Failure method for case '{case}' is not implemented."
            )

    def _run_pod_network_failures(self):
        """Handle Pod Network Failure scenario."""
        log.info("Bringing down Pod network interface.")
        # Implement pod network failure logic here

    def _run_node_network_failure(self):
        """Handle Node Network Failure scenario."""
        log.info("Bringing down Node network interfaces.")
        node_types = ["master", "worker"]
        for node_type in node_types:
            node_ip = self.cluster_obj.random_node_ip(node_type)
            self.cluster_obj.change_node_network_interface_state(
                node_ip=node_ip, node_type=node_type, connect=False
            )
            try:
                time.sleep(60)  # Simulate network being down
            finally:
                self.cluster_obj.change_node_network_interface_state(
                    node_ip=node_ip, node_type=node_type, connect=True
                )
                log.info(f"Network interface on node {node_ip} restored.")
