import logging
import random
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.resiliency.cluster_failures import get_cluster_object

log = logging.getLogger(__name__)


class NodeFailures:
    SCENARIO_NAME = "NODE_FAILURES"
    FAILURE_METHODS = {
        "POWEROFF_NODE": "_run_poweroff_node",
        "NODE_DRAIN": "_run_node_drain",
    }

    def __init__(self, failure_data):
        self.failure_data = failure_data
        self.failure_case_name = self._get_failure_case()
        self.scenario_name = self.SCENARIO_NAME
        self.cluster_obj = get_cluster_object()

    def _get_failure_case(self):
        """Retrieve the failure case name from the provided failure data."""
        if not self.failure_data:
            log.error("Failure data is empty.")
            return None
        return next(iter(self.failure_data))

    def run(self):
        """Run the failure scenario based on the failure case."""
        if not self.failure_case_name:
            log.error("No valid failure case name found. Exiting run method.")
            return

        method_name = self.FAILURE_METHODS.get(self.failure_case_name)
        if method_name and hasattr(self, method_name):
            failure_method = getattr(self, method_name)
            failure_method()
            self._post_scenario_checks()
        else:
            raise NotImplementedError(
                f"Failure method for '{self.failure_case_name}' is not implemented."
            )

    def _run_poweroff_node(self):
        """Simulate the reboot of nodes."""
        log.info("Running Failure Case: POWEROFF_NODE.")
        node_types = self.failure_data[self.failure_case_name].get("NODE_TYPE", [])
        poweroff_iteration = self.failure_data[self.failure_case_name].get(
            "ITERATION", 0
        )

        for _ in range(poweroff_iteration):
            node_type = random.choice(node_types)
            log.info(f"Rebooting {node_type} node.")
            self.cluster_obj.reboot_node(node_type=node_type)
            log.info(f"{node_type.capitalize()} node rebooted.")

    def _run_node_drain(self):
        """Simulate draining of nodes."""
        log.info("Running Failure Case: NODE_DRAIN.")
        # Implement node drain logic here
        log.info("Draining node...")

    def _post_scenario_checks(self):
        """Perform post-scenario checks to ensure the cluster is healthy."""
        log.info(f"Running post-scenario checks for {self.scenario_name}.")
        log.info("Verifying that Ceph health is OK (retrying if necessary).")
        ceph_health_check(tries=45, delay=60)
