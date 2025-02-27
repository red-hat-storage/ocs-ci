import yaml
import os
import logging
from ocs_ci.ocs import constants
from ocs_ci.resiliency.node_failures import NodeFailures
from ocs_ci.resiliency.network_failures import NetworkFailures
from ocs_ci.helpers.sanity_helpers import Sanity

log = logging.getLogger(__name__)


class ResiliencyConfig:
    """Handles loading and parsing of the resiliency configuration."""

    CONFIG_FILE = os.path.join(constants.RESILIENCY_DIR, "conf", "resiliency.yaml")

    def __init__(self):
        self.data = self.load_yaml(self.CONFIG_FILE)
        resiliency = self.data.get("RESILIENCY", {})
        self.run_config = resiliency.get("RUN_CONFIG", {})
        self.stop_when_ceph_unhealthy = self.run_config.get(
            "STOP_WHEN_CEPH_UNHEALTHY", False
        )
        self.iterate_scenarios = self.run_config.get("ITERATE_SCENARIOS", False)
        self.failure_scenarios = resiliency.get("FAILURE_SCENARIOS", [])

    @staticmethod
    def load_yaml(file_path):
        """Load and parse the YAML file."""
        try:
            with open(file_path, "r") as file:
                return yaml.safe_load(file) or {}
        except FileNotFoundError:
            log.error(f"YAML file not found: {file_path}")
            return {}
        except yaml.YAMLError as exc:
            log.error(f"Error parsing YAML file {file_path}: {exc}")
            return {}

    def __repr__(self):
        """Representation of the ResiliencyConfig object."""
        return (
            f"ResiliencyConfig("
            f"stop_when_ceph_unhealthy={self.stop_when_ceph_unhealthy}, "
            f"iterate_scenarios={self.iterate_scenarios}, "
            f"failure_scenarios={self.failure_scenarios})"
        )


class ResiliencyFailures:
    """Handles loading failure cases from the configuration and iterating over them."""

    SCENARIO_DIR = os.path.join(constants.RESILIENCY_DIR, "conf")

    def __init__(self, scenario_name, failure_method=None):
        self.scenario_name = scenario_name
        self.failure_method = failure_method
        self.failure_cases_data = self.get_failure_cases_data()
        self.workload = self.failure_cases_data.get("WORKLOAD", "")
        self.failure_list = self.get_failure_list()
        self._iterator = iter(self.failure_list)

    def get_failure_cases_data(self):
        """Load the YAML file containing failure case details for the given scenario."""
        log.info(
            f"Searching for scenario '{self.scenario_name}' in directory: {self.SCENARIO_DIR}"
        )
        scenario_file = f"{self.scenario_name.lower()}.yaml"
        file_path = os.path.join(self.SCENARIO_DIR, scenario_file)

        if os.path.isfile(file_path):
            data = ResiliencyConfig.load_yaml(file_path)
            if self.scenario_name in data:
                log.info(
                    f"Found scenario '{self.scenario_name}' in file: {scenario_file}"
                )
                return data[self.scenario_name]
            else:
                log.error(
                    f"Scenario '{self.scenario_name}' not found in file: {scenario_file}"
                )
        else:
            log.error(
                f"Scenario file '{scenario_file}' not found in directory: {self.SCENARIO_DIR}"
            )
        return {}

    def get_failure_list(self):
        """Retrieve and optionally filter the failure list based on the failure method."""
        failures = self.failure_cases_data.get("FAILURES", [])
        if self.failure_method:
            # Filter the failures to include only those matching the failure method
            filtered_failures = [
                {self.failure_method: failure[self.failure_method]}
                for failure in failures
                if self.failure_method in failure
            ]
            if not filtered_failures:
                log.warning(
                    f"No failures found for failure method '{self.failure_method}' in scenario '{self.scenario_name}'."
                )
            return filtered_failures
        return failures

    def __iter__(self):
        """Return an iterator over the failure list."""
        self._iterator = iter(self.failure_list)
        return self._iterator


class Resiliency:
    """Main class for running resiliency tests."""

    def __init__(self, scenario, failure_method=None):
        self.scenario_name = scenario
        self.resiliency_failures = ResiliencyFailures(scenario, failure_method)
        self.sanity_helpers = Sanity()

    def post_scenario_check(self):
        """Perform post-scenario checks like Ceph health and logs."""
        log.info("Checking Ceph health...")
        self.sanity_helpers.health_check(tries=40)
        log.info("Running must-gather logs...")

    def start(self):
        """Iterate over and inject the failures one by one."""
        for failure_case in self.resiliency_failures:
            self.inject_failure(failure_case)
            self.post_scenario_check()

    def inject_failure(self, failure):
        """Inject the failure into the system."""
        log.info(f"Running failure case for scenario '{self.scenario_name}': {failure}")
        failure_obj = InjectFailures(self.scenario_name, failure)
        failure_obj.run_failure_case()

    def cleanup(self):
        """Cleanup method after the scenario is completed."""
        log.info("Cleaning up after the scenario...")


class InjectFailures:
    """Handles the actual injection of failures based on the scenario."""

    SCENARIO_CLASSES = {
        NetworkFailures.SCENARIO_NAME: NetworkFailures,
        NodeFailures.SCENARIO_NAME: NodeFailures,
    }

    def __init__(self, scenario, failure_case):
        self.scenario = scenario
        self.failure_case = failure_case

    def failure_object(self):
        scenario_class = self.SCENARIO_CLASSES.get(self.scenario)
        if scenario_class:
            return scenario_class(self.failure_case)
        else:
            raise NotImplementedError(
                f"No implementation for scenario '{self.scenario}'"
            )

    def run_failure_case(self):
        """Inject the failure into the cluster."""
        log.info(
            f"Injecting failure into the cluster for scenario '{self.scenario}'..."
        )
        failure_obj = self.failure_object()
        failure_obj.run()
