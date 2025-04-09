"""
This module provides the core framework for running resiliency test scenarios
on an OpenShift cluster. It parses scenario configurations, identifies failures
to inject, and manages their execution and post-failure validations.

Components:

- ResiliencyConfig: Loads and parses the global resiliency YAML configuration.
- ResiliencyFailures: Loads failure scenarios from individual YAML files.
- Resiliency: Orchestrates scenario execution, injection, and validation.
- InjectFailures: Dynamically maps and executes failure scenarios.

Supports testing of OpenShift cluster behavior under failure conditions such as:
- Node shutdowns
- Network disruptions
- Disk removals
- Zone-level platform failures
"""

import yaml
import os
import glob
import logging
import subprocess
from ocs_ci.ocs import constants
from ocs_ci.resiliency.platform_failures import PlatformFailures
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.utility.utils import remove_ceph_crashes, get_ceph_crashes
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    CephHealthException,
    NoRunningCephToolBoxException,
)

log = logging.getLogger(__name__)


class ResiliencyConfig:
    """Loads and parses the main resiliency YAML configuration."""

    CONFIG_FILE = os.path.join(constants.RESILIENCY_DIR, "conf", "resiliency.yaml")

    def __init__(self):
        """Initializes the resiliency configuration object."""
        self.data = self.load_yaml(self.CONFIG_FILE)
        resiliency = self.data.get("RESILIENCY", {})
        self.run_config = resiliency.get("RUN_CONFIG", {})
        self.stop_when_ceph_unhealthy = self.run_config.get(
            "STOP_WHEN_CEPH_UNHEALTHY", False
        )
        self.stop_when_ceph_crashed = self.run_config.get(
            "STOP_WHEN_CEPH_CRASHED", False
        )
        self.iterate_scenarios = self.run_config.get("ITERATE_SCENARIOS", False)
        self.namespace = self.run_config.get("NAMESPACE", "default")
        self.failure_scenarios = resiliency.get("FAILURE_SCENARIOS", {})

    def get_all_failure_methods(self):
        """Collects all failure methods from every failure category.

        Returns:
            list: A list of all failure method names.
        """
        methods = []
        for category, failures in self.failure_scenarios.items():
            methods.extend(failures)
        return methods

    def get_failures_by_category(self, category_name):
        """Fetch failure methods for a given scenario category.

        Args:
            category_name (str): The failure category name.

        Returns:
            list: List of failure method names.
        """
        return self.failure_scenarios.get(category_name.upper(), [])

    @staticmethod
    def load_yaml(file_path):
        """Loads a YAML file and returns the parsed data.

        Args:
            file_path (str): Path to the YAML file.

        Returns:
            dict: Parsed YAML content.
        """
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
        """Provides string representation of the configuration.

        Returns:
            str: Summary of configuration.
        """
        return (
            f"ResiliencyConfig("
            f"stop_when_ceph_unhealthy={self.stop_when_ceph_unhealthy}, "
            f"stop_when_ceph_crashed={self.stop_when_ceph_crashed}, "
            f"iterate_scenarios={self.iterate_scenarios}, "
            f"namespace='{self.namespace}', "
            f"failure_scenarios={self.failure_scenarios})"
        )


class ResiliencyFailures:
    """Iterates through failure cases in a resiliency scenario."""

    SCENARIO_DIR = os.path.join(constants.RESILIENCY_DIR, "conf")

    def __init__(self, scenario_category, failure_method=None):
        """
        Initializes failure case loader and iterator.

        Args:
            scenario_category (str): Top-level scenario category (e.g., PLATFORM_FAILURES).
            failure_method (str, optional): Specific failure method to run.
        """
        self.scenario_category = scenario_category.upper()
        self.failure_method = failure_method
        self.config = ResiliencyConfig()
        self.failure_cases_data = self.load_category_yaml()
        self.failure_dict = self._get_failure_dict()
        self.workload = self.get_workload()
        self.failure_list = self.get_failure_list()
        self.index = 0

    def load_category_yaml(self):
        """Loads YAML file for a specific scenario category.

        Returns:
            dict: Dictionary of failures under the given category.
        """
        yaml_files = glob.glob(os.path.join(self.SCENARIO_DIR, "*.yaml"))
        for file_path in yaml_files:
            data = self.config.load_yaml(file_path)
            if data.get("SCENARIO_NAME", "").upper() == self.scenario_category:
                log.info(f"Loaded failure data from: {file_path}")
                return data.get("FAILURES", {})
        log.error(
            f"Scenario category '{self.scenario_category}' not found in any YAML file."
        )
        return {}

    def _get_failure_dict(self):
        """Retrieves failure dict.

        Returns:
            dict: Failure methods and configuration.
        """
        return self.failure_cases_data

    def get_workload(self):
        """Get workload associated with a specific failure method.

        Returns:
            list | str: Workload(s) associated with the failure.
        """
        if self.failure_method and self.failure_method in self.failure_dict:
            return self.failure_dict[self.failure_method].get("WORKLOAD", "")
        return ""

    def get_failure_list(self):
        """Gets list of failure cases, optionally filtered.

        Returns:
            list: List of failure dicts to iterate over.
        """
        if self.failure_method:
            if self.failure_method in self.failure_dict:
                return [{self.failure_method: self.failure_dict[self.failure_method]}]
            else:
                log.warning(
                    f"Failure method '{self.failure_method}' not found in category '{self.scenario_category}'."
                )
                return []
        return [{k: v} for k, v in self.failure_dict.items()]

    def __iter__(self):
        self.index = 0
        return self

    def __next__(self):
        """Python iterator next method.

        Returns:
            dict: Next failure scenario.

        Raises:
            StopIteration: If all failures are exhausted.
        """
        if self.index < len(self.failure_list):
            result = self.failure_list[self.index]
            self.index += 1
            return result
        raise StopIteration


class Resiliency:
    """Orchestrates a full resiliency scenario run."""

    def __init__(self, scenario, failure_method=None):
        """
        Initializes the scenario.

        Args:
            scenario (str): Scenario category (e.g., PLATFORM_FAILURES).
            failure_method (str, optional): Specific method to run.
        """
        self.scenario_name = scenario
        self.failure_method = failure_method
        self.resiliency_failures = ResiliencyFailures(scenario, self.failure_method)

    def pre_scenario_check(self):
        """Perform health checks and gather logs before scenario execution."""
        log.info("Checking Ceph health...")
        ceph_health_check(fix_ceph_health=True)
        log.info("Removing any existing Ceph crash logs...")
        toolbox = pod.get_ceph_tools_pod()
        remove_ceph_crashes(toolbox)
        log.info("Running must-gather logs...")

    def post_scenario_check(self):
        """Perform health checks and gather logs after scenario execution."""

        log.info("Removing any existing Ceph crash logs...")
        toolbox = pod.get_ceph_tools_pod()
        ceph_crashes = get_ceph_crashes(toolbox)
        if ceph_crashes:
            log.error(f"Ceph crash logs found: {ceph_crashes}")
            raise Exception("Ceph crash logs found after scenario execution.")
        log.info("Checking Ceph health...")
        if not ceph_health_check(fix_ceph_health=True):
            log.error("Ceph health check failed after scenario execution.")

    def start(self):
        """Start running all failure scenarios under this configuration."""
        for failure_case in self.resiliency_failures:
            if not isinstance(failure_case, dict):
                log.error("Failure case is not a valid dictionary.")
                continue
            self.pre_scenario_check()
            log.info(f"Running failure case: {failure_case}")
            self.inject_failure(failure_case)
            self.post_scenario_check()

    def inject_failure(self, failure):
        """Inject a failure into the system.

        Args:
            failure (dict): The failure method and its configuration.
        """
        log.info(f"Running failure case for scenario '{self.scenario_name}': {failure}")
        failure_obj = InjectFailures(self.scenario_name, failure)
        failure_obj.run_failure_case()

    def cleanup(self):
        """Cleanup resources after scenario execution."""
        log.info("Cleaning up after the scenario...")


class InjectFailures:
    """Handles mapping and execution of failure injection based on scenario."""

    SCENARIO_CLASSES = {
        PlatformFailures.SCENARIO_NAME: PlatformFailures,
    }

    def __init__(self, scenario, failure_case):
        """
        Initializes failure injection handler.

        Args:
            scenario (str): Scenario name.
            failure_case (dict): Failure case configuration.
        """
        self.scenario = scenario
        self.failure_case_data = failure_case
        self.failure_method = list(self.failure_case_data)[0]

    def pre_failure_injection_check(self):
        """Perform checks before injecting a failure."""
        log.info("Performing pre-failure injection checks...")

    def post_failure_injection_check(self):
        """Perform checks after injecting a failure."""
        log.info("Performing post-failure injection checks...")
        try:
            ceph_health_check(fix_ceph_health=True)
        except (
            CephHealthException,
            CommandFailed,
            subprocess.TimeoutExpired,
            NoRunningCephToolBoxException,
        ) as e:
            log.error(f"Ceph health check failed after failure injection. : {e}")

    def failure_object(self):
        """Get the failure scenario class instance.

        Returns:
            object: Initialized failure scenario handler.

        Raises:
            NotImplementedError: If the scenario is not supported.
        """
        scenario_class = self.SCENARIO_CLASSES.get(self.scenario)
        if scenario_class:
            return scenario_class(self.failure_case_data)
        raise NotImplementedError(f"No implementation for scenario '{self.scenario}'")

    def run_failure_case(self):
        """Execute failure scenario."""
        log.info(
            f"Injecting failure into the cluster for scenario '{self.scenario}'..."
        )
        self.pre_failure_injection_check()
        failure_obj = self.failure_object()
        failure_obj.run(self.failure_method)
        self.post_failure_injection_check()
        log.info(
            f"Failure injection for scenario '{self.scenario}' completed successfully."
        )
