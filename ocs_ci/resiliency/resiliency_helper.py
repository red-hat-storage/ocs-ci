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
import threading
import time
import random
from ocs_ci.ocs import constants
from ocs_ci.resiliency.platform_failures import PlatformFailures
from ocs_ci.resiliency.storagecluster_component_failure import (
    StorageClusterComponentFailures,
)
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    CephHealthException,
    NoRunningCephToolBoxException,
    TimeoutExpiredError,
)
from ocs_ci.ocs.resources.pod import delete_pod_by_phase
from ocs_ci.resiliency.resiliency_tools import CephStatusTool
from concurrent.futures import (
    ThreadPoolExecutor,
    as_completed,
    CancelledError,
    TimeoutError as ThreadTimeoutError,
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
        self.cephtool = CephStatusTool()
        self.resiliency_failures = ResiliencyFailures(scenario, self.failure_method)

    def pre_scenario_check(self):
        """Perform health checks and gather logs before scenario execution."""
        self.cephtool.wait_till_ceph_status_became_healthy()
        log.info("Removing any existing Ceph crash logs...")
        self.cephtool.archive_ceph_crashes()

    def post_scenario_check(self):
        """Perform health checks and gather logs after scenario execution."""

        log.info("Removing any existing Ceph crash logs...")
        ceph_crashes = self.cephtool.check_ceph_crashes()
        if ceph_crashes:
            log.error(f"Ceph crash logs found: {ceph_crashes}")
            raise Exception("Ceph crash logs found after scenario execution.")

        log.info("Checking Ceph health...")
        if not ceph_health_check(fix_ceph_health=True, tries=25):
            log.error("Ceph health check failed after scenario execution.")

        log.info("Ceph health check passed after scenario execution.")

        # Removing Failed and Succeeded pods
        # When we run node debug pods are created in the namespace
        # and they are not deleted automatically.
        delete_pod_by_phase(
            "succeeded",
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        delete_pod_by_phase(
            "failed",
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )

    def start(self):
        """Start running all failure scenarios under this configuration."""
        for failure_case in self.resiliency_failures:
            if not isinstance(failure_case, dict):
                log.error("Failure case is not a valid dictionary.")
                continue
            self.pre_scenario_check()
            log.info(f"Running failure case: {failure_case}")
            try:
                self.inject_failure(failure_case)
            except (TimeoutExpiredError, CommandFailed, CephHealthException) as e:
                log.error(f"Failure case execution failed: {e}")
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
        StorageClusterComponentFailures.SCENARIO_NAME: StorageClusterComponentFailures,
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


class WorkloadScalingHelper:
    """
    Helper class for managing workload scaling operations in background threads.

    This class provides functionality to:
    - Scale workloads randomly up or down within defined limits
    - Run scaling operations in parallel using ThreadPoolExecutor
    - Manage background scaling threads with proper error handling
    - Wait for scaling completion with timeout support
    """

    def __init__(self, min_replicas=1, max_replicas=5):
        """
        Initialize the WorkloadScalingHelper.

        Args:
            min_replicas: Minimum number of replicas allowed
            max_replicas: Maximum number of replicas allowed
        """
        self.min_replicas = min_replicas
        self.max_replicas = max_replicas
        self._scaling_threads = []

    def start_background_scaling(self, workloads, delay=30):
        """
        Start scaling workloads in background using a separate thread.

        Args:
            workloads: List of workload objects to scale
            delay: Delay in seconds before starting scaling operations

        Returns:
            threading.Thread: The scaling thread
        """

        def scale_workloads():
            """Scale workloads after delay."""
            log.info(f"Waiting {delay} seconds before starting scaling operations")
            time.sleep(delay)

            with ThreadPoolExecutor(max_workers=len(workloads)) as executor:
                scaling_futures = {
                    executor.submit(self.scale_single_workload, workload): workload
                    for workload in workloads
                }

                for future in as_completed(scaling_futures):
                    workload = scaling_futures[future]
                    future.result()
                    log.info(
                        f"Successfully scaled workload {workload.workload_impl.deployment_name}"
                    )

        scaling_thread = threading.Thread(target=scale_workloads, daemon=True)
        scaling_thread.start()
        self._scaling_threads.append(scaling_thread)
        return scaling_thread

    def scale_single_workload(self, workload):
        """
        Randomly scale a single workload up or down within limits.

        Args:
            workload: The workload object to scale

        Returns:
            bool: True if the workload was scaled, False otherwise
        """
        current_replicas = workload.workload_impl.current_replicas
        deployment_name = workload.workload_impl.deployment_name

        log.info(f"Current replicas for {deployment_name}: {current_replicas}")

        action = random.choice(["up", "down"])

        if action == "up":
            if current_replicas >= self.max_replicas:
                log.info(
                    f"Already at max replicas ({self.max_replicas}). "
                    f"Skipping scale up for {deployment_name}"
                )
                return False

            desired_count = random.randint(current_replicas + 1, self.max_replicas)
            log.info(f"Scaling up {deployment_name} to {desired_count} replicas")
            workload.scale_up_pods(desired_count)
            return True

        else:  # action == "down"
            if current_replicas <= self.min_replicas:
                log.info(
                    f"Already at min replicas ({self.min_replicas}). "
                    f"Skipping scale down for {deployment_name}"
                )
                return False

            desired_count = random.randint(self.min_replicas, current_replicas - 1)
            log.info(f"Scaling down {deployment_name} to {desired_count} replicas")
            workload.scale_down_pods(desired_count)
            return True

    def wait_for_scaling_completion(self, scaling_thread, timeout=120):
        """
        Wait for scaling thread to complete with timeout.

        Args:
            scaling_thread: The scaling thread to wait for
            timeout: Maximum time to wait in seconds

        Returns:
            bool: True if thread completed, False if not started or timeout occurred
        """
        if not scaling_thread:
            log.warning("No scaling thread was provided")
            return False

        if not scaling_thread.is_alive():
            log.warning("Scaling thread is not running or already completed")
            return False

        log.info(f"Waiting for scaling operations to complete (timeout: {timeout}s)")
        scaling_thread.join(timeout=timeout)

        if scaling_thread.is_alive():
            log.warning("Scaling operations did not complete within timeout")
            return False

        log.info("Scaling operations completed successfully")
        return True

    def wait_for_all_scaling_threads(self, timeout=120):
        """
        Wait for all scaling threads managed by this helper to complete.

        Args:
            timeout: Maximum time to wait in seconds for each thread

        Returns:
            bool: True if all threads completed, False if any timed out
        """
        all_completed = True
        for thread in self._scaling_threads:
            if not self.wait_for_scaling_completion(thread, timeout):
                all_completed = False
        return all_completed

    def scale_workloads_synchronously(self, workloads):
        """
        Scale all workloads synchronously (blocking operation).

        Args:
            workloads: List of workload objects to scale
        """
        log.info(f"Starting synchronous scaling of {len(workloads)} workloads")

        for workload in workloads:
            if self.scale_single_workload(workload):
                log.info(
                    f"Successfully scaled workload {workload.workload_impl.deployment_name}"
                )
            else:
                log.info(
                    f"Workload {workload.workload_impl.deployment_name} already at limits, skipping"
                )

    def scale_workloads_parallel(self, workloads, max_workers=None):
        """
        Scale all workloads in parallel (blocking operation).

        Args:
            workloads: List of workload objects to scale
            max_workers: Maximum number of worker threads. If None, uses len(workloads)
        """
        if max_workers is None:
            max_workers = len(workloads)

        log.info(
            f"Starting parallel scaling of {len(workloads)} workloads with {max_workers} workers"
        )

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all scaling tasks
            scaling_futures = {
                executor.submit(self.scale_single_workload, workload): workload
                for workload in workloads
            }

            # Wait for all scaling operations to complete
            for future in as_completed(scaling_futures):
                workload = scaling_futures[future]
                try:
                    future.result()  # This will raise exception if scaling failed
                    log.info(
                        f"Successfully scaled workload {workload.workload_impl.deployment_name}"
                    )
                except CancelledError:
                    log.warning(
                        f"Scaling task was cancelled for workload {workload.workload_impl.deployment_name}"
                    )
                except ThreadTimeoutError:
                    log.error(
                        f"Scaling task timed out for workload {workload.workload_impl.deployment_name}"
                    )
                except (ValueError, RuntimeError) as e:
                    log.error(
                        f"Error while scaling workload {workload.workload_impl.deployment_name}: {e}"
                    )

    def cleanup(self, timeout=60):
        """
        Cleanup any running scaling threads.

        Args:
            timeout: Maximum time to wait for each thread to complete
        """
        log.info("Cleaning up scaling threads...")
        for thread in self._scaling_threads:
            if thread.is_alive():
                thread.join(timeout=timeout)
                if thread.is_alive():
                    log.warning(
                        f"Scaling thread did not complete within {timeout}s timeout"
                    )

        self._scaling_threads.clear()
        log.info("Scaling thread cleanup completed")
