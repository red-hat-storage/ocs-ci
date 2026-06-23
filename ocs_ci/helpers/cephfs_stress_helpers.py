"""
CephFS Stress Test Helper Module

This module provides comprehensive utilities for managing CephFS stress tests,
including pod/job creation, background health monitoring, cluster verification
and resource cleanup.

"""

import os
import json
from pathlib import Path
import gc
import logging
import threading

from prettytable import PrettyTable

from ocs_ci.framework import config
from ocs_ci.ocs.constants import (
    CEPHFS_STRESS_POD_YAML,
    CEPHFS_STRESS_JOB_YAML,
    STATUS_RUNNING,
)
from ocs_ci.utility import templating
from ocs_ci.utility.utils import ocsci_log_path
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.job import get_job_pods
from ocs_ci.helpers.helpers import (
    validate_pod_oomkilled,
    get_mon_db_size_in_kb,
    create_pod,
    create_project,
    create_pvc,
    wait_for_resource_state,
    get_current_test_name,
)
from ocs_ci.ocs.resources.pod import (
    check_pods_in_running_state,
    get_all_pods,
    get_mon_pods,
    pod_resource_utilization_raw_output_from_adm_top,
)
from ocs_ci.utility.prometheus import PrometheusAPI
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.ocs.cluster import (
    get_osd_utilization,
    get_percent_used_capacity,
)
from ocs_ci.ocs.node import (
    get_node_resource_utilization_from_adm_top,
    get_node_resource_utilization_from_oc_describe,
)
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import CommandFailed, PodsNotRunningError, PodStabilityError
from ocs_ci.ocs.resources import pod as pod_module


logger = logging.getLogger(__name__)


class CephFSStressTestManager:
    """
    Manages CephFS stress test lifecycle with pod/job creation, background health monitoring
    and resource cleanup.

    """

    def __init__(self, namespace):
        """
        Initialize the CephFS Stress Test Manager

        Args:
            namespace: namespace for stress test resources

        """
        self.namespace = namespace
        self.stop_event = threading.Event()
        self.verification_lock = threading.Lock()
        self.verification_failures = []
        self.created_resources = []
        self.background_checks_thread = None
        self.checks_paused = False
        self.standby_pod = None
        # Reuse PrometheusAPI instance to prevent memory leaks from creating new instances
        self.prometheus_api = PrometheusAPI(threading_lock=self.verification_lock)

    def setup_stress_test_environment(self, pvc_size):
        """
        Creates the foundational resources (PVC and Standby Pod) for the stress test.

        Args:
            pvc_size (str): Size of pvc to create

        Returns:
            tuple: Created PVC and standby pod objs

        """
        pvc_obj = create_pvc(
            sc_name=constants.CEPHFILESYSTEM_SC,
            namespace=self.namespace,
            size=pvc_size,
            access_mode=constants.ACCESS_MODE_RWX,
            pvc_name="cephfs-stress-pvc",
        )
        standby_pod_obj = create_pod(
            interface_type=constants.CEPHFILESYSTEM,
            pvc_name=pvc_obj.name,
            namespace=self.namespace,
            pod_name="standby-cephfs-stress-pod",
            volumemounts=[{"name": "mypvc", "mountPath": "/mnt"}],
        )
        self.created_resources.append(standby_pod_obj)
        self.standby_pod = standby_pod_obj

        return pvc_obj, standby_pod_obj

    def create_cephfs_stress_pod(
        self,
        pvc_name,
        base_dir=None,
        files_size=None,
        operations=None,
        base_file_count=None,
        multiplication_factors=None,
        threads=None,
    ):
        """
        Creates a CephFS stress pod, utilizing smallfiles to generate numerous files and directories.

        The pod is configured with various parameters to stress CephFS and
        gradually increases load in incremental stages.

        Args:
            pvc_name (str) : Name of the PersistentVolumeClaim
            base_dir (str, optional): Directory used by smallfile to perform file and directory operations
            files_size (str, optional): Size of each file in KB
            operations (str, optional): File operations to perform (e.g., append, stat, chmod, ls-l, etc),
            Pass as a comma-separated string
            base_file_count (str, optional): Base file count, to multiply with scaling factor
            multiplication_factors (str, optional): Dynamic scaling of file creation
            - base_file_count * MULTIPLICATION_FACTORS
            threads (str, optional): Number of threads to use for the operation.

        Returns:
            pod_obj: The created Pod object after it's in a running state

        Raises:
            AssertionError: If the pod creation fails

        """
        env_vars = {
            "BASE_DIR": base_dir,
            "FILES_SIZE": files_size,
            "OPERATIONS": operations,
            "BASE_FILE_COUNT": base_file_count,
            "MULTIPLICATION_FACTORS": multiplication_factors,
            "THREADS": threads,
        }
        cephfs_stress_pod_data = templating.load_yaml(CEPHFS_STRESS_POD_YAML)
        cephfs_stress_pod_data["metadata"]["namespace"] = self.namespace
        cephfs_stress_pod_data["spec"]["volumes"][0]["persistentVolumeClaim"][
            "claimName"
        ] = pvc_name
        logger.info("Set environment variables in the pod template")
        self._set_env_vars(cephfs_stress_pod_data, env_vars, type=constants.POD)
        cephfs_stress_pod_obj = pod.Pod(**cephfs_stress_pod_data)
        logger.info(f"Creating CephFS stress pod with PVC: {pvc_name}")
        created_resource = cephfs_stress_pod_obj.create()
        assert created_resource, f"Failed to create Pod {cephfs_stress_pod_obj.name}"
        self.created_resources.append(cephfs_stress_pod_obj)

        logger.info("Waiting for Cephfs stress pod to start")
        self._wait_with_retry(cephfs_stress_pod_obj, STATUS_RUNNING, 300)

        return cephfs_stress_pod_obj

    def create_cephfs_stress_job(
        self,
        pvc_name,
        base_dir=None,
        files_size=None,
        operations=None,
        base_file_count=None,
        multiplication_factors=None,
        threads=None,
        parallelism=None,
        completions=None,
    ):
        """
        Creates a CephFS stress Job. This job launches concurrent pods based on the configured
        parallelism count, where each pod executes generate numerous small files and directories.
        Configured with specific parameters, the workload stresses CephFS by gradually increasing
        the load in incremental stages.

        Args:
            pvc_name (str) : Name of the PersistentVolumeClaim
            base_dir (str, optional): Directory used by smallfile to perform file and directory operations
            files_size (str, optional): Size of each file in KB
            operations (str, optional): File operations to perform (e.g., append, stat, chmod, ls-l, etc),
            Pass as a comma-separated string
            base_file_count (str, optional): Base file count, to multiply with scaling factor
            multiplication_factors (str, optional): Dynamic scaling of file creation
            - base_file_count * MULTIPLICATION_FACTORS
            threads (str, optional): Number of threads to use for the operation.
            parallelism (str, optional): Specifies how many pod replicas running in parallel should execute a job.
            completions (str, optional): Specifies how many times the Pod must finish successfully before the entire
            Job is marked as "Complete.

        Returns:
            cephfs_stress_job_obj(OCS): The created Job object after it's in a running state

        Raises:
            AssertionError: If the Job creation fails

        """
        env_vars = {
            "BASE_DIR": base_dir,
            "FILES_SIZE": files_size,
            "OPERATIONS": operations,
            "BASE_FILE_COUNT": base_file_count,
            "MULTIPLICATION_FACTORS": multiplication_factors,
            "THREADS": threads,
        }
        cephfs_stress_job_data = templating.load_yaml(CEPHFS_STRESS_JOB_YAML)
        cephfs_stress_job_data["metadata"]["namespace"] = self.namespace
        cephfs_stress_job_data["spec"]["template"]["spec"]["volumes"][0][
            "persistentVolumeClaim"
        ]["claimName"] = pvc_name
        if parallelism:
            cephfs_stress_job_data["spec"]["parallelism"] = parallelism
        if completions:
            cephfs_stress_job_data["spec"]["completions"] = completions
        logger.info("Set environment variables in the pod template")
        self._set_env_vars(cephfs_stress_job_data, env_vars, type=constants.JOB)
        job_name = cephfs_stress_job_data["metadata"]["name"]
        job_ocs_obj = OCS(**cephfs_stress_job_data)
        created_resource = job_ocs_obj.create()
        assert created_resource, f"Failed to create Job {job_ocs_obj.name}"
        logger.info(f"Waiting for Job {job_ocs_obj.name} to start")
        job_ocp_obj = ocp.OCP(
            kind=constants.JOB, namespace=self.namespace, resource_name=job_name
        )
        job_ocp_dict = job_ocp_obj.get(resource_name=job_ocp_obj.resource_name)
        cephfs_stress_job_obj = OCS(**job_ocp_dict)
        self.created_resources.append(cephfs_stress_job_obj)

        self._wait_with_retry(cephfs_stress_job_obj, STATUS_RUNNING, 300)

        return cephfs_stress_job_obj

    @retry(CommandFailed, tries=3, delay=2, backoff=1)
    def _wait_with_retry(self, resource, state, timeout):
        """
        Wrapper to retry wait_for_resource_state in case of transient failures

        Args:
          resource (OCS): The OCS resource object to wait for
          state (str): The desired state (e.g., constants.STATUS_RUNNING)
          timeout (int): Maximum time in seconds to wait for the resource
              to reach the desired state

        Raises:
          CommandFailed: If the resource fails to reach the desired state
              after all retry attempts are exhausted, or if an oc command
              fails due to kubeconfig issues that persist across retries
          ResourceWrongStatusException: If the resource reaches a wrong status
              within the timeout period

        """
        wait_for_resource_state(resource, state=state, timeout=timeout)

    def _set_env_vars(self, resource_data, env_vars, type):
        """
        Updates the pod's environment variables in the container spec based on the provided mapping

        Args:
            resource_data (dict): The resource_data specification loaded from YAML.
            env_vars (dict): Dictionary mapping env variable names to their desired values.
            type (str): pod type, either a regular pod or a job

        """
        if type == constants.POD:
            container_env = resource_data["spec"]["containers"][0].get("env", [])
        elif type == constants.JOB:
            container_env = resource_data["spec"]["template"]["spec"]["containers"][
                0
            ].get("env", [])
        else:
            raise ValueError(f"Unsupported pod_type: '{type}'. Expected POD or JOB.")
        for env in container_env:
            name = env.get("name")
            if name in env_vars and env_vars[name] is not None:
                env["value"] = str(env_vars[name])

    def start_background_checks(self, interval_minutes=5):
        """
        Starts the background thread ('StressWatchdog') for continuous cluster monitoring.

        The background thread runs periodic health checks and verifications at the
        specified interval. If the thread is already running, this method returns
        without creating a new thread.

        Args:
            interval_minutes: Interval in minutes between check executions

        """
        if self.background_checks_thread and self.background_checks_thread.is_alive():
            logger.warning(
                "Background checks ('StressWatchdog') thread is already running..."
            )
            return

        self.stop_event.clear()
        self.background_checks_thread = threading.Thread(
            target=self._continuous_checks_runner,
            args=(interval_minutes,),
            name="StressWatchdog-Thread",
            daemon=True,
        )
        self.background_checks_thread.start()
        logger.info("Background checks ('StressWatchdog')thread started.")

    def stop_background_checks(self, timeout=10):
        """
        Signals the background thread ('StressWatchdog') to stop and waits for it to join.

        """
        if self.background_checks_thread and self.background_checks_thread.is_alive():
            logger.info(
                "Signaling Background checks ('StressWatchdog') thread to stop..."
            )
            self.stop_event.set()
            self.background_checks_thread.join(timeout=timeout)

            if self.background_checks_thread.is_alive():
                logger.warning(
                    f"Background thread did not stop within {timeout}s - "
                    "abandoning (daemon thread will terminate on exit)"
                )
            else:
                logger.info("Background checks ('StressWatchdog') thread stopped.")

    def pause_background_checks(self):
        """
        Pause background verification checks temporarily.

        This is useful during intentional disruptions (e.g., node failures, pod restarts)
        where verification failures are expected and should not fail the test.
        """
        with self.verification_lock:
            self.checks_paused = True
        logger.info(
            "Background verification checks PAUSED - "
            "Verifications will be skipped until resumed"
        )

    def resume_background_checks(self):
        """
        Resume background verification checks after they were paused.

        Should be called after cluster has recovered from intentional disruptions
        and is expected to be in a healthy state.
        """
        with self.verification_lock:
            self.checks_paused = False
        logger.info(
            "Background verification checks RESUMED - "
            "Verifications will now run normally"
        )

    def _continuous_checks_runner(self, interval_minutes):
        """
        Background thread worker that runs continuous health checks.

        This function runs in a background thread, continuously checking for a 'stop_event'.
        It loops until the 'stop_event' is set, sleeping for the specified interval
        in an interruptible way. If the 'stop_event' is set by another thread, the sleep
        is interrupted and the function exits.

        Args:
            interval_minutes: The interval in minutes between check executions

        """
        logger.info(f"Monitor Loop Started (Interval: {interval_minutes}m)")

        while not self.stop_event.is_set():
            if self.stop_event.is_set():
                break

            try:
                self._run_cluster_health_checks()
                self._run_strict_verifications()
            except Exception as e:
                logger.error(
                    f"Unexpected error in background checks loop: {e}", exc_info=True
                )

            if self.stop_event.is_set():
                break

            logger.info(
                f"Pausing for {interval_minutes} minutes before the next round "
                "of periodic cluster and verification checks"
            )
            interval_seconds = interval_minutes * 60
            if self.stop_event.wait(timeout=interval_seconds):
                break

        logger.info("Stop signal received - Background checks loop exiting")

    def _run_strict_verifications(self):
        """
        Run strict verification checks that determine if the test FAILS

        If any verification function raises an AssertionError, this function
        catches it, logs the failure, records it in the 'validation_failures'
        list and signals the main 'stop_event' to stop the entire test

        It also catches any other exception as a verification script failure and performs
        the same stop procedure.

        Raises:
            AssertionError: If any verification function returns False
            Exception: If any verification script fails unexpectedly

        """
        # Check if verifications are paused (e.g., during intentional disruptions)
        with self.verification_lock:
            if self.checks_paused:
                logger.info(
                    "\n=================================================="
                    "\n  VERIFICATION CHECKS PAUSED - SKIPPING          "
                    "\n  (Checks paused during intentional disruptions) "
                    "\n=================================================="
                    "\n"
                )
                return

        logger.info(
            "\n=================================================="
            "\n      STARTING STRICT VERIFICATION CHECKS         "
            "\n=================================================="
            "\n"
        )

        verifications_to_run = [
            check_ceph_health,
            verify_openshift_storage_ns_pods_in_running_state,
            verify_no_filesystem_hangs,
        ]
        try:
            for verification_func in verifications_to_run:
                if self.stop_event.is_set():
                    logger.info("Stop signal received - aborting checks")
                    return
                # Check if paused before each verification (handles mid-execution pause)
                with self.verification_lock:
                    if self.checks_paused:
                        logger.info(
                            "Verification checks were paused mid-execution - stopping verifications"
                        )
                        return

                func_name = verification_func.__name__
                logger.debug(f"Running verification: {func_name}")
                result = verification_func(stress_manager=self)
                if result is False:
                    logger.error(f"Verification {func_name} returned False")
                    raise AssertionError(
                        f"Verification failed: {func_name} returned False"
                    )
                logger.info(f"VERIFICATION {func_name} PASSED")

        except AssertionError as ae:
            logger.error(f"VERIFICATION {func_name} FAILED: {ae}")
            with self.verification_lock:
                self.verification_failures.append(str(ae))
            logger.info("Signaling the main thread and this thread to stop")
            self.stop_event.set()

        except Exception as e:
            logger.error(f"Verification check FAILED: {e}", exc_info=True)
            with self.verification_lock:
                self.verification_failures.append(
                    f"Verification script {func_name} failed: {e}"
                )
            self.stop_event.set()

        logger.info(
            "\n=================================================="
            "\n    FINISHED STRICT VERIFICATION CHECKS           "
            "\n=================================================="
            "\n"
        )

    def _run_cluster_health_checks(self):
        """
        Runs stress-specific cluster health checks.

        These checks are informational and do not fail the test. They collect
        metrics and resource utilization data for monitoring purposes.

        """
        logger.info(
            "\n=================================================="
            "\n             STARTING CLUSTER CHECKS              "
            "\n=================================================="
            "\n"
        )

        checks_to_run = [
            (check_prometheus_alerts, {"stress_manager": self}),
            (check_mds_pods_resource_utilization, {}),
            (get_mon_db_usage, {}),
            (get_nodes_resource_utilization, {}),
            (get_pods_resource_utilization, {}),
            (get_osd_disk_utilization, {}),
            (verify_openshift_storage_ns_pods_health, {}),
        ]
        for check_func, kwargs in checks_to_run:
            if self.stop_event.is_set():
                logger.info("Stop signal received - aborting checks")
                return
            func_name = check_func.__name__
            logger.info(f"Running cluster check: {func_name}")

            try:
                check_func(**kwargs)
                logger.info(f"CLUSTER CHECK {func_name} PASSED")
            except Exception as e:
                logger.error(f"CLUSTER CHECK {func_name} FAILED: {e}", exc_info=True)

        logger.info(
            "\n=================================================="
            "\n             FINISHED CLUSTER CHECKS              "
            "\n=================================================="
            "\n"
        )

    def teardown(self):
        """
        Stops background checks, collects output directory, and deletes created resources.

        """
        logger.info("--- Starting Test Teardown ---")
        self.stop_background_checks()

        # Collect output directory using standby pod (guaranteed to be running)
        if self.standby_pod:
            logger.info("Collecting output directory from shared CephFS mount...")
            try:
                collect_stress_job_output_directory(self.standby_pod)
            except Exception as e:
                logger.warning(f"Failed to collect output directory: {e}")
        else:
            logger.warning("No standby pod available for output collection")

        if self.verification_failures:
            logger.error(
                f"Test finished with {len(self.verification_failures)} background failures."
            )
            for f in self.verification_failures:
                logger.error(f"Failure: {f}")

        logger.info(f"Cleaning up {len(self.created_resources)} resources...")
        for resource in reversed(self.created_resources):
            try:
                resource.delete()
            except Exception as e:
                logger.warning(f"Failed to delete {resource.name}: {e}")


def check_ceph_health(stress_manager=None):
    """
    Checks the health of the Ceph cluster.

    Args:
        stress_manager: CephFSStressTestManager instance to check pause status

    Raises:
        Exception: If Ceph cluster is not healthy

    """
    # Check if verifications are paused (for in-progress checks)
    if stress_manager and hasattr(stress_manager, "checks_paused"):
        with stress_manager.verification_lock:
            if stress_manager.checks_paused:
                logger.info("Ceph health check skipped - verifications are paused")
                return True

    logger.info(
        "\n=================================================="
        "\n             VERIFICATION CHECK: Ceph health      "
        "\n=================================================="
        "\n"
    )
    ceph_health_check(namespace=config.ENV_DATA["cluster_namespace"])
    logger.info("\n Ceph cluster is healthy" "\n")


def verify_openshift_storage_ns_pods_in_running_state(stress_manager=None):
    """
    Verifies that all pods in the openshift-storage namespace are in a 'Running' state.
    Retries on CommandFailed, then raises PodsNotRunningError if verification fails after all retries.

    Args:
        stress_manager: CephFSStressTestManager instance to check pause status

    Raises:
        PodsNotRunningError: If not all pods are in the 'Running' state after all retries

    """
    # Check if verifications are paused (for in-progress checks)
    if stress_manager and hasattr(stress_manager, "checks_paused"):
        with stress_manager.verification_lock:
            if stress_manager.checks_paused:
                logger.info(
                    "Pods running state check skipped - verifications are paused"
                )
                return True

    @retry(CommandFailed, tries=3, delay=60, backoff=1)
    def _check_pods_running():
        # Re-check pause status during retries
        if stress_manager and hasattr(stress_manager, "checks_paused"):
            with stress_manager.verification_lock:
                if stress_manager.checks_paused:
                    logger.info(
                        "Pods running state check skipped during retry - verifications are paused"
                    )
                    return True
        logger.info(
            "\n===================================================="
            "\n VERIFICATION CHECK: Openshift-storage pods status  "
            "\n===================================================="
            "\n"
        )
        result = check_pods_in_running_state(
            namespace=config.ENV_DATA["cluster_namespace"]
        )
        if not result:
            raise CommandFailed(
                "Not all Pods in the openshift-storage are in Running state"
            )
        logger.info(
            "All the Pods in the openshift-storage namespace are in Running state"
        )
        return True

    try:
        return _check_pods_running()
    except CommandFailed as e:
        raise PodsNotRunningError(str(e))


@retry(CommandFailed, tries=3, delay=60, backoff=1)
def get_filtered_pods():
    """
    Gets a list of all pods running in the openshift-storage namespace, excluding specific patterns.

    Returns:
        list : list of filtered pod objects

    """
    list_of_all_pods = get_all_pods(namespace=config.ENV_DATA["cluster_namespace"])
    ignore_pods = [
        constants.ROOK_CEPH_OSD_PREPARE,
        constants.ROOK_CEPH_DRAIN_CANARY,
        "debug",
        constants.REPORT_STATUS_TO_PROVIDER_POD,
        constants.STATUS_REPORTER,
        "ceph-file-controller-detect-version",
    ]
    filtered_list_objs = [
        pod_obj
        for pod_obj in list_of_all_pods
        if not any(pod_name in pod_obj.name for pod_name in ignore_pods)
    ]
    # Clean up the full list to prevent memory accumulation
    del list_of_all_pods
    return filtered_list_objs


def verify_openshift_storage_ns_pods_health(stress_manager=None):
    """
    Validates that all the Pods in the openshift-storage namespace are healthy.
    Retries on CommandFailed, then raises PodStabilityError if verification fails after all retries.

    It checks for:
    1. Pods with OOMKilled containers (fails the test)
    2. Pods with restarts (informational only, logged as warning)

    Args:
        stress_manager: CephFSStressTestManager instance to check pause status

    Raises:
        PodStabilityError: If any pod is found to have OOMKilled containers after all retries

    """
    # Check if verifications are paused (for in-progress checks)
    if stress_manager and hasattr(stress_manager, "checks_paused"):
        with stress_manager.verification_lock:
            if stress_manager.checks_paused:
                logger.info("Pods health check skipped - verifications are paused")
                return True

    @retry(CommandFailed, tries=3, delay=60, backoff=1)
    def _check_pods_health():
        # Re-check pause status during retries
        if stress_manager and hasattr(stress_manager, "checks_paused"):
            with stress_manager.verification_lock:
                if stress_manager.checks_paused:
                    logger.info(
                        "Pods health check skipped during retry - verifications are paused"
                    )
                    return True

        logger.info(
            "\n===================================================="
            "\n VERIFICATION CHECK: Openshift-storage pods health  "
            "\n===================================================="
            "\n"
        )
        pod_objs = get_filtered_pods()
        pod_restarts = []
        oomkilled_pods = []

        for pod_obj in pod_objs:
            # Fetch pod data once and reuse to avoid redundant API calls
            pod_data = pod_obj.get()
            pod_name = pod_data.get("metadata", {}).get("name")

            container_statuses = pod_data.get("status", {}).get("containerStatuses", [])
            if not container_statuses:
                logger.warning(f"Pod {pod_name} has no containerStatuses")
                continue

            # Check restart counts for all containers
            total_restarts = 0
            container_restart_details = []
            for item in container_statuses:
                container_name = item.get("name")
                restart_count = item.get("restartCount", 0)
                if restart_count > 0:
                    total_restarts += restart_count
                    container_restart_details.append(
                        f"{container_name}:{restart_count}"
                    )

            if total_restarts > 0:
                container_details = ", ".join(container_restart_details)
                logger.info(
                    f"Pod {pod_name} has {total_restarts} total restart(s) "
                    f"across containers: {container_details}"
                )
                pod_restarts.append(
                    f"{pod_name} (restarts: {total_restarts}, "
                    f"details: {container_details})"
                )

            # Check for OOMKilled containers
            for item in container_statuses:
                container_name = item.get("name")
                if not validate_pod_oomkilled(
                    pod_name=pod_name, container=container_name
                ):
                    oomkilled_pods.append(
                        f"Pod: {pod_name}, Container: {container_name}"
                    )

        if pod_restarts:
            logger.warning(
                f"Found {len(pod_restarts)} pods with restarts: {pod_restarts}"
            )

        if oomkilled_pods:
            logger.error("Openshift-storage pods health check verification failed")
            logger.error(
                f"Found {len(oomkilled_pods)} OOMKilled containers: {oomkilled_pods}"
            )
            raise CommandFailed(
                "Openshift-storage pods health check verification failed due to OOMKilled containers"
            )

        # Explicitly clean up pod objects to prevent memory leaks
        del pod_objs
        del pod_restarts
        del oomkilled_pods
        gc.collect()

        logger.info("All pods in the openshift-storage namespace are healthy (no OOMs)")
        return True

    try:
        return _check_pods_health()
    except CommandFailed as e:
        raise PodStabilityError(str(e))


@retry(CommandFailed, tries=3, delay=60, backoff=1)
def check_prometheus_alerts(stress_manager):
    """
    Fetches alerts from the PrometheusAPI and logs alerts in a tabulated format

    Args:
        stress_manager: CephFSStressTestManager instance with prometheus_api

    """
    prometheus_alert_list = list()
    stress_manager.prometheus_api.prometheus_log(prometheus_alert_list)
    table = PrettyTable()
    table.field_names = ["Alert Name", "Description", "State"]
    table.align = "l"
    table.max_width["Description"] = 50
    alert_names_seen = set()
    for alert in prometheus_alert_list:
        alert_name = alert["labels"]["alertname"].strip()
        if alert_name in alert_names_seen:
            continue
        description = alert["annotations"]["description"]
        table.add_row([alert_name, description, alert["state"]])
        alert_names_seen.add(alert_name)
    logger.info(
        "\n=================================================="
        "\n         CLUSTER CHECK: prometheus alerts         "
        "\n=================================================="
        f"\n{table}"
        "\n"
    )

    # Clean up to prevent memory accumulation
    del prometheus_alert_list
    del alert_names_seen
    del table


@retry(CommandFailed, tries=3, delay=60, backoff=1)
def check_mds_pods_resource_utilization():
    """
    Gets the current resource utilization of MDS pods from 'adm top' command.

    """
    logger.info(
        "\n=================================================="
        "\n    CLUSTER CHECK: MDS Pods resource utilization  "
        "\n=================================================="
        f"\n{pod_resource_utilization_raw_output_from_adm_top(selector=constants.MDS_APP_LABEL)}"
        "\n"
    )


@retry(CommandFailed, tries=3, delay=60, backoff=1)
def get_mon_db_usage():
    """
    Retrieves the MON DB pod usage for all MON pods.

    """
    mon_db_usage = {}
    mon_pods = get_mon_pods()
    for mon_pod in mon_pods:
        mon_db_usage[f"{mon_pod.name}"] = f"{get_mon_db_size_in_kb(mon_pod)}KB"
    logger.info(
        "\n=================================================="
        "\n         CLUSTER CHECK: MON DB Usage              "
        "\n=================================================="
        f"\n Current Mon db usage: {mon_db_usage}"
        "\n"
    )


@retry(CommandFailed, tries=3, delay=60, backoff=1)
def get_nodes_resource_utilization():
    """
    Gets the node cpu and memory utilization in percentage using 'adm top' and 'oc describe'
    for both master and worker node types

    """
    logger.info(
        "\n=================================================="
        "\n   CLUSTER CHECK: NODES resources utilization     "
        "\n=================================================="
        "\n"
    )
    for node_type in ["master", "worker"]:
        get_node_resource_utilization_from_adm_top(
            node_type=node_type, print_table=True
        )
        get_node_resource_utilization_from_oc_describe(
            node_type=node_type, print_table=True
        )


@retry(CommandFailed, tries=3, delay=60, backoff=1)
def get_pods_resource_utilization():
    """
    Gets pod memory utilization using adm top command in raw output format.

    """
    out = pod_resource_utilization_raw_output_from_adm_top(
        namespace=config.ENV_DATA["cluster_namespace"]
    )
    logger.info(
        "\n==============================================================="
        "\n   CLUSTER CHECK: Openshift-storage PODS resources utilization "
        "\n==============================================================="
        f"\n {out}"
        "\n"
    )


@retry(CommandFailed, tries=3, delay=60, backoff=1)
def get_osd_disk_utilization():
    """
    Gets disk utilization for individual OSDs and the total used capacity in the cluster.

    """
    osd_filled_dict = get_osd_utilization()
    logger.info(f"OSD Utilization: {osd_filled_dict}")
    total_used_capacity = get_percent_used_capacity()
    logger.info(
        "\n=================================================="
        "\n   CLUSTER CHECK: OSD disk  utilization           "
        "\n=================================================="
        f"\n The percentage of the total used capacity in the cluster: {total_used_capacity}"
        "\n"
    )


@retry(CommandFailed, tries=3, delay=60, backoff=1)
def get_mount_subdirs(pod_obj):
    """
    Retrieves a list of subdirectories located at the root of the PVC mount of the given pod.

    Args:
        pod_obj (obj): pod object

    Returns:
        list: A list of directory names found at the mount path

    Raises:
        ValueError: If mount path cannot be determined
        CommandFailed: If command execution fails

    """
    try:
        mount_path = (
            pod_obj.get()
            .get("spec")
            .get("containers")[0]
            .get("volumeMounts")[0]
            .get("mountPath")
        )

        if not mount_path:
            raise ValueError("Mount path not found in pod specification")
        out = pod_obj.exec_sh_cmd_on_pod(command=f"ls {mount_path}")
        subdirs = out.split()
        return subdirs

    except (KeyError, IndexError) as e:
        raise ValueError(f"Invalid pod structure: {e}")


def run_stress_cleanup(pod_obj, top_dir, timeout=3600, parallelism_count=25):
    """
    Executes a parallelized deletion of a directory structure.

    This function utilizes 'find' combined with 'xargs -P' to spawn
    multiple deletion processes simultaneously.

    Args:
        pod_obj (obj): The app pod obj to execute commands on
        top_dir (str): The relative directory name to delete (e.g: 'cephfs-stress-job-xx')
        timeout (int, optional): Max time in seconds to wait for cleanup. Defaults to 3600
        parallelism_count (int, optional): Number of concurrent 'rm' processes to spawn
                                           inside the pod. Defaults to 25

    """
    try:
        mount_path = (
            pod_obj.get()
            .get("spec")
            .get("containers")[0]
            .get("volumeMounts")[0]
            .get("mountPath")
        )
        if not mount_path:
            raise ValueError("Mount path not found in pod specification")
        full_path = f"{mount_path}/{top_dir}"
        cmd = f'find {full_path} -name "thrd_*" -type d -prune -print0 | xargs -0 -n 1 -P {parallelism_count} rm -rf'
        logger.info(
            f"Starting parallelized cleanup of {full_path} "
            f"(timeout: {timeout}s, parallelism: {parallelism_count})"
        )
        pod_obj.exec_sh_cmd_on_pod(command=cmd, timeout=timeout)
        logger.info(f"Successfully deleted all files in {full_path}")

    except (KeyError, IndexError) as e:
        raise ValueError(f"Invalid pod structure: {e}")


def collect_stress_job_pod_logs(stress_job_obj, dir_name=None):
    """
    Collect stress job pod logs and store them in ocs-ci log directory.

    Args:
        stress_job_obj: Stress job object whose pod logs need to be collected
        dir_name (str): Optional subdirectory name. By default logs are stored in
            ocs-ci-logs-<run_id>/<test_name>/failed_stress_job_logs directory.
            When dir_name is provided, logs are stored in
            ocs-ci-logs-<run_id>/<test_name>/failed_stress_job_logs/<dir_name>

    """
    tmp_path = Path(ocsci_log_path())
    base_log_dir = os.path.join(
        tmp_path, get_current_test_name(), "failed_stress_job_logs"
    )
    destination_dir = f"{base_log_dir}/{dir_name}" if dir_name else base_log_dir
    if not os.path.isdir(destination_dir):
        Path(destination_dir).mkdir(parents=True, exist_ok=True)

    logger.info(
        f"Collecting logs from stress job {stress_job_obj.name} pods to {destination_dir}"
    )
    try:
        stress_job_pods = get_job_pods(
            job_name=stress_job_obj.name, namespace=stress_job_obj.namespace
        )
        if not stress_job_pods:
            logger.warning(f"No pods found for stress job {stress_job_obj.name}")
            return
        for stress_job_pod in stress_job_pods:
            pod_name = stress_job_pod.get("metadata", {}).get("name")
            if not pod_name:
                logger.warning("Pod name not found in stress job pod metadata")
                continue
            try:
                logger.info(f"Collecting logs from pod {pod_name}")
                logs = pod.get_pod_logs(
                    pod_name=pod_name,
                    namespace=stress_job_obj.namespace,
                    all_containers=True,
                )
                log_file_path = os.path.join(destination_dir, f"{pod_name}.log")
                with open(log_file_path, "w") as log_file:
                    log_file.write(logs if logs else "No logs available")

                logger.info(f"Logs saved to {log_file_path}")
            except Exception as e:
                logger.error(f"Failed to collect logs from pod {pod_name}: {e}")
    except Exception as e:
        logger.error(f"Failed to collect stress job pod logs: {e}")


def collect_stress_job_output_directory(standby_pod_obj, dir_name=None):
    """
    Collect entire output directory from shared CephFS mount and store in ocs-ci log directory.
    This collects all files including monitoring logs, hang markers, and test artifacts.

    Uses the standby pod (which is always running) to access the shared CephFS mount,
    avoiding issues with completed job pods that can't execute commands.

    Args:
        standby_pod_obj: Standby pod object that mounts the shared PVC (must be running)
        dir_name (str): Optional subdirectory name. By default files are stored in
            ocs-ci-logs-<run_id>/<test_name>/stress_output directory.
            When dir_name is provided, files are stored in
            ocs-ci-logs-<run_id>/<test_name>/stress_output/<dir_name>

    """
    tmp_path = Path(ocsci_log_path())
    base_output_dir = os.path.join(tmp_path, get_current_test_name(), "stress_output")
    destination_dir = f"{base_output_dir}/{dir_name}" if dir_name else base_output_dir
    if not os.path.isdir(destination_dir):
        Path(destination_dir).mkdir(parents=True, exist_ok=True)

    logger.info(
        f"Collecting output directory from shared CephFS mount to {destination_dir}"
    )

    try:
        logger.info(
            f"Using standby pod {standby_pod_obj.name} for collection (always running)"
        )
        pod_obj = standby_pod_obj

        try:
            logger.info(f"Collecting shared output directory from pod {pod_obj.name}")

            output_dir = os.environ.get("OUTPUT_DIR", "/mnt/output")

            check_cmd = f"test -d {output_dir} && echo 'EXISTS' || echo 'NOT_EXISTS'"
            result = pod_obj.exec_sh_cmd_on_pod(command=check_cmd, timeout=30)

            if "NOT_EXISTS" in result:
                logger.warning(
                    f"Output directory {output_dir} not found in pod {pod_obj.name}"
                )
                return

            # First, count total files
            logger.info(f"Counting files in {output_dir}")
            count_cmd = f"find {output_dir} -type f 2>/dev/null | wc -l"
            file_count_result = pod_obj.exec_sh_cmd_on_pod(
                command=count_cmd, timeout=120
            )
            total_files = (
                int(file_count_result.strip())
                if file_count_result.strip().isdigit()
                else 0
            )
            logger.info(f"Total files found: {total_files}")

            logger.info(f"Collecting directory structure from {output_dir}")
            tree_cmd = f"find {output_dir} -type f 2>/dev/null"
            file_list = pod_obj.exec_sh_cmd_on_pod(command=tree_cmd, timeout=300)

            if file_list and file_list.strip():
                files = file_list.strip().split("\n")
                actual_files = len(files)
                logger.info(f"Collecting {actual_files} files from shared mount")

                if actual_files != total_files:
                    logger.warning(
                        f"File count mismatch: expected {total_files}, got {actual_files}. "
                        "Some files may have been created/deleted during collection."
                    )

                collected_count = 0
                failed_count = 0

                for file_path in files:
                    if not file_path or not file_path.startswith(output_dir):
                        continue

                    try:
                        # Get relative path
                        rel_path = file_path.replace(f"{output_dir}/", "")
                        local_file_path = os.path.join(destination_dir, rel_path)

                        # Create parent directories if needed
                        local_file_dir = os.path.dirname(local_file_path)
                        if not os.path.isdir(local_file_dir):
                            Path(local_file_dir).mkdir(parents=True, exist_ok=True)

                        # Copy file content
                        cat_cmd = f"cat {file_path}"
                        file_content = pod_obj.exec_sh_cmd_on_pod(
                            command=cat_cmd, timeout=60
                        )

                        with open(local_file_path, "w") as f:
                            f.write(file_content if file_content else "")

                        logger.debug(f"Collected: {rel_path}")
                        collected_count += 1

                    except Exception as e:
                        logger.warning(f"Failed to collect file {file_path}: {e}")
                        failed_count += 1

                logger.info(
                    f"Collection complete: {collected_count} files collected, "
                    f"{failed_count} files failed out of {actual_files} total files. "
                    f"Output saved to {destination_dir}"
                )
            else:
                logger.info("No files found in output directory")

        except Exception as e:
            logger.error(
                f"Failed to collect output directory from pod {pod_obj.name}: {e}"
            )

    except Exception as e:
        logger.error(f"Failed to collect stress job output directory: {e}")


def check_for_filesystem_hangs(namespace, output_dir="/mnt/output"):
    """
    Check for filesystem hang markers created by the monitoring script.

    This function checks all pods in the given namespace for hang marker files
    that indicate the filesystem monitoring detected a genuine hang.

    Args:
        namespace (str): Namespace to check for hang markers
        output_dir (str): Output directory path where hang markers are stored

    Returns:
        tuple: (hang_detected: bool, hang_details: list of dicts)

    Raises:
        Exception: If hang markers are found (genuine filesystem hang detected)

    """
    logger.info("Checking for filesystem hang markers...")
    hang_markers_found = []

    try:
        all_pods = pod_module.get_all_pods(namespace=namespace)

        for pod_obj in all_pods:
            pod_name = pod_obj.name

            if not any(x in pod_name for x in ["cephfs-stress", "stress-pod"]):
                continue

            try:
                logger.info(
                    f"Checking if hang_markers directory exists and has files in pod {pod_name}"
                )
                hang_marker_dir = f"{output_dir}/{pod_name}/hang_markers"
                check_cmd = f"ls -la {hang_marker_dir} 2>/dev/null || echo 'NO_MARKERS'"
                result = pod_obj.exec_sh_cmd_on_pod(command=check_cmd, timeout=30)

                if "NO_MARKERS" not in result and "HANG_DETECTED" in result:
                    logger.warning(f"Hang markers found in pod {pod_name}")

                    logger.info("Getting the marker file contents")
                    list_cmd = f"find {hang_marker_dir} -name 'HANG_DETECTED_*.json' 2>/dev/null"
                    marker_files = pod_obj.exec_sh_cmd_on_pod(
                        command=list_cmd, timeout=30
                    )

                    for marker_file in marker_files.strip().split("\n"):
                        if marker_file:
                            try:
                                cat_cmd = f"cat {marker_file}"
                                marker_content = pod_obj.exec_sh_cmd_on_pod(
                                    command=cat_cmd, timeout=30
                                )

                                hang_info = json.loads(marker_content)
                                hang_info["pod_name"] = pod_name
                                hang_markers_found.append(hang_info)

                                logger.error(
                                    f"Filesystem hang detected in pod {pod_name}:\n"
                                    f"  Monitor Type: {hang_info.get('monitor_type')}\n"
                                    f"  Command: {hang_info.get('command')}\n"
                                    f"  Timestamp: {hang_info.get('timestamp')}\n"
                                    f"  Details: {hang_info.get('details')}"
                                )

                            except Exception as e:
                                logger.error(
                                    f"Failed to parse hang marker {marker_file}: {e}"
                                )
                                raise CommandFailed(
                                    f"Failed to parse hang marker file {marker_file}. "
                                    f"This may indicate a real hang or transient file issue: {e}"
                                )

            except Exception as e:
                logger.debug(f"Could not check pod {pod_name} for hang markers: {e}")
                continue

    except Exception as e:
        logger.error(f"Error checking for filesystem hangs: {e}")
        raise CommandFailed(f"Failed to inspect pods for filesystem hangs: {e}")

    if hang_markers_found:
        logger.critical(
            f"FILESYSTEM HANG DETECTED: {len(hang_markers_found)} hang marker(s) found\n"
        )
        return True, hang_markers_found
    else:
        logger.info("No filesystem hang markers found")
        return False, []


def collect_monitoring_logs(stress_job_obj, dir_name=None):
    """
    Collect filesystem monitoring logs from stress job pods.

    Args:
        stress_job_obj: Stress job object whose monitoring logs need to be collected
        dir_name (str): Optional subdirectory name for organizing logs

    """
    tmp_path = Path(ocsci_log_path())
    base_log_dir = os.path.join(tmp_path, get_current_test_name(), "monitoring_logs")
    destination_dir = f"{base_log_dir}/{dir_name}" if dir_name else base_log_dir

    if not os.path.isdir(destination_dir):
        Path(destination_dir).mkdir(parents=True, exist_ok=True)

    logger.info(
        f"Collecting monitoring logs from stress job {stress_job_obj.name} pods to {destination_dir}"
    )

    try:
        stress_job_pods = get_job_pods(
            job_name=stress_job_obj.name, namespace=stress_job_obj.namespace
        )

        if not stress_job_pods:
            logger.warning(f"No pods found for stress job {stress_job_obj.name}")
            return

        for stress_job_pod in stress_job_pods:
            pod_name = stress_job_pod.get("metadata", {}).get("name")
            if not pod_name:
                continue

            try:
                logger.info(f"Collecting monitoring logs from pod {pod_name}")
                pod_obj = pod_module.get_pod_obj(
                    name=pod_name, namespace=stress_job_obj.namespace
                )
                output_dir = os.environ.get("OUTPUT_DIR", "/mnt/output")
                monitoring_log_dir = f"{output_dir}/{pod_name}/monitoring_logs"

                list_cmd = (
                    f"ls {monitoring_log_dir}/*.log 2>/dev/null || echo 'NO_LOGS'"
                )
                result = pod_obj.exec_sh_cmd_on_pod(command=list_cmd, timeout=30)

                if "NO_LOGS" not in result:
                    log_files = result.strip().split("\n")

                    for log_file in log_files:
                        if log_file and log_file.endswith(".log"):
                            try:
                                cat_cmd = f"cat {log_file}"
                                log_content = pod_obj.exec_sh_cmd_on_pod(
                                    command=cat_cmd, timeout=60
                                )
                                log_filename = os.path.basename(log_file)
                                local_log_path = os.path.join(
                                    destination_dir, f"{pod_name}_{log_filename}"
                                )

                                with open(local_log_path, "w") as f:
                                    f.write(log_content)

                                logger.info(f"Saved monitoring log to {local_log_path}")

                            except Exception as e:
                                logger.warning(f"Failed to collect log {log_file}: {e}")
                else:
                    logger.info(f"No monitoring logs found in pod {pod_name}")

            except Exception as e:
                logger.error(
                    f"Failed to collect monitoring logs from pod {pod_name}: {e}"
                )

    except Exception as e:
        logger.error(f"Failed to collect monitoring logs: {e}")


@retry(CommandFailed, tries=3, delay=60, backoff=1)
def verify_no_filesystem_hangs(stress_manager=None):
    """
    Verification function to check for filesystem hangs detected by monitoring.

    Args:
        stress_manager: CephFSStressTestManager instance to check pause status and get namespace

    Returns:
        bool: True if no hangs detected, raises exception if hangs found

    Raises:
        Exception: If filesystem hangs are detected

    """
    if stress_manager and hasattr(stress_manager, "checks_paused"):
        with stress_manager.verification_lock:
            if stress_manager.checks_paused:
                logger.info("Filesystem hang check skipped - verifications are paused")
                return True
    logger.info(
        "\n===================================================="
        "\n VERIFICATION CHECK: Filesystem Hang Detection     "
        "\n===================================================="
        "\n"
    )

    # Get namespace from stress_manager if available, otherwise use default
    namespace = (
        stress_manager.namespace
        if stress_manager and hasattr(stress_manager, "namespace")
        else config.ENV_DATA["cluster_namespace"]
    )
    hang_detected, hang_details = check_for_filesystem_hangs(namespace)
    if hang_detected:
        error_msg = (
            f"Filesystem hang detected! {len(hang_details)} hang marker(s) found.\n"
            "Hang details:\n"
        )
        for hang in hang_details:
            error_msg += (
                f"  - Pod: {hang.get('pod_name')}\n"
                f"    Monitor: {hang.get('monitor_type')}\n"
                f"    Command: {hang.get('command')}\n"
                f"    Time: {hang.get('timestamp')}\n"
                f"    Details: {hang.get('details')}\n"
            )
        raise Exception(error_msg)
    logger.info("No filesystem hangs detected")
    return True


def create_cephfs_subvolume_workload(
    teardown_project_factory=None,
    project_name="cephfs-subvolume-metrics-test",
    pvc_size="5Gi",
    fio_size="5GB",
    fio_rate="100m",
    fio_runtime=360,
):
    """
    Create a CephFS subvolume workload: namespace, PVC, pod, and running FIO.

    Provisions a CephFS subvolume by creating a namespace and RWX PVC, then
    starts FIO on a pod so that Prometheus scrapes non-zero subvolume metrics.

    Args:
        teardown_project_factory (callable, optional): Pytest fixture that
            registers the project for deletion at test teardown. Pass the
            ``teardown_project_factory`` fixture from the test function.
        project_name (str): Name of the namespace/project to create.
        pvc_size (str): PVC capacity (e.g. '5Gi').
        fio_size (str): Total IO size for the FIO workload (e.g. '5GB').
        fio_rate (str): FIO target rate (e.g. '100m' for 100 MB/s). A higher
            rate makes the subvolume appear in the top-10 subvolume list.
        fio_runtime (int): FIO ``--runtime`` in seconds. Must be long enough
            to still be running when the UI is checked; default 360 s (6 min)
            covers the typical setup + 2-min Prometheus wait + UI assertions.

    Returns:
        tuple: (project_obj, pvc_obj, pod_obj)
    """
    project_obj = create_project(project_name=project_name)
    if teardown_project_factory:
        teardown_project_factory(project_obj)

    pvc_obj = create_pvc(
        sc_name=constants.CEPHFILESYSTEM_SC,
        namespace=project_obj.namespace,
        size=pvc_size,
        access_mode=constants.ACCESS_MODE_RWX,
    )

    pod_obj = create_pod(
        pvc_name=pvc_obj.name,
        namespace=project_obj.namespace,
        interface_type=constants.CEPHFILESYSTEM,
    )
    wait_for_resource_state(pod_obj, state=STATUS_RUNNING, timeout=300)
    pod_obj.run_io(
        storage_type=constants.WORKLOAD_STORAGE_TYPE_FS,
        size=fio_size,
        rate=fio_rate,
        runtime=fio_runtime,
    )

    return project_obj, pvc_obj, pod_obj


def create_cephfs_subvolume_workloads(
    count=3,
    teardown_project_factory=None,
    project_name_prefix="cephfs-subvolume-metrics-test",
    pvc_size="5Gi",
    fio_size="5GB",
    fio_rate="100m",
    fio_runtime=360,
):
    """
    Create multiple CephFS subvolume workloads by calling
    :func:`create_cephfs_subvolume_workload` ``count`` times.

    Each workload gets its own namespace named
    ``<project_name_prefix>-<index>`` (e.g.
    ``cephfs-subvolume-metrics-test-0``).

    Args:
        count (int): Number of workloads (subvolumes) to create.
        teardown_project_factory (callable, optional): Pytest fixture that
            registers each project for deletion at test teardown.
        project_name_prefix (str): Common prefix for namespace names.
        pvc_size (str): PVC capacity for each workload (e.g. '5Gi').
        fio_size (str): Total IO size per FIO workload (e.g. '5GB').
        fio_rate (str): FIO target rate per workload (e.g. '100m').
        fio_runtime (int): FIO ``--runtime`` in seconds (default 360).

    Returns:
        list[tuple]: One ``(project_obj, pvc_obj, pod_obj)`` tuple per
            workload, in creation order.
    """
    workloads = []
    for i in range(count):
        project_name = f"{project_name_prefix}-{i}"
        workloads.append(
            create_cephfs_subvolume_workload(
                teardown_project_factory=teardown_project_factory,
                project_name=project_name,
                pvc_size=pvc_size,
                fio_size=fio_size,
                fio_rate=fio_rate,
                fio_runtime=fio_runtime,
            )
        )
    return workloads
