"""
CephFS Stress Test Helper Module

This module provides comprehensive utilities for managing CephFS stress tests,
including pod/job creation, background health monitoring, cluster verification
and resource cleanup.

"""

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
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.resources import pod
from ocs_ci.helpers.helpers import (
    validate_pod_oomkilled,
    validate_pods_are_running_and_not_restarted,
    get_mon_db_size_in_kb,
    create_pod,
    create_pvc,
    wait_for_resource_state,
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
        check_prometheus_alerts

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
        )
        self.created_resources.append(standby_pod_obj)

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
        wait_for_resource_state(
            cephfs_stress_pod_obj, state=STATUS_RUNNING, timeout=300
        )

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

        wait_for_resource_state(
            cephfs_stress_job_obj, state=STATUS_RUNNING, timeout=300
        )

        return cephfs_stress_job_obj

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

    def stop_background_checks(self):
        """
        Signals the background thread ('StressWatchdog') to stop and waits for it to join.

        """
        if self.background_checks_thread and self.background_checks_thread.is_alive():
            logger.info(
                "Signaling Background checks ('StressWatchdog') thread to stop..."
            )
            self.stop_event.set()
            self.background_checks_thread.join()
            if self.background_checks_thread.is_alive():
                logger.warning(
                    "Background checks ('StressWatchdog') thread did not exit cleanly!"
                )
            else:
                logger.info("Background checks ('StressWatchdog') thread stopped.")

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
            try:
                self._run_cluster_health_checks()
                self._run_strict_verifications()
            except Exception as e:
                logger.error(
                    f"Unexpected error in background checks loop: {e}", exc_info=True
                )
            logger.info(
                f"Pausing for {interval_minutes} minutes before the next round "
                "of periodic cluster and verification checks"
            )
            if self.stop_event.wait(timeout=interval_minutes):
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
        logger.info(
            "\n=================================================="
            "\n      STARTING STRICT VERIFICATION CHECKS         "
            "\n=================================================="
            "\n"
        )

        verifications_to_run = [
            check_ceph_health,
            verify_openshift_storage_ns_pods_in_running_state,
            verify_openshift_storage_ns_pods_health,
        ]
        try:
            for verification_func in verifications_to_run:
                func_name = verification_func.__name__
                logger.debug(f"Running verification: {func_name}")
                result = verification_func()
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
            (check_prometheus_alerts, {"threading_lock": self.verification_lock}),
            (check_mds_pods_resource_utilization, {}),
            (get_mon_db_usage, {}),
            (get_nodes_resource_utilization, {}),
            (get_pods_resource_utilization, {}),
            (get_osd_disk_utilization, {}),
        ]
        for check_func, kwargs in checks_to_run:
            func_name = check_func.__name__
            logger.debug(f"Running cluster check: {func_name}")

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
        Stops background checks and deletes created resources.

        """
        logger.info("--- Starting Test Teardown ---")
        self.stop_background_checks()
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


def check_ceph_health():
    """
    Checks the health of the Ceph cluster.

    Raises:
        Exception: If Ceph cluster is not healthy

    """
    logger.info(
        "\n=================================================="
        "\n             VERIFICATION CHECK: Ceph health      "
        "\n=================================================="
        "\n"
    )
    ceph_health_check(namespace=config.ENV_DATA["cluster_namespace"])
    logger.info("\n Ceph cluster is healthy" "\n")


@retry(CommandFailed, tries=3, delay=60, backoff=1)
def verify_openshift_storage_ns_pods_in_running_state():
    """
    Verifies that all pods in the openshift-storage namespace are in a 'Running' state.

    Raises:
        PodsNotRunningError: If not all pods are in the 'Running' state

    """
    logger.info(
        "\n===================================================="
        "\n VERIFICATION CHECK: Openshift-storage pods status  "
        "\n===================================================="
        "\n"
    )
    result = check_pods_in_running_state(namespace=config.ENV_DATA["cluster_namespace"])
    if not result:
        raise PodsNotRunningError(
            "Not all Pods in the openshift-storage are in Running state"
        )
    logger.info("All the Pods in the openshift-storage namespace are in Running state")


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
    return filtered_list_objs


@retry(CommandFailed, tries=3, delay=60, backoff=1)
def verify_openshift_storage_ns_pods_health():
    """
    Validates that all the Pods in the openshift-storage namespace are healthy

    It checks for two conditions:
    1. Pods have not been restarted
    2. Pods have not been OOMKilled

    Raises:
        AssertionError: If any pod is found to have restarts or OOMKills.

    """
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
        pod_name = pod_obj.get().get("metadata").get("name")
        if not validate_pods_are_running_and_not_restarted(
            pod_name=pod_name,
            pod_restart_count=0,
            namespace=config.ENV_DATA["cluster_namespace"],
        ):
            pod_restarts.append(pod_name)

        for item in pod_obj.get().get("status").get("containerStatuses"):
            container_name = item.get("name")
            if not validate_pod_oomkilled(pod_name=pod_name, container=container_name):
                oomkilled_pods.append(f"Pod: {pod_name}, Container: {container_name}")

    if pod_restarts or oomkilled_pods:
        logger.error("Openshift-storage pods health check verification failed")
        if pod_restarts:
            logger.error(f"Found {len(pod_restarts)} restarted pods: {pod_restarts}")
        if oomkilled_pods:
            logger.error(
                f"Found {len(oomkilled_pods)} OOMKilled containers: {oomkilled_pods}"
            )
        raise PodStabilityError(
            "Openshift-storage pods health check verification failed"
        )

    logger.info(
        "All pods in the openshift-storage namespace are healthy (no restarts or OOMs)"
    )


@retry(CommandFailed, tries=3, delay=60, backoff=1)
def check_prometheus_alerts(threading_lock):
    """
    Fetches alerts from the PrometheusAPI and logs alerts in a tabulated format

    Args:
        threading_lock ([threading.Lock]): A threading lock for synchronization

    """
    prometheus_alert_list = list()
    prometheus_api = PrometheusAPI(threading_lock=threading_lock)
    prometheus_api.prometheus_log(prometheus_alert_list)
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
