import logging
import threading

from prettytable import PrettyTable

from ocs_ci.framework import config
from ocs_ci.helpers import helpers
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


logger = logging.getLogger(__name__)


stop_event = threading.Event()
verification_failures = []
verification_lock = threading.Lock()


def create_cephfs_stress_project(project_name):
    """
    Create a new CephFS stress test project

    Args:
       project_name (str): Project name to be created

    Returns:
        ocs_ci.ocs.ocp.OCP: Project object

    """
    proj_obj = helpers.create_project(project_name=project_name)
    return proj_obj


def create_cephfs_stress_pod(
    namespace,
    pvc_name,
    base_dir=None,
    files_size=None,
    operations=None,
    base_file_count=None,
    multiplication_factor=None,
    threads=None,
):
    """
    Creates a CephFS stress pod, utilizing smallfiles to generate numerous files and directories.
    The pod is configured with various parameters to stress CephFS, it
    gradually increases load on CephFS in incremental stages.

    Args:
        base_dir (str, optional): Directory used by smallfile to perform file and directory operations
        files_size (str, optional): Size of each file in KB
        operations (str, optional): File operations to perform (e.g., append, stat, chmod, ls-l, etc),
        Pass as a comma-separated string
        base_file_count (str, optional): Base file count, to multiply with scaling factor
        multiplication_factor (str, optional): Dynamic scaling of file creation
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
        "MULTIPLICATION_FACTOR": multiplication_factor,
        "THREADS": threads,
    }
    cephfs_stress_pod_data = templating.load_yaml(CEPHFS_STRESS_POD_YAML)
    cephfs_stress_pod_data["metadata"]["namespace"] = namespace
    cephfs_stress_pod_data["spec"]["volumes"][0]["persistentVolumeClaim"][
        "claimName"
    ] = pvc_name
    logger.info("Set environment variables in the pod template")
    set_env_vars(cephfs_stress_pod_data, env_vars, pod_type=constants.POD)
    cephfs_stress_pod_obj = pod.Pod(**cephfs_stress_pod_data)
    logger.info("Creating Cephfs stress pod")
    created_resource = cephfs_stress_pod_obj.create()
    assert created_resource, f"Failed to create Pod {cephfs_stress_pod_obj.name}"

    logger.info("Waiting for Cephfs stress pod to start")
    helpers.wait_for_resource_state(
        cephfs_stress_pod_obj, state=STATUS_RUNNING, timeout=300
    )

    return cephfs_stress_pod_obj


def set_env_vars(pod_data, env_vars, type):
    """
    Updates the pod's environment variables in the container spec based on the provided mapping

    Args:
        pod_data (dict): The pod specification loaded from YAML.
        env_vars (dict): Dictionary mapping env variable names to their desired values.
        type (str): pod type, either a regular pod or a job pod

    """
    if type == constants.POD:
        container_env = pod_data["spec"]["containers"][0].get("env", [])
    elif type == constants.JOB:
        container_env = pod_data["spec"]["template"]["spec"]["containers"][0].get(
            "env", []
        )
    else:
        raise ValueError(f"Unsupported pod_type: '{type}'. Expected POD or JOB.")
    for env in container_env:
        name = env.get("name")
        if name in env_vars:
            value = env_vars[name]
            if value is not None:
                env["value"] = str(value)


def create_cephfs_stress_job(
    namespace,
    pvc_name,
    base_dir=None,
    files_size=None,
    operations=None,
    base_file_count=None,
    multiplication_factor=None,
    threads=None,
    parallelism=None,
):
    """
    Creates a CephFS stress Job. This job launches concurrent pods based on the configured
    parallelism count, where each pod executes generate numerous small files and directories.
    Configured with specific parameters, the workload stresses CephFS by gradually increasing
    the load in incremental stages.

    Args:
        base_dir (str, optional): Directory used by smallfile to perform file and directory operations
        files_size (str, optional): Size of each file in KB
        operations (str, optional): File operations to perform (e.g., append, stat, chmod, ls-l, etc),
        Pass as a comma-separated string
        base_file_count (str, optional): Base file count, to multiply with scaling factor
        multiplication_factor (str, optional): Dynamic scaling of file creation
          - base_file_count * MULTIPLICATION_FACTORS
        threads (str, optional): Number of threads to use for the operation.
        parallelism (str, optional): Specifies how many pod replicas running in parallel should execute a job.

    Returns:
        cephfs_stress_job_obj(OCS): The created Job object after it's in a running state

    Raises:
        AssertionError: If the pod creation fails

    """
    env_vars = {
        "BASE_DIR": base_dir,
        "FILES_SIZE": files_size,
        "OPERATIONS": operations,
        "BASE_FILE_COUNT": base_file_count,
        "MULTIPLICATION_FACTOR": multiplication_factor,
        "THREADS": threads,
    }
    cephfs_stress_job_data = templating.load_yaml(CEPHFS_STRESS_JOB_YAML)
    cephfs_stress_job_data["metadata"]["namespace"] = namespace
    cephfs_stress_job_data["spec"]["template"]["spec"]["volumes"][0][
        "persistentVolumeClaim"
    ]["claimName"] = pvc_name
    if parallelism:
        cephfs_stress_job_data["spec"]["parallelism"] = parallelism
    logger.info("Set environment variables in the pod template")
    set_env_vars(cephfs_stress_job_data, env_vars, pod_type=constants.JOB)
    job_name = cephfs_stress_job_data["metadata"]["name"]
    job_ocs_obj = OCS(**cephfs_stress_job_data)
    created_resource = job_ocs_obj.create()
    assert created_resource, f"Failed to create Job {job_ocs_obj.name}"

    logger.info(f"Waiting for Job {job_ocs_obj.name} to start")
    job_ocp_obj = ocp.OCP(
        kind=constants.JOB, namespace=namespace, resource_name=job_name
    )
    job_ocp_dict = job_ocp_obj.get(resource_name=job_ocp_obj.resource_name)
    cephfs_stress_job_obj = OCS(**job_ocp_dict)

    helpers.wait_for_resource_state(
        cephfs_stress_job_obj, state=STATUS_RUNNING, timeout=300
    )

    return cephfs_stress_job_obj


def check_prometheus_alerts(threading_lock=None):
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


def check_mds_pods_resource_utilization():
    """
    Get's the current resource utilization of MDS pods by fetching raw output from 'adm top' command

    """
    logger.info(
        "\n=================================================="
        "\n    CLUSTER CHECK: MDS Pods resource utilization  "
        "\n=================================================="
        f"\n{pod_resource_utilization_raw_output_from_adm_top(selector=constants.MDS_APP_LABEL)}"
        "\n"
    )


def get_mon_db_usage():
    """
    Retrieves the MON DB pod usage

    """
    mon_db_usage = {}
    # Check mon db usage
    mon_pods = get_mon_pods()
    for mon_pod in mon_pods:
        # Get mon db size
        mon_db_usage[f"{mon_pod.name}"] = f"{get_mon_db_size_in_kb(mon_pod)}KB"
    logger.info(
        "\n=================================================="
        "\n         CLUSTER CHECK: MON DB Usage              "
        "\n=================================================="
        f"\n Current Mon db usage: {mon_db_usage}"
        "\n"
    )


def get_nodes_resource_utilization():
    """
    Gets the node's cpu and memory utilization in percentage using 'adm top' and 'oc describe'
    for both master and worker node types

    """
    logger.info(
        "\n=================================================="
        "\n   CLUSTER CHECK: NODES resources utilization     "
        "\n=================================================="
        "\n"
    )
    # Get the cpu and memory of each nodes from adm top
    get_node_resource_utilization_from_adm_top(node_type="master", print_table=True)
    get_node_resource_utilization_from_adm_top(node_type="worker", print_table=True)
    # Get the cpu and memory from describe of nodes
    get_node_resource_utilization_from_oc_describe(node_type="master", print_table=True)
    get_node_resource_utilization_from_oc_describe(node_type="worker", print_table=True)


def get_pods_resource_utilization():
    """
    Get's the pod's memory utilization using adm top command in a raw output

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


def get_osd_disk_utilization():
    """
    Get's the disk utilization for individual OSDs and the total used capacity in the cluster

    """
    # Get OSD utilization
    osd_filled_dict = get_osd_utilization()
    logger.info(f"OSD Utilization: {osd_filled_dict}")
    # Get the percentage of the total used capacity in the cluster
    total_used_capacity = get_percent_used_capacity()
    logger.info(
        "\n=================================================="
        "\n   CLUSTER CHECK: OSD disk  utilization           "
        "\n=================================================="
        f"\n The percentage of the total used capacity in the cluster: {total_used_capacity}"
        "\n"
    )


def run_cluster_checks(threading_lock=None):
    """
    Runs stress specific cluster checks

    """
    logger.info(
        "\n=================================================="
        "\n             STARTING CLUSTER CHECKS              "
        "\n=================================================="
        "\n"
    )

    checks_to_run = [
        (check_prometheus_alerts, {"threading_lock": threading_lock}),
        (check_mds_pods_resource_utilization, {}),
        (get_mon_db_usage, {}),
        (get_nodes_resource_utilization, {}),
        (get_pods_resource_utilization, {}),
        (get_osd_disk_utilization, {}),
    ]
    for check_func, kwargs in checks_to_run:
        func_name = check_func.__name__
        logger.debug(f"Running check: {func_name}")

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


def check_ceph_health():
    """
    This function checks the health of the Ceph cluster

    """
    logger.info(
        "\n=================================================="
        "\n             VERIFICATION CHECK: Ceph health      "
        "\n=================================================="
        "\n"
    )
    ceph_health_check(namespace=config.ENV_DATA["cluster_namespace"])
    logger.info("\n Ceph cluster is healthy" "\n")


def verify_openshift_storage_ns_pods_in_running_state():
    """
    Verifies that all pods in the openshift-storage namespace are in a 'Running' state.

    Raises:
        AssertionError: If not all pods are in the 'Running' state

    """
    logger.info(
        "\n===================================================="
        "\n VERIFICATION CHECK: Openshift-storage pods status  "
        "\n===================================================="
        "\n"
    )
    out = check_pods_in_running_state(namespace=config.ENV_DATA["cluster_namespace"])
    assert out, "Not all Pods in the openshift-storage are in Running state"
    logger.info("All the Pods in the openshift-storage namespace are in Running state")


def get_filtered_pods():
    """
    Get's a list of all pods running in the openshift-storage namespace, ignoring few set of pods

    Returns:
        list (pods_obj): list of all filtered pods object

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
        assert False, "Openshift-storage pods health check verification failed"
    else:
        logger.info(
            "All pods in the openshift-storage namespace are healthy (no restarts or OOMs)"
        )


def run_verification_checks():
    """
    Function to run verification checks

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
        "\n             STARTING VERIFICATION CHECKS         "
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
                raise AssertionError(f"Verification failed: {func_name} returned False")
            logger.info(f"VERIFICATION {func_name} PASSED")

    except AssertionError as ae:
        # catch verification assert
        logger.error(f"VERIFICATION {func_name} FAILED: {ae}")
        with verification_lock:
            verification_failures.append(str(ae))
        logger.info("Signaling the main thread and this thread to stop")
        stop_event.set()

    except Exception as e:
        # catch any other verification check script crash
        logger.error(f"Verification check FAILED: {e}", exc_info=True)
        with verification_lock:
            verification_failures.append(f"Verification script {func_name} failed: {e}")
        stop_event.set()

    logger.info(
        "\n=================================================="
        "\n           FINISHED VERIFICATION CHECKS           "
        "\n=================================================="
        "\n"
    )


def continuous_checks_runner(interval_minutes, threading_lock=None):
    """
    This function runs in a background thread, continuously checking for a 'stop_event'.

    It loops until the 'stop_event' is set. It sleeps for the specified 'interval_minutes'
    in an interruptible way. If the 'stop_event' is set by another thread, the sleep
    is interrupted and the function exits.

    If the sleep times out normally, it proceeds to run the background
    and validation check functions

    Args:
        interval_minutes (int): The interval in minutes between check executions

    """
    interval_seconds = interval_minutes * 60
    logger.info(
        f"Check Runner thread started. Will run checks every {interval_minutes} min."
    )

    while True:
        logger.info(f"Running periodic checks loop (Interval: {interval_minutes} min)")
        run_cluster_checks(threading_lock=threading_lock)
        run_verification_checks()
        logger.info(
            f"Pausing for {interval_minutes} mins before the subsequent round of periodic cluster and"
            "verification checks."
        )

        if stop_event.wait(timeout=interval_seconds):
            break

    logger.info("Check Runner thread: Stop signal received, exiting")
