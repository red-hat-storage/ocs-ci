from logging import getLogger
from time import sleep

from ocs_ci.framework import config
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.resources.pod import (
    delete_osd_removal_job,
    get_pods_having_label,
    get_ceph_tools_pod,
    run_osd_removal_job,
    verify_osd_removal_job_completed_successfully,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.constants import (
    DEFAULT_CEPHBLOCKPOOL,
    DEFAULT_STORAGE_CLUSTER,
    OSD_APP_LABEL,
    CEPHBLOCKPOOL,
    STORAGECLASS,
    DEPLOYMENT,
    STORAGECLUSTER,
    STATUS_READY,
    REPLICA1_STORAGECLASS,
)
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.helpers.helpers import wait_for_osds_down
from ocs_ci.utility.utils import TimeoutSampler


log = getLogger(__name__)

_FAILURE_DOMAINS = None


def get_failure_domains() -> list[str]:
    """
    Gets Cluster Failure Domains

    Returns:
        list: Failure Domains names
    """
    global _FAILURE_DOMAINS
    if _FAILURE_DOMAINS is None:
        try:
            _FAILURE_DOMAINS = config.ENV_DATA.get(
                "worker_availability_zones", get_failures_domain_name()
            )
        except CommandFailed as e:
            print(f"Error initializing FAILURE_DOMAINS: {e}")
            _FAILURE_DOMAINS = []
    return _FAILURE_DOMAINS


def get_failures_domain_name() -> list[str]:
    """
    Fetch Failure domains from cephblockpools names

    Returns:
        list[str]: list with failure domain names

    """
    cbp_object = OCP(kind=CEPHBLOCKPOOL, namespace=config.ENV_DATA["cluster_namespace"])
    failure_domains = list()
    cephblockpools_names = list()
    prefix = DEFAULT_CEPHBLOCKPOOL
    items = cbp_object.data.get("items", [])
    for i in range(len(items)):
        name = items[i].get("metadata", {}).get("name")
        if name:
            cephblockpools_names.append(name)
        log.info(f"Cephblockpool names:{cephblockpools_names}")

    for name in cephblockpools_names:
        if name.startswith(prefix):
            corrected_name = name[len(prefix) :].lstrip("-")
            log.info(corrected_name)
            if corrected_name:
                failure_domains.append(corrected_name)

    log.info(f"Failure domains:{failure_domains}")

    return failure_domains


def get_replica_1_osds(failure_domains: list[str] = None) -> dict:
    """
    Gets the names and IDs of OSD associated with replica1

    Args:
        failure_domains (list[str]): List of failure domain names.
                                   If None, will fetch from get_failure_domains()

    Returns:
        dict: osd name(str): osd id(str)

    """
    replica1_osds = dict()
    all_osds = get_pods_having_label(label=OSD_APP_LABEL)
    domains = failure_domains if failure_domains is not None else get_failure_domains()
    for domain in domains:
        for osd in all_osds:
            if osd["metadata"]["labels"]["ceph.rook.io/DeviceSet"] == domain:
                replica1_osds[osd["metadata"]["name"]] = osd["metadata"]["labels"][
                    "ceph-osd-id"
                ]
    log.info(replica1_osds)
    return replica1_osds


def wait_for_replica1_osds(
    expected_count: int = None,
    timeout: int = 300,
    sleep: int = 15,
) -> dict:
    """
    Wait for replica-1 OSDs to be created.

    Args:
        expected_count (int): Expected number of OSDs. If None, return on first OSD found.
        timeout (int): Maximum time to wait in seconds.
        sleep (int): Time to sleep between checks in seconds.

    Returns:
        dict: OSDs that match replica-1 criteria.

    Raises:
        TimeoutExpiredError: If OSDs are not created within timeout.

    """
    log.info(f"Waiting for replica-1 OSDs to be created (timeout={timeout}s)")

    for osds in TimeoutSampler(timeout=timeout, sleep=sleep, func=get_replica_1_osds):
        if osds and (expected_count is None or len(osds) >= expected_count):
            log.info(f"Found replica-1 OSDs: {osds}")
            return osds


def get_replica1_osd_deployment() -> list[str]:
    """
    Gets the names of OSD deployments associated with replica1

    Returns:
        list[str]: deployment names

    """
    dep_obj = OCP(kind=DEPLOYMENT)
    deployments = dep_obj.get()["items"]
    replica1_osd_deployments = list()
    osd_deployment = list()
    for deployment in deployments:
        if (
            "metadata" in deployment
            and "labels" in deployment["metadata"]
            and "app.kubernetes.io/name" in deployment["metadata"]["labels"]
        ):
            if deployment["metadata"]["labels"]["app.kubernetes.io/name"] == "ceph-osd":
                osd_deployment.append(deployment)

    for deployment in osd_deployment:
        if (
            deployment["metadata"]["labels"]["ceph.rook.io/DeviceSet"]
            in get_failure_domains()
        ):
            log.info(deployment["metadata"]["name"])
            replica1_osd_deployments.append(deployment["metadata"]["name"])

    return replica1_osd_deployments


def scaledown_deployment(deployment_names: list[str]) -> None:
    """
    Scale down deployments to 0

    Args:
        deployments_name (list[str]): list of deployment names.

    """
    log.info("Starts Scaledown deployments")
    deployment_obj = OCP(
        kind=DEPLOYMENT, namespace=config.ENV_DATA["cluster_namespace"]
    )
    for deployment in deployment_names:
        deployment_obj.exec_oc_cmd(f"scale deployment {deployment} --replicas=0")
        log.info(f"scaling to 0: {deployment}")


def count_osd_pods() -> int:
    """
    Gets OSDs count in a cluster

    Returns:
        int: number of OSDs in cluster

    """
    return len(get_pods_having_label(label=OSD_APP_LABEL))


def delete_replica_1_sc() -> None:
    """
    Deletes storage class associated with replica1

    """
    sc_obj = OCP(kind=STORAGECLASS, resource_name=REPLICA1_STORAGECLASS)
    try:
        sc_obj.delete(resource_name=REPLICA1_STORAGECLASS)
    except CommandFailed as e:
        if "Error is Error from server (NotFound)" in str(e):
            log.info(
                f"{REPLICA1_STORAGECLASS} not found, assuming it was already deleted"
            )
        else:
            raise CommandFailed(f"Failed to delete storage class: {str(e)}")


def _retry_osd_removal(osd_name: str, osd_id: str) -> bool:
    """
    Retry OSD removal after job cleanup for AlreadyExists errors

    Args:
        osd_name (str): Name of the OSD being removed
        osd_id (str): ID of the OSD being removed

    Returns:
        bool: True if retry successful, False if failed

    Raises:
        CommandFailed: If cleanup or removal operations fail
    """
    log.info("Attempting additional job cleanup and retry")
    try:
        delete_osd_removal_job()
        sleep(5)  # Brief wait
        run_osd_removal_job(osd_ids=[osd_id])
        verify_osd_removal_job_completed_successfully(osd_id)
        delete_osd_removal_job()
        log.info(f"Successfully removed OSD {osd_name} after retry")
        return True
    except CommandFailed as e:
        log.error(f"Retry failed for OSD {osd_name}: {e}")
        return False


def sequential_remove_replica1_osds(failure_domains: list[str] = None) -> dict:
    """
    Sequentially remove OSDs associated with replica-1 for safer cluster operation.
    Removes OSDs one by one with cluster health validation between removals.

    Args:
        failure_domains (list[str]): List of failure domain names.
                                   If None, will fetch from get_failure_domains()

    Returns:
        dict: Summary of removal operation with counts and any failures
    """
    log.info("Starting sequential removal of replica-1 OSDs")

    # Get all replica-1 OSDs and deployments
    replica1_osds = get_replica_1_osds(failure_domains)
    deployments_name = get_replica1_osd_deployment()

    if not replica1_osds:
        log.warning("No replica-1 OSDs found – skipping sequential OSD removal")
        return {"removed": 0, "failed": 0, "skipped": 0}

    log.info(f"Found {len(replica1_osds)} replica-1 OSDs to remove sequentially")
    log.info(f"OSD deployments: {deployments_name}")
    log.info(f"OSD names and IDs: {replica1_osds}")

    removal_summary = {"removed": 0, "failed": 0, "skipped": 0, "failures": []}

    # PHASE 1: Scale down ALL replica-1 OSD deployments first (bulk operation)
    log.info("PHASE 1: Scaling down ALL replica-1 OSD deployments before removal")
    deployment_obj = OCP(
        kind=DEPLOYMENT, namespace=config.ENV_DATA["cluster_namespace"]
    )

    # Dual detection approach: Traditional + fallback
    if deployments_name:
        log.info(f"Scaling down deployments {deployments_name}")
        scaledown_deployment(deployments_name)
    else:
        log.info(
            "No replica-1 deployments found via failure domains. Using direct deployment targeting."
        )
        # Fallback: Direct deployment targeting by OSD ID
        for osd_name, osd_id in replica1_osds.items():
            deployment_name = f"rook-ceph-osd-{osd_id}"
            try:
                log.info(f"Scaling down deployment {deployment_name} to 0 replicas")
                deployment_obj.exec_oc_cmd(
                    f"scale deployment {deployment_name} --replicas=0"
                )
                log.info(f"Successfully scaled down {deployment_name}")
            except CommandFailed as e:
                log.warning(f"Failed to scale down {deployment_name}: {e}")

    # PHASE 2: Wait for OSDs to go "down" in Ceph
    log.info("PHASE 2: Waiting for OSDs to be marked as 'down' in Ceph")
    target_osd_ids = list(replica1_osds.values())
    wait_for_osds_down(osd_ids=target_osd_ids, timeout=300, sleep=10)
    log.info("All deployments scaled down - OSDs are now 'down' and removable")

    # PHASE 3: Sequential OSD removal with comprehensive job cleanup
    log.info("PHASE 3: Starting sequential OSD removal")
    for osd_name, osd_id in replica1_osds.items():
        try:
            log.info(f"Starting removal of OSD {osd_name} (ID: {osd_id})")

            # Comprehensive job cleanup before each removal
            log.info("Checking for existing OSD removal job...")
            try:
                job_ocp = OCP(
                    kind="Job", namespace=config.ENV_DATA["cluster_namespace"]
                )
                existing_job = job_ocp.get(
                    resource_name="ocs-osd-removal-job", dont_raise=True
                )
                if existing_job:
                    log.warning("Found existing OSD removal job, deleting it first...")

                    # Get logs from existing job pod before deleting (for debugging)
                    try:
                        job_pods = get_pods_having_label(
                            label="job-name=ocs-osd-removal-job",
                            namespace=config.ENV_DATA["cluster_namespace"],
                        )
                        if job_pods:
                            pod_name = job_pods[0]["metadata"]["name"]
                            log.info(f"Getting logs from existing job pod: {pod_name}")
                            pod_logs = job_ocp.get_logs(name=pod_name)
                            log.info(f"Existing job pod logs:\n{pod_logs}")
                    except CommandFailed as e:
                        log.warning(f"Could not get logs from existing job pod: {e}")

                    # Delete the existing job
                    delete_osd_removal_job()
                    log.info("Existing OSD removal job deleted")
            except CommandFailed as e:
                log.warning(f"Error checking/cleaning existing job: {e}")

            # Remove single OSD using removal job with enhanced error handling
            log.info(f"Running OSD removal job for OSD {osd_id}")
            try:
                run_osd_removal_job(osd_ids=[osd_id])

                # Verify removal completed
                verify_osd_removal_job_completed_successfully(osd_id)  # Single OSD

                # Clean up removal job
                delete_osd_removal_job()

                # Wait for cluster stabilization
                log.info(
                    f"OSD {osd_name} removed successfully, waiting for cluster stabilization"
                )
                sleep(30)  # Brief pause between removals

                removal_summary["removed"] += 1
                log.info(
                    f"Successfully removed OSD {osd_name} ({removal_summary['removed']}/{len(replica1_osds)})"
                )

            except CommandFailed as e:
                error_msg = str(e).lower()
                if "alreadyexists" in error_msg:
                    log.error(f"Job already exists error for OSD {osd_name}: {e}")

                    if _retry_osd_removal(osd_name, osd_id):
                        removal_summary["removed"] += 1
                    else:
                        removal_summary["failed"] += 1
                        removal_summary["failures"].append(
                            {
                                "osd": osd_name,
                                "error": "AlreadyExists + retry failed",
                            }
                        )

                elif "osd is healthy" in error_msg:
                    log.error(
                        f"OSD {osd_name} is still healthy and cannot be removed: {e}"
                    )
                    log.warning(
                        "This indicates deployment scaling may not have worked properly"
                    )
                    removal_summary["failed"] += 1
                    removal_summary["failures"].append(
                        {
                            "osd": osd_name,
                            "error": f"OSD healthy (deployment scaling issue): {e}",
                        }
                    )

                else:
                    log.error(f"Command failed for OSD {osd_name}: {e}")
                    removal_summary["failed"] += 1
                    removal_summary["failures"].append(
                        {"osd": osd_name, "error": f"CommandFailed: {e}"}
                    )

        except CommandFailed as e:
            log.error(f"Unexpected error removing OSD {osd_name}: {e}")
            removal_summary["failed"] += 1
            removal_summary["failures"].append(
                {"osd": osd_name, "error": f"Unexpected: {e}"}
            )
            # Continue with next OSD even if this one fails

    # Final summary
    log.info(f"Sequential OSD removal completed: {removal_summary}")

    if removal_summary["failed"] > 0:
        log.warning(f"Some OSDs failed to remove: {removal_summary['failures']}")

    return removal_summary


def delete_replica1_cephblockpools_cr(cbp_object: OCP):
    """
    Delete only the CephBlockPool CRs that belong to the
    replica‑1 (non‑resilient) feature.

    Args:
        cbp_object (ocs_ci.ocs.ocp.OCP): OCP(kind="CephBlockPool") wrapper
    """
    for pool in cbp_object.get()["items"]:
        spec = pool["spec"]
        # replica‑1 pools have both deviceClass and size == 1
        if spec.get("deviceClass") and spec.get("replicated", {}).get("size") == 1:
            name = pool["metadata"]["name"]
            log.info(
                "Deleting replica‑1 CephBlockPool %s (deviceClass=%s)",
                name,
                spec["deviceClass"],
            )
            OCS(**pool).delete(force=True)


def modify_replica1_osd_count(new_osd_count):
    """
    Modify number of OSDs associated with replica1

    Args:
        new_osd_count (str): number, represent the duplicatoin number of replica1 osd.
        for instance, selecting 2, creates 6 osds

    """
    storage_cluster = OCP(kind=STORAGECLUSTER, name=DEFAULT_STORAGE_CLUSTER)
    storage_cluster.exec_oc_cmd(
        f"patch storagecluster {DEFAULT_STORAGE_CLUSTER} -n {config.ENV_DATA['cluster_namespace']} "
        f'--type json --patch \'[{{"op": "replace", "path": '
        f'"/spec/managedResources/cephNonResilientPools/count", "value": {new_osd_count} }}]\''
    )

    storage_cluster.wait_for_resource(condition=STATUS_READY)


def get_device_class_from_ceph() -> dict:
    """
    Gets device class from ceph by executing 'ceph df osd tree'

    Returns:
        dict: device class ("osd name": "device class")

    """
    ceph_pod = get_ceph_tools_pod()
    output = ceph_pod.exec_cmd_on_pod("ceph osd df tree -f json-pretty")
    device_class = dict()
    nodes = output["nodes"]
    for node in nodes:
        if node["type"] == "osd":
            device_class[node["name"]] = node.get("device_class", "unknown")
    log.info(f"Device class: {device_class}")
    return device_class


def get_all_osd_names_by_device_class(osd_dict: dict, device_class: str) -> list:
    """
    Gets all OSD names by its device class

    Args:
        osd_dict (dict): OSD data
        device_class (str): name of device class to search for

    Returns:
        list: OSD names haveing requested device class
    """
    return [
        osd_name
        for osd_name, class_value in osd_dict.items()
        if class_value == device_class
    ]


def get_osd_kb_used_data() -> dict:
    """
    Retrieves the KB used data for each OSD from the Ceph cluster.

    Returns:
        dict: kb_used_data("osd_name": kb_used_data)

    """
    ceph_pod = get_ceph_tools_pod()
    output = ceph_pod.exec_cmd_on_pod("ceph osd df tree -f json-pretty")
    log.info(f"DF tree: {output}")
    nodes = output["nodes"]
    kb_used_data = dict()
    for node in nodes:
        if node["type"] == "osd":
            kb_used_data[node["name"]] = node.get("kb_used_data")
    log.info(f"KB Used per OSD: {kb_used_data}")

    return kb_used_data


def get_osd_pgs_used() -> dict:
    """
    Retrieves the PG used for each OSD from the Ceph cluster.

    Returns:
        dict: pgs_used("osd_name": pg_used)

    """
    ceph_pod = get_ceph_tools_pod()
    output = ceph_pod.exec_cmd_on_pod("ceph osd df tree -f json-pretty")
    nodes = output["nodes"]
    pgs_used = dict()
    for node in nodes:
        if node["type"] == "osd":
            pgs_used[node["name"]] = node.get("pgs", 0)
    log.info(f"Placement Groups Used per OSD: {pgs_used}")

    return pgs_used
