from logging import getLogger
from time import sleep

from ocs_ci.framework import config
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


def get_replica_1_osds() -> dict:
    """
    Gets the names and IDs of OSD associated with replica1

    Returns:
        dict: osd name(str): osd id(str)

    """
    replica1_osds = dict()
    all_osds = get_pods_having_label(label=OSD_APP_LABEL)
    for domain in get_failure_domains():
        for osd in all_osds:
            if osd["metadata"]["labels"]["ceph.rook.io/DeviceSet"] == domain:
                replica1_osds[osd["metadata"]["name"]] = osd["metadata"]["labels"][
                    "ceph-osd-id"
                ]
    log.info(replica1_osds)
    return replica1_osds


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


def purge_replica1_osd():
    """
    Purge OSDs associated with replica1
        1. scale down its deployments to 0
        2. use OSD removal template

    """
    deployments_name = get_replica1_osd_deployment()
    log.info(f"Deployments Name: {deployments_name}")
    scaledown_deployment(deployments_name)
    replica1_osds = get_replica_1_osds()
    log.info(f"OSDS : {replica1_osds.keys()}")
    log.info(f"OSD IDs: {replica1_osds.values()}")
    run_osd_removal_job(osd_ids=replica1_osds.values())
    verify_osd_removal_job_completed_successfully("4")
    sleep(120)
    delete_osd_removal_job()


def delete_replica1_cephblockpools_cr(cbp_object: OCP):
    """
    Deletes CR of cephblockpools associated with replica1

    Args:
        cbp_object (ocp.OCP): OCP object with kind=CEPHBLOCKPOOL

    """
    for i in range(0, len((cbp_object.data["items"]))):
        cbp_cr_name = cbp_object.data["items"][i]["spec"]["deviceClass"]
        log.info(f"cbp_cr_name: {cbp_cr_name}")
        if cbp_cr_name in get_failure_domains():
            log.info(f"Deleting {DEFAULT_CEPHBLOCKPOOL}-{cbp_cr_name}")
            cbp_object.delete(resource_name=(f"{DEFAULT_CEPHBLOCKPOOL}-{cbp_cr_name}"))


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
