import logging
import json

from ocs_ci.ocs import constants
from ocs_ci.framework import config
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import check_pods_in_statuses
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import CommandFailed, LVMOHealthException
from ocs_ci.helpers.helpers import clean_all_test_projects


log = logging.getLogger(__name__)

LS_DEVICE_BY_PATH = "ls /dev/disk/by-path/*"
LS_DEVICE_BY_ID = "ls /dev/disk/by-id/*"


def lvmo_health_check_base():
    """
    Check health of lvm cluster by checking the following:
    lvm cluster cr status,
    lvm pods status

    Returns:
        bool: True if all checks passed, raise exception otherwise
    """
    lvm_clustername = get_lvm_cluster_name()
    oc_obj = OCP(
        namespace=config.ENV_DATA["cluster_namespace"],
        kind="lvmcluster",
        resource_name=lvm_clustername,
    )

    try:
        lvmcluster_status = oc_obj.get(lvm_clustername)
        lvmcluster_status.get("status")["ready"]
    except (KeyError, TypeError) as e:
        log.info("lvm cluster status is not available")
        raise e
    except LVMOHealthException:
        log.error("lvmcluster is not ready")
        raise LVMOHealthException(
            f"LVM Cluster status is {lvmcluster_status['status']['ready']}"
        )

    if not check_pods_in_statuses("Running"):
        log.error("one or more pods is not in running state")
        raise LVMOHealthException("status is not 'Running'")

    log.info("LVM Cluster Health Check Completed Successfully")

    return True


def lvmo_health_check(tries=20, delay=30):
    """
    Priodic check LVMO cluster health

    Args:
        tries (int): number of retries.
        delay (int): delay between retries.

    Returns:
        bool: True if cluster is healthy, raise exception otherwise.
    """
    return retry(
        (LVMOHealthException, KeyError, TypeError),
        tries=tries,
        delay=delay,
        backoff=1,
    )(lvmo_health_check_base)()


def get_sno_disks_by_path(node=constants.SNO_NODE_NAME):
    """
    Get list of storage devices by it's path as listed on node

    args:
        node (str): node name

    Returns:
        list: list of storage devices full-path (str)
    """
    storage_disks_count = config.DEPLOYMENT.get("lvmo_disks")
    oc_obj = OCP()
    disks = oc_obj.exec_oc_debug_cmd(node, cmd_list=[LS_DEVICE_BY_PATH])
    raw_disks_list = disks.split("\n")
    raw_disks_list = list(filter(None, raw_disks_list))
    disks_by_path = list()
    for line in raw_disks_list[-storage_disks_count:]:
        if "sda" not in line:
            disk_name = line
            disks_by_path.append(disk_name)

    return disks_by_path


def get_sno_blockdevices(node=constants.SNO_NODE_NAME):
    """
    Gets list of storage devices by it's names

    args:
        node (str): node name

    Returns:
        list: list of storage devices full-names (str)

    """
    storage_disks_count = config.DEPLOYMENT.get("lvmo_disks")
    disks_by_name = list()
    oc_obj = OCP()
    disks = oc_obj.exec_oc_debug_cmd(node, cmd_list=["lsblk --json"])
    disks = json.loads(disks)
    for disk in range(1, (storage_disks_count + 1)):
        disk_name = "/dev/" + (disks["blockdevices"][disk]["name"])
        disks_by_name.append(disk_name)

    return disks_by_name


def delete_lvm_cluster():
    """
    Delete lvm cluster if exists

    raise:
        execption if lvmcluster cant be deleted
    """
    clean_all_test_projects()
    lvm_clustername = get_lvm_cluster_name()
    lmvcluster = OCP(kind="LVMCluster", namespace=config.ENV_DATA["cluster_namespace"])
    try:
        lmvcluster.delete(resource_name=lvm_clustername)
    except CommandFailed as e:
        if f'lvmclusters.lvm.topolvm.io "{lvm_clustername}" not found' not in str(e):
            raise e
        else:
            log.info("LVMCluster not found, procced with creation of new one")


def get_lvm_cluster_name():
    """
    Get LVM clustername Dynamically

    Returns:
        (str) lvm cluster name.
    """
    if "ocs_registry_image" in config.DEPLOYMENT.keys():
        if "lvms" in config.DEPLOYMENT["ocs_registry_image"]:
            return constants.LVMSCLUSTER
        return constants.LVMCLUSTER
    return ""
