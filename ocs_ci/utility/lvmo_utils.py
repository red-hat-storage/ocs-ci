import logging
import json

from ocs_ci.ocs import constants
from ocs_ci.framework import config
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import check_pods_in_statuses
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import LVMOHealthException
from ocs_ci.helpers.helpers import clean_all_test_projects


log = logging.getLogger(__name__)

storage_disks_count = config.DEPLOYMENT.get("lvmo_disks")


def lvmo_health_check_base():
    """
    Check health of lvm cluster by checking the following:
    lvm cluster cr status,
    lvm pods status

    Returns:
        bool: True if all checks passed, raise exception otherwise
    """
    oc_obj = OCP(
        namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        kind="lvmcluster",
        resource_name="lvmcluster",
    )
    lvmcluster_status = oc_obj.get("lvmcluster")
    if not (lvmcluster_status["status"]["ready"]):
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
        (LVMOHealthException),
        tries=tries,
        delay=delay,
        backoff=1,
    )(lvmo_health_check_base)()


def get_disks_by_path():
    oc_obj = OCP()
    disks = oc_obj.exec_oc_debug_cmd(
        node=constants.SNO_NODE_NAME, cmd_list=["ls /dev/disk/by-path"]
    )
    raw_disks_list = disks.split("\n")
    raw_disks_list = list(filter(None, raw_disks_list))
    disks_by_path = list()
    for line in raw_disks_list[-storage_disks_count:]:
        disk_name = "/dev/disk/by-path/" + line
        disks_by_path.append(disk_name)

    return disks_by_path


def get_disks_by_name():
    disks_by_name = list()
    oc_obj = OCP()
    disks = oc_obj.exec_oc_debug_cmd(
        node=constants.SNO_NODE_NAME, cmd_list=["lsblk --json"]
    )
    disks = json.loads(disks)
    for disk in range(1, (storage_disks_count + 1)):
        disk_name = "/dev/" + (disks["blockdevices"][disk]["name"])
        disks_by_name.append(disk_name)

    return disks_by_name


def delete_lvm_cluster():
    clean_all_test_projects()
    lmvcluster = OCP(kind="LVMCluster", namespace=constants.OPENSHIFT_STORAGE_NAMESPACE)
    lmvcluster.delete(resource_name=constants.LVMCLUSTER)
