import logging

from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import check_pods_in_statuses
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import LVMOHealthException

log = logging.getLogger(__name__)


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
