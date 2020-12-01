import logging
import time

from ocs_ci.framework import config
from ocs_ci.ocs import constants, exceptions, ocp
from ocs_ci.utility.vsphere import VSPHERE
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import run_cmd, TimeoutSampler

logger = logging.getLogger(__name__)


@retry(
    (exceptions.PendingCSRException, exceptions.TimeoutExpiredError),
    tries=4,
    delay=10,
    backoff=1,
)
def approve_pending_csr(expected=None):
    """
    After node addition CSR could be in pending state, we have to approve it.

    Args:
        expected (int): Expected number of CSRs. By default, it will approve
            all the pending CSRs if exists.

    Raises:
        exceptions.PendingCSRException
        exceptions.TimeoutExpiredError

    """
    for pending_csrs in TimeoutSampler(300, 10, get_pending_csr):
        if not expected:
            if pending_csrs:
                logger.info(f"Pending CSRs: {pending_csrs}")
                approve_csrs(pending_csrs)
            break
        if len(pending_csrs) >= expected:
            logger.info(f"Pending CSRs: {pending_csrs}")
            approve_csrs(pending_csrs)
            break
        logger.info(f"Expected: {expected} but found pending csr: {len(pending_csrs)}")
    check_no_pending_csr()
    logger.info("All CSRs approved")


@retry(exceptions.PendingCSRException, tries=2, delay=300, backoff=1)
def check_no_pending_csr():
    """
    Check whether we have any pending CSRs.

    Raises:
        exceptions.PendingCSRException

    """
    logger.info("Checking for Pending CSRs")
    pending_csrs = get_pending_csr()
    logger.debug(f"pending CSRs: {pending_csrs}")
    if pending_csrs:
        logger.warning(f"{pending_csrs} are not Approved")
        approve_csrs(pending_csrs)
        raise exceptions.PendingCSRException("Some CSRs are in 'Pending' state")


def get_csr_resource():
    """
    Retrieve the latest CSR data

    Returns:
        ocp.OCP: CSR data

    """
    logger.info("Retrieving CSR data")
    return ocp.OCP(kind="csr", namespace=constants.DEFAULT_NAMESPACE)


@retry(exceptions.CommandFailed, tries=7, delay=5, backoff=3)
def get_pending_csr():
    """
    Gets the pending CSRs

    Returns:
        list: list of pending CSRs

    """
    csr_conf = get_csr_resource()
    return [
        item["metadata"]["name"]
        for item in csr_conf.data.get("items")
        if not item.get("status")
    ]


@retry(exceptions.CommandFailed, tries=7, delay=5, backoff=3)
def approve_csrs(pending_csrs):
    """
    Approves the CSRs

    Args:
        csrs (list): List of CSRs

    """
    base_cmd = "oc adm certificate approve"
    csrs = " ".join([str(csr) for csr in pending_csrs])
    cmd = f"{base_cmd} {csrs}"
    logger.info("Approving pending CSRs")
    run_cmd(cmd)


def get_nodes_csr():
    """
    Fetches the CSRs

    Returns:
        dict: Dictionary with node as keys and CSRs as values
            e.g:{
                'control-plane-1': ['csr-6hx8z'],
                'compute-2': ['csr-blz2n'],
                'compute-0': ['csr-p46bz'],
                'control-plane-2': ['csr-tzhn5'],
                'control-plane-0': ['csr-wm4g5']
                }

    """
    csr_nodes = {}
    csr_data = get_csr_resource().get()
    for item in csr_data["items"]:
        node = item["spec"]["username"].split(":")[-1]
        if node not in csr_nodes.keys():
            csr_nodes[node] = []
        csr_name = item["metadata"]["name"]
        csr_nodes[node].append(csr_name)
    return csr_nodes


def wait_for_all_nodes_csr_and_approve(timeout=900, sleep=10, expected_node_num=None):
    """
    Wait for CSR to generate for nodes

    Args:
        timeout (int): Time in seconds to wait
        sleep (int): Sampling time in seconds
        expected_node_num (int): Number of nodes to verify CSR is generated

    Returns:
         bool: True if all nodes are generated CSR

    Raises:
        TimeoutExpiredError: in case CSR not found

    """
    start_time = time.time()
    reboot_timeout = 120
    vsphere_object = None
    if config.ENV_DATA["platform"] == constants.VSPHERE_PLATFORM:
        vsphere_object = VSPHERE(
            config.ENV_DATA["vsphere_server"],
            config.ENV_DATA["vsphere_user"],
            config.ENV_DATA["vsphere_password"],
        )

    if not expected_node_num:
        # expected number of nodes is total of master, worker nodes and
        # bootstrapper node
        expected_node_num = (
            config.ENV_DATA["master_replicas"] + config.ENV_DATA["worker_replicas"] + 1
        )
    for csr_nodes in TimeoutSampler(timeout=timeout, sleep=sleep, func=get_nodes_csr):
        logger.debug(f"CSR data: {csr_nodes}")
        if len(csr_nodes.keys()) == expected_node_num:
            logger.info(f"CSR generated for all {expected_node_num} nodes")
            approve_pending_csr()
            return
        logger.warning(
            f"Some nodes are not generated CSRs. Expected"
            f" {expected_node_num} but found {len(csr_nodes.keys())} CSRs."
            f"retrying again"
        )
        # approve the pending CSRs here since newly added nodes will not
        # generate CSR till existing CSRs are approved
        pending_csrs = get_pending_csr()
        if pending_csrs:
            approve_csrs(pending_csrs)
        # In vSphere deployment it sometime happes that VM doesn't get ip and
        # then we need to restart it to make our CI more stable and let the VM
        # to get IP and continue with loading ignition config. The rester of
        # the VMs happens only once in reboot_timeout (120 seconds).
        if vsphere_object and time.time() - start_time >= reboot_timeout:
            start_time = time.time()
            vsphere_object.find_vms_without_ip_and_restart()
