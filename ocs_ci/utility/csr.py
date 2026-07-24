import logging
import time

from ocs_ci.framework import config
from ocs_ci.ocs import constants, exceptions, ocp
from ocs_ci.utility.vsphere import VSPHERE
from ocs_ci.utility.vsphere_nodes import VSPHERENode
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import run_cmd, TimeoutSampler, get_ocp_version
from semantic_version import Version

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


def fix_nodes_with_wrong_ostree_image(vsphere_object, csr_nodes):
    """
    Detect and fix nodes that have an IP but are running the wrong RHCOS
    base image (missing kubelet/CRI-O). This happens when a node is
    provisioned with the base RHCOS OCI archive instead of the full OCP
    node image.

    For each node that has an IP but no CSR, SSH in and check kubelet
    status. If kubelet is inactive due to a wrong ostree image, rebase
    to the correct image (obtained from a working node) and reboot.

    Args:
        vsphere_object (VSPHERE): vSphere connection object
        csr_nodes (dict): Current CSR nodes dict from get_nodes_csr()

    Returns:
        bool: True if any node was rebased and rebooted

    """
    cluster_name = config.ENV_DATA.get("cluster_name")
    dc = config.ENV_DATA["vsphere_datacenter"]
    cluster = config.ENV_DATA["vsphere_cluster"]

    all_vms = vsphere_object.get_all_vms_in_pool(cluster_name, dc, cluster)
    node_vms = {
        vm.name: vm
        for vm in all_vms
        if vm.name.startswith("compute") or vm.name.startswith("control-plane")
    }

    nodes_with_csr = set(csr_nodes.keys())
    missing_nodes = []
    for vm_name, vm in node_vms.items():
        if vm_name not in nodes_with_csr and vm.summary.guest.ipAddress:
            missing_nodes.append((vm_name, vm.summary.guest.ipAddress))

    if not missing_nodes:
        return False

    logger.info(
        f"Nodes with IP but no CSR: " f"{[(name, ip) for name, ip in missing_nodes]}"
    )

    correct_image = None
    for vm_name, vm in node_vms.items():
        if vm_name in nodes_with_csr and vm.summary.guest.ipAddress:
            try:
                working_node = VSPHERENode(vm.summary.guest.ipAddress)
                correct_image = working_node.get_active_ostree_image()
                if correct_image and "openshift-release-dev" in correct_image:
                    # Strip the ostree transport prefix to get the registry ref
                    if ":" in correct_image:
                        correct_image = correct_image.split(":", 1)[1]
                    logger.info(
                        f"Got correct ostree image from {vm_name}: {correct_image}"
                    )
                    break
            except Exception as e:
                logger.warning(
                    f"Failed to get ostree image from working node {vm_name}: {e}"
                )
                continue

    if not correct_image:
        logger.warning("Could not determine correct ostree image from working nodes")
        return False

    fixed = False
    for node_name, node_ip in missing_nodes:
        try:
            node = VSPHERENode(node_ip)
            if node.is_kubelet_active():
                logger.info(f"{node_name} kubelet is active, skipping ostree check")
                continue

            current_image = node.get_active_ostree_image()
            logger.info(f"{node_name} current ostree image: {current_image}")

            if current_image and "openshift-release-dev" in current_image:
                logger.info(
                    f"{node_name} has correct OCP image, "
                    f"kubelet issue is not image-related"
                )
                continue

            logger.warning(
                f"{node_name} is running wrong RHCOS image: {current_image}. "
                f"Rebasing to {correct_image}"
            )
            retcode, stdout, stderr = node.rpm_ostree_rebase(correct_image)
            if retcode != 0:
                logger.error(f"rpm-ostree rebase failed on {node_name}: {stderr}")
                continue

            logger.info(f"Rebooting {node_name} after successful rebase")
            node.reboot()
            fixed = True
        except Exception as e:
            logger.warning(
                f"Failed to fix ostree image on {node_name} ({node_ip}): {e}"
            )

    return fixed


def wait_for_all_nodes_csr_and_approve(
    timeout=50, sleep=10, expected_node_num=1, ignore_existing_csr=None
):
    """
    Wait for CSR to generate for nodes

    Args:
        timeout (int): Time in seconds to wait
        sleep (int): Sampling time in seconds
        expected_node_num (int): Number of nodes to verify CSR is generated
        ignore_existing_csr (dct): Existing CSR to ignore
        e.g:{
            'compute-1': ['csr-64vkw']
            }

    Returns:
         bool: True if all nodes are generated CSR

    Raises:
        TimeoutExpiredError: in case CSR not found

    """
    start_time = time.time()
    reboot_timeout = 300
    vsphere_object = None
    is_vms_without_ip = False
    is_ostree_fix_attempted = False
    if config.ENV_DATA["platform"] == constants.VSPHERE_PLATFORM:
        vsphere_object = VSPHERE(
            config.ENV_DATA["vsphere_server"],
            config.ENV_DATA["vsphere_user"],
            config.ENV_DATA["vsphere_password"],
        )

    if not expected_node_num:
        # expected number of nodes is total of master, worker nodes and
        # bootstrapper node
        # In OCP 4.8, an extra CSR (openshift-authenticator) is added
        ocp_version = get_ocp_version()
        expected_node_num = (
            config.ENV_DATA["master_replicas"] + config.ENV_DATA["worker_replicas"] + 1
        )
        if Version.coerce(ocp_version) == Version.coerce("4.8"):
            expected_node_num += 1
        # In OCP 4.9, openshift-monitoring CSR is added
        if Version.coerce(ocp_version) >= Version.coerce("4.9"):
            expected_node_num += 2

    if ignore_existing_csr:
        node_name_to_ignore = list(ignore_existing_csr.keys())[0]

    for csr_nodes in TimeoutSampler(timeout=timeout, sleep=sleep, func=get_nodes_csr):
        logger.debug(f"CSR data: {csr_nodes}")
        if ignore_existing_csr:
            # If new and old csr data for ignore node is same, then delete the entry
            # from current csr.
            if (
                csr_nodes[node_name_to_ignore]
                == ignore_existing_csr[node_name_to_ignore]
            ):
                logger.debug(f"Ignoring already existing CSR {ignore_existing_csr}")
                del csr_nodes[node_name_to_ignore]
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
        # In vSphere deployment it sometime happens that VM doesn't get ip and
        # then we need to restart it to make our CI more stable and let the VM
        # to get IP and continue with loading ignition config. The restart of
        # the VMs happens only once in reboot_timeout (300 seconds).
        if vsphere_object and time.time() - start_time >= reboot_timeout:
            start_time = time.time()
            if not is_vms_without_ip:
                vms_without_ip = vsphere_object.find_vms_without_ip(
                    config.ENV_DATA.get("cluster_name"),
                    config.ENV_DATA["vsphere_datacenter"],
                    config.ENV_DATA["vsphere_cluster"],
                )
                if vms_without_ip:
                    vsphere_object.restart_vms(vms_without_ip, force=True)
                    # over-writing start_time here so that we have actual reboot timeout
                    # calculated from the point after restarting vms
                    start_time = time.time()
                else:
                    is_vms_without_ip = True
            if not is_ostree_fix_attempted:
                is_ostree_fix_attempted = True
                try:
                    if fix_nodes_with_wrong_ostree_image(vsphere_object, csr_nodes):
                        logger.info(
                            "Nodes with wrong ostree image were rebased "
                            "and rebooted, resetting timeout"
                        )
                        start_time = time.time()
                except Exception:
                    logger.exception("Failed to fix nodes with wrong ostree image")
