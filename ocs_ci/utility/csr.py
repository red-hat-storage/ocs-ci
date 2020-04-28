import logging

from ocs_ci.ocs import constants, exceptions, ocp
from ocs_ci.ocs.node import get_all_nodes
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import run_cmd, TimeoutSampler

logger = logging.getLogger(__name__)


@retry(
    (exceptions.PendingCSRException, exceptions.TimeoutExpiredError),
    tries=4,
    delay=10,
    backoff=1
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
        logger.info(
            f"Expected: {expected} but found pending csr: {len(pending_csrs)}"
        )
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
    csr_conf = get_csr_resource()
    pending = False
    for item in csr_conf.data.get('items'):
        if item.get('status') == {}:
            logger.warning(
                f"{item.get('metadata').get('name')} is not Approved"
            )
            pending = True
    if pending:
        raise exceptions.PendingCSRException(
            "Some CSRs are in 'Pending' state"
        )


def get_csr_resource():
    """
    Retrieve the latest CSR data

    Returns:
        ocp.OCP: CSR data

    """
    logger.info("Retrieving CSR data")
    return ocp.OCP(kind='csr', namespace=constants.DEFAULT_NAMESPACE)


def get_pending_csr():
    """
    Gets the pending CSRs

    Returns:
        list: list of pending CSRs

    """
    csr_conf = get_csr_resource()
    return [
        item['metadata']['name'] for item in csr_conf.data.get('items')
        if not item.get('status')
    ]


def approve_csrs(pending_csrs):
    """
    Approves the CSRs

    Args:
        csrs (list): List of CSRs

    """
    base_cmd = "oc adm certificate approve"
    csrs = ' '.join([str(csr) for csr in pending_csrs])
    cmd = f"{base_cmd} {csrs}"
    logger.info(f"Approving pending CSRs")
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
    for item in csr_data['items']:
        node = item['spec']['username'].split(":")[-1]
        if node not in csr_nodes.keys():
            csr_nodes[node] = []
        csr_name = item['metadata']['name']
        csr_nodes[node].append(csr_name)
    return csr_nodes


def wait_for_all_nodes_csr(timeout=900, sleep=10):
    """
    Wait for CSR to generate for nodes

    Args:
        timeout (int): Time in seconds to wait
        sleep (int): Sampling time in seconds

    Returns:
         bool: True if all nodes are generated CSR

    Raises:
        TimeoutExpiredError: in case CSR not found

    """
    pending = False
    all_nodes = get_all_nodes()
    for csr_nodes in TimeoutSampler(
            timeout=timeout, sleep=sleep, func=get_nodes_csr()
    ):
        pending_nodes = []
        logger.debug(f"CSR data: {csr_nodes}")
        for node in all_nodes:
            if node not in csr_nodes.keys():
                logger.info(f"{node} CSR is not generated")
                pending = True
                pending_nodes.append(node)
        if not pending:
            logger.info("CSR generated for all nodes in cluster")
            return
        logger.warning(
            f"Nodes {pending_nodes} are not generated CSR. retrying again"
        )
