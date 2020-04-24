import logging

from ocs_ci.ocs import constants, exceptions, ocp
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import run_cmd

logger = logging.getLogger(__name__)


@retry(exceptions.PendingCSRException, tries=4, delay=10, backoff=1)
def approve_pending_csr():
    """
    After node addition CSR could be in pending state, we have to approve it.

    Raises:
        exceptions.PendingCSRException

    """
    base_cmd = "oc adm certificate approve"
    pending_csrs = get_pending_csr()
    if pending_csrs:
        logger.info(f"Pending CSRs: {pending_csrs}")
        csrs = ' '.join([str(csr) for csr in pending_csrs])
        cmd = f"{base_cmd} {csrs}"
        logger.info("Approving pending CSRs")
        run_cmd(cmd)

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
