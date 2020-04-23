import logging

from ocs_ci.ocs import constants, exceptions, ocp
from ocs_ci.utility.retry import retry

logger = logging.getLogger(__name__)


@retry(exceptions.PendingCSRException, tries=4, delay=10, backoff=1)
def approve_pending_csr():
    """
    After node addition CSR could be in pending state, we have to approve it.

    Raises:
        exceptions.PendingCSRException

    """
    logger.info("Approving CSRs")
    base_cmd = "adm certificate approve"
    csr_conf = get_csr_resource()
    for item in csr_conf.data.get('items'):
        cmd = f"{base_cmd} {item.get('metadata').get('name')}"
        csr_conf.exec_oc_cmd(cmd)

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
