import logging

from ocs_ci.ocs import constants, exceptions, ocp
from ocs_ci.ocs.resources import ocs
from ocs_ci.utility.retry import retry

logger = logging.getLogger(__name__)


@retry(exceptions.PendingCSRException, tries=4, delay=10, backoff=1)
def approve_pending_csr():
    """
    After node addition CSR could be in pending state, we have to approve it.

    Raises:
        exceptions.PendingCSRException

    """
    cmd = "adm certificate approve"
    csr_conf = get_csr_resource()
    for item in csr_conf.data.get('items'):
        cmd = f"{cmd} {item.get('metadata').get('name')}"
        csr_conf.ocp.exec_oc_cmd(cmd)

    try:
        check_no_pending_csr(csr_conf)
        logger.info("All CSRs approved")
    except exceptions.PendingCSRException:
        logger.error("Failed to approve all CSRs")
        raise


@retry(exceptions.PendingCSRException, tries=2, delay=300, backoff=1)
def check_no_pending_csr():
    """
    Check whether we have any pending CSRs.

    Raises:
        exceptions.PendingCSRException

    """
    # Load the latest state of csr
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
    Retrieve the latest CSR resource data

    Returns:
        ocs.OCS: CSR resource data
    """
    csr_conf = ocs.OCS(
        **ocp.OCP(kind='csr', namespace=constants.DEFAULT_NAMESPACE).get()
    )
    return csr_conf.get()
