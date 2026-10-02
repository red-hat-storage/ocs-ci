import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    tier2,
    managed_service_required,
    skipif_ms_provider,
    blue_squad,
)
from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(__name__)


@blue_squad
@pytest.mark.polarion_id("OCS-2718")
@tier2
@managed_service_required
@skipif_ms_provider
def deprecated_test_capacity_workload_alerts(
    notification_emails_required, workload_storageutilization_97p_rbd
):
    """
    Test that there are appropriate alert emails when ceph cluster is utilized
    via RBD interface.

    """
    logger.info("Starting test: Verify capacity alert emails for RBD workload")

    logger.test_step("Verify cluster utilization completed and alert emails sent")
    logger.info(f"Expected notification recipients: {notification_emails_required}")
    logger.warning(
        "Manual verification required: Check that proper capacity alert emails "
        f"were sent to {notification_emails_required}"
    )
    # TODO(fbalak): automate checking of email content

    logger.info(
        "Test completed: Cluster utilization reached, manual email verification needed"
    )


def setup_module(module):
    logger.info("Setting up module: Storing original user for cleanup")
    ocs_obj = OCP()
    module.original_user = ocs_obj.get_user_name()
    logger.info(f"Original user stored: {module.original_user}")


def teardown_module(module):
    logger.info("Tearing down module: Restoring original user")
    ocs_obj = OCP()
    ocs_obj.login_as_user(module.original_user)
    logger.info(f"Restored user: {module.original_user}")
