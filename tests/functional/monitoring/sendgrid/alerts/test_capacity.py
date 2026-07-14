import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    tier2,
    managed_service_required,
    skipif_ms_provider,
    blue_squad,
)
from ocs_ci.ocs.ocp import OCP

log = logging.getLogger(__name__)


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
    log.warning(
        "Cluster utilization is completed. "
        f"There should be proper emails sent to {notification_emails_required}"
    )
    # TODO(fbalak): automate checking of email content


def setup_module(module):
    ocs_obj = OCP()
    module.original_user = ocs_obj.get_user_name()


def teardown_module(module):
    ocs_obj = OCP()
    ocs_obj.login_as_user(module.original_user)
