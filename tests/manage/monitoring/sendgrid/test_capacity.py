import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    tier2,
    managed_service_required,
    skipif_ms_provider,
)
from ocs_ci.ocs.ocp import OCP

log = logging.getLogger(__name__)


@pytest.mark.polarion_id("OCS-2718")
@tier2
@managed_service_required
@skipif_ms_provider
def test_capacity_workload_alerts(
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


def teardown_module():
    ocs_obj = OCP()
    ocs_obj.login_as_sa()
