import logging

from ocs_ci.framework.pytest_customization import marks
from ocs_ci.framework.pytest_customization.marks import tier1, blue_squad
from ocs_ci.framework.testlib import skipif_ocs_version, skipif_ocp_version
from ocs_ci.ocs import constants
from ocs_ci.utility import prometheus
from ocs_ci.framework.pytest_customization.marks import (
    skipif_managed_service,
    runs_on_provider,
)

logger = logging.getLogger(__name__)


@blue_squad
@tier1
@skipif_ocs_version("<4.6")
@skipif_ocp_version("<4.6")
@marks.polarion_id("OCS-2375")
@marks.bugzilla("1836299")
@skipif_managed_service
@runs_on_provider
def test_hpa_maxreplica_alert(threading_lock):
    """
    Test to verify that no HPA max replica alert is triggered
    """
    api = prometheus.PrometheusAPI(threading_lock=threading_lock)

    logger.info(
        f"Verifying whether {constants.ALERT_KUBEHPAREPLICASMISMATCH} "
        f"has not been triggered"
    )
    alerts = api.wait_for_alert(
        name=constants.ALERT_KUBEHPAREPLICASMISMATCH, timeout=10, sleep=1
    )
    if len(alerts) > 0:
        assert (
            False
        ), f"Failed: There should be no {constants.ALERT_KUBEHPAREPLICASMISMATCH} alert"
