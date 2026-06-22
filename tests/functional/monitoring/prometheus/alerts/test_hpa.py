import logging

from ocs_ci.framework.pytest_customization import marks
from ocs_ci.framework.pytest_customization.marks import tier1, blue_squad
from ocs_ci.framework.testlib import (
    runs_on_provider,
    skipif_ocs_version,
    skipif_ocp_version,
)
from ocs_ci.ocs import constants
from ocs_ci.utility import prometheus
from ocs_ci.framework.pytest_customization.marks import skipif_managed_service

logger = logging.getLogger(__name__)


@blue_squad
@tier1
@skipif_ocs_version("<4.6")
@skipif_ocp_version("<4.6")
@marks.polarion_id("OCS-2375")
@runs_on_provider
@skipif_managed_service
def test_hpa_maxreplica_alert(threading_lock):
    """
    Test to verify that no HPA max replica alert is triggered
    """
    logger.info("Starting test: Verify no HPA max replica mismatch alerts")

    logger.test_step("Initialize Prometheus API and check for HPA alerts")
    api = prometheus.PrometheusAPI(threading_lock=threading_lock)
    target_alert = constants.ALERT_KUBEHPAREPLICASMISMATCH
    logger.info(f"Checking for alert: {target_alert} (timeout=10s)")

    alerts = api.wait_for_alert(name=target_alert, timeout=10, sleep=1)

    logger.test_step("Verify no HPA replica mismatch alerts are present")
    alert_count = len(alerts)
    logger.assertion(
        f"Alert count for {target_alert}: expected=0, actual={alert_count}"
    )

    if alert_count > 0:
        logger.debug(f"Unexpected alerts found: {alerts}")
        assert False, f"Failed: There should be no {target_alert} alert"

    logger.info("Test passed: No HPA max replica mismatch alerts detected")
