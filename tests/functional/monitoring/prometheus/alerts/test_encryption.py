import logging
import pytest
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import blue_squad
from ocs_ci.framework.testlib import (
    tier4a,
    runs_on_provider,
    skipif_managed_service,
    skipif_no_kms,
)
from ocs_ci.ocs import constants
from ocs_ci.utility import prometheus
from ocs_ci.ocs.ocp import OCP


logger = logging.getLogger(__name__)


@blue_squad
@tier4a
@pytest.mark.polarion_id("OCS-5154")
@skipif_no_kms
@runs_on_provider
@skipif_managed_service
def test_kms_unavailable(measure_rewrite_kms_endpoint, threading_lock):
    """
    Test that there is appropriate alert when KMS is unavailable and that
    this alert is cleared when the KMS endpoint is back online.

    """
    logger.info("Starting test: Verify KMS unavailable alert triggers and clears")

    logger.test_step("Initialize Prometheus API and retrieve alerts")
    api = prometheus.PrometheusAPI(threading_lock=threading_lock)

    # get alerts from time when manager deployment was scaled down
    alerts = measure_rewrite_kms_endpoint.get("prometheus_alerts")
    logger.info(f"Number of alerts retrieved: {len(alerts) if alerts else 0}")

    logger.test_step("Prepare alert validation parameters")
    target_label = constants.ALERT_KMSSERVERCONNECTIONALERT
    config_namespace = config.ENV_DATA["cluster_namespace"]
    config_cluster = config.ENV_DATA["storage_cluster_name"]
    target_msg = (
        "Storage Cluster KMS Server is in un-connected state. Please check "
        f"KMS config in namespace:cluster {config_namespace}:{config_cluster}."
    )
    states = ["pending", "firing"]
    severity = "error"

    logger.info(f"Target alert: {target_label}")
    logger.info(f"Namespace: {config_namespace}, Cluster: {config_cluster}")
    logger.debug(f"Expected states: {states}, severity: {severity}")

    logger.test_step("Validate KMS server connection alert is present")
    prometheus.check_alert_list(
        label=target_label,
        msg=target_msg,
        alerts=alerts,
        states=states,
        severity=severity,
    )
    logger.info(f"Alert {target_label} validated successfully")

    logger.test_step("Verify alert is cleared after KMS endpoint restoration")
    stop_time = measure_rewrite_kms_endpoint.get("stop")
    clearance_timeout = 300
    logger.info(
        f"Checking alert clearance (stop_time: {stop_time}, timeout: {clearance_timeout}min)"
    )

    api.check_alert_cleared(
        label=target_label,
        measure_end_time=stop_time,
        time_min=clearance_timeout,
    )
    logger.info(f"Alert {target_label} cleared successfully")

    logger.info("Test passed: KMS unavailable alert triggered and cleared as expected")


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
