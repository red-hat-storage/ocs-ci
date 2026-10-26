import logging
import pytest

from semantic_version import Version

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import blue_squad
from ocs_ci.framework.testlib import tier4c, runs_on_provider, skipif_managed_service
from ocs_ci.ocs import constants
from ocs_ci.utility import prometheus
from ocs_ci.ocs.ocp import OCP


logger = logging.getLogger(__name__)


@blue_squad
@tier4c
@pytest.mark.polarion_id("OCS-2323")
@runs_on_provider
@skipif_managed_service
def test_rgw_unavailable(measure_stop_rgw, threading_lock):
    """
    Test that there is appropriate alert when RGW is unavailable and that
    this alert is cleared when the RGW interface is back online.

    """
    logger.info("Starting test: Verify RGW unavailable alert triggers and clears")

    logger.test_step("Initialize Prometheus API and retrieve alerts from RGW downtime")
    api = prometheus.PrometheusAPI(threading_lock=threading_lock)
    alerts = measure_stop_rgw.get("prometheus_alerts")
    target_label = constants.ALERT_CLUSTEROBJECTSTORESTATE
    logger.info(f"Target alert: {target_label}")
    logger.info(f"Number of alerts retrieved: {len(alerts) if alerts else 0}")

    logger.test_step("Determine expected alert message based on OCS version")
    ocs_version = config.ENV_DATA["ocs_version"]
    logger.info(f"OCS version: {ocs_version}")

    if Version.coerce(ocs_version) < Version.coerce("4.7"):
        target_msg = (
            "Cluster Object Store is in unhealthy state for more than 15s. "
            "Please check Ceph cluster health or RGW connection."
        )
        logger.info("Using pre-4.7 alert message format")
    else:
        target_msg = (
            "Cluster Object Store is in unhealthy state or number of ready replicas for "
            "Rook Ceph RGW deployments is less than the desired replicas in "
            f"namespace:cluster {config.ENV_DATA['cluster_namespace']}:."
        )
        logger.info("Using OCS 4.7+ alert message format")

    states = ["pending", "firing"]
    logger.debug(f"Expected alert states: {states}")

    logger.test_step(
        "Validate ClusterObjectStoreState alert is present with correct properties"
    )
    prometheus.check_alert_list(
        label=target_label,
        msg=target_msg,
        alerts=alerts,
        states=states,
        severity="error",
    )
    logger.info(f"Alert {target_label} validated successfully")

    logger.test_step("Verify alert is cleared after RGW becomes available")
    stop_time = measure_stop_rgw.get("stop")
    logger.info(f"Checking alert clearance after time: {stop_time}")
    api.check_alert_cleared(
        label=target_label, measure_end_time=stop_time, time_min=300
    )
    logger.info(f"Alert {target_label} cleared successfully")

    logger.info("Test passed: RGW unavailable alert triggered and cleared as expected")


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
