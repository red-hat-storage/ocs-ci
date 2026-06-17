import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import blue_squad
from ocs_ci.framework.testlib import (
    tier4c,
    runs_on_provider,
    hci_provider_and_client_required,
)
from ocs_ci.ocs import constants
from ocs_ci.utility import prometheus
from ocs_ci.ocs.ocp import OCP


logger = logging.getLogger(__name__)


@blue_squad
@tier4c
@runs_on_provider
@hci_provider_and_client_required
@pytest.mark.polarion_id("OCS-5392")
def test_change_client_ocs_version_and_stop_heartbeat(
    measure_change_client_ocs_version_and_stop_heartbeat, threading_lock
):
    """
    Test that there are appropriate alerts raised when ocs version of client
    is changed to a different version and those alerts are cleared when the
    heartbeat is resumed. During the test is stopped heartbeat cronjob on
    client in order to stop overwritting the version set for testing. When the
    heartbeat is resumed thereshould be also resumed version reporting so the
    version should contain previous version.

    """
    logger.info("Starting test: Verify client version change and heartbeat stop alerts")

    logger.test_step("Initialize Prometheus API and retrieve test metadata")
    api = prometheus.PrometheusAPI(threading_lock=threading_lock)

    alerts = measure_change_client_ocs_version_and_stop_heartbeat.get(
        "prometheus_alerts"
    )
    client_name = measure_change_client_ocs_version_and_stop_heartbeat.get(
        "metadata"
    ).get("client_name")
    cluster_namespace = config.ENV_DATA["cluster_namespace"]
    cluster_name = config.ENV_DATA["storage_cluster_name"]

    logger.info(f"Client name: {client_name}")
    logger.info(f"Cluster namespace: {cluster_namespace}")
    logger.info(f"Cluster name: {cluster_name}")
    logger.info(f"Number of alerts retrieved: {len(alerts) if alerts else 0}")

    logger.test_step("Define expected alerts for client version change and heartbeat")
    target_alerts = [
        {
            "label": constants.ALERT_STORAGECLIENTHEARTBEATMISSED,
            "msg": (
                f"Storage Client ({client_name}) heartbeat missed for more than 120 (s) "
                f"in namespace:cluster {cluster_namespace}:{cluster_name}."
            ),
            "severity": "warning",
        },
        {
            "label": constants.ALERT_STORAGECLIENTHEARTBEATMISSED,
            "msg": (
                f"Storage Client ({client_name}) heartbeat missed for more than 300 (s) "
                f"in namespace:cluster {cluster_namespace}:{cluster_name}."
            ),
            "severity": "critical",
        },
        {
            "label": constants.ALERT_STORAGECLIENTINCOMPATIBLEOPERATORVERSION,
            "msg": (
                f"Storage Client Operator ({client_name}) differs by more than 1 minor "
                f"version in namespace:cluster {cluster_namespace}:{cluster_name}."
            ),
            "severity": "critical",
        },
    ]
    states = ["firing"]
    logger.info(f"Checking {len(target_alerts)} expected alerts")
    logger.debug(f"Expected alert states: {states}")

    logger.test_step("Validate and verify clearance for each expected alert")
    for i, target_alert in enumerate(target_alerts, 1):
        alert_label = target_alert["label"]
        alert_severity = target_alert["severity"]
        logger.info(
            f"Processing alert {i}/{len(target_alerts)}: {alert_label} "
            f"(severity: {alert_severity})"
        )

        logger.debug(f"Validating alert {alert_label} is present in firing state")
        prometheus.check_alert_list(
            label=alert_label,
            msg=target_alert["msg"],
            alerts=alerts,
            states=states,
            severity=alert_severity,
        )
        logger.info(f"Alert {alert_label} validated successfully")

        logger.debug(
            f"Verifying alert {alert_label} is cleared after heartbeat resumes"
        )
        api.check_alert_cleared(
            label=alert_label,
            measure_end_time=measure_change_client_ocs_version_and_stop_heartbeat.get(
                "stop"
            ),
            time_min=300,
        )
        logger.info(f"Alert {alert_label} cleared successfully")

    logger.info(
        "Test passed: All client version and heartbeat alerts triggered and cleared as expected"
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
