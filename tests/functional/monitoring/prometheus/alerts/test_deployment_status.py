import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import blue_squad, provider_mode
from ocs_ci.framework.testlib import (
    tier4c,
    skipif_managed_service,
    runs_on_provider,
    skipif_ocs_version,
)
from ocs_ci.ocs import constants
from ocs_ci.utility import prometheus
from ocs_ci.ocs.ocp import OCP


logger = logging.getLogger(__name__)


@provider_mode
@blue_squad
@tier4c
@pytest.mark.polarion_id("OCS-1052")
@skipif_managed_service
@runs_on_provider
def test_ceph_manager_stopped(measure_stop_ceph_mgr, threading_lock):
    """
    Test that there is appropriate alert when ceph manager
    is unavailable and that this alert is cleared when the manager
    is back online.
    """
    logger.info("Starting test: Verify Ceph manager stopped alert triggers and clears")

    logger.test_step("Initialize Prometheus API and retrieve alerts")
    api = prometheus.PrometheusAPI(threading_lock=threading_lock)
    alerts = measure_stop_ceph_mgr.get("prometheus_alerts")
    target_label = constants.ALERT_MGRISABSENT
    target_msg = "Storage metrics collector service not available anymore."
    states = ["pending", "firing"]

    logger.info(f"Target alert: {target_label}")
    logger.info(f"Number of alerts retrieved: {len(alerts) if alerts else 0}")
    logger.debug(f"Expected states: {states}, severity: critical")

    logger.test_step("Validate MgrIsAbsent alert is present with correct properties")
    prometheus.check_alert_list(
        label=target_label,
        msg=target_msg,
        alerts=alerts,
        states=states,
        severity="critical",
    )
    logger.info(f"Alert {target_label} validated successfully")

    logger.test_step("Verify alert is cleared after manager recovery")
    stop_time = measure_stop_ceph_mgr.get("stop")
    logger.info(f"Checking alert clearance after time: {stop_time}")
    api.check_alert_cleared(label=target_label, measure_end_time=stop_time)
    logger.info(f"Alert {target_label} cleared successfully")

    logger.info(
        "Test passed: Ceph manager stopped alert triggered and cleared as expected"
    )


@provider_mode
@blue_squad
@tier4c
@pytest.mark.polarion_id("OCS-904")
@skipif_managed_service
@runs_on_provider
def test_ceph_monitor_stopped(measure_stop_ceph_mon, threading_lock):
    """
    Test that there is appropriate alert related to ceph monitor quorum
    when there is even number of ceph monitors and that this alert
    is cleared when monitors are back online.
    """
    logger.info("Starting test: Verify Ceph monitor stopped alerts trigger and clear")

    logger.test_step("Initialize Prometheus API and retrieve alerts")
    api = prometheus.PrometheusAPI(threading_lock=threading_lock)
    alerts = measure_stop_ceph_mon.get("prometheus_alerts")
    logger.info(f"Number of alerts retrieved: {len(alerts) if alerts else 0}")

    alert_configs = [
        (
            constants.ALERT_MONQUORUMATRISK,
            "Storage quorum at risk",
            ["pending"],
            "error",
        ),
        (
            constants.ALERT_CLUSTERWARNINGSTATE,
            "Storage cluster is in degraded state",
            ["pending"],
            "warning",
        ),
    ]
    logger.info(f"Checking {len(alert_configs)} alert types")

    logger.test_step("Validate and verify clearance for each monitor alert")
    for i, (target_label, target_msg, target_states, target_severity) in enumerate(
        alert_configs, 1
    ):
        logger.info(
            f"Processing alert {i}/{len(alert_configs)}: {target_label} "
            f"(severity: {target_severity})"
        )

        logger.debug(f"Validating {target_label} with states={target_states}")
        prometheus.check_alert_list(
            label=target_label,
            msg=target_msg,
            alerts=alerts,
            states=target_states,
            severity=target_severity,
        )
        logger.info(f"Alert {target_label} validated successfully")

        logger.debug(f"Verifying {target_label} is cleared")
        api.check_alert_cleared(
            label=target_label, measure_end_time=measure_stop_ceph_mon.get("stop")
        )
        logger.info(f"Alert {target_label} cleared successfully")

    logger.info(
        "Test passed: All Ceph monitor alerts triggered and cleared as expected"
    )


@provider_mode
@blue_squad
@tier4c
@pytest.mark.polarion_id("OCS-2724")
@pytest.mark.parametrize("create_mon_quorum_loss", [True])
@skipif_managed_service
@runs_on_provider
@skipif_ocs_version("<4.9")
def test_ceph_mons_quorum_lost(measure_stop_ceph_mon, threading_lock):
    """
    Test to verify that CephMonQuorumLost alert is seen and
    that this alert is cleared when monitors are back online.
    """
    logger.info(
        "Starting test: Verify Ceph monitor quorum lost alert triggers and clears"
    )

    logger.test_step("Initialize Prometheus API and retrieve alerts")
    api = prometheus.PrometheusAPI(threading_lock=threading_lock)
    alerts = measure_stop_ceph_mon.get("prometheus_alerts")
    target_label = constants.ALERT_MONQUORUMLOST
    target_msg = "Storage quorum is lost"
    target_states = ["pending", "firing"]

    logger.info(f"Target alert: {target_label}")
    logger.info(f"Number of alerts retrieved: {len(alerts) if alerts else 0}")
    logger.debug(f"Expected states: {target_states}, severity: critical")

    logger.test_step("Validate MonQuorumLost alert is present with correct properties")
    prometheus.check_alert_list(
        label=target_label,
        msg=target_msg,
        alerts=alerts,
        states=target_states,
        severity="critical",
    )
    logger.info(f"Alert {target_label} validated successfully")

    logger.test_step("Verify alert is cleared after monitors recovery")
    stop_time = measure_stop_ceph_mon.get("stop")
    logger.info(f"Checking alert clearance after time: {stop_time}")
    api.check_alert_cleared(label=target_label, measure_end_time=stop_time)
    logger.info(f"Alert {target_label} cleared successfully")

    logger.info(
        "Test passed: Monitor quorum lost alert triggered and cleared as expected"
    )


@blue_squad
@tier4c
@pytest.mark.polarion_id("OCS-900")
@skipif_managed_service
@runs_on_provider
def test_ceph_osd_stopped(measure_stop_ceph_osd, threading_lock):
    """
    Test that there is appropriate alert related to situation when ceph osd
    is down. Alert is cleared when osd disk is back online.
    """
    logger.info("Starting test: Verify Ceph OSD stopped alerts trigger and clear")

    logger.test_step("Initialize Prometheus API and retrieve alerts")
    api = prometheus.PrometheusAPI(threading_lock=threading_lock)
    alerts = measure_stop_ceph_osd.get("prometheus_alerts")
    logger.info(f"Number of alerts retrieved: {len(alerts) if alerts else 0}")

    alert_configs = [
        (
            constants.ALERT_OSDDISKNOTRESPONDING,
            "Disk not responding",
            ["pending", "firing"],
            "error",
            True,
        ),
        (
            constants.ALERT_DATARECOVERYTAKINGTOOLONG,
            "Data recovery is slow",
            ["pending"],
            "warning",
            True,
        ),
        (
            constants.ALERT_CLUSTERWARNINGSTATE,
            "Storage cluster is in degraded state",
            ["pending", "firing"],
            "warning",
            True,
        ),
    ]
    logger.info(f"Checking {len(alert_configs)} alert types")

    logger.test_step("Validate and verify clearance for each OSD alert")
    # the time to wait is increased because it takes more time for osd pod
    # to be ready than for other pods
    osd_up_wait = 360
    logger.debug(f"OSD clearance timeout: {osd_up_wait}min")

    for i, (
        target_label,
        target_msg,
        target_states,
        target_severity,
        ignore,
    ) in enumerate(alert_configs, 1):
        logger.info(
            f"Processing alert {i}/{len(alert_configs)}: {target_label} "
            f"(severity: {target_severity}, ignore_more: {ignore})"
        )

        logger.debug(f"Validating {target_label} with states={target_states}")
        prometheus.check_alert_list(
            label=target_label,
            msg=target_msg,
            alerts=alerts,
            states=target_states,
            severity=target_severity,
            ignore_more_occurences=ignore,
        )
        logger.info(f"Alert {target_label} validated successfully")

        logger.debug(f"Verifying {target_label} is cleared (timeout={osd_up_wait}min)")
        api.check_alert_cleared(
            label=target_label,
            measure_end_time=measure_stop_ceph_osd.get("stop"),
            time_min=osd_up_wait,
        )
        logger.info(f"Alert {target_label} cleared successfully")

    logger.info("Test passed: All Ceph OSD alerts triggered and cleared as expected")


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
