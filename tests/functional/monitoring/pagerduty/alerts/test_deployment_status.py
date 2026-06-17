import loggerging
import pytest

from ocs_ci.framework.pytest_customization.marks import blue_squad
from ocs_ci.framework.testlib import (
    managed_service_required,
    skipif_ms_consumer,
    tier4,
    tier4b,
    tier4c,
    runs_on_provider,
)
from ocs_ci.ocs import constants
from ocs_ci.utility import pagerduty


logger = loggerging.getLogger(__name__)


@blue_squad
@tier4
@tier4c
@managed_service_required
@runs_on_provider
@pytest.mark.polarion_id("OCS-2766")
def deprecated_test_ceph_manager_stopped_pd(measure_stop_ceph_mgr):
    """
    Test that there is appropriate incident in PagerDuty when ceph manager
    is unavailable and that this incident is cleared when the manager
    is back online.
    """
    logger.info("Starting test: Verify Ceph manager stopped PagerDuty incidents")

    logger.test_step("Initialize PagerDuty API and retrieve incidents")
    api = pagerduty.PagerDutyAPI()
    incidents = measure_stop_ceph_mgr.get("pagerduty_incidents")
    logger.info(f"Number of incidents retrieved: {len(incidents) if incidents else 0}")

    target_labels = [
        constants.ALERT_MGRISABSENT,
        constants.ALERT_MGRISMISSINGREPLICAS,
    ]
    logger.info(f"Checking {len(target_labels)} alert types")

    logger.test_step("Validate and verify clearance for each manager alert")
    for i, target_label in enumerate(target_labels, 1):
        logger.info(f"Processing alert {i}/{len(target_labels)}: {target_label}")

        # TODO(fbalak): check the whole string in summary and incident alerts
        logger.debug(f"Checking incident list for {target_label} with urgency=high")
        incident_found = pagerduty.check_incident_list(
            summary=target_label,
            incidents=incidents,
            urgency="high",
        )
        logger.assertion(
            f"Incident check for {target_label}: expected=True, actual={incident_found}"
        )
        assert incident_found, f"No high-urgency incident found for {target_label}"

        logger.debug(f"Verifying incident {target_label} is cleared")
        api.check_incident_cleared(
            summary=target_label,
            measure_end_time=measure_stop_ceph_mgr.get("stop"),
            pagerduty_service_ids=[pagerduty.get_pagerduty_service_id()],
        )
        logger.info(f"Alert {target_label} verified and cleared successfully")

    logger.info("Test passed: All Ceph manager incidents triggered and cleared")


@blue_squad
@tier4
@tier4c
@managed_service_required
@runs_on_provider
@pytest.mark.polarion_id("OCS-2769")
def deprecated_test_ceph_osd_stopped_pd(measure_stop_ceph_osd):
    """
    Test that there are appropriate incidents in PagerDuty when ceph osd
    is unavailable and that these incidents are cleared when the osd
    is back online.
    """
    logger.info("Starting test: Verify Ceph OSD stopped PagerDuty incidents")

    logger.test_step("Initialize PagerDuty API and retrieve incidents")
    api = pagerduty.PagerDutyAPI()
    incidents = measure_stop_ceph_osd.get("pagerduty_incidents")
    logger.info(f"Number of incidents retrieved: {len(incidents) if incidents else 0}")

    logger.test_step("Verify at least one OSD-related incident exists")
    # check that at least one of incidents CephOSDDisdUnavailable or
    # CephOSDDiskNotResponding is correctly raised
    logger.debug(f"Checking for {constants.ALERT_OSDDISKUNAVAILABLE}")
    disk_unavailable = pagerduty.check_incident_list(
        summary=constants.ALERT_OSDDISKUNAVAILABLE,
        incidents=incidents,
        urgency="high",
    )
    logger.debug(f"Checking for {constants.ALERT_OSDDISKNOTRESPONDING}")
    disk_not_responding = pagerduty.check_incident_list(
        summary=constants.ALERT_OSDDISKNOTRESPONDING,
        incidents=incidents,
        urgency="high",
    )

    osd_incident_found = disk_unavailable or disk_not_responding
    logger.assertion(
        f"OSD incident check: disk_unavailable={disk_unavailable}, "
        f"disk_not_responding={disk_not_responding}, at_least_one=expected"
    )
    assert osd_incident_found, (
        f"No high-urgency OSD incident found. "
        f"Expected {constants.ALERT_OSDDISKUNAVAILABLE} or {constants.ALERT_OSDDISKNOTRESPONDING}"
    )
    logger.info(
        f"OSD incident found: unavailable={disk_unavailable}, not_responding={disk_not_responding}"
    )

    logger.test_step("Verify both OSD alert types are cleared")
    logger.debug(f"Verifying {constants.ALERT_OSDDISKUNAVAILABLE} is cleared")
    api.check_incident_cleared(
        summary=constants.ALERT_OSDDISKUNAVAILABLE,
        measure_end_time=measure_stop_ceph_osd.get("stop"),
        pagerduty_service_ids=[pagerduty.get_pagerduty_service_id()],
    )
    logger.info(f"{constants.ALERT_OSDDISKUNAVAILABLE} cleared successfully")

    logger.debug(f"Verifying {constants.ALERT_OSDDISKNOTRESPONDING} is cleared")
    api.check_incident_cleared(
        summary=constants.ALERT_OSDDISKNOTRESPONDING,
        measure_end_time=measure_stop_ceph_osd.get("stop"),
        pagerduty_service_ids=[pagerduty.get_pagerduty_service_id()],
    )
    logger.info(f"{constants.ALERT_OSDDISKNOTRESPONDING} cleared successfully")

    logger.info("Test passed: Ceph OSD incidents triggered and cleared")


@blue_squad
@tier4
@tier4b
@managed_service_required
@skipif_ms_consumer
@pytest.mark.polarion_id("OCS-2770")
@pytest.mark.skip(reason="Shutting down 2 nodes at the same time is not supported")
def depricated_test_stop_worker_nodes_pd(measure_stop_worker_nodes):
    """
    Test that there are appropriate incidents in PagerDuty when two worker
    nodes are unavailable and that these incidents are cleared when those nodes
    are back online.
    """
    logger.info("Starting test: Verify worker node stopped PagerDuty incidents")

    logger.test_step("Initialize PagerDuty API and retrieve incidents")
    api = pagerduty.PagerDutyAPI()
    incidents = measure_stop_worker_nodes.get("pagerduty_incidents")
    logger.info(f"Number of incidents retrieved: {len(incidents) if incidents else 0}")

    logger.test_step("Validate and verify clearance for NodeDown alert")
    # check that incident CephNodeDown is correctly raised
    for target_label in [
        constants.ALERT_NODEDOWN,
    ]:
        logger.info(f"Checking incident for alert: {target_label}")

        incident_found = pagerduty.check_incident_list(
            summary=target_label,
            incidents=incidents,
            urgency="high",
        )
        logger.assertion(
            f"Incident check for {target_label}: expected=True, actual={incident_found}"
        )
        assert incident_found, f"No high-urgency incident found for {target_label}"

        logger.debug(f"Verifying incident {target_label} is cleared")
        api.check_incident_cleared(
            summary=target_label,
            measure_end_time=measure_stop_worker_nodes.get("stop"),
            pagerduty_service_ids=[pagerduty.get_pagerduty_service_id()],
        )
        logger.info(f"Alert {target_label} verified and cleared successfully")

    logger.info("Test passed: Worker node incidents triggered and cleared")


@blue_squad
@tier4
@tier4c
@managed_service_required
@runs_on_provider
@pytest.mark.polarion_id("OCS-3716")
def deprecated_test_ceph_monitor_stopped_pd(measure_stop_ceph_mon):
    """
    Test that there are appropriate incidents in PagerDuty when ceph monitor
    is unavailable and that these incidents are cleared when the monitor
    is back online.
    """
    logger.info("Starting test: Verify Ceph monitor stopped PagerDuty incidents")

    logger.test_step("Initialize PagerDuty API and retrieve incidents")
    api = pagerduty.PagerDutyAPI()
    incidents = measure_stop_ceph_mon.get("pagerduty_incidents")
    logger.info(f"Number of incidents retrieved: {len(incidents) if incidents else 0}")

    target_labels = [
        constants.ALERT_MONQUORUMATRISK,
        constants.ALERT_CLUSTERWARNINGSTATE,
    ]
    logger.info(f"Checking {len(target_labels)} alert types")

    logger.test_step("Validate and verify clearance for each monitor alert")
    # check that incidents CephMonQuorumAtRisk and CephClusterWarningState
    # alert are correctly raised
    for i, target_label in enumerate(target_labels, 1):
        logger.info(f"Processing alert {i}/{len(target_labels)}: {target_label}")

        incident_found = pagerduty.check_incident_list(
            summary=target_label,
            incidents=incidents,
            urgency="high",
        )
        logger.assertion(
            f"Incident check for {target_label}: expected=True, actual={incident_found}"
        )
        assert incident_found, f"No high-urgency incident found for {target_label}"

        # adding 1 extra minute to wait for clearing of the alert
        # CephMonQuorumAtRisk alert takes longer time to be cleared
        time_min = 480
        logger.debug(f"Verifying {target_label} is cleared (timeout={time_min}min)")
        api.check_incident_cleared(
            summary=target_label,
            measure_end_time=measure_stop_ceph_mon.get("stop"),
            time_min=time_min,
            pagerduty_service_ids=[pagerduty.get_pagerduty_service_id()],
        )
        logger.info(f"Alert {target_label} verified and cleared successfully")

    logger.info("Test passed: All Ceph monitor incidents triggered and cleared")


@blue_squad
@tier4
@tier4c
@managed_service_required
@runs_on_provider
@pytest.mark.polarion_id("OCS-3717")
@pytest.mark.parametrize("create_mon_quorum_loss", [True])
def deprecated_test_ceph_mons_quorum_lost_pd(measure_stop_ceph_mon):
    """
    Test that there are appropriate incidents in PagerDuty when ceph monitors
    except one are unavailable and that these incidents are cleared when the
    monitor is back online.
    """
    logger.info("Starting test: Verify Ceph monitor quorum lost PagerDuty incidents")

    logger.test_step("Initialize PagerDuty API and retrieve incidents")
    api = pagerduty.PagerDutyAPI()
    incidents = measure_stop_ceph_mon.get("pagerduty_incidents")
    logger.info(f"Number of incidents retrieved: {len(incidents) if incidents else 0}")

    logger.test_step("Validate MonQuorumLost incident and verify clearance")
    # check that incident CephMonQuorumLost is correctly raised
    target_label = constants.ALERT_MONQUORUMLOST
    logger.info(f"Checking incident for alert: {target_label}")

    incident_found = pagerduty.check_incident_list(
        summary=target_label,
        incidents=incidents,
        urgency="high",
    )
    logger.assertion(
        f"Incident check for {target_label}: expected=True, actual={incident_found}"
    )
    assert incident_found, f"No high-urgency incident found for {target_label}"

    logger.debug(f"Verifying incident {target_label} is cleared")
    api.check_incident_cleared(
        summary=target_label,
        measure_end_time=measure_stop_ceph_mon.get("stop"),
        pagerduty_service_ids=[pagerduty.get_pagerduty_service_id()],
    )
    logger.info(f"Alert {target_label} verified and cleared successfully")

    logger.info("Test passed: Monitor quorum lost incident triggered and cleared")
