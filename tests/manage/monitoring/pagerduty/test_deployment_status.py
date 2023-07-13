import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    bugzilla,
    managed_service_required,
    skipif_ms_consumer,
    tier4,
    tier4b,
    tier4c,
    runs_on_provider,
)
from ocs_ci.ocs import constants
from ocs_ci.utility import pagerduty


log = logging.getLogger(__name__)


@tier4
@tier4c
@managed_service_required
@runs_on_provider
@bugzilla("2033284")
@pytest.mark.polarion_id("OCS-2766")
def test_ceph_manager_stopped_pd(measure_stop_ceph_mgr):
    """
    Test that there is appropriate incident in PagerDuty when ceph manager
    is unavailable and that this incident is cleared when the manager
    is back online.
    """
    api = pagerduty.PagerDutyAPI()

    # get incidents from time when manager deployment was scaled down
    incidents = measure_stop_ceph_mgr.get("pagerduty_incidents")
    for target_label in [
        constants.ALERT_MGRISABSENT,
        constants.ALERT_MGRISMISSINGREPLICAS,
    ]:

        # TODO(fbalak): check the whole string in summary and incident alerts
        assert pagerduty.check_incident_list(
            summary=target_label,
            incidents=incidents,
            urgency="high",
        )
        api.check_incident_cleared(
            summary=target_label,
            measure_end_time=measure_stop_ceph_mgr.get("stop"),
            pagerduty_service_ids=[config.RUN["pagerduty_service_id"]],
        )


@tier4
@tier4c
@managed_service_required
@runs_on_provider
@pytest.mark.polarion_id("OCS-2769")
def test_ceph_osd_stopped_pd(measure_stop_ceph_osd):
    """
    Test that there are appropriate incidents in PagerDuty when ceph osd
    is unavailable and that these incidents are cleared when the osd
    is back online.
    """
    api = pagerduty.PagerDutyAPI()

    # get incidents from time when osd deployment was scaled down
    incidents = measure_stop_ceph_osd.get("pagerduty_incidents")

    # check that incident CephOSDDisdUnavailable is correctly raised
    for target_label in [
        constants.ALERT_OSDDISKUNAVAILABLE,
    ]:
        assert pagerduty.check_incident_list(
            summary=target_label,
            incidents=incidents,
            urgency="high",
        )
        api.check_incident_cleared(
            summary=target_label,
            measure_end_time=measure_stop_ceph_osd.get("stop"),
            pagerduty_service_ids=[config.RUN["pagerduty_service_id"]],
        )


@tier4
@tier4b
@managed_service_required
@skipif_ms_consumer
@bugzilla("2072612")
@pytest.mark.polarion_id("OCS-2770")
@pytest.mark.skip(reason="Shutting down 2 nodes at the same time is not supported")
def test_stop_worker_nodes_pd(measure_stop_worker_nodes):
    """
    Test that there are appropriate incidents in PagerDuty when two worker
    nodes are unavailable and that these incidents are cleared when those nodes
    are back online.
    """
    api = pagerduty.PagerDutyAPI()

    # get incidents from time when node is down
    incidents = measure_stop_worker_nodes.get("pagerduty_incidents")

    # check that incident CephNodeDown is correctly raised
    for target_label in [
        constants.ALERT_NODEDOWN,
    ]:
        assert pagerduty.check_incident_list(
            summary=target_label,
            incidents=incidents,
            urgency="high",
        )
        api.check_incident_cleared(
            summary=target_label,
            measure_end_time=measure_stop_worker_nodes.get("stop"),
            pagerduty_service_ids=[config.RUN["pagerduty_service_id"]],
        )


@tier4
@tier4c
@managed_service_required
@runs_on_provider
@pytest.mark.polarion_id("OCS-3716")
def test_ceph_monitor_stopped_pd(measure_stop_ceph_mon):
    """
    Test that there are appropriate incidents in PagerDuty when ceph monitor
    is unavailable and that these incidents are cleared when the monitor
    is back online.
    """
    api = pagerduty.PagerDutyAPI()

    # get incidents from time when monitor deployment was scaled down
    incidents = measure_stop_ceph_mon.get("pagerduty_incidents")

    # check that incidents CephMonQuorumAtRisk and CephClusterWarningState
    # alert are correctly raised
    for target_label in [
        constants.ALERT_MONQUORUMATRISK,
        constants.ALERT_CLUSTERWARNINGSTATE,
    ]:
        assert pagerduty.check_incident_list(
            summary=target_label,
            incidents=incidents,
            urgency="high",
        )
        # adding 1 extra minute to wait for clearing of the alert
        # CephMonQuorumAtRisk alert takes longer time to be cleared
        time_min = 480
        api.check_incident_cleared(
            summary=target_label,
            measure_end_time=measure_stop_ceph_mon.get("stop"),
            time_min=time_min,
            pagerduty_service_ids=[config.RUN["pagerduty_service_id"]],
        )


@tier4
@tier4c
@managed_service_required
@runs_on_provider
@bugzilla("2076670")
@pytest.mark.polarion_id("OCS-3717")
@pytest.mark.parametrize("create_mon_quorum_loss", [True])
def test_ceph_mons_quorum_lost_pd(measure_stop_ceph_mon):
    """
    Test that there are appropriate incidents in PagerDuty when ceph monitors
    except one are unavailable and that these incidents are cleared when the
    monitor is back online.
    """
    api = pagerduty.PagerDutyAPI()

    # get incidents from time when monitor deployments were scaled down
    incidents = measure_stop_ceph_mon.get("pagerduty_incidents")

    # check that incident CephMonQuorumLost is correctly raised
    target_label = constants.ALERT_MONQUORUMLOST
    assert pagerduty.check_incident_list(
        summary=target_label,
        incidents=incidents,
        urgency="high",
    )
    api.check_incident_cleared(
        summary=target_label,
        measure_end_time=measure_stop_ceph_mon.get("stop"),
        pagerduty_service_ids=[config.RUN["pagerduty_service_id"]],
    )
