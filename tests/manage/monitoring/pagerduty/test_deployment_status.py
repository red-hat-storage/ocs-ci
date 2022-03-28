import logging
import pytest

from ocs_ci.framework.testlib import (
    bugzilla,
    managed_service_required,
    tier4,
    tier4b,
    tier4c,
)
from ocs_ci.ocs import constants
from ocs_ci.utility import pagerduty


log = logging.getLogger(__name__)


@tier4
@tier4c
@managed_service_required
@bugzilla("1998056")
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
    target_label = constants.ALERT_MGRISABSENT

    # TODO(fbalak): check the whole string in summary and incident alerts
    assert pagerduty.check_incident_list(
        summary=target_label,
        incidents=incidents,
        urgency="high",
    )
    api.check_incident_cleared(
        summary=target_label, measure_end_time=measure_stop_ceph_mgr.get("stop")
    )


@tier4
@tier4c
@managed_service_required
@pytest.mark.polarion_id("OCS-2769")
def test_ceph_osd_stopped_pd(measure_stop_ceph_osd):
    """
    Test that there are appropriate incidents in PagerDuty when ceph osd
    is unavailable and that these incidents are cleared when the osd
    is back online.
    """
    api = pagerduty.PagerDutyAPI()

    # get incidents from time when manager deployment was scaled down
    incidents = measure_stop_ceph_osd.get("pagerduty_incidents")

    # check that incidents CephOSDDisdNotResponding and CephClusterWarningState
    # alert are correctly raised
    for target_label in [
        constants.ALERT_OSDDISKNOTRESPONDING,
        constants.ALERT_CLUSTERWARNINGSTATE,
    ]:
        assert pagerduty.check_incident_list(
            summary=target_label,
            incidents=incidents,
            urgency="high",
        )
        api.check_incident_cleared(
            summary=target_label, measure_end_time=measure_stop_ceph_osd.get("stop")
        )


@tier4
@tier4b
@managed_service_required
@pytest.mark.polarion_id("OCS-2770")
def test_stop_worker_nodes_pd(measure_stop_worker_nodes):
    """
    Test that there are appropriate incidents in PagerDuty when two worker
    nodes are unavailable and that these incidents are cleared when those nodes
    are back online.
    """
    api = pagerduty.PagerDutyAPI()

    # get incidents from time when manager deployment was scaled down
    incidents = measure_stop_worker_nodes.get("pagerduty_incidents")

    # check that incidents  and CephClusterErrorState
    # alert are correctly raised
    for target_label in [
        constants.ALERT_NODEDOWN,
    ]:
        assert pagerduty.check_incident_list(
            summary=target_label,
            incidents=incidents,
            urgency="high",
        )
        api.check_incident_cleared(
            summary=target_label, measure_end_time=measure_stop_worker_nodes.get("stop")
        )
