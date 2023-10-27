import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import blue_squad
from ocs_ci.framework.testlib import (
    tier4c,
    bugzilla,
    skipif_managed_service,
    skipif_ocs_version,
)
from ocs_ci.ocs import constants
from ocs_ci.utility import prometheus
from ocs_ci.ocs.ocp import OCP


log = logging.getLogger(__name__)


@blue_squad
@tier4c
@pytest.mark.polarion_id("OCS-1052")
@skipif_managed_service
def test_ceph_manager_stopped(measure_stop_ceph_mgr, threading_lock):
    """
    Test that there is appropriate alert when ceph manager
    is unavailable and that this alert is cleared when the manager
    is back online.
    """
    api = prometheus.PrometheusAPI(threading_lock=threading_lock)

    # get alerts from time when manager deployment was scaled down
    alerts = measure_stop_ceph_mgr.get("prometheus_alerts")
    target_label = constants.ALERT_MGRISABSENT
    target_msg = "Storage metrics collector service not available anymore."
    states = ["pending", "firing"]

    prometheus.check_alert_list(
        label=target_label,
        msg=target_msg,
        alerts=alerts,
        states=states,
        severity="critical",
    )
    api.check_alert_cleared(
        label=target_label, measure_end_time=measure_stop_ceph_mgr.get("stop")
    )


@blue_squad
@tier4c
@pytest.mark.polarion_id("OCS-904")
@skipif_managed_service
def test_ceph_monitor_stopped(measure_stop_ceph_mon, threading_lock):
    """
    Test that there is appropriate alert related to ceph monitor quorum
    when there is even number of ceph monitors and that this alert
    is cleared when monitors are back online.
    """
    api = prometheus.PrometheusAPI(threading_lock=threading_lock)

    # get alerts from time when manager deployment was scaled down
    alerts = measure_stop_ceph_mon.get("prometheus_alerts")
    for target_label, target_msg, target_states, target_severity in [
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
    ]:
        prometheus.check_alert_list(
            label=target_label,
            msg=target_msg,
            alerts=alerts,
            states=target_states,
            severity=target_severity,
        )
        api.check_alert_cleared(
            label=target_label, measure_end_time=measure_stop_ceph_mon.get("stop")
        )


@blue_squad
@tier4c
@bugzilla("1944513")
@pytest.mark.polarion_id("OCS-2724")
@pytest.mark.parametrize("create_mon_quorum_loss", [True])
@skipif_managed_service
@skipif_ocs_version("<4.9")
def test_ceph_mons_quorum_lost(measure_stop_ceph_mon, threading_lock):
    """
    Test to verify that CephMonQuorumLost alert is seen and
    that this alert is cleared when monitors are back online.
    """
    api = prometheus.PrometheusAPI(threading_lock=threading_lock)

    # get alerts from time when manager deployment was scaled down
    alerts = measure_stop_ceph_mon.get("prometheus_alerts")
    target_label = constants.ALERT_MONQUORUMLOST
    target_msg = "Storage quorum is lost"
    target_states = ["pending", "firing"]

    prometheus.check_alert_list(
        label=target_label,
        msg=target_msg,
        alerts=alerts,
        states=target_states,
        severity="critical",
    )
    api.check_alert_cleared(
        label=target_label, measure_end_time=measure_stop_ceph_mon.get("stop")
    )


@blue_squad
@tier4c
@pytest.mark.polarion_id("OCS-900")
@skipif_managed_service
def test_ceph_osd_stopped(measure_stop_ceph_osd, threading_lock):
    """
    Test that there is appropriate alert related to situation when ceph osd
    is down. Alert is cleared when osd disk is back online.
    """
    api = prometheus.PrometheusAPI(threading_lock=threading_lock)

    # get alerts from time when manager deployment was scaled down
    alerts = measure_stop_ceph_osd.get("prometheus_alerts")
    for target_label, target_msg, target_states, target_severity, ignore in [
        (
            constants.ALERT_OSDDISKNOTRESPONDING,
            "Disk not responding",
            ["pending", "firing"],
            "error",
            False,
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
            False,
        ),
    ]:
        prometheus.check_alert_list(
            label=target_label,
            msg=target_msg,
            alerts=alerts,
            states=target_states,
            severity=target_severity,
            ignore_more_occurences=ignore,
        )
        # the time to wait is increased because it takes more time for osd pod
        # to be ready than for other pods
        osd_up_wait = 360
        api.check_alert_cleared(
            label=target_label,
            measure_end_time=measure_stop_ceph_osd.get("stop"),
            time_min=osd_up_wait,
        )


def teardown_module():
    ocs_obj = OCP()
    ocs_obj.login_as_sa()
