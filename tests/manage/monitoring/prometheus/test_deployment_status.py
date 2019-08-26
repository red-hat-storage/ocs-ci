import logging
import pytest
import time

from ocs_ci.framework.testlib import tier4
from ocs_ci.utility.prometheus import PrometheusAPI


log = logging.getLogger(__name__)


@tier4
@pytest.mark.polarion_id("OCS-1052")
def test_ceph_manager_stopped(workload_stop_ceph_mgr):
    """
    Test that there is appropriate alert when ceph manager
    is unavailable and that this alert is cleared when the manager
    is back online.
    """
    prometheus = PrometheusAPI()

    # get alerts from time when manager deployment was scaled down
    alerts = workload_stop_ceph_mgr.get('prometheus_alerts')
    target_label = 'CephMgrIsAbsent'
    target_msg = 'Storage metrics collector service not available anymore.'
    target_alerts = [
        alert
        for alert
        in alerts
        if alert.get('labels').get('alertname') == target_label
    ]
    log.info(f"Checking properties of found {target_label} alerts")
    msg = f"Incorrect number of {target_label} alerts"
    assert len(target_alerts) == 2, msg

    msg = 'Alert message is not correct'
    assert target_alerts[0]['annotations']['message'] == target_msg, msg

    msg = 'First alert doesn\'t have warning severity'
    assert target_alerts[0]['annotations']['severity_level'] == 'warning', msg

    msg = 'First alert is not in pending state'
    assert target_alerts[0]['state'] == 'pending', msg

    msg = 'Alert message is not correct'
    assert target_alerts[1]['annotations']['message'] == target_msg, msg

    msg = 'Second alert doesn\'t have warning severity'
    assert target_alerts[1]['annotations']['severity_level'] == 'warning', msg

    msg = 'First alert is not in firing state'
    assert target_alerts[1]['state'] == 'firing', msg

    log.info(f"Alerts were triggered correctly during utilization")

    # seconds to wait before alert is cleared after measurement is finished
    time_min = 30

    time_actual = time.time()
    time_wait = int(
        (workload_stop_ceph_mgr.get('stop') + time_min) - time_actual
    )
    if time_wait > 0:
        log.info(f"Waiting for approximately {time_wait} seconds for alerts "
                 f"to be cleared ({time_min} seconds since measurement end)")
    else:
        time_wait = 1
    cleared_alerts = prometheus.wait_for_alert(
        name=target_label,
        state=None,
        timeout=time_wait
    )
    log.info(f"Cleared alerts: {cleared_alerts}")
    assert len(cleared_alerts) == 0, f"{target_label} alerts were not cleared"
    log.info(f"{target_label} alerts were cleared")
