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

    alerts = workload_stop_ceph_mgr['prometheus_alerts']
    target_label = 'CephMgrIsAbsent'
    target_alerts = [
        alert for alert in alerts if alert[
            'labels']['alertname'] == target_label
    ]
    assert len(
        target_alerts) == 2, f"Incorrect number of {target_label} alerts"
    assert target_alerts[0]['annotations'][
        'severity_level'
    ] == 'warning', 'First alert doesn\'t have warning severity'
    assert target_alerts[0][
        'state'
    ] == 'pending', 'First alert is not in pending state'
    assert target_alerts[1]['annotations'][
        'severity_level'
    ] == 'warning', 'Second alert doesn\'t have warning severity'
    assert target_alerts[1][
        'state'
    ] == 'firing', 'First alert is not in firing state'

    # seconds to wait before alert is cleared after measurement is finished
    time_min = 30

    time_actual = time.time()
    time_sleep = int(
        (workload_stop_ceph_mgr['stop'] + time_min) - time_actual
    )
    if time_sleep > 0:
        log.info(f"Waiting for approximately {time_sleep} seconds for alerts "
                 f"to be cleared ({time_min} seconds since measurement end)")
        # search every 5 seconds if alerts are already cleared
        while time_sleep > 0:
            alerts_response = prometheus.get(
                'alerts',
                payload={
                    'silenced': False,
                    'inhibited': False,
                }
            )
            assert alerts_response.ok is True, 'Prometheus API request failed'
            target_alerts = [
                alert for alert in alerts if alert[
                    'labels']['alertname'] == target_label
            ]
            log.info(f"Checking for {target_label} alerts... "
                     f"{len(target_alerts)} found")
            if len(target_alerts) == 0:
                log.info('Alerts already cleared, continuing...')
                break
            time.sleep(5)
            time_sleep -= 5
    else:
        alerts_response = prometheus.get(
            'alerts',
            payload={
                'silenced': False,
                'inhibited': False,
            }
        )
    assert alerts_response.ok is True, 'Prometheus API request failed'
    log.info('Getting Prometheus alerts to check if alert is cleared.')
    alerts = alerts_response.json()['data']['alerts']
    log.info(f"Prometheus Alerts: {alerts}")
    target_alerts = [
        alert for alert in alerts if alert[
            'labels']['alertname'] == target_label
    ]
    assert len(target_alerts) == 0, f"Too many {target_label} alerts"
