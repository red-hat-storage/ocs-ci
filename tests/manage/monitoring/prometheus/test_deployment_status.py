import logging
import time

import ocs_ci.utility.prometheus


log = logging.getLogger(__name__)


def test_ceph_manager_stopped(workload_stop_ceph_mgr):
    """
    Test that there is appropriate alert when ceph manager
    is unavailable and that this alert is cleared when the manager
    is back online.
    """
    prometheus = ocs_ci.utility.prometheus.PrometheusAPI()

    alerts = workload_stop_ceph_mgr['prometheus_alerts']
    target_label = 'CephMgrIsAbsent'
    target_alerts = [
        alert for alert in alerts if alert[
            'labels']['alertname'] == target_label
    ]
    assert len(
        target_alerts) == 2, f"Incorrect number of {target_label} alerts"
    assert target_alerts[0]['annotations']['severity_level'] == 'warning'
    assert target_alerts[0]['state'] == 'pending'
    assert target_alerts[1]['annotations']['severity_level'] == 'warning'
    assert target_alerts[1]['state'] == 'firing'

    # seconds to wait before alert is cleared after measurement is finished
    time_min = 20

    time_actual = int(time.time())
    time_sleep = (workload_stop_ceph_mgr['stop'] + time_min) - time_actual
    if time_sleep > 0:
        log.info(f"Waiting {time_sleep} seconds "
                 f"({time_min} seconds since measurement end)")
        time.sleep(time_sleep)
    alerts_response = prometheus.get(
        'alerts',
        payload={
            'silenced': False,
            'inhibited': False,
        }
    )
    assert alerts_response.ok is True
    log.info('Getting Prometheus alerts to check if alert is cleared.')
    alerts = alerts_response.json()['data']['alerts']
    log.info(f"Prometheus Alerts: {alerts}")
    target_alerts = [
        alert for alert in alerts if alert[
            'labels']['alertname'] == target_label
    ]
    assert len(target_alerts) == 0, f"Too many {target_label} alerts"
