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

    alerts_response = prometheus.get(
        'alerts',
        payload={
            'silenced': False,
            'inhibited': False,
            'start': int(workload_stop_ceph_mgr['start']),
            'stop': int(workload_stop_ceph_mgr['stop']),
        }
    )
    assert alerts_response.ok is True
    alerts = alerts_response.json()['data']['alerts']
    log.info('Getting Prometheus alerts to check if alert is present.')
    log.info(f"Prometheus Alerts: {alerts}")
    target_label = 'CephMgrIsAbsent'
    target_alerts = [alert for alert in alerts if alert['labels']['alertname'] == target_label]
    assert len(target_alerts) == 1, f"Incorrect number of {target_label} alerts"

    # seconds to wait before alert is cleared
    wait_clear = 20
    time.sleep(wait_clear+5)
    alerts_response = prometheus.get(
        'alerts',
        payload={
            'silenced': False,
            'inhibited': False,
            'start': int(workload_stop_ceph_mgr['stop'])+wait_clear,
            'stop': int(workload_stop_ceph_mgr['stop'])+wait_clear+5,
        }
    )
    assert alerts_response.ok is True
    log.info('Getting Prometheus alerts to check if alert is cleared.')
    alerts = alerts_response.json()['data']['alerts']
    log.info(f"Prometheus Alerts: {alerts}")
    target_alerts = [alert for alert in alerts if alert['labels']['alertname'] == target_label]
    assert len(target_alerts) == 0, f"Too many {target_label} alerts"

    alerts_response = prometheus.get(
        'alerts',
        payload={
            'silenced': False,
            'inhibited': False,
            'start': int(workload_stop_ceph_mgr['start']),
            'stop': int(workload_stop_ceph_mgr['stop']),
        }
    )
    assert alerts_response.ok is True
    alerts = alerts_response.json()['data']['alerts']
    log.info('Getting Prometheus alerts to check if alert is present.')
    log.info(f"Prometheus Alerts: {alerts}")
