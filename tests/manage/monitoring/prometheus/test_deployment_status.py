import logging
import pytest
import time

from ocs_ci.framework.testlib import tier4
from ocs_ci.utility import prometheus


log = logging.getLogger(__name__)


@tier4
@pytest.mark.polarion_id("OCS-1052")
def test_ceph_manager_stopped(workload_stop_ceph_mgr):
    """
    Test that there is appropriate alert when ceph manager
    is unavailable and that this alert is cleared when the manager
    is back online.
    """
    api = prometheus.PrometheusAPI()

    # get alerts from time when manager deployment was scaled down
    alerts = workload_stop_ceph_mgr.get('prometheus_alerts')
    target_label = 'CephMgrIsAbsent'
    target_msg = 'Storage metrics collector service not available anymore.'
    prometheus.check_alert_list(target_label, target_msg, alerts)

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
    cleared_alerts = api.wait_for_alert(
        name=target_label,
        state=None,
        timeout=time_wait
    )
    log.info(f"Cleared alerts: {cleared_alerts}")
    assert len(cleared_alerts) == 0, f"{target_label} alerts were not cleared"
    log.info(f"{target_label} alerts were cleared")
