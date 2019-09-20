import logging
import pytest

from ocs_ci.framework.testlib import tier4
from ocs_ci.ocs import constants
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
    target_label = constants.ALERT_MGRISABSENT
    target_msg = 'Storage metrics collector service not available anymore.'
    states = ['pending', 'firing']

    prometheus.check_alert_list(
        label=target_label,
        msg=target_msg,
        alerts=alerts,
        states=states,
        severity='critical')
    api.check_alert_cleared(
        label=target_label,
        measure_end_time=workload_stop_ceph_mgr.get('stop')
    )


@tier4
@pytest.mark.polarion_id("OCS-904")
def test_ceph_monitor_stopped(workload_stop_ceph_mon):
    """
    Test that there is appropriate alert related to ceph monitor quorum
    when there is even number of ceph monitors and that this alert
    is cleared when monitors are back online.
    """
    api = prometheus.PrometheusAPI()

    # get alerts from time when manager deployment was scaled down
    alerts = workload_stop_ceph_mon.get('prometheus_alerts')
    for target_label, target_msg, target_states, target_severity in [
        (
            constants.ALERT_MONQUORUMATRISK,
            'Storage quorum at risk',
            ['pending'],
            'error'
        ),
        (
            constants.ALERT_CLUSTERWARNINGSTATE,
            'Storage cluster is in degraded state',
            ['pending', 'firing'],
            'warning'
        )
    ]:
        prometheus.check_alert_list(
            label=target_label,
            msg=target_msg,
            alerts=alerts,
            states=target_states,
            severity=target_severity
        )
        api.check_alert_cleared(
            label=target_label,
            measure_end_time=workload_stop_ceph_mon.get('stop')
        )
