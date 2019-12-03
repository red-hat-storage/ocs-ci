import logging
import pytest

from ocs_ci.framework.testlib import tier1
from ocs_ci.ocs import constants
from ocs_ci.utility import prometheus

log = logging.getLogger(__name__)


@tier1
@pytest.mark.polarion_id("OCS-899")
def test_capacity_workload_rbd_alerts(
    workload_storageutilization_80p_rbd,
    workload_storageutilization_90p_rbd
):
    """
    Test that there are appropriate alerts when ceph cluster is utilized.
    """
    api = prometheus.PrometheusAPI()
    measure_end_time = max([
        workload_storageutilization_80p_rbd.get('stop'),
        workload_storageutilization_90p_rbd.get('stop')
    ])

    # Check utilization on 80%
    alerts = workload_storageutilization_80p_rbd.get('prometheus_alerts')
    target_label = constants.ALERT_CLUSTERNEARFULL
    target_msg = 'Storage cluster is nearing full. Expansion is required.'
    target_states = ['pending', 'firing']
    target_severity = 'warning'
    prometheus.check_alert_list(
        label=target_label,
        msg=target_msg,
        alerts=alerts,
        states=target_states,
        severity=target_severity,
        ignore_more_occurences=True
    )
    # the time to wait is increased because it takes more time for Ceph
    # cluster to delete all data
    pg_wait = 300
    api.check_alert_cleared(
        label=target_label,
        measure_end_time=measure_end_time,
        time_min=pg_wait
    )

    # Check utilization on 90%
    alerts = workload_storageutilization_90p_rbd.get('prometheus_alerts')
    for target_label, target_msg, target_states, target_severity in [
        (
            constants.ALERT_CLUSTERNEARFULL,
            'Storage cluster is nearing full. Expansion is required.',
            ['pending', 'firing'],
            'warning'
        ),
        (
            constants.ALERT_CLUSTERCRITICALLYFULL,
            'Storage cluster is critically full and needs immediate expansion',
            ['pending', 'firing'],
            'error'
        ),
    ]:
        prometheus.check_alert_list(
            label=target_label,
            msg=target_msg,
            alerts=alerts,
            states=target_states,
            severity=target_severity,
            ignore_more_occurences=True
        )
        # the time to wait is increased because it takes more time for Ceph
        # cluster to delete all data
        pg_wait = 300
        api.check_alert_cleared(
            label=target_label,
            measure_end_time=measure_end_time,
            time_min=pg_wait
        )


@tier1
@pytest.mark.polarion_id("OCS-1934")
def test_capacity_workload_cephfs_alerts(
    workload_storageutilization_80p_cephfs,
    workload_storageutilization_90p_cephfs
):
    """
    Test that there are appropriate alerts when ceph cluster is utilized.
    """
    api = prometheus.PrometheusAPI()
    measure_end_time = max([
        workload_storageutilization_80p_cephfs.get('stop'),
        workload_storageutilization_90p_cephfs.get('stop')
    ])

    # Check utilization on 80%
    alerts = workload_storageutilization_80p_cephfs.get('prometheus_alerts')
    target_label = constants.ALERT_CLUSTERNEARFULL
    target_msg = 'Storage cluster is nearing full. Expansion is required.'
    target_states = ['pending', 'firing']
    target_severity = 'warning'
    prometheus.check_alert_list(
        label=target_label,
        msg=target_msg,
        alerts=alerts,
        states=target_states,
        severity=target_severity,
        ignore_more_occurences=True
    )
    # the time to wait is increased because it takes more time for Ceph
    # cluster to delete all data
    pg_wait = 300
    api.check_alert_cleared(
        label=target_label,
        measure_end_time=measure_end_time,
        time_min=pg_wait
    )

    # Check utilization on 90%
    alerts = workload_storageutilization_90p_cephfs.get('prometheus_alerts')
    for target_label, target_msg, target_states, target_severity in [
        (
            constants.ALERT_CLUSTERNEARFULL,
            'Storage cluster is nearing full. Expansion is required.',
            ['pending', 'firing'],
            'warning'
        ),
        (
            constants.ALERT_CLUSTERCRITICALLYFULL,
            'Storage cluster is critically full and needs immediate expansion',
            ['pending', 'firing'],
            'error'
        ),
    ]:
        prometheus.check_alert_list(
            label=target_label,
            msg=target_msg,
            alerts=alerts,
            states=target_states,
            severity=target_severity,
            ignore_more_occurences=True
        )
        # the time to wait is increased because it takes more time for Ceph
        # cluster to delete all data
        pg_wait = 300
        api.check_alert_cleared(
            label=target_label,
            measure_end_time=measure_end_time,
            time_min=pg_wait
        )
