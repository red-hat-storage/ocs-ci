import logging
import pytest

from ocs_ci.framework.testlib import tier1
from ocs_ci.ocs import constants
from ocs_ci.utility import prometheus

log = logging.getLogger(__name__)


@pytest.mark.parametrize(
    argnames='interface',
    argvalues=[
        pytest.param(
            'rbd',
            marks=[pytest.mark.polarion_id("OCS-899"), tier1]
        ),
        pytest.param(
            'cephfs',
            marks=[pytest.mark.polarion_id("OCS-1934"), tier1]
        )
    ]
)
def test_capacity_workload_alerts(
    workload_storageutilization_95p_rbd,
    workload_storageutilization_95p_cephfs,
    interface
):
    """
    Test that there are appropriate alerts when ceph cluster is utilized.
    """
    api = prometheus.PrometheusAPI()
    measure_end_time = max([
        workload_storageutilization_95p_rbd.get('stop'),
        workload_storageutilization_95p_cephfs.get('stop'),
    ])
    if interface == 'rbd':
        workload_storageutilization_95p = workload_storageutilization_95p_rbd
    elif interface == 'cephfs':
        workload_storageutilization_95p = workload_storageutilization_95p_cephfs

    # Check utilization on 95%
    alerts = workload_storageutilization_95p.get('prometheus_alerts')
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
