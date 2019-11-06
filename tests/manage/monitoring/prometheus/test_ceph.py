import logging
import pytest

from ocs_ci.framework.testlib import tier4
from ocs_ci.ocs import constants
from ocs_ci.utility import prometheus

log = logging.getLogger(__name__)


@tier4
@pytest.mark.polarion_id("OCS-903")
def test_corrupt_pg_alerts(measure_corrupt_pg):
    """
    Test that there are appropriate alerts when Placement group
    on one OSD is corrupted.ceph manager
    is unavailable and that this alert is cleared when the manager
    is back online.
    """
    api = prometheus.PrometheusAPI()

    alerts = measure_corrupt_pg.get('prometheus_alerts')
    for target_label, target_msg, target_states, target_severity in [
        (
            constants.ALERT_PGREPAIRTAKINGTOOLONG,
            'Self heal problems detected',
            ['pending'],
            'warning'
        ),
        (
            constants.ALERT_CLUSTERERRORSTATE,
            'Storage cluster is in error state',
            ['pending', 'firing'],
            'error'
        )
    ]:
        prometheus.check_alert_list(
            label=target_label,
            msg=target_msg,
            alerts=alerts,
            states=target_states,
            severity=target_severity
        )
        # the time to wait is increased because it takes more time for Ceph
        # cluster to resolve its issues
        pg_wait = 360
        api.check_alert_cleared(
            label=target_label,
            measure_end_time=measure_corrupt_pg.get('stop'),
            time_min=pg_wait
        )
