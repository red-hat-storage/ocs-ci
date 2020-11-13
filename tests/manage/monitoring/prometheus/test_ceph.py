import logging
import pytest

from ocs_ci.framework.testlib import tier4, tier4a
from ocs_ci.ocs import constants
from ocs_ci.utility import prometheus
from ocs_ci.ocs.ocp import OCP

log = logging.getLogger(__name__)


@tier4
@tier4a
@pytest.mark.polarion_id("OCS-903")
@pytest.mark.skip(reason="measure_corrupt_pg fixture makes current test runs unstable")
def test_corrupt_pg_alerts(measure_corrupt_pg):
    """
    Test that there are appropriate alerts when Placement group
    on one OSD is corrupted.ceph manager
    is unavailable and that this alert is cleared when the manager
    is back online.
    """
    api = prometheus.PrometheusAPI()

    alerts = measure_corrupt_pg.get("prometheus_alerts")
    for target_label, target_msg, target_states, target_severity in [
        (
            constants.ALERT_PGREPAIRTAKINGTOOLONG,
            "Self heal problems detected",
            ["pending"],
            "warning",
        ),
        (
            constants.ALERT_CLUSTERERRORSTATE,
            "Storage cluster is in error state",
            ["pending", "firing"],
            "error",
        ),
    ]:
        prometheus.check_alert_list(
            label=target_label,
            msg=target_msg,
            alerts=alerts,
            states=target_states,
            severity=target_severity,
        )
        # the time to wait is increased because it takes more time for Ceph
        # cluster to resolve its issues
        pg_wait = 360
        api.check_alert_cleared(
            label=target_label,
            measure_end_time=measure_corrupt_pg.get("stop"),
            time_min=pg_wait,
        )


@tier4
@tier4a
@pytest.mark.polarion_id("OCS-898")
@pytest.mark.skip(reason="measure_corrupt_pg fixture makes current test runs unstable")
def test_ceph_health(measure_stop_ceph_mon, measure_corrupt_pg):
    """
    Test that there are appropriate alerts for Ceph health triggered.
    For this check of Ceph Warning state is used measure_stop_ceph_mon
    utilization monitor and for Ceph Error state is used measure_corrupt_pg
    utilization.
    """
    api = prometheus.PrometheusAPI()

    alerts = measure_stop_ceph_mon.get("prometheus_alerts")
    target_label = constants.ALERT_CLUSTERWARNINGSTATE
    target_msg = "Storage cluster is in degraded state"
    target_states = ["pending", "firing"]
    target_severity = "warning"
    prometheus.check_alert_list(
        label=target_label,
        msg=target_msg,
        alerts=alerts,
        states=target_states,
        severity=target_severity,
    )
    api.check_alert_cleared(
        label=target_label,
        measure_end_time=measure_stop_ceph_mon.get("stop"),
    )

    alerts = measure_corrupt_pg.get("prometheus_alerts")
    target_label = constants.ALERT_CLUSTERERRORSTATE
    target_msg = "Storage cluster is in error state"
    target_states = ["pending", "firing"]
    target_severity = "error"
    prometheus.check_alert_list(
        label=target_label,
        msg=target_msg,
        alerts=alerts,
        states=target_states,
        severity=target_severity,
    )
    # the time to wait is increased because it takes more time for Ceph
    # cluster to resolve its issues
    pg_wait = 360
    api.check_alert_cleared(
        label=target_label,
        measure_end_time=measure_corrupt_pg.get("stop"),
        time_min=pg_wait,
    )


def teardown_module():
    ocs_obj = OCP()
    ocs_obj.login_as_sa()
