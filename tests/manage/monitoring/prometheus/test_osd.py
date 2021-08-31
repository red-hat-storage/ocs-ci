import pytest
from ocs_ci.framework.pytest_customization.marks import tier4c
from ocs_ci.ocs import constants
from ocs_ci.utility import prometheus


@pytest.mark.polarion_id("OCS-2615")
@pytest.mark.bugzilla("1935342")
@tier4c
def test_osd_flapping_alert(measure_ceph_osd_flapping):
    """
    Prometheus alert for OSD restart. alert notifies if an OpenShift Container
    Storage OSD restarts more than 5 times in 5 minutes.

    """
    api = prometheus.PrometheusAPI()
    alerts = measure_ceph_osd_flapping.get("prometheus_alerts")
    expected_alert_msg = (
        "Storage daemon osd.0 has restarted 5 times in the last 5 minutes. Please check "
        "the pod events or ceph status to find out the cause."
    )
    target_label = constants.ALERT_CEPHOSDFLAPPING

    prometheus.check_alert_list(
        label=target_label,
        msg=expected_alert_msg,
        alerts=alerts,
        states=["firing"],
        severity="critical",
    )

    api.check_alert_cleared(
        label=target_label, measure_end_time=measure_ceph_osd_flapping.get("stop")
    )
