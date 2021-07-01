import logging
import pytest

from ocs_ci.framework.testlib import tier4, tier4a
from ocs_ci.ocs import constants
from ocs_ci.utility import prometheus
from ocs_ci.ocs.ocp import OCP


log = logging.getLogger(__name__)


@tier4
@tier4a
@pytest.mark.polarion_id("OCS-2323")
@pytest.mark.bugzilla("1953615")
def test_rgw_unavailable(measure_stop_rgw):
    """
    Test that there is appropriate alert when RGW is unavailable and that
    this alert is cleared when the RGW interface is back online.

    """
    api = prometheus.PrometheusAPI()

    # get alerts from time when manager deployment was scaled down
    alerts = measure_stop_rgw.get("prometheus_alerts")
    target_label = constants.ALERT_CLUSTEROBJECTSTORESTATE
    target_msg = (
        "Cluster Object Store is in unhealthy state for more than 15s. "
        "Please check Ceph cluster health or RGW connection."
    )
    states = ["pending", "firing"]

    prometheus.check_alert_list(
        label=target_label,
        msg=target_msg,
        alerts=alerts,
        states=states,
        severity="error",
    )
    api.check_alert_cleared(
        label=target_label, measure_end_time=measure_stop_rgw.get("stop")
    )


def teardown_module():
    ocs_obj = OCP()
    ocs_obj.login_as_sa()
