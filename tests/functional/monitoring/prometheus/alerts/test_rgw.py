import logging
import pytest

from semantic_version import Version

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import blue_squad
from ocs_ci.framework.testlib import tier4c, skipif_managed_service
from ocs_ci.ocs import constants
from ocs_ci.utility import prometheus
from ocs_ci.ocs.ocp import OCP


log = logging.getLogger(__name__)


@blue_squad
@tier4c
@pytest.mark.polarion_id("OCS-2323")
@skipif_managed_service
def test_rgw_unavailable(measure_stop_rgw, threading_lock):
    """
    Test that there is appropriate alert when RGW is unavailable and that
    this alert is cleared when the RGW interface is back online.

    """

    api = prometheus.PrometheusAPI(threading_lock=threading_lock)

    # get alerts from time when manager deployment was scaled down
    alerts = measure_stop_rgw.get("prometheus_alerts")
    target_label = constants.ALERT_CLUSTEROBJECTSTORESTATE
    # The alert message is changed since OCS 4.7
    ocs_version = config.ENV_DATA["ocs_version"]
    if Version.coerce(ocs_version) < Version.coerce("4.7"):
        target_msg = (
            "Cluster Object Store is in unhealthy state for more than 15s. "
            "Please check Ceph cluster health or RGW connection."
        )
    else:
        target_msg = (
            "Cluster Object Store is in unhealthy state or number of ready replicas for "
            "Rook Ceph RGW deployments is less than the desired replicas in "
            f"namespace:cluster {config.ENV_DATA['cluster_namespace']}:."
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
        label=target_label, measure_end_time=measure_stop_rgw.get("stop"), time_min=300
    )


def setup_module(module):
    ocs_obj = OCP()
    module.original_user = ocs_obj.get_user_name()


def teardown_module(module):
    ocs_obj = OCP()
    ocs_obj.login_as_user(module.original_user)
