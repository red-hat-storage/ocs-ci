import logging
import pytest
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import blue_squad
from ocs_ci.framework.testlib import tier4a, skipif_managed_service, skipif_no_kms
from ocs_ci.ocs import constants
from ocs_ci.utility import prometheus
from ocs_ci.ocs.ocp import OCP


log = logging.getLogger(__name__)


@blue_squad
@tier4a
@pytest.mark.polarion_id("OCS-5154")
@skipif_no_kms
@skipif_managed_service
def test_kms_unavailable(measure_rewrite_kms_endpoint, threading_lock):
    """
    Test that there is appropriate alert when KMS is unavailable and that
    this alert is cleared when the KMS endpoint is back online.

    """
    api = prometheus.PrometheusAPI(threading_lock=threading_lock)

    # get alerts from time when manager deployment was scaled down
    alerts = measure_rewrite_kms_endpoint.get("prometheus_alerts")
    target_label = constants.ALERT_KMSSERVERCONNECTIONALERT
    config_namespace = config.ENV_DATA["cluster_namespace"]
    config_cluster = config.ENV_DATA["storage_cluster_name"]
    target_msg = (
        "Storage Cluster KMS Server is in un-connected state. Please check "
        f"KMS config in namespace:cluster {config_namespace}:{config_cluster}."
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
        label=target_label,
        measure_end_time=measure_rewrite_kms_endpoint.get("stop"),
        time_min=300,
    )


def teardown_module():
    ocs_obj = OCP()
    ocs_obj.login_as_sa()
