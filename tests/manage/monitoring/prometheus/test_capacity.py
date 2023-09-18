import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    tier2,
    gather_metrics_on_fail,
    skipif_managed_service,
)
from ocs_ci.ocs import constants
from ocs_ci.utility import prometheus
from ocs_ci.ocs.ocp import OCP

log = logging.getLogger(__name__)


@pytest.mark.polarion_id("OCS-899")
@pytest.mark.bugzilla("1943137")
@pytest.mark.bugzilla("2237742")
@tier2
@gather_metrics_on_fail(
    "ceph_cluster_total_used_bytes", "cluster:memory_usage_bytes:sum"
)
@skipif_managed_service
def test_rbd_capacity_workload_alerts(workload_storageutilization_97p_rbd):
    """
    Test that there are appropriate alerts when ceph cluster is utilized
    via RBD interface.
    """
    api = prometheus.PrometheusAPI()
    measure_end_time = workload_storageutilization_97p_rbd.get("stop")

    # Check utilization on 97%
    alerts = workload_storageutilization_97p_rbd.get("prometheus_alerts")

    if config.ENV_DATA.get("ocs_version") == "4.2":
        nearfull_message = "Storage cluster is nearing full. Expansion is required."
        criticallfull_mesage = (
            "Storage cluster is critically full and needs immediate expansion"
        )
    else:
        # since OCS 4.3
        nearfull_message = (
            "Storage cluster is nearing full. Data deletion or cluster "
            "expansion is required."
        )
        criticallfull_mesage = (
            "Storage cluster is critically full and needs immediate data "
            "deletion or cluster expansion."
        )

    for target_label, target_msg, target_states, target_severity in [
        (
            constants.ALERT_CLUSTERNEARFULL,
            nearfull_message,
            ["pending", "firing"],
            "warning",
        ),
        (
            constants.ALERT_CLUSTERCRITICALLYFULL,
            criticallfull_mesage,
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
            ignore_more_occurences=True,
        )
        # the time to wait is increased because it takes more time for Ceph
        # cluster to delete all data
        pg_wait = 300
        api.check_alert_cleared(
            label=target_label, measure_end_time=measure_end_time, time_min=pg_wait
        )


@pytest.mark.polarion_id("OCS-1934")
@pytest.mark.bugzilla("1943137")
@pytest.mark.bugzilla("2237742")
@tier2
@gather_metrics_on_fail(
    "ceph_cluster_total_used_bytes", "cluster:memory_usage_bytes:sum"
)
@skipif_managed_service
def test_cephfs_capacity_workload_alerts(workload_storageutilization_97p_cephfs):
    """
    Test that there are appropriate alerts when ceph cluster is utilized.
    """
    api = prometheus.PrometheusAPI()
    measure_end_time = workload_storageutilization_97p_cephfs.get("stop")

    # Check utilization on 97%
    alerts = workload_storageutilization_97p_cephfs.get("prometheus_alerts")

    if config.ENV_DATA.get("ocs_version") == "4.2":
        nearfull_message = "Storage cluster is nearing full. Expansion is required."
        criticallfull_mesage = (
            "Storage cluster is critically full and needs immediate expansion"
        )
    else:
        # since OCS 4.3
        nearfull_message = (
            "Storage cluster is nearing full. Data deletion or cluster "
            "expansion is required."
        )
        criticallfull_mesage = (
            "Storage cluster is critically full and needs immediate data "
            "deletion or cluster expansion."
        )

    for target_label, target_msg, target_states, target_severity in [
        (
            constants.ALERT_CLUSTERNEARFULL,
            nearfull_message,
            ["pending", "firing"],
            "warning",
        ),
        (
            constants.ALERT_CLUSTERCRITICALLYFULL,
            criticallfull_mesage,
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
            ignore_more_occurences=True,
        )
        # the time to wait is increased because it takes more time for Ceph
        # cluster to delete all data
        pg_wait = 300
        api.check_alert_cleared(
            label=target_label, measure_end_time=measure_end_time, time_min=pg_wait
        )


def teardown_module():
    ocs_obj = OCP()
    ocs_obj.login_as_sa()
