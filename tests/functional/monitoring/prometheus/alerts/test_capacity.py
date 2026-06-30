import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    tier2,
    gather_metrics_on_fail,
    skipif_managed_service,
    runs_on_provider,
    blue_squad,
)
from ocs_ci.ocs import constants
from ocs_ci.utility import prometheus
from ocs_ci.ocs.ocp import OCP

log = logging.getLogger(__name__)


@blue_squad
@pytest.mark.polarion_id("OCS-899")
@tier2
@gather_metrics_on_fail(
    "ceph_cluster_total_used_bytes", "cluster:memory_usage_bytes:sum"
)
@skipif_managed_service
@runs_on_provider
def test_rbd_capacity_workload_alerts(
    workload_storageutilization_97p_rbd, threading_lock
):
    """
    Test that there are appropriate alerts when ceph cluster is utilized
    via RBD interface.
    """
    api = prometheus.PrometheusAPI(threading_lock=threading_lock)
    measure_start_time = workload_storageutilization_97p_rbd.get("start")
    measure_end_time = workload_storageutilization_97p_rbd.get("stop")

    # Check utilization on 97%
    alerts = workload_storageutilization_97p_rbd.get("prometheus_alerts")
    if config.ENV_DATA.get("ocs_version") >= "4.21":
        disk_util_query = (
            "max by (ceph_daemon, device, instance, managedBy) ("
            'label_replace(ceph_disk_occupation{job="rook-ceph-mgr"}, "device", "$1", "device", "/dev/(.*)") '
            "* on (instance, device) group_left () "
            'max by (instance, device) (rate(node_disk_io_time_seconds_total{job="node-exporter"}[5m]))) * 100'
        )
        log.info("Querying Prometheus for disk utilization metrics")
        disk_util_results = api.query_range(
            query=disk_util_query,
            start=measure_start_time,
            end=measure_end_time,
            step=60,
        )
        high_util_disks = []
        max_utilization_per_disk = {}
        for result in disk_util_results:
            metric_info = result.get("metric", {})
            device_id = f"{metric_info.get('ceph_daemon', 'unknown')}-{metric_info.get('device', 'unknown')}"
            values = result.get("values", [])
            if not values:
                log.warning(f"No values found for disk {device_id}")
                continue
            max_util = max(float(value) for timestamp, value in values)
            max_utilization_per_disk[device_id] = {
                "metric": metric_info,
                "max_utilization": max_util,
            }
            log.info(f"Disk {device_id}: max utilization = {max_util:.2f}%")
            if max_util > 90:
                high_util_disks.append(
                    {"metric": metric_info, "max_utilization": max_util}
                )
                log.info(
                    f"High disk utilization detected: {device_id} = {max_util:.2f}%"
                )

        if high_util_disks:
            log.info(
                f"Disk utilization > 90% detected."
                f"Verifying {constants.ALERT_ODF_DISK_UTILIZATION_HIGH} alert is present."
            )
            disk_alert_found = False
            for alert in alerts:
                if alert.get("labels").get(
                    "alertname"
                ) == constants.ALERT_ODF_DISK_UTILIZATION_HIGH and (
                    alert.get("state") == "pending" or alert.get("state") == "firing"
                ):
                    disk_alert_found = True
                    log.info(
                        f"{constants.ALERT_ODF_DISK_UTILIZATION_HIGH} alert found in state: {alert.get('state')}"
                    )
                    break

            assert disk_alert_found, (
                f"Disk utilization > 90% detected but {constants.ALERT_ODF_DISK_UTILIZATION_HIGH} "
                f"alert not found in pending or firing state. "
                f"High utilization disks: {high_util_disks}"
            )
        else:
            log.info(
                "No disks with utilization > 90% detected. Skipping disk utilization alert check."
            )
    else:
        log.info(
            "OCS version is less than 4.21. Skipping disk utilization alert check."
        )

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


@blue_squad
@pytest.mark.polarion_id("OCS-1934")
@tier2
@gather_metrics_on_fail(
    "ceph_cluster_total_used_bytes", "cluster:memory_usage_bytes:sum"
)
@skipif_managed_service
@runs_on_provider
def test_cephfs_capacity_workload_alerts(
    workload_storageutilization_97p_cephfs, threading_lock
):
    """
    Test that there are appropriate alerts when ceph cluster is utilized.
    """
    api = prometheus.PrometheusAPI(threading_lock=threading_lock)
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


def setup_module(module):
    ocs_obj = OCP()
    module.original_user = ocs_obj.get_user_name()


def teardown_module(module):
    ocs_obj = OCP()
    ocs_obj.login_as_user(module.original_user)
