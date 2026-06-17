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

logger = logging.getLogger(__name__)


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
    logger.info("Starting test: Verify capacity alerts during RBD utilization workload")

    logger.test_step("Initialize Prometheus API and retrieve workload data")
    api = prometheus.PrometheusAPI(threading_lock=threading_lock)
    measure_start_time = workload_storageutilization_97p_rbd.get("start")
    measure_end_time = workload_storageutilization_97p_rbd.get("stop")
    logger.info(
        f"Measurement period: start={measure_start_time}, end={measure_end_time}"
    )

    # Check utilization on 97%
    alerts = workload_storageutilization_97p_rbd.get("prometheus_alerts")
    logger.info(f"Number of alerts retrieved: {len(alerts) if alerts else 0}")
    ocs_version = config.ENV_DATA.get("ocs_version")
    logger.info(f"OCS version: {ocs_version}")

    if ocs_version >= "4.21":
        logger.test_step("Check disk utilization metrics (OCS 4.21+)")

        disk_util_query = (
            "max by (ceph_daemon, device, instance, managedBy) ("
            'label_replace(ceph_disk_occupation{job="rook-ceph-mgr"}, "device", "$1", "device", "/dev/(.*)") '
            "* on (instance, device) group_left () "
            'max by (instance, device) (rate(node_disk_io_time_seconds_total{job="node-exporter"}[5m]))) * 100'
        )
        logger.debug(f"Disk utilization query: {disk_util_query}")
        logger.info("Querying Prometheus for disk utilization metrics")

        disk_util_results = api.query_range(
            query=disk_util_query,
            start=measure_start_time,
            end=measure_end_time,
            step=60,
        )
        logger.info(f"Query returned {len(disk_util_results)} disk metric time series")

        high_util_disks = []
        max_utilization_per_disk = {}

        logger.debug(f"Analyzing {len(disk_util_results)} disks for utilization")
        for i, result in enumerate(disk_util_results, 1):
            metric_info = result.get("metric", {})
            device_id = f"{metric_info.get('ceph_daemon', 'unknown')}-{metric_info.get('device', 'unknown')}"
            values = result.get("values", [])

            if not values:
                logger.warning(
                    f"Disk {i}/{len(disk_util_results)} ({device_id}): No values found"
                )
                continue

            max_util = max(float(value) for timestamp, value in values)
            max_utilization_per_disk[device_id] = {
                "metric": metric_info,
                "max_utilization": max_util,
            }

            logger.debug(
                f"Disk {i}/{len(disk_util_results)} ({device_id}): max utilization = {max_util:.2f}%"
            )

            if max_util > 90:
                high_util_disks.append(
                    {"metric": metric_info, "max_utilization": max_util}
                )
                logger.info(
                    f"High disk utilization detected: {device_id} = {max_util:.2f}%"
                )

        logger.info(f"Summary: {len(high_util_disks)} disks with utilization > 90%")

        if high_util_disks:
            logger.info(
                f"Disk utilization > 90% detected. "
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
                    alert_state = alert.get("state")
                    logger.info(
                        f"{constants.ALERT_ODF_DISK_UTILIZATION_HIGH} alert found in state: {alert_state}"
                    )
                    break

            logger.assertion(
                f"Disk utilization alert present: expected=True (high util disks={len(high_util_disks)}), "
                f"actual={disk_alert_found}"
            )

            assert disk_alert_found, (
                f"Disk utilization > 90% detected but {constants.ALERT_ODF_DISK_UTILIZATION_HIGH} "
                f"alert not found in pending or firing state. "
                f"High utilization disks: {high_util_disks}"
            )
            logger.info("Disk utilization alert validated successfully")
        else:
            logger.info(
                "No disks with utilization > 90% detected. Skipping disk utilization alert check."
            )
    else:
        logger.info(
            f"OCS version {ocs_version} is less than 4.21. Skipping disk utilization alert check."
        )

    logger.test_step("Validate cluster capacity alerts")

    if ocs_version == "4.2":
        nearfull_message = "Storage cluster is nearing full. Expansion is required."
        criticallfull_mesage = (
            "Storage cluster is critically full and needs immediate expansion"
        )
        logger.debug("Using OCS 4.2 alert messages")
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
        logger.debug("Using OCS 4.3+ alert messages")

    alert_configs = [
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
    ]
    logger.info(f"Checking {len(alert_configs)} capacity alert types")

    # the time to wait is increased because it takes more time for Ceph
    # cluster to delete all data
    pg_wait = 300
    logger.debug(f"Alert clearance timeout: {pg_wait}min")

    for i, (target_label, target_msg, target_states, target_severity) in enumerate(
        alert_configs, 1
    ):
        logger.info(
            f"Processing alert {i}/{len(alert_configs)}: {target_label} "
            f"(severity: {target_severity})"
        )

        logger.debug(f"Validating {target_label} with states={target_states}")
        prometheus.check_alert_list(
            label=target_label,
            msg=target_msg,
            alerts=alerts,
            states=target_states,
            severity=target_severity,
            ignore_more_occurences=True,
        )
        logger.info(f"Alert {target_label} validated successfully")

        logger.debug(f"Verifying {target_label} is cleared (timeout={pg_wait}min)")
        api.check_alert_cleared(
            label=target_label, measure_end_time=measure_end_time, time_min=pg_wait
        )
        logger.info(f"Alert {target_label} cleared successfully")

    logger.info("Test passed: RBD capacity alerts validated successfully")


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
    logger.info(
        "Starting test: Verify capacity alerts during CephFS utilization workload"
    )

    logger.test_step("Initialize Prometheus API and retrieve workload data")
    api = prometheus.PrometheusAPI(threading_lock=threading_lock)
    measure_end_time = workload_storageutilization_97p_cephfs.get("stop")
    logger.info(f"Measurement end time: {measure_end_time}")

    # Check utilization on 97%
    alerts = workload_storageutilization_97p_cephfs.get("prometheus_alerts")
    logger.info(f"Number of alerts retrieved: {len(alerts) if alerts else 0}")

    logger.test_step("Validate cluster capacity alerts")

    ocs_version = config.ENV_DATA.get("ocs_version")
    logger.info(f"OCS version: {ocs_version}")

    if ocs_version == "4.2":
        nearfull_message = "Storage cluster is nearing full. Expansion is required."
        criticallfull_mesage = (
            "Storage cluster is critically full and needs immediate expansion"
        )
        logger.debug("Using OCS 4.2 alert messages")
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
        logger.debug("Using OCS 4.3+ alert messages")

    alert_configs = [
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
    ]
    logger.info(f"Checking {len(alert_configs)} capacity alert types")

    # the time to wait is increased because it takes more time for Ceph
    # cluster to delete all data
    pg_wait = 300
    logger.debug(f"Alert clearance timeout: {pg_wait}min")

    for i, (target_label, target_msg, target_states, target_severity) in enumerate(
        alert_configs, 1
    ):
        logger.info(
            f"Processing alert {i}/{len(alert_configs)}: {target_label} "
            f"(severity: {target_severity})"
        )

        logger.debug(f"Validating {target_label} with states={target_states}")
        prometheus.check_alert_list(
            label=target_label,
            msg=target_msg,
            alerts=alerts,
            states=target_states,
            severity=target_severity,
            ignore_more_occurences=True,
        )
        logger.info(f"Alert {target_label} validated successfully")

        logger.debug(f"Verifying {target_label} is cleared (timeout={pg_wait}min)")
        api.check_alert_cleared(
            label=target_label, measure_end_time=measure_end_time, time_min=pg_wait
        )
        logger.info(f"Alert {target_label} cleared successfully")

    logger.info("Test passed: CephFS capacity alerts validated successfully")


def setup_module(module):
    logger.info("Setting up module: Storing original user for cleanup")
    ocs_obj = OCP()
    module.original_user = ocs_obj.get_user_name()
    logger.info(f"Original user stored: {module.original_user}")


def teardown_module(module):
    logger.info("Tearing down module: Restoring original user")
    ocs_obj = OCP()
    ocs_obj.login_as_user(module.original_user)
    logger.info(f"Restored user: {module.original_user}")
