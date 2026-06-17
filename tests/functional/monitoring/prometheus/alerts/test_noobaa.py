import logging

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import blue_squad
from ocs_ci.framework.testlib import (
    polarion_id,
    post_upgrade,
    runs_on_provider,
    skipif_aws_creds_are_missing,
    skipif_disconnected_cluster,
    skipif_managed_service,
    tier1,
    tier2,
    tier4a,
    mcg,
)
from ocs_ci.ocs import constants
from ocs_ci.utility import prometheus, version
from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(__name__)


@mcg
@blue_squad
@tier2
@polarion_id("OCS-1254")
@skipif_managed_service
@runs_on_provider
@skipif_disconnected_cluster
@skipif_aws_creds_are_missing
def test_noobaa_bucket_quota(measure_noobaa_exceed_bucket_quota, threading_lock):
    """
    Test that there are appropriate alerts when NooBaa Bucket Quota is reached.
    """
    logger.info("Starting test: Verify NooBaa bucket quota alerts trigger and clear")

    logger.test_step("Initialize Prometheus API and retrieve alerts")
    api = prometheus.PrometheusAPI(threading_lock=threading_lock)

    alerts = measure_noobaa_exceed_bucket_quota.get("prometheus_alerts")
    logger.info(f"Number of alerts retrieved: {len(alerts) if alerts else 0}")

    logger.test_step("Determine expected alerts based on OCS version")
    ocs_version = version.get_semantic_ocs_version_from_config()
    logger.info(f"OCS version: {ocs_version}")

    # since version 4.5 all NooBaa alerts have defined Pending state
    if ocs_version < version.VERSION_4_5:
        logger.debug(
            "Using OCS < 4.5 alert configuration (no pending state for some alerts)"
        )
        expected_alerts = [
            (
                constants.ALERT_BUCKETREACHINGQUOTASTATE,
                "A NooBaa Bucket Is In Reaching Quota State",
                ["firing"],
                "warning",
            ),
            (
                constants.ALERT_BUCKETERRORSTATE,
                "A NooBaa Bucket Is In Error State",
                ["pending", "firing"],
                "warning",
            ),
            (
                constants.ALERT_BUCKETEXCEEDINGQUOTASTATE,
                "A NooBaa Bucket Is In Exceeding Quota State",
                ["firing"],
                "warning",
            ),
        ]
    elif ocs_version < version.VERSION_4_13:
        logger.debug("Using OCS 4.5-4.12 alert configuration (with pending states)")
        expected_alerts = [
            (
                constants.ALERT_BUCKETREACHINGQUOTASTATE,
                "A NooBaa Bucket Is In Reaching Quota State",
                ["pending", "firing"],
                "warning",
            ),
            (
                constants.ALERT_BUCKETERRORSTATE,
                "A NooBaa Bucket Is In Error State",
                ["pending", "firing"],
                "warning",
            ),
            (
                constants.ALERT_BUCKETEXCEEDINGQUOTASTATE,
                "A NooBaa Bucket Is In Exceeding Quota State",
                ["pending", "firing"],
                "warning",
            ),
        ]
    else:
        logger.debug("Using OCS >= 4.13 alert configuration (size quota alerts)")
        expected_alerts = [
            (
                constants.ALERT_BUCKETREACHINGSIZEQUOTASTATE,
                "A NooBaa Bucket Is In Reaching Size Quota State",
                ["pending", "firing"],
                "warning",
            ),
            (
                constants.ALERT_BUCKETERRORSTATE,
                "A NooBaa Bucket Is In Error State",
                ["pending", "firing"],
                "warning",
            ),
            (
                constants.ALERT_BUCKETEXCEEDINGSIZEQUOTASTATE,
                "A NooBaa Bucket Is In Exceeding Size Quota State",
                ["pending", "firing"],
                "warning",
            ),
        ]

    logger.info(f"Checking {len(expected_alerts)} NooBaa bucket quota alert types")

    logger.test_step("Validate and verify clearance for each bucket quota alert")
    # the time to wait is increased because it takes more time for OCS
    # cluster to resolve its issues
    pg_wait = 480
    stop_time = measure_noobaa_exceed_bucket_quota.get("stop")
    logger.debug(f"Alert clearance timeout: {pg_wait}min, stop_time: {stop_time}")

    for i, (target_label, target_msg, target_states, target_severity) in enumerate(
        expected_alerts, 1
    ):
        logger.info(
            f"Processing alert {i}/{len(expected_alerts)}: {target_label} "
            f"(severity: {target_severity})"
        )

        logger.debug(f"Validating {target_label} with states={target_states}")
        prometheus.check_alert_list(
            label=target_label,
            msg=target_msg,
            alerts=alerts,
            states=target_states,
            severity=target_severity,
        )
        logger.info(f"Alert {target_label} validated successfully")

        logger.debug(f"Verifying {target_label} is cleared (timeout={pg_wait}min)")
        api.check_alert_cleared(
            label=target_label,
            measure_end_time=stop_time,
            time_min=pg_wait,
        )
        logger.info(f"Alert {target_label} cleared successfully")

    logger.info("Test passed: NooBaa bucket quota alerts validated successfully")


@mcg
@blue_squad
@tier4a
@runs_on_provider
@polarion_id("OCS-2498")
@skipif_managed_service
@skipif_disconnected_cluster
@skipif_aws_creds_are_missing
def test_noobaa_ns_bucket(measure_noobaa_ns_target_bucket_deleted, threading_lock):
    """
    Test that there are appropriate alerts when target bucket used of
    namespace store used in namespace bucket is deleted.
    """
    logger.info(
        "Starting test: Verify NooBaa namespace bucket alerts when target bucket deleted"
    )

    logger.test_step("Initialize Prometheus API and retrieve alerts")
    api = prometheus.PrometheusAPI(threading_lock=threading_lock)

    alerts = measure_noobaa_ns_target_bucket_deleted.get("prometheus_alerts")
    logger.info(f"Number of alerts retrieved: {len(alerts) if alerts else 0}")

    logger.test_step("Define expected namespace bucket error alerts")
    expected_alerts = [
        (
            constants.ALERT_NAMESPACEBUCKETERRORSTATE,
            "A NooBaa Namespace Bucket Is In Error State",
            ["pending", "firing"],
            "warning",
        ),
        (
            constants.ALERT_NAMESPACERESOURCEERRORSTATE,
            "A NooBaa Namespace Resource Is In Error State",
            ["pending", "firing"],
            "warning",
        ),
    ]
    logger.info(f"Checking {len(expected_alerts)} namespace bucket alert types")

    logger.test_step("Validate and verify clearance for each namespace bucket alert")
    # the time to wait is increased because it takes more time for NooBaa
    # to clear the alert
    pg_wait = 600
    stop_time = measure_noobaa_ns_target_bucket_deleted.get("stop")
    logger.debug(f"Alert clearance timeout: {pg_wait}min, stop_time: {stop_time}")

    for i, (target_label, target_msg, target_states, target_severity) in enumerate(
        expected_alerts, 1
    ):
        logger.info(
            f"Processing alert {i}/{len(expected_alerts)}: {target_label} "
            f"(severity: {target_severity})"
        )

        logger.debug(f"Validating {target_label} with states={target_states}")
        prometheus.check_alert_list(
            label=target_label,
            msg=target_msg,
            alerts=alerts,
            states=target_states,
            severity=target_severity,
        )
        logger.info(f"Alert {target_label} validated successfully")

        logger.debug(f"Verifying {target_label} is cleared (timeout={pg_wait}min)")
        api.check_alert_cleared(
            label=target_label,
            measure_end_time=stop_time,
            time_min=pg_wait,
        )
        logger.info(f"Alert {target_label} cleared successfully")

    logger.info("Test passed: NooBaa namespace bucket alerts validated successfully")


@mcg
@blue_squad
@tier1
@post_upgrade
@polarion_id("OCS-7915")
@runs_on_provider
def test_noobaa_prometheus_rules_exist():
    """
    Verify that the NooBaa PrometheusRule CR exists and contains rule groups.
    """
    logger.info(
        "Starting test: Verify NooBaa PrometheusRule CR exists and contains rule groups"
    )

    logger.test_step("Retrieve NooBaa PrometheusRule CR")
    namespace = config.ENV_DATA["cluster_namespace"]
    rule_name = "noobaa-prometheus-rules"
    logger.info(f"Looking for PrometheusRule: {rule_name} in namespace {namespace}")

    prometheus_rule = OCP(
        api_version="monitoring.coreos.com/v1",
        kind="PrometheusRule",
        namespace=namespace,
    )
    rule = prometheus_rule.get(resource_name=rule_name)
    logger.debug(f"Retrieved PrometheusRule CR: {rule_name}")

    logger.test_step("Validate rule groups are present")
    groups = rule.get("spec", {}).get("groups", [])
    group_count = len(groups)
    group_names = [g["name"] for g in groups]

    logger.info(f"Found {group_count} rule groups: {group_names}")

    has_groups = group_count > 0
    logger.assertion(
        f"PrometheusRule has rule groups: expected>0, actual={group_count}, has_groups={has_groups}"
    )
    assert has_groups, "noobaa-prometheus-rules PrometheusRule CR has no rule groups"

    logger.info("Test passed: NooBaa PrometheusRule CR validated successfully")


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
