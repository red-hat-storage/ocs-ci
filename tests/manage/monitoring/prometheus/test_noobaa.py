import logging

from ocs_ci.framework.testlib import (
    polarion_id,
    skipif_aws_creds_are_missing,
    skipif_disconnected_cluster,
    skipif_managed_service,
    tier2,
    tier4a,
)
from ocs_ci.ocs import constants
from ocs_ci.utility import prometheus, version
from ocs_ci.ocs.ocp import OCP

log = logging.getLogger(__name__)


@tier2
@polarion_id("OCS-1254")
@skipif_managed_service
@skipif_disconnected_cluster
@skipif_aws_creds_are_missing
def test_noobaa_bucket_quota(measure_noobaa_exceed_bucket_quota):
    """
    Test that there are appropriate alerts when NooBaa Bucket Quota is reached.
    """
    api = prometheus.PrometheusAPI()

    alerts = measure_noobaa_exceed_bucket_quota.get("prometheus_alerts")

    # since version 4.5 all NooBaa alerts have defined Pending state
    if version.get_semantic_ocs_version_from_config() < version.VERSION_4_5:
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
    elif version.get_semantic_ocs_version_from_config() < version.VERSION_4_13:
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
        expected_alerts = [
            (
                constants.ALERT_BUCKETREACHINGQUANTITYQUOTASTATE,
                "A NooBaa Bucket Is In Reaching Quantity Quota State",
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
                constants.ALERT_BUCKETEXCEEDINGQUANTITYQUOTASTATE,
                "A NooBaa Bucket Is In Exceeding Quantity Quota State",
                ["pending", "firing"],
                "warning",
            ),
        ]

    for target_label, target_msg, target_states, target_severity in expected_alerts:
        prometheus.check_alert_list(
            label=target_label,
            msg=target_msg,
            alerts=alerts,
            states=target_states,
            severity=target_severity,
        )
        # the time to wait is increased because it takes more time for OCS
        # cluster to resolve its issues
        pg_wait = 480
        api.check_alert_cleared(
            label=target_label,
            measure_end_time=measure_noobaa_exceed_bucket_quota.get("stop"),
            time_min=pg_wait,
        )


@tier4a
@polarion_id("OCS-2498")
@skipif_managed_service
@skipif_disconnected_cluster
@skipif_aws_creds_are_missing
def test_noobaa_ns_bucket(measure_noobaa_ns_target_bucket_deleted):
    """
    Test that there are appropriate alerts when target bucket used of
    namespace store used in namespace bucket is deleted.
    """
    api = prometheus.PrometheusAPI()

    alerts = measure_noobaa_ns_target_bucket_deleted.get("prometheus_alerts")

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

    for target_label, target_msg, target_states, target_severity in expected_alerts:
        prometheus.check_alert_list(
            label=target_label,
            msg=target_msg,
            alerts=alerts,
            states=target_states,
            severity=target_severity,
        )
        # the time to wait is increased because it takes more time for NooBaa
        # to clear the alert
        pg_wait = 600
        api.check_alert_cleared(
            label=target_label,
            measure_end_time=measure_noobaa_ns_target_bucket_deleted.get("stop"),
            time_min=pg_wait,
        )


def teardown_module():
    ocs_obj = OCP()
    ocs_obj.login_as_sa()
