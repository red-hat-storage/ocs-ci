import logging

from ocs_ci.framework import config
from ocs_ci.framework.testlib import polarion_id, bugzilla, tier4, tier4a
from ocs_ci.ocs import constants
from ocs_ci.utility import prometheus
from ocs_ci.ocs.ocp import OCP

log = logging.getLogger(__name__)


@tier4
@tier4a
@polarion_id("OCS-1254")
@bugzilla("1835290")
def test_noobaa_bucket_quota(measure_noobaa_exceed_bucket_quota):
    """
    Test that there are appropriate alerts when NooBaa Bucket Quota is reached.
    """
    api = prometheus.PrometheusAPI()

    alerts = measure_noobaa_exceed_bucket_quota.get("prometheus_alerts")

    # since version 4.5 all NooBaa alerts have defined Pending state
    if float(config.ENV_DATA["ocs_version"]) < 4.5:
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
    else:
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


def teardown_module():
    ocs_obj = OCP()
    ocs_obj.login_as_sa()
