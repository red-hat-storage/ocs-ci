import logging
import pytest

from ocs_ci.framework.testlib import tier4
from ocs_ci.ocs import constants
from ocs_ci.utility import prometheus
from ocs_ci.ocs.ocp import OCP

log = logging.getLogger(__name__)


@tier4
@pytest.mark.polarion_id("OCS-1254")
def test_noobaa_bucket_quota(measure_noobaa_exceed_bucket_quota):
    """
    Test that there are appropriate alerts when NooBaa Bucket Quota is reached.
    """
    api = prometheus.PrometheusAPI()

    alerts = measure_noobaa_exceed_bucket_quota.get('prometheus_alerts')
    for target_label, target_msg, target_states, target_severity in [
        (
            constants.ALERT_BUCKETREACHINGQUOTASTATE,
            'A NooBaa Bucket Is In Reaching Quota State',
            ['firing'],
            'warning'
        ),
        (
            constants.ALERT_BUCKETERRORSTATE,
            'A NooBaa Bucket Is In Error State',
            ['pending', 'firing'],
            'warning'
        ),
        (
            constants.ALERT_BUCKETEXCEEDINGQUOTASTATE,
            'A NooBaa Bucket Is In Exceeding Quota State',
            ['firing'],
            'warning'
        )
    ]:
        prometheus.check_alert_list(
            label=target_label,
            msg=target_msg,
            alerts=alerts,
            states=target_states,
            severity=target_severity
        )
        # the time to wait is increased because it takes more time for OCS
        # cluster to resolve its issues
        pg_wait = 480
        api.check_alert_cleared(
            label=target_label,
            measure_end_time=measure_noobaa_exceed_bucket_quota.get('stop'),
            time_min=pg_wait
        )


def teardown_module():
    ocs_obj = OCP()
    ocs_obj.login_as_sa()
