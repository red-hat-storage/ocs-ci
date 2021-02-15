import logging

from ocs_ci.framework.pytest_customization import marks
from ocs_ci.framework.pytest_customization.marks import tier1
from ocs_ci.framework.testlib import skipif_ocs_version, skipif_ocp_version
from ocs_ci.ocs import constants
from ocs_ci.utility import prometheus
from ocs_ci.framework.pytest_customization.marks import skipif_openshift_dedicated

logger = logging.getLogger(__name__)


@tier1
@skipif_ocs_version("<4.6")
@skipif_ocp_version("<4.6")
@marks.polarion_id("OCS-2375")
@marks.bugzilla("1836299")
@skipif_openshift_dedicated
def test_hpa_maxreplica_alert():
    """
    Test to verify that no HPA max replica alert is triggered
    """
    api = prometheus.PrometheusAPI()

    logger.info(
        f"Verifying whether {constants.ALERT_KUBEHPAREPLICASMISMATCH} "
        f"has not been triggered"
    )
    alerts = api.wait_for_alert(
        name=constants.ALERT_KUBEHPAREPLICASMISMATCH, timeout=10, sleep=1
    )
    if len(alerts) > 0:
        assert (
            False
        ), f"Failed: There should be no {constants.ALERT_KUBEHPAREPLICASMISMATCH} alert"
