import logging

from ocs_ci.framework.pytest_customization import marks
from ocs_ci.framework.pytest_customization.marks import tier1
from ocs_ci.framework.testlib import skipif_ocs_version, skipif_ocp_version
from ocs_ci.ocs import constants, defaults, ocp
from ocs_ci.utility import prometheus

logger = logging.getLogger(__name__)


@tier1
@skipif_ocs_version('<4.6')
@skipif_ocp_version('<4.6')
@marks.polarion_id("OCS-2375")
@marks.bugzilla('1836299')
def test_hpa_maxreplica_alert():
    """
    Test that there is no HPA max replica alert triggered
    """
    api = prometheus.PrometheusAPI()

    logger.info(
        f"Verifying whether {constants.ALERT_KUBEHPAREPLICASMISMATCH} "
        f"has not been triggered"
    )
    alerts = api.wait_for_alert(name=constants.ALERT_KUBEHPAREPLICASMISMATCH, timeout=10, sleep=1)
    if len(alerts) > 0:
        assert False, f"Failed: There should be no {constants.ALERT_KUBEHPAREPLICASMISMATCH} alert"


@tier1
@skipif_ocs_version('<4.6')
@skipif_ocp_version('<4.6')
@marks.polarion_id("OCS-2376")
@marks.bugzilla('1873162')
def test_hpa_noobaa_endpoint_metric():
    """
    Test to verify HPA noobaa-endpoint cpu metrics is available
    """
    metric_key = 'currentCPUUtilizationPercentage'

    ocp_obj = ocp.OCP(
        kind=constants.HPA, resource_name='noobaa-endpoint',
        namespace=defaults.ROOK_CLUSTER_NAMESPACE)

    hpa = ocp_obj.get()['status']

    if metric_key not in hpa:
        assert False, "Failed: noobaa-endpoint cpu metrics is unavailable"
    else:
        assert hpa[metric_key] >= 0, "Failed: noobaa-endpoint cpu metrics is unknown"
        logger.info(f"Current resource cpu utilized: {hpa[metric_key]}%")
