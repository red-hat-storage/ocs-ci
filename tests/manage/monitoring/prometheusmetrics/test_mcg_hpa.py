import logging

from ocs_ci.framework.pytest_customization import marks
from ocs_ci.framework.pytest_customization.marks import tier1
from ocs_ci.framework.testlib import skipif_ocs_version, skipif_ocp_version
from ocs_ci.ocs import constants, defaults, ocp
from ocs_ci.framework.pytest_customization.marks import skipif_managed_service

logger = logging.getLogger(__name__)


@tier1
@skipif_ocs_version("<4.6")
@skipif_ocp_version("<4.6")
@marks.polarion_id("OCS-2376")
@marks.bugzilla("1873162")
@skipif_managed_service
def test_hpa_noobaa_endpoint_metric():
    """
    Test to verify HPA noobaa-endpoint cpu metrics is available.
    Since 4.10, it uses horizontal-pod-autoscaler-v2 API.
    """
    ocp_obj = ocp.OCP(
        kind=constants.HPA,
        resource_name="noobaa-endpoint",
        namespace=defaults.ROOK_CLUSTER_NAMESPACE,
    )

    status = ocp_obj.get()["status"]
    assert "currentMetrics" in status, "Failed: metrics not provided in noobaa-endpoint"

    cpu_utilization = None
    for metric in status["currentMetrics"]:
        if metric["type"] != "Resource":
            continue
        if metric["resource"]["name"] != "cpu":
            continue
        cpu_utilization = metric["resource"]["current"]["averageUtilization"]
    assert (
        cpu_utilization is not None
    ), "Failed: noobaa-endpoint cpu metrics not available"
    assert cpu_utilization >= 0
    logger.info("Current resource cpu utilized: %d%%", cpu_utilization)
