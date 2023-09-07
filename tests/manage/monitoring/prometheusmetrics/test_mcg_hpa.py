import logging

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization import marks
from ocs_ci.framework.pytest_customization.marks import skipif_managed_service
from ocs_ci.framework.pytest_customization.marks import tier1
from ocs_ci.framework.testlib import skipif_ocs_version, skipif_ocp_version
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.version import get_ocp_version
from ocs_ci.utility.version import get_semantic_version, VERSION_4_10

logger = logging.getLogger(__name__)


@tier1
@skipif_ocs_version("<4.6")
@skipif_ocp_version("<4.6")
@marks.polarion_id("OCS-2376")
@marks.bugzilla("1873162")
@skipif_managed_service
def test_hpa_noobaa_endpoint_metric():
    """
    Test to verify HPA noobaa-hpav2 cpu metrics is available.
    Since 4.10, it uses horizontal-pod-autoscaler-v2 API.
    """
    ocp_version = get_semantic_version(get_ocp_version(), only_major_minor=True)
    ocp_obj = ocp.OCP(
        kind=constants.HPA,
        resource_name="noobaa-hpav2",
        namespace=config.ENV_DATA["cluster_namespace"],
    )
    status = ocp_obj.get()["status"]
    logger.info("Looking for cpu utilization value for hpa/noobaa-hpav2")
    cpu_utilization = None
    if ocp_version < VERSION_4_10:
        logger.info("using horizontal-pod-autoscaler-v1 API")
        assert (
            "currentCPUUtilizationPercentage" in status
        ), "Failed: noobaa-hpav2 cpu metrics is unavailable"
        cpu_utilization = status["currentCPUUtilizationPercentage"]
    else:
        logger.info("using horizontal-pod-autoscaler-v2 API")
        assert (
            "currentMetrics" in status
        ), "Failed: metrics not provided in noobaa-hpav2"
        for metric in status["currentMetrics"]:
            if metric["type"] != "Resource":
                continue
            if metric["resource"]["name"] != "cpu":
                continue
            cpu_utilization = metric["resource"]["current"]["averageUtilization"]
    assert cpu_utilization is not None, "Failed: noobaa-hpav2 cpu metrics not available"
    assert cpu_utilization >= 0
    logger.info("Current resource cpu utilized: %d%%", cpu_utilization)
