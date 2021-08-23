import logging

from ocs_ci.framework.pytest_customization import marks
from ocs_ci.framework.pytest_customization.marks import tier1, ignore_data_rebalance
from ocs_ci.framework.testlib import skipif_ocs_version, skipif_ocp_version
from ocs_ci.ocs import constants, defaults, ocp
from ocs_ci.framework.pytest_customization.marks import skipif_openshift_dedicated

logger = logging.getLogger(__name__)


@tier1
@skipif_ocs_version("<4.6")
@skipif_ocp_version("<4.6")
@marks.polarion_id("OCS-2376")
@marks.bugzilla("1873162")
@skipif_openshift_dedicated
@ignore_data_rebalance
def test_hpa_noobaa_endpoint_metric():
    """
    Test to verify HPA noobaa-endpoint cpu metrics is available
    """
    metric_key = "currentCPUUtilizationPercentage"

    ocp_obj = ocp.OCP(
        kind=constants.HPA,
        resource_name="noobaa-endpoint",
        namespace=defaults.ROOK_CLUSTER_NAMESPACE,
    )

    hpa = ocp_obj.get()["status"]

    assert metric_key in hpa, "Failed: noobaa-endpoint cpu metrics is unavailable"
    assert hpa[metric_key] >= 0, "Failed: noobaa-endpoint cpu metrics is unknown"
    logger.info(f"Current resource cpu utilized: {hpa[metric_key]}%")
