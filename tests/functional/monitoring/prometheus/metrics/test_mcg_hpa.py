import logging

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization import marks
from ocs_ci.framework.pytest_customization.marks import (
    blue_squad,
    runs_on_provider,
    skipif_managed_service,
    tier1,
    mcg,
)
from ocs_ci.framework.testlib import skipif_ocs_version, skipif_ocp_version
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.version import get_ocp_version
from ocs_ci.utility.version import get_semantic_version, VERSION_4_10

logger = logging.getLogger(__name__)


@mcg
@blue_squad
@tier1
@runs_on_provider
@skipif_ocs_version("<4.6")
@skipif_ocp_version("<4.6")
@marks.polarion_id("OCS-2376")
@skipif_managed_service
def test_hpa_noobaa_endpoint_metric():
    """
    Test to verify HPA noobaa-hpav2 cpu metrics is available.
    Since 4.10, it uses horizontal-pod-autoscaler-v2 API.
    """
    logger.info("Starting test: Verify NooBaa HPA CPU metrics are available")

    logger.test_step("Determine OCP version and HPA API version")
    ocp_version = get_semantic_version(get_ocp_version(), only_major_minor=True)
    logger.info(f"OCP version detected: {ocp_version}")

    logger.test_step("Retrieve HPA status for noobaa-hpav2")
    namespace = config.ENV_DATA["cluster_namespace"]
    logger.info(f"Querying HPA resource: noobaa-hpav2 in namespace {namespace}")

    ocp_obj = ocp.OCP(
        kind=constants.HPA,
        resource_name="noobaa-hpav2",
        namespace=namespace,
    )
    status = ocp_obj.get()["status"]
    logger.debug(f"HPA status retrieved: {status}")

    logger.test_step("Extract CPU utilization metric based on API version")
    cpu_utilization = None

    if ocp_version < VERSION_4_10:
        logger.info(
            f"Using horizontal-pod-autoscaler-v1 API (OCP {ocp_version} < 4.10)"
        )

        cpu_field_present = "currentCPUUtilizationPercentage" in status
        logger.assertion(
            f"HPA v1 CPU field present: expected=True, actual={cpu_field_present}"
        )
        assert cpu_field_present, "Failed: noobaa-hpav2 cpu metrics is unavailable"

        cpu_utilization = status["currentCPUUtilizationPercentage"]
        logger.debug(f"CPU utilization from v1 API: {cpu_utilization}%")

    else:
        logger.info(
            f"Using horizontal-pod-autoscaler-v2 API (OCP {ocp_version} >= 4.10)"
        )

        metrics_present = "currentMetrics" in status
        logger.assertion(
            f"HPA v2 currentMetrics field present: expected=True, actual={metrics_present}"
        )
        assert metrics_present, "Failed: metrics not provided in noobaa-hpav2"

        metrics_count = len(status["currentMetrics"])
        logger.debug(f"Processing {metrics_count} metrics from HPA v2 API")

        for i, metric in enumerate(status["currentMetrics"], 1):
            logger.debug(
                f"Checking metric {i}/{metrics_count}: type={metric.get('type')}, "
                f"resource={metric.get('resource', {}).get('name')}"
            )

            if metric["type"] != "Resource":
                continue
            if metric["resource"]["name"] != "cpu":
                continue

            cpu_utilization = metric["resource"]["current"]["averageUtilization"]
            logger.debug(f"CPU utilization from v2 API: {cpu_utilization}%")
            break

    logger.test_step("Validate CPU utilization metric")
    metric_found = cpu_utilization is not None
    logger.assertion(
        f"CPU utilization metric found: expected=True, actual={metric_found}"
    )
    assert metric_found, "Failed: noobaa-hpav2 cpu metrics not available"

    metric_valid = cpu_utilization >= 0
    logger.assertion(
        f"CPU utilization value valid: expected>=0, actual={cpu_utilization}, valid={metric_valid}"
    )
    assert metric_valid, f"CPU utilization should be >= 0, got {cpu_utilization}"

    logger.info(f"Current resource cpu utilized: {cpu_utilization}%")
    logger.info("Test passed: NooBaa HPA CPU metrics validated successfully")
