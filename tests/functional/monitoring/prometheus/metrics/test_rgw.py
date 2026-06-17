# -*- coding: utf8 -*-
"""
Test cases here performs Prometheus queries directly without a workload, to
check that OCS Monitoring is configured and available as expected for RGW.
"""

import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import blue_squad
from ocs_ci.framework.testlib import (
    skipif_managed_service,
    runs_on_provider,
    skipif_ocs_version,
    tier4c,
)
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs import metrics
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility.prometheus import PrometheusAPI


logger = logging.getLogger(__name__)


@blue_squad
@skipif_ocs_version("<4.6")
@tier4c
@pytest.mark.polarion_id("OCS-2385")
@skipif_managed_service
@runs_on_provider
def test_ceph_rgw_metrics_after_metrics_exporter_respin(
    rgw_deployments, threading_lock
):
    """
    RGW metrics should be provided via OCP Prometheus even after
    ocs-metrics-exporter pod is respinned.

    """
    logger.info(
        "Starting test: Verify RGW metrics availability after metrics exporter respin"
    )

    logger.test_step("Locate and delete ocs-metrics-exporter pod")
    namespace = config.ENV_DATA["cluster_namespace"]
    selector = "app.kubernetes.io/name=ocs-metrics-exporter"
    logger.info(
        f"Looking for metrics exporter pod (namespace: {namespace}, selector: {selector})"
    )

    pod_obj = ocp.OCP(kind=constants.POD, namespace=namespace)
    metrics_pods = pod_obj.get(selector=selector)["items"]

    pod_count = len(metrics_pods)
    logger.assertion(f"Metrics exporter pod count: expected=1, actual={pod_count}")
    assert pod_count == 1, f"Expected exactly 1 metrics exporter pod, found {pod_count}"

    metrics_pod_data = metrics_pods[0]
    pod_name = metrics_pod_data.get("metadata", {}).get("name", "unknown")
    logger.info(f"Found metrics exporter pod: {pod_name}")

    metrics_pod = OCS(**metrics_pod_data)
    logger.info(f"Deleting pod {pod_name} (force=True)")
    metrics_pod.delete(force=True)
    logger.info(f"Pod {pod_name} deleted successfully")

    logger.test_step("Wait for ocs-metrics-exporter pod to be recreated")
    logger.info(
        "Waiting for new metrics exporter pod (condition: Running, timeout: 600s)"
    )

    pod_ready = pod_obj.wait_for_resource(
        condition="Running",
        selector=selector,
        resource_count=1,
        timeout=600,
    )
    logger.assertion(
        f"New metrics exporter pod running: expected=True, actual={pod_ready}"
    )
    assert pod_ready, "Metrics exporter pod did not reach Running state within timeout"
    logger.info("New metrics exporter pod is running")

    logger.test_step("Collect and validate RGW metrics from Prometheus")
    prometheus = PrometheusAPI(threading_lock=threading_lock)
    expected_metrics_count = len(metrics.ceph_rgw_metrics)
    logger.info(f"Checking {expected_metrics_count} RGW metrics for availability")
    logger.debug(f"RGW metrics to check: {metrics.ceph_rgw_metrics}")

    list_of_metrics_without_results = metrics.get_missing_metrics(
        prometheus, metrics.ceph_rgw_metrics
    )

    missing_count = len(list_of_metrics_without_results)
    logger.info(
        f"Metrics validation: {expected_metrics_count - missing_count}/{expected_metrics_count} metrics have data"
    )

    if list_of_metrics_without_results:
        logger.warning(
            f"Missing metrics ({missing_count}): {list_of_metrics_without_results}"
        )

    logger.assertion(
        f"All RGW metrics available: expected=0 missing, actual={missing_count} missing"
    )

    msg = (
        "OCS Monitoring should provide some value(s) for tested rgw metrics, "
        "so that the list of metrics without results is empty."
    )
    assert list_of_metrics_without_results == [], msg

    logger.info("Test passed: All RGW metrics available after metrics exporter respin")
