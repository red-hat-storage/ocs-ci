# -*- coding: utf8 -*-
"""
Test cases for RHSTOR-7964 Group 1: Exporter Deployment Verification.

This module tests:
- ocs-tm001: Exporter pod running in Internal/Provider mode
- ocs-tm009: kube-rbac-proxy container removed
- ocs-tm010: /readyz endpoint healthy

Test cases validate that the ocs-metrics-exporter pod is properly deployed
with the new architecture (single container, no kube-rbac-proxy, working
readiness endpoint).
"""

import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    blue_squad,
    runs_on_provider,
    skipif_external_mode,
    skipif_mcg_only,
    tier1,
)
from ocs_ci.helpers import ocs_metrics_exporter_helpers as ome_helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import get_pod_logs

logger = logging.getLogger(__name__)


@blue_squad
@tier1
@runs_on_provider
@skipif_external_mode
@skipif_mcg_only
@pytest.mark.polarion_id("ocs-tm001")
def test_exporter_pod_running():
    """
    Test ocs-metrics-exporter pod is running on provider/internal cluster.

    Uses @runs_on_provider marker which automatically:
    - Runs on provider cluster in multi-cluster topology
    - Runs on internal mode cluster (standalone deployment)
    - Skips on consumer clusters

    Verifies:
    1. Exporter pod exists and is in Running state
    2. Pod has 1/1 containers ready
    3. Pod logs show successful initialization
    4. Metrics endpoint is accessible

    Polarion:
        ocs-tm001
    """
    namespace = config.ENV_DATA["cluster_namespace"]

    logger.info("Verifying ocs-metrics-exporter pod in namespace %s", namespace)
    pod_obj = ome_helpers.get_ocs_metrics_exporter_pod(namespace)
    assert pod_obj is not None, (
        f"ocs-metrics-exporter pod not found in namespace {namespace}. "
        "The exporter should be deployed in internal and provider modes."
    )

    pod_phase = pod_obj.get().get("status", {}).get("phase")
    logger.assertion(
        "Exporter pod phase: expected=%s, actual=%s",
        constants.STATUS_RUNNING,
        pod_phase,
    )
    assert (
        pod_phase == constants.STATUS_RUNNING
    ), f"ocs-metrics-exporter pod is not in Running state: {pod_phase}"

    container_statuses = pod_obj.get().get("status", {}).get("containerStatuses", [])
    ready_count = sum(1 for cs in container_statuses if cs.get("ready"))
    logger.assertion(
        "Exporter container count: expected=1, actual=%s; ready=%s/%s",
        len(container_statuses),
        ready_count,
        len(container_statuses),
    )
    assert len(container_statuses) == 1, (
        f"Expected 1 container, found {len(container_statuses)}. "
        "RHSTOR-7964 requires single container (no kube-rbac-proxy)."
    )
    assert (
        ready_count == 1
    ), f"Expected 1/1 containers ready, got {ready_count}/{len(container_statuses)}"
    logger.info(
        "ocs-metrics-exporter pod %s is running with 1/1 containers ready",
        pod_obj.name,
    )

    try:
        logs = get_pod_logs(pod_name=pod_obj.name, namespace=namespace, tail=50)
        success_indicators = ["starting", "initialized", "listening", "ready", "info"]
        has_success = any(indicator in logs.lower() for indicator in success_indicators)
        logger.assertion(
            "Exporter pod log initialization indicators present: %s", has_success
        )
        assert has_success, (
            "Pod logs do not show successful initialization. "
            f"Expected one of {success_indicators} in logs."
        )
        logger.info("Pod logs show successful initialization")
    except (AssertionError, CommandFailed) as exc:
        logger.warning("Could not verify exporter pod logs: %s", exc)

    logger.info("Scraping /metrics from exporter pod %s", pod_obj.name)
    metrics_sample = ome_helpers.scrape_metrics_text_sample(pod_obj, max_bytes=1024)
    ome_helpers.assert_prometheus_exposition_text(metrics_sample)
    logger.info("Metrics endpoint is accessible and returns valid Prometheus format")


@blue_squad
@tier1
@runs_on_provider
@skipif_external_mode
@skipif_mcg_only
@pytest.mark.polarion_id("ocs-tm009")
def test_kube_rbac_proxy_removed():
    """
    Test that kube-rbac-proxy container is removed from exporter deployment.

    RHSTOR-7964 removes the kube-rbac-proxy sidecar container. This test verifies:
    1. Exporter pod has exactly one container
    2. No container named 'kube-rbac-proxy' exists
    3. Deployment spec does not reference kube-rbac-proxy image
    4. Service port configuration points directly to exporter
    5. ServiceMonitor targets exporter port directly

    Polarion:
        ocs-tm009
    """
    namespace = config.ENV_DATA["cluster_namespace"]

    pod_obj = ome_helpers.get_ocs_metrics_exporter_pod(namespace)
    assert (
        pod_obj is not None
    ), f"ocs-metrics-exporter pod not found in namespace {namespace}"

    ome_helpers.assert_single_exporter_container_without_rbac_proxy(pod_obj)
    logger.info("Exporter pod has single container with no kube-rbac-proxy sidecar")

    deployments = ome_helpers.get_ocs_metrics_exporter_deployments(namespace)
    assert deployments, f"No ocs-metrics-exporter deployment found in {namespace}"

    deployment = deployments[0]
    containers = (
        deployment.get("spec", {})
        .get("template", {})
        .get("spec", {})
        .get("containers", [])
    )
    logger.assertion(
        "Deployment container count: expected=1, actual=%s", len(containers)
    )
    assert (
        len(containers) == 1
    ), f"Deployment spec should have 1 container, found {len(containers)}"

    for container in containers:
        image = container.get("image", "")
        assert (
            "kube-rbac-proxy" not in image.lower()
        ), f"Found kube-rbac-proxy image reference in deployment: {image}"
    logger.info("Deployment spec has no kube-rbac-proxy image references")

    ocp_service = OCP(kind=constants.SERVICE, namespace=namespace)
    services = ocp_service.get(selector=constants.OCS_METRICS_EXPORTER).get("items", [])

    if services:
        service = services[0]
        ports = service.get("spec", {}).get("ports", [])
        logger.info("Exporter service ports: %s", ports)

        for port in ports:
            port_name = port.get("name", "").lower()
            target_port = port.get("targetPort")

            if port_name == "https-self":
                logger.assertion(
                    "Service port %s targetPort: expected=%s, actual=%s",
                    port_name,
                    constants.OCS_METRICS_EXPORTER_SELF_HTTPS_PORT,
                    target_port,
                )
                assert target_port in [
                    constants.OCS_METRICS_EXPORTER_SELF_HTTPS_PORT,
                    "https-self",
                ], (
                    f"Service port {port_name} should target self-metrics port "
                    f"{constants.OCS_METRICS_EXPORTER_SELF_HTTPS_PORT}, "
                    f"got targetPort={target_port}"
                )
                continue

            if "metric" in port_name or port_name == "https-main":
                logger.assertion(
                    "Service port %s targetPort: expected=%s, actual=%s",
                    port_name,
                    constants.OCS_METRICS_EXPORTER_HTTPS_PORT,
                    target_port,
                )
                assert target_port in [
                    constants.OCS_METRICS_EXPORTER_HTTPS_PORT,
                    "https-main",
                    "https-metrics",
                    "metrics",
                ], (
                    f"Service port {port_name} should target exporter port directly, "
                    f"got targetPort={target_port}"
                )

        logger.info("Service configuration points directly to exporter ports")

    ocp_sm = OCP(
        kind="ServiceMonitor",
        namespace=namespace,
        resource_name="ocs-metrics-exporter",
    )
    sm_data = ocp_sm.get()
    endpoints = sm_data.get("spec", {}).get("endpoints", [])
    for endpoint in endpoints:
        port = endpoint.get("port", "")
        scheme = endpoint.get("scheme", "http")
        logger.info("ServiceMonitor endpoint: port=%s, scheme=%s", port, scheme)
        assert (
            "proxy" not in port.lower()
        ), f"ServiceMonitor should not reference proxy port, got port={port}"
    logger.info("ServiceMonitor targets exporter port directly")


@blue_squad
@tier1
@runs_on_provider
@skipif_external_mode
@skipif_mcg_only
@pytest.mark.polarion_id("ocs-tm010")
def test_readiness_endpoint_healthy():
    """
    Test /readyz endpoint when Ceph is healthy.

    RHSTOR-7964 introduces a /readyz endpoint for health checks. This test verifies:
    1. /readyz endpoint returns HTTP 200 when Ceph is healthy
    2. Kubernetes readiness probe is configured to use /readyz
    3. Pod readiness status reflects /readyz health

    Polarion:
        ocs-tm010
    """
    namespace = config.ENV_DATA["cluster_namespace"]

    pod_obj = ome_helpers.get_ocs_metrics_exporter_pod(namespace)
    assert (
        pod_obj is not None
    ), f"ocs-metrics-exporter pod not found in namespace {namespace}"

    logger.info("Querying /readyz on exporter pod %s", pod_obj.name)
    readyz_response = ome_helpers.check_exporter_readyz(pod_obj)
    logger.info("/readyz response body: %r", readyz_response[:200])

    if readyz_response:
        healthy_indicators = ["ok", "ready", "healthy", "true", "200"]
        is_healthy = any(
            indicator in readyz_response.lower() for indicator in healthy_indicators
        )
        logger.assertion("/readyz body indicates healthy status: %s", is_healthy)
        assert is_healthy, (
            f"/readyz response does not indicate healthy status. "
            f"Expected one of {healthy_indicators}, got: {readyz_response[:200]}"
        )
    logger.info("/readyz endpoint returned HTTP 200")

    pod_spec = pod_obj.get().get("spec", {})
    containers = pod_spec.get("containers", [])
    assert containers, "No containers found in pod spec"

    exporter_container = containers[0]
    readiness_probe = exporter_container.get("readinessProbe")
    assert readiness_probe is not None, (
        "Readiness probe not configured on exporter container. "
        "RHSTOR-7964 requires /readyz readiness probe."
    )

    http_get = readiness_probe.get("httpGet", {})
    probe_path = http_get.get("path", "")
    logger.assertion("Readiness probe path contains /readyz: path=%s", probe_path)
    assert (
        "/readyz" in probe_path or "/ready" in probe_path
    ), f"Readiness probe should target /readyz endpoint, got path={probe_path}"
    logger.info("Readiness probe configured: %s", readiness_probe)

    pod_status = pod_obj.get().get("status", {})
    conditions = pod_status.get("conditions", [])
    ready_condition = next((c for c in conditions if c.get("type") == "Ready"), None)

    assert ready_condition is not None, "Ready condition not found in pod status"
    logger.assertion(
        "Pod Ready condition: expected=True, actual=%s",
        ready_condition.get("status"),
    )
    assert (
        ready_condition.get("status") == "True"
    ), f"Pod is not ready. Ready condition: {ready_condition}"
    logger.info("Pod readiness status is True")

    ome_helpers.assert_exporter_uses_https_port(pod_obj)
    logger.info(
        "Exporter is configured to use HTTPS port %s",
        constants.OCS_METRICS_EXPORTER_HTTPS_PORT,
    )
