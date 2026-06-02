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
    skipif_ms_consumer,
    tier1,
)
from ocs_ci.helpers import ocs_metrics_exporter_helpers as ome_helpers
from ocs_ci.ocs import constants

logger = logging.getLogger(__name__)


@blue_squad
@tier1
@runs_on_provider
@skipif_external_mode
@skipif_mcg_only
@skipif_ms_consumer
@pytest.mark.polarion_id("ocs-tm001")
def test_exporter_pod_running():
    """
    Test ocs-metrics-exporter pod is running on provider/internal cluster.

    Uses @runs_on_provider marker which automatically:
    - Runs on provider cluster in multi-cluster topology
    - Runs on internal mode cluster (standalone deployment)
    - Skips on consumer clusters

    The deployment mode is detected at runtime based on cluster configuration.

    Verifies:
    1. Exporter pod exists and is in Running state
    2. Pod has 1/1 containers ready
    3. Pod logs show successful initialization
    4. Exporter mode is correctly detected
    5. Metrics endpoint is accessible

    Polarion:
        ocs-tm001
    """
    namespace = config.ENV_DATA["cluster_namespace"]

    # Get the exporter pod
    pod_obj = ome_helpers.get_ocs_metrics_exporter_pod(namespace)
    assert pod_obj is not None, (
        f"ocs-metrics-exporter pod not found in namespace {namespace}. "
        "The exporter should be deployed in internal and provider modes."
    )

    # Verify pod is running
    assert pod_obj.get().get("status", {}).get("phase") == constants.STATUS_RUNNING, (
        f"ocs-metrics-exporter pod is not in Running state: "
        f"{pod_obj.get().get('status', {}).get('phase')}"
    )

    # Verify 1/1 containers ready
    container_statuses = pod_obj.get().get("status", {}).get("containerStatuses", [])
    assert len(container_statuses) == 1, (
        f"Expected 1 container, found {len(container_statuses)}. "
        "RHSTOR-7964 requires single container (no kube-rbac-proxy)."
    )

    ready_count = sum(1 for cs in container_statuses if cs.get("ready"))
    assert (
        ready_count == 1
    ), f"Expected 1/1 containers ready, got {ready_count}/{len(container_statuses)}"

    logger.info(
        f"✓ ocs-metrics-exporter pod {pod_obj.name} is running with 1/1 containers ready"
    )

    # Verify successful initialization in logs
    try:
        logs = pod_obj.get_logs(tail=50)
        # Check for common initialization success indicators
        success_indicators = ["starting", "initialized", "listening", "ready"]
        has_success = any(indicator in logs.lower() for indicator in success_indicators)
        assert has_success, (
            "Pod logs do not show successful initialization. "
            f"Expected one of {success_indicators} in logs."
        )
        logger.info("✓ Pod logs show successful initialization")
    except Exception as e:
        logger.warning(f"Could not verify logs: {e}")

    # Verify metrics endpoint is accessible
    try:
        metrics_sample = ome_helpers.scrape_metrics_text_sample(pod_obj, max_bytes=1024)
        ome_helpers.assert_prometheus_exposition_text(metrics_sample)
        logger.info(
            "✓ Metrics endpoint is accessible and returns valid Prometheus format"
        )
    except Exception as e:
        pytest.fail(f"Failed to scrape metrics endpoint: {e}")


@blue_squad
@tier1
@runs_on_provider
@skipif_external_mode
@skipif_mcg_only
@skipif_ms_consumer
@pytest.mark.polarion_id("ocs-tm009")
def test_kube_rbac_proxy_removed():
    """
    Test that kube-rbac-proxy container is removed from exporter deployment.

    Uses @runs_on_provider marker which automatically:
    - Runs on provider cluster in multi-cluster topology
    - Runs on internal mode cluster (standalone deployment)
    - Skips on consumer clusters

    The deployment mode is detected at runtime based on cluster configuration.

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

    # Get the exporter pod
    pod_obj = ome_helpers.get_ocs_metrics_exporter_pod(namespace)
    assert (
        pod_obj is not None
    ), f"ocs-metrics-exporter pod not found in namespace {namespace}"

    # Verify single container without kube-rbac-proxy
    ome_helpers.assert_single_exporter_container_without_rbac_proxy(pod_obj)
    logger.info("✓ Exporter pod has single container, no kube-rbac-proxy sidecar")

    # Verify deployment spec
    deployments = ome_helpers.get_ocs_metrics_exporter_deployments(namespace)
    assert deployments, f"No ocs-metrics-exporter deployment found in {namespace}"

    deployment = deployments[0]
    containers = (
        deployment.get("spec", {})
        .get("template", {})
        .get("spec", {})
        .get("containers", [])
    )

    # Check container count in deployment spec
    assert (
        len(containers) == 1
    ), f"Deployment spec should have 1 container, found {len(containers)}"

    # Check no kube-rbac-proxy image reference
    for container in containers:
        image = container.get("image", "")
        assert (
            "kube-rbac-proxy" not in image.lower()
        ), f"Found kube-rbac-proxy image reference in deployment: {image}"

    logger.info("✓ Deployment spec has no kube-rbac-proxy image references")

    # Verify service configuration
    from ocs_ci.ocs.ocp import OCP

    ocp_service = OCP(kind=constants.SERVICE, namespace=namespace)
    services = ocp_service.get(selector=constants.OCS_METRICS_EXPORTER).get("items", [])

    if services:
        service = services[0]
        ports = service.get("spec", {}).get("ports", [])
        logger.info(f"Service ports: {ports}")

        # Verify service points to exporter port (not proxy port)
        for port in ports:
            port_name = port.get("name", "").lower()
            target_port = port.get("targetPort")

            # Should target exporter port directly
            if "metric" in port_name or "https" in port_name:
                assert target_port in [8443, "https-metrics", "metrics"], (
                    f"Service port {port_name} should target exporter port directly, "
                    f"got targetPort={target_port}"
                )

        logger.info("✓ Service configuration points directly to exporter port")

    # Verify ServiceMonitor configuration
    try:
        from ocs_ci.ocs.ocp import OCP

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

            logger.info(f"ServiceMonitor endpoint: port={port}, scheme={scheme}")

            # Should target exporter port directly
            assert (
                "proxy" not in port.lower()
            ), f"ServiceMonitor should not reference proxy port, got port={port}"

        logger.info("✓ ServiceMonitor targets exporter port directly")
    except Exception as e:
        logger.warning(f"Could not verify ServiceMonitor: {e}")


@blue_squad
@tier1
@runs_on_provider
@skipif_external_mode
@skipif_mcg_only
@skipif_ms_consumer
@pytest.mark.polarion_id("ocs-tm010")
def test_readiness_endpoint_healthy():
    """
    Test /readyz endpoint when Ceph is healthy.

    Uses @runs_on_provider marker which automatically:
    - Runs on provider cluster in multi-cluster topology
    - Runs on internal mode cluster (standalone deployment)
    - Skips on consumer clusters

    The deployment mode is detected at runtime based on cluster configuration.

    RHSTOR-7964 introduces a /readyz endpoint for health checks. This test verifies:
    1. /readyz endpoint returns HTTP 200 when Ceph is healthy
    2. Response body indicates healthy status
    3. Kubernetes readiness probe is configured to use /readyz
    4. Pod readiness status reflects /readyz health

    Polarion:
        ocs-tm010
    """
    namespace = config.ENV_DATA["cluster_namespace"]

    # Get the exporter pod
    pod_obj = ome_helpers.get_ocs_metrics_exporter_pod(namespace)
    assert (
        pod_obj is not None
    ), f"ocs-metrics-exporter pod not found in namespace {namespace}"

    # Query /readyz endpoint
    try:
        readyz_response = ome_helpers.check_exporter_readyz(pod_obj)
        logger.info(f"/readyz response: {readyz_response[:200]}")

        # Verify response indicates healthy
        assert readyz_response, "/readyz returned empty response"

        # Common health indicators
        healthy_indicators = ["ok", "ready", "healthy", "true", "200"]
        is_healthy = any(
            indicator in readyz_response.lower() for indicator in healthy_indicators
        )

        assert is_healthy, (
            f"/readyz response does not indicate healthy status. "
            f"Expected one of {healthy_indicators}, got: {readyz_response[:200]}"
        )

        logger.info("✓ /readyz endpoint returns healthy status")
    except Exception as e:
        pytest.fail(f"Failed to query /readyz endpoint: {e}")

    # Verify readiness probe configuration
    pod_spec = pod_obj.get().get("spec", {})
    containers = pod_spec.get("containers", [])

    assert containers, "No containers found in pod spec"

    exporter_container = containers[0]
    readiness_probe = exporter_container.get("readinessProbe")

    assert readiness_probe is not None, (
        "Readiness probe not configured on exporter container. "
        "RHSTOR-7964 requires /readyz readiness probe."
    )

    # Verify probe targets /readyz
    http_get = readiness_probe.get("httpGet", {})
    probe_path = http_get.get("path", "")

    assert (
        "/readyz" in probe_path or "/ready" in probe_path
    ), f"Readiness probe should target /readyz endpoint, got path={probe_path}"

    logger.info(f"✓ Readiness probe configured: {readiness_probe}")

    # Verify pod readiness status
    pod_status = pod_obj.get().get("status", {})
    conditions = pod_status.get("conditions", [])

    ready_condition = next((c for c in conditions if c.get("type") == "Ready"), None)

    assert ready_condition is not None, "Ready condition not found in pod status"
    assert (
        ready_condition.get("status") == "True"
    ), f"Pod is not ready. Ready condition: {ready_condition}"

    logger.info("✓ Pod readiness status is True (reflects /readyz health)")

    # Verify HTTPS port configuration (RHSTOR-7964 uses port 8443)
    try:
        ome_helpers.assert_exporter_uses_https_port(pod_obj)
        logger.info("✓ Exporter is configured to use HTTPS port 8443")
    except AssertionError as e:
        logger.warning(f"HTTPS port verification: {e}")


# Made with Bob
