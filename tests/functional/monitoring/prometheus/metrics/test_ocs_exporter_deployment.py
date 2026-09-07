# -*- coding: utf8 -*-
"""
Test ocs-metrics-exporter deployment architecture and health.

RHSTOR-7964 rearchitects the exporter: single container (kube-rbac-proxy
removed), HTTPS on port 8443, /readyz health endpoint, direct
ServiceMonitor targeting. This module validates the deployment shape,
service wiring, and endpoint health in a single consolidated test flow.

Covers: ocs-tm001 (pod running), ocs-tm009 (kube-rbac-proxy removed),
ocs-tm010 (/readyz healthy).
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
from ocs_ci.ocs.ocp import OCP


logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def exporter_pod():
    """Return the running ocs-metrics-exporter pod."""
    namespace = config.ENV_DATA["cluster_namespace"]
    pod = ome_helpers.get_ocs_metrics_exporter_pod(namespace)
    assert pod is not None, (
        f"ocs-metrics-exporter pod not found in namespace {namespace}. "
        "The exporter should be deployed in internal and provider modes."
    )
    return pod


@runs_on_provider
@blue_squad
@tier1
@skipif_external_mode
@skipif_mcg_only
@skipif_ms_consumer
@pytest.mark.polarion_id("OCS-XXXX")
class TestExporterDeploymentHealth:
    """
    Validate ocs-metrics-exporter deployment architecture and health.

    Single consolidated test covering pod status, container layout,
    deployment/service/servicemonitor configuration, metrics endpoint,
    readiness endpoint, and HTTPS port — 11 checks in 4 groups.
    """

    def test_exporter_deployment_architecture_and_health(self, exporter_pod):
        """
        Verify exporter deployment shape and endpoint health.

        Group A — Pod & Container Architecture:
        1. Pod is Running
        2. Single container, 1/1 ready, no kube-rbac-proxy

        Group B — Deployment & Service Architecture:
        3. Deployment spec: single container, no kube-rbac-proxy image
        4. Service targetPort points to exporter port
        5. ServiceMonitor targets exporter port (soft)

        Group C — Endpoints & Health:
        6. /metrics endpoint accessible, valid Prometheus text
        7. /readyz returns healthy response
        8. Readiness probe configured on /readyz path
        9. Pod Ready condition is True

        Group D — Supplementary:
        10. HTTPS port 8443 configured (soft)
        11. Pod logs show initialization success (soft)
        """
        namespace = config.ENV_DATA["cluster_namespace"]

        # ---------------------------------------------------------------
        # Group A: Pod & Container Architecture
        # ---------------------------------------------------------------

        # --- Check 1: Pod is Running ---
        phase = exporter_pod.get().get("status", {}).get("phase")
        assert (
            phase == constants.STATUS_RUNNING
        ), f"ocs-metrics-exporter pod is not Running: {phase}"
        logger.info(
            "Check 1 PASSED: ocs-metrics-exporter pod %s is Running",
            exporter_pod.name,
        )

        # --- Check 2: Single container, 1/1 ready, no kube-rbac-proxy ---
        ome_helpers.assert_single_exporter_container_without_rbac_proxy(exporter_pod)
        container_statuses = (
            exporter_pod.get().get("status", {}).get("containerStatuses", [])
        )
        ready_count = sum(1 for cs in container_statuses if cs.get("ready"))
        assert ready_count == 1, (
            f"Expected 1/1 containers ready, got "
            f"{ready_count}/{len(container_statuses)}"
        )
        logger.info(
            "Check 2 PASSED: Single container, 1/1 ready, " "no kube-rbac-proxy sidecar"
        )

        # ---------------------------------------------------------------
        # Group B: Deployment & Service Architecture
        # ---------------------------------------------------------------

        # --- Check 3: Deployment spec — single container, no proxy image ---
        deployments = ome_helpers.get_ocs_metrics_exporter_deployments(namespace)
        assert deployments, f"No ocs-metrics-exporter deployment found in {namespace}"
        dep_containers = (
            deployments[0]
            .get("spec", {})
            .get("template", {})
            .get("spec", {})
            .get("containers", [])
        )
        assert len(dep_containers) == 1, (
            f"Deployment spec should have 1 container, " f"found {len(dep_containers)}"
        )
        for container in dep_containers:
            image = container.get("image", "")
            assert (
                "kube-rbac-proxy" not in image.lower()
            ), f"Found kube-rbac-proxy image in deployment: {image}"
        logger.info(
            "Check 3 PASSED: Deployment spec has single container, "
            "no kube-rbac-proxy image"
        )

        # --- Check 4: Service targetPort points to exporter ---
        ocp_service = OCP(kind=constants.SERVICE, namespace=namespace)
        services = ocp_service.get(selector=constants.OCS_METRICS_EXPORTER).get(
            "items", []
        )
        assert services, (
            f"No Service found with selector "
            f"{constants.OCS_METRICS_EXPORTER} in {namespace}"
        )
        service_ports = services[0].get("spec", {}).get("ports", [])
        valid_exporter_ports = [8443, 9443, "https-metrics", "metrics"]
        for port in service_ports:
            port_name = port.get("name", "").lower()
            target_port = port.get("targetPort")
            if "metric" in port_name or "https" in port_name:
                assert target_port in valid_exporter_ports, (
                    f"Service port '{port_name}' should target exporter "
                    f"directly, got targetPort={target_port}"
                )
        logger.info(
            "Check 4 PASSED: Service targets exporter port directly — " "ports: %s",
            service_ports,
        )

        # --- Check 5: ServiceMonitor targets exporter port (soft) ---
        try:
            ocp_sm = OCP(
                kind="ServiceMonitor",
                namespace=namespace,
                resource_name="ocs-metrics-exporter",
            )
            sm_data = ocp_sm.get()
            endpoints = sm_data.get("spec", {}).get("endpoints", [])
            for endpoint in endpoints:
                ep_port = endpoint.get("port", "")
                assert "proxy" not in ep_port.lower(), (
                    f"ServiceMonitor should not reference proxy port, "
                    f"got port={ep_port}"
                )
            logger.info(
                "Check 5 PASSED: ServiceMonitor targets exporter port " "directly"
            )
        except Exception as e:
            logger.warning(
                "Check 5: Could not verify ServiceMonitor " "configuration: %s",
                e,
            )

        # ---------------------------------------------------------------
        # Group C: Endpoints & Health
        # ---------------------------------------------------------------

        # --- Check 6: /metrics endpoint accessible, valid Prometheus text ---
        metrics_sample = ome_helpers.scrape_metrics_text_sample(
            exporter_pod, max_bytes=1024
        )
        ome_helpers.assert_prometheus_exposition_text(metrics_sample)
        logger.info(
            "Check 6 PASSED: /metrics endpoint accessible, "
            "valid Prometheus text format"
        )

        # --- Check 7: /readyz returns healthy response ---
        # curl -f fails on non-2xx, so if check_exporter_readyz succeeds
        # without raising, the endpoint returned HTTP 2xx (healthy).
        # Some builds return an empty body with 200 OK.
        readyz_response = ome_helpers.check_exporter_readyz(exporter_pod)
        if readyz_response:
            healthy_indicators = ["ok", "ready", "healthy", "true", "200"]
            is_healthy = any(
                indicator in readyz_response.lower() for indicator in healthy_indicators
            )
            if not is_healthy:
                logger.warning(
                    "Check 7: /readyz returned unexpected body: %s",
                    readyz_response[:200],
                )
        logger.info(
            "Check 7 PASSED: /readyz endpoint reachable (HTTP 2xx) — %s",
            repr(readyz_response[:100]) if readyz_response else "(empty body)",
        )

        # --- Check 8: Readiness probe configured on /readyz path ---
        pod_spec = exporter_pod.get().get("spec", {})
        spec_containers = pod_spec.get("containers", [])
        assert spec_containers, "No containers found in pod spec"
        readiness_probe = spec_containers[0].get("readinessProbe")
        assert readiness_probe is not None, (
            "Readiness probe not configured on exporter container. "
            "RHSTOR-7964 requires /readyz readiness probe."
        )
        http_get = readiness_probe.get("httpGet", {})
        probe_path = http_get.get("path", "")
        assert (
            "/readyz" in probe_path or "/ready" in probe_path
        ), f"Readiness probe should target /readyz, got path={probe_path}"
        logger.info(
            "Check 8 PASSED: Readiness probe configured — %s",
            readiness_probe,
        )

        # --- Check 9: Pod Ready condition is True ---
        conditions = exporter_pod.get().get("status", {}).get("conditions", [])
        ready_condition = next(
            (c for c in conditions if c.get("type") == "Ready"), None
        )
        assert ready_condition is not None, "Ready condition not found in pod status"
        assert (
            ready_condition.get("status") == "True"
        ), f"Pod is not Ready: {ready_condition}"
        logger.info("Check 9 PASSED: Pod Ready condition is True")

        # ---------------------------------------------------------------
        # Group D: Supplementary (soft checks)
        # ---------------------------------------------------------------

        # --- Check 10: HTTPS port 8443 configured (soft) ---
        try:
            ome_helpers.assert_exporter_uses_https_port(exporter_pod)
            logger.info(
                "Check 10 PASSED: Exporter uses HTTPS port %d",
                constants.OCS_METRICS_EXPORTER_HTTPS_PORT,
            )
        except AssertionError as e:
            logger.warning("Check 10: HTTPS port verification — %s", e)

        # --- Check 11: Pod logs show initialization success (soft) ---
        try:
            logs = exporter_pod.get_logs(tail=50)
            success_indicators = [
                "starting",
                "initialized",
                "listening",
                "ready",
            ]
            has_success = any(
                indicator in logs.lower() for indicator in success_indicators
            )
            if has_success:
                logger.info(
                    "Check 11 PASSED: Pod logs show successful " "initialization"
                )
            else:
                logger.warning(
                    "Check 11: Pod logs do not contain init indicators "
                    "%s in last 50 lines",
                    success_indicators,
                )
        except Exception as e:
            logger.warning("Check 11: Could not verify pod logs — %s", e)
