# -*- coding: utf8 -*-
"""
Test ocs-metrics-exporter deployment resource tuning and performance.

RHSTOR-7964 rearchitects the exporter: this module validates that the
deployment is properly configured with appropriate resource limits,
requests, and scheduling constraints, and that the exporter performs
well under repeated metrics scrapes.

Covers: ocs-tm029 (resource limits/requests), ocs-tm030 (performance).
"""

import logging
import shlex
import time

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    blue_squad,
    runs_on_provider,
    skipif_external_mode,
    skipif_mcg_only,
    skipif_ms_consumer,
    tier2,
)
from ocs_ci.helpers import ocs_metrics_exporter_helpers as ome_helpers
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


@pytest.fixture(scope="module")
def exporter_deployment():
    """Return the first ocs-metrics-exporter Deployment dict."""
    namespace = config.ENV_DATA["cluster_namespace"]
    deployments = ome_helpers.get_ocs_metrics_exporter_deployments(namespace)
    assert (
        deployments
    ), f"No ocs-metrics-exporter deployment found in namespace {namespace}"
    return deployments[0]


def _parse_memory_to_mb(value):
    """Convert a Kubernetes memory string (e.g. '128Mi', '1Gi') to MB."""
    if value.endswith("Gi"):
        return float(value[:-2]) * 1024
    if value.endswith("Mi"):
        return float(value[:-2])
    if value.endswith("Ki"):
        return float(value[:-2]) / 1024
    return float(value) / (1024 * 1024)


def _parse_cpu_to_millicores(value):
    """Convert a Kubernetes CPU string (e.g. '100m', '0.5') to millicores."""
    if isinstance(value, str) and value.endswith("m"):
        return float(value[:-1])
    return float(value) * 1000


@runs_on_provider
@blue_squad
@tier2
@skipif_external_mode
@skipif_mcg_only
@skipif_ms_consumer
@pytest.mark.polarion_id("OCS-6029")
class TestDeploymentTuning:
    """
    Validate ocs-metrics-exporter resource configuration and performance.

    Single consolidated test covering resource requests/limits,
    production-readiness thresholds, runtime resource health,
    and metrics scrape performance — 11 checks in 3 groups.
    """

    def test_resource_configuration_and_performance(
        self, exporter_pod, exporter_deployment
    ):
        """
        Verify resource tuning and performance under load.

        Group A — Resource Configuration:
        1. Deployment has resource requests (memory + cpu)
        2. Deployment has resource limits (memory + cpu)
        3. Pod resource allocation matches deployment spec
        4. Resource values are production-ready

        Group B — Runtime Resource Health:
        5. Memory usage within limits (soft)
        6. No OOMKilled in container status
        7. No resource warning strings in pod logs (soft)

        Group C — Performance:
        8. Baseline metrics scrape completes
        9. Average scrape time under 5 seconds
        10. No significant performance degradation (soft)
        11. Exporter still responsive after load
        """
        namespace = config.ENV_DATA["cluster_namespace"]

        # ---------------------------------------------------------------
        # Group A: Resource Configuration
        # ---------------------------------------------------------------

        dep_containers = (
            exporter_deployment.get("spec", {})
            .get("template", {})
            .get("spec", {})
            .get("containers", [])
        )
        assert dep_containers, "No containers found in deployment spec"
        exporter_container = None
        for c in dep_containers:
            if "ocs-metrics-exporter" in c.get("name", ""):
                exporter_container = c
                break
        if exporter_container is None:
            exporter_container = dep_containers[0]

        resources = exporter_container.get("resources", {})

        # --- Check 1: Resource requests defined ---
        requests = resources.get("requests", {})
        assert requests, (
            "Deployment container has no resource requests defined. "
            "Production deployments must declare memory and cpu requests."
        )
        assert "memory" in requests, "Resource requests missing 'memory'"
        assert "cpu" in requests, "Resource requests missing 'cpu'"
        logger.info(
            "Check 1 PASSED: Resource requests defined — memory=%s, cpu=%s",
            requests.get("memory"),
            requests.get("cpu"),
        )

        # --- Check 2: Resource limits defined ---
        limits = resources.get("limits", {})
        assert limits, (
            "Deployment container has no resource limits defined. "
            "Production deployments must declare memory and cpu limits."
        )
        assert "memory" in limits, "Resource limits missing 'memory'"
        assert "cpu" in limits, "Resource limits missing 'cpu'"
        logger.info(
            "Check 2 PASSED: Resource limits defined — memory=%s, cpu=%s",
            limits.get("memory"),
            limits.get("cpu"),
        )

        # --- Check 3: Pod resource allocation matches deployment spec ---
        ocp_pod = OCP(kind="Pod", namespace=namespace)
        pod_dict = ocp_pod.get(resource_name=exporter_pod.name)
        pod_containers = pod_dict.get("spec", {}).get("containers", [])
        pod_exporter = None
        for c in pod_containers:
            if "ocs-metrics-exporter" in c.get("name", ""):
                pod_exporter = c
                break
        if pod_exporter is None:
            pod_exporter = pod_containers[0]

        pod_resources = pod_exporter.get("resources", {})
        pod_requests = pod_resources.get("requests", {})
        pod_limits = pod_resources.get("limits", {})
        assert pod_requests == requests, (
            f"Pod requests {pod_requests} do not match "
            f"deployment requests {requests}"
        )
        assert pod_limits == limits, (
            f"Pod limits {pod_limits} do not match " f"deployment limits {limits}"
        )
        logger.info("Check 3 PASSED: Pod resource allocation matches deployment spec")

        # --- Check 4: Resource values are production-ready ---
        mem_request_mb = _parse_memory_to_mb(requests["memory"])
        assert mem_request_mb >= 64, (
            f"Memory request {requests['memory']} ({mem_request_mb:.0f}MB) "
            f"is below minimum 64MB for production"
        )
        cpu_request_mc = _parse_cpu_to_millicores(requests["cpu"])
        assert cpu_request_mc >= 10, (
            f"CPU request {requests['cpu']} ({cpu_request_mc:.0f}m) "
            f"is below minimum 10m for production"
        )
        logger.info(
            "Check 4 PASSED: Resource values production-ready — "
            "memory=%s (%.0fMB >= 64MB), cpu=%s (%.0fm >= 10m)",
            requests["memory"],
            mem_request_mb,
            requests["cpu"],
            cpu_request_mc,
        )

        # ---------------------------------------------------------------
        # Group B: Runtime Resource Health
        # ---------------------------------------------------------------

        # --- Check 5: Memory usage within limits (soft) ---
        try:
            cgroup_inner = (
                "cat /sys/fs/cgroup/memory/memory.usage_in_bytes 2>/dev/null "
                "|| cat /sys/fs/cgroup/memory.current 2>/dev/null "
                "|| echo UNAVAILABLE"
            )
            cgroup_cmd = f"sh -c {shlex.quote(cgroup_inner)}"
            usage_raw = exporter_pod.exec_cmd_on_pod(cgroup_cmd, out_yaml_format=False)
            if usage_raw and "UNAVAILABLE" not in usage_raw:
                usage_bytes = int(usage_raw.strip())
                usage_mb = usage_bytes / (1024 * 1024)
                limit_mb = _parse_memory_to_mb(limits["memory"])
                usage_pct = (usage_mb / limit_mb) * 100 if limit_mb > 0 else 0
                if usage_pct < 90:
                    logger.info(
                        "Check 5 PASSED: Memory usage %.1fMB / %.1fMB "
                        "(%.1f%% < 90%%)",
                        usage_mb,
                        limit_mb,
                        usage_pct,
                    )
                else:
                    logger.warning(
                        "Check 5: Memory usage %.1fMB / %.1fMB "
                        "(%.1f%% >= 90%% threshold)",
                        usage_mb,
                        limit_mb,
                        usage_pct,
                    )
            else:
                logger.warning("Check 5: cgroup memory stats unavailable in container")
        except Exception as exc:
            logger.warning("Check 5: Could not read cgroup memory — %s", exc)

        # --- Check 6: No OOMKilled in container status ---
        container_statuses = pod_dict.get("status", {}).get("containerStatuses", [])
        for cs in container_statuses:
            last_state = cs.get("lastState", {})
            terminated = last_state.get("terminated", {})
            reason = terminated.get("reason", "")
            assert reason != "OOMKilled", (
                f"Container '{cs.get('name')}' was OOMKilled. "
                f"Review memory limits: {limits}"
            )
        logger.info("Check 6 PASSED: No OOMKilled in container status")

        # --- Check 7: No resource warning strings in pod logs (soft) ---
        try:
            log_cmd = f"oc logs {exporter_pod.name} -n {namespace} --tail=200"
            log_result = ome_helpers.exec_cmd(log_cmd, secrets=[])
            raw_logs = log_result.stdout or b""
            logs = (
                raw_logs.decode().strip()
                if isinstance(raw_logs, bytes)
                else raw_logs.strip()
            )
            resource_warnings = [
                "out of memory",
                "oom",
                "memory limit",
                "cpu throttling",
            ]
            found_warnings = [kw for kw in resource_warnings if kw in logs.lower()]
            if found_warnings:
                logger.warning(
                    "Check 7: Resource warnings found in pod logs: %s",
                    found_warnings,
                )
            else:
                logger.info("Check 7 PASSED: No resource warning strings in pod logs")
        except Exception as exc:
            logger.warning("Check 7: Could not check pod logs — %s", exc)

        # ---------------------------------------------------------------
        # Group C: Performance
        # ---------------------------------------------------------------

        endpoint = ome_helpers.resolve_metrics_endpoint(exporter_pod)
        url = endpoint["url"]

        def _build_timed_curl(token=None):
            parts = [
                "curl",
                "-sS",
                "--connect-timeout",
                "5",
                "--max-time",
                "15",
                "-f",
                "-o",
                "/dev/null",
                "-w",
                "%{time_total}",
            ]
            if endpoint["tls_skip_verify"]:
                parts.append("-k")
            if endpoint["bearer_auth"] and token:
                parts.extend(["-H", f"Authorization: Bearer {token}"])
            parts.append(url)
            return " ".join(shlex.quote(p) for p in parts)

        # --- Check 8: Baseline metrics scrape completes ---
        bearer_token = None
        if endpoint["bearer_auth"]:
            bearer_token = ome_helpers.create_prometheus_k8s_bearer_token()

        curl_cmd = _build_timed_curl(bearer_token)
        t0 = time.time()
        baseline_time_str = exporter_pod.exec_cmd_on_pod(
            curl_cmd, out_yaml_format=False
        )
        wall_time = time.time() - t0
        try:
            baseline_time = float(baseline_time_str.strip())
        except (ValueError, AttributeError):
            baseline_time = wall_time
        logger.info(
            "Check 8 PASSED: Baseline scrape completed in %.3fs "
            "(curl reported: %.3fs)",
            wall_time,
            baseline_time,
        )

        # --- Check 9: Average scrape time under 5 seconds ---
        num_scrapes = 5
        scrape_times = []
        for i in range(num_scrapes):
            if endpoint["bearer_auth"]:
                bearer_token = ome_helpers.create_prometheus_k8s_bearer_token()
            cmd = _build_timed_curl(bearer_token)
            t0 = time.time()
            time_str = exporter_pod.exec_cmd_on_pod(cmd, out_yaml_format=False)
            wt = time.time() - t0
            try:
                scrape_time = float(time_str.strip())
            except (ValueError, AttributeError):
                scrape_time = wt
            scrape_times.append(scrape_time)
            logger.info("  Scrape %d/%d: %.3fs", i + 1, num_scrapes, scrape_time)

        avg_time = sum(scrape_times) / len(scrape_times)
        min_time = min(scrape_times)
        max_time = max(scrape_times)
        logger.info(
            "Scrape stats: avg=%.3fs, min=%.3fs, max=%.3fs",
            avg_time,
            min_time,
            max_time,
        )
        assert avg_time < 5.0, (
            f"Average scrape time {avg_time:.3f}s exceeds 5.0s threshold. "
            f"Times: {[f'{t:.3f}' for t in scrape_times]}"
        )
        logger.info("Check 9 PASSED: Average scrape time %.3fs < 5.0s", avg_time)

        # --- Check 10: No significant performance degradation (soft) ---
        half = len(scrape_times) // 2
        if half > 0:
            first_half_avg = sum(scrape_times[:half]) / half
            second_half_avg = sum(scrape_times[half:]) / (len(scrape_times) - half)
            if first_half_avg > 0:
                degradation = (
                    (second_half_avg - first_half_avg) / first_half_avg
                ) * 100
            else:
                degradation = 0
            if degradation < 50:
                logger.info(
                    "Check 10 PASSED: No significant degradation "
                    "(%.1f%% < 50%% threshold)",
                    degradation,
                )
            else:
                logger.warning(
                    "Check 10: Performance degradation detected — "
                    "first-half avg %.3fs, second-half avg %.3fs "
                    "(%.1f%% degradation >= 50%% threshold)",
                    first_half_avg,
                    second_half_avg,
                    degradation,
                )

        # --- Check 11: Exporter still responsive after load ---
        if endpoint["bearer_auth"]:
            bearer_token = ome_helpers.create_prometheus_k8s_bearer_token()
        final_sample = ome_helpers.scrape_metrics_text_sample(
            exporter_pod, bearer_token=bearer_token, max_bytes=1024  # gitleaks:allow
        )
        assert final_sample and len(final_sample) > 100, (
            "Exporter became unresponsive after load test. "
            f"Response length: {len(final_sample) if final_sample else 0}"
        )
        logger.info(
            "Check 11 PASSED: Exporter still responsive — " "response length %d bytes",
            len(final_sample),
        )
