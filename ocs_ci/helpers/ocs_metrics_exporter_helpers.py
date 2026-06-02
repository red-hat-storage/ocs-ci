# -*- coding: utf8 -*-
"""
Helper utilities for ocs-metrics-exporter pod validation and metrics scraping.

Provides functions to locate the exporter deployment, resolve its /metrics
endpoint (HTTP or HTTPS), scrape Prometheus text exposition, and run
structural assertions on the pod's container layout and port configuration.
"""

import logging
import re
import shlex

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import Pod, get_pods_having_label
from ocs_ci.utility.utils import exec_cmd

logger = logging.getLogger(__name__)

PROMETHEUS_K8S_SA = "prometheus-k8s"
OPENSHIFT_MONITORING_NS = "openshift-monitoring"


def get_ocs_metrics_exporter_pod(namespace=None):
    """
    Return the single running ocs-metrics-exporter Pod object, or None if not found.

    Args:
        namespace (str): Storage namespace; defaults from config.

    Returns:
        Pod or None
    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    pods = get_pods_having_label(constants.OCS_METRICS_EXPORTER, namespace=namespace)
    running = [
        p for p in pods if p.get("status", {}).get("phase") == constants.STATUS_RUNNING
    ]
    if not running:
        return None
    return Pod(**running[0])


def get_ocs_metrics_exporter_deployments(namespace=None):
    """
    Return raw Deployment items for ocs-metrics-exporter in the storage namespace.

    Args:
        namespace (str): openshift-storage (or cluster_namespace); defaults from config.

    Returns:
        list: Kubernetes Deployment dict items (may be empty if not deployed).
    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    ocp_deployment = OCP(kind=constants.DEPLOYMENT, namespace=namespace)
    return ocp_deployment.get(selector=constants.OCS_METRICS_EXPORTER).get("items", [])


def resolve_metrics_endpoint(pod_obj):
    """
    Resolve /metrics URL and curl options from pod container ports.

    Prefers HTTPS on 8443 (RHSTOR-7964 / kube TLS stack) over plain HTTP metrics.

    Args:
        pod_obj (Pod): ocs-metrics-exporter pod

    Returns:
        dict: keys ``url`` (str), ``tls_skip_verify`` (bool), ``bearer_auth`` (bool)
    """
    https_port = None
    http_port = None
    for container in pod_obj.pod_data.get("spec", {}).get("containers", []):
        for port_def in container.get("ports") or []:
            name = (port_def.get("name") or "").lower()
            container_port = port_def.get("containerPort")
            if not container_port:
                continue
            if container_port == 8443 or "https" in name:
                https_port = container_port
            elif "metric" in name or name in ("http", "probe"):
                http_port = container_port

    if https_port:
        return {
            "url": f"https://127.0.0.1:{https_port}/metrics",
            "tls_skip_verify": True,
            "bearer_auth": True,
        }
    port = http_port or 8080
    return {
        "url": f"http://127.0.0.1:{port}/metrics",
        "tls_skip_verify": False,
        "bearer_auth": False,
    }


def create_prometheus_k8s_bearer_token():
    """
    Create a short-lived token for prometheus-k8s in openshift-monitoring (same as manual QA).

    Used to authorize ``curl`` to the exporter's TLS /metrics listener inside the pod.

    Returns:
        str: bearer token (sensitive; pass to ``exec_cmd_on_pod(..., secrets=[token])``).

    Raises:
        CommandFailed: if ``oc create token`` is not supported or SA is missing.
    """
    base_cmd = f"oc create token {PROMETHEUS_K8S_SA} -n {OPENSHIFT_MONITORING_NS}"
    last_exc = None
    for suffix in (" --duration=15m", ""):
        cmd = base_cmd + suffix
        try:
            completed = exec_cmd(cmd, secrets=[])
            token = (completed.stdout or "").strip()
            if token:
                return token
        except CommandFailed as exc:
            last_exc = exc
            continue
    msg = (
        "failed to create prometheus-k8s token in openshift-monitoring "
        "(tried with and without --duration); check OCP version and RBAC"
    )
    if last_exc:
        raise CommandFailed(msg) from last_exc
    raise CommandFailed(msg)


def scrape_metrics_text_sample(pod_obj, bearer_token=None, max_bytes=8192):
    """
    Curl /metrics from inside the exporter pod (loopback), matching manual QA.

    For HTTPS (e.g. 8443), uses ``curl -sk`` and ``Authorization: Bearer`` from
    ``prometheus-k8s`` unless ``bearer_token`` is passed in.

    Args:
        pod_obj (Pod): exporter pod
        bearer_token (str): optional pre-created token; if None and bearer auth is
            required, ``create_prometheus_k8s_bearer_token()`` is used.
        max_bytes (int): limit response size for logging and assertions

    Returns:
        str: beginning of Prometheus text exposition
    """
    endpoint = resolve_metrics_endpoint(pod_obj)
    url = endpoint["url"]
    secrets = []
    parts = [
        "curl",
        "-sS",
        "--connect-timeout",
        "5",
        "--max-time",
        "15",
        "-f",
    ]
    if endpoint["tls_skip_verify"]:
        parts.append("-k")
    if endpoint["bearer_auth"]:
        token = bearer_token or create_prometheus_k8s_bearer_token()
        secrets.append(token)
        parts.extend(["-H", f"Authorization: Bearer {token}"])
    parts.append(url)
    inner = " ".join(shlex.quote(p) for p in parts) + f" | head -c {max_bytes}"
    cmd = f"sh -c {shlex.quote(inner)}"
    return pod_obj.exec_cmd_on_pod(
        cmd, out_yaml_format=False, secrets=secrets if secrets else None
    )


def assert_prometheus_exposition_text(text):
    """
    Assert the payload looks like Prometheus text exposition (not HTML/JSON error page).

    Args:
        text (str): body from /metrics

    Raises:
        AssertionError: if body does not match minimal Prometheus text format heuristics.
    """
    assert text and text.strip(), "metrics endpoint returned an empty body"
    stripped = text.lstrip()
    first_line = stripped.split("\n", 1)[0]
    prom_comment = first_line.startswith("# HELP") or first_line.startswith("# TYPE")
    prom_metric = bool(re.match(r"^[a-zA-Z_:][a-zA-Z0-9_:]*(?:\{|\s)", first_line))
    assert prom_comment or prom_metric, (
        "expected Prometheus text format from /metrics (line starting with "
        f"'# HELP', '# TYPE', or metric_name); got first line: {first_line[:200]!r}"
    )


def assert_single_exporter_container_without_rbac_proxy(pod_obj):
    """
    Assert the exporter pod has exactly one container and no kube-rbac-proxy sidecar.

    Args:
        pod_obj (Pod): ocs-metrics-exporter pod

    Raises:
        AssertionError: if container layout does not match expected RHSTOR-7964 shape.
    """
    containers = pod_obj.pod_data.get("spec", {}).get("containers", [])
    names = [c.get("name", "") for c in containers]
    msg_count = (
        f"ocs-metrics-exporter must run a single container; got {len(names)}: {names!r}"
    )
    assert len(names) == 1, msg_count
    assert "kube-rbac-proxy" not in names, (
        "kube-rbac-proxy sidecar must not be present on ocs-metrics-exporter "
        f"(RHSTOR-7964); containers={names!r}"
    )


def check_exporter_readyz(pod_obj, bearer_token=None):
    """
    Probe /readyz on the exporter pod and return the response body.

    Args:
        pod_obj (Pod): ocs-metrics-exporter pod
        bearer_token (str): optional pre-created bearer token

    Returns:
        str: response body (expect ``ok`` or similar)
    """
    endpoint = resolve_metrics_endpoint(pod_obj)
    url = endpoint["url"].replace(
        constants.OCS_METRICS_EXPORTER_METRICS_PATH,
        constants.OCS_METRICS_EXPORTER_READYZ_PATH,
    )
    secrets = []
    parts = ["curl", "-sS", "--connect-timeout", "5", "--max-time", "10", "-f"]
    if endpoint["tls_skip_verify"]:
        parts.append("-k")
    if endpoint["bearer_auth"]:
        token = bearer_token or create_prometheus_k8s_bearer_token()
        secrets.append(token)
        parts.extend(["-H", f"Authorization: Bearer {token}"])
    parts.append(url)
    cmd = " ".join(shlex.quote(p) for p in parts)
    return pod_obj.exec_cmd_on_pod(
        cmd, out_yaml_format=False, secrets=secrets if secrets else None
    )


def assert_exporter_uses_https_port(pod_obj):
    """
    Assert the exporter pod is configured to listen on HTTPS port 8443.

    Args:
        pod_obj (Pod): ocs-metrics-exporter pod

    Raises:
        AssertionError: if port 8443 is not found in any container spec.
    """
    for container in pod_obj.pod_data.get("spec", {}).get("containers", []):
        for port_def in container.get("ports") or []:
            if (
                port_def.get("containerPort")
                == constants.OCS_METRICS_EXPORTER_HTTPS_PORT
            ):
                return
    raise AssertionError(
        f"ocs-metrics-exporter pod does not declare port "
        f"{constants.OCS_METRICS_EXPORTER_HTTPS_PORT} (HTTPS); "
        f"containers={[c.get('name') for c in pod_obj.pod_data.get('spec', {}).get('containers', [])]}"
    )


# Made with Bob
