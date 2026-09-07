# -*- coding: utf8 -*-
"""
Minimal helpers for ocs-metrics-exporter deployment and health tests.

Self-contained module for independent PR verification of RHSTOR-7964
exporter deployment tests. Provides pod lookup, endpoint scraping,
container layout assertions, and readiness probing.
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
    Return the single running ocs-metrics-exporter Pod object, or None.

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
    Return raw Deployment items for ocs-metrics-exporter.

    Args:
        namespace (str): Defaults from config.

    Returns:
        list: Kubernetes Deployment dict items.
    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    ocp_deployment = OCP(kind=constants.DEPLOYMENT, namespace=namespace)
    return ocp_deployment.get(selector=constants.OCS_METRICS_EXPORTER).get("items", [])


def resolve_metrics_endpoint(pod_obj):
    """
    Resolve /metrics URL and curl options from pod container ports.

    Prefers HTTPS on 8443 over plain HTTP.

    Args:
        pod_obj (Pod): ocs-metrics-exporter pod

    Returns:
        dict: keys ``url``, ``tls_skip_verify``, ``bearer_auth``
    """
    https_port = None
    http_port = None
    for container in pod_obj.pod_data.get("spec", {}).get("containers", []):
        for port_def in container.get("ports") or []:
            name = (port_def.get("name") or "").lower()
            container_port = port_def.get("containerPort")
            if not container_port:
                continue
            if container_port == 8443:
                https_port = container_port
            elif "https" in name and https_port is None:
                https_port = container_port
            elif "metric" in name or name in ("http", "probe"):
                http_port = container_port

    if https_port:
        return {
            "url": f"https://127.0.0.1:{https_port}"
            f"{constants.OCS_METRICS_EXPORTER_METRICS_PATH}",
            "tls_skip_verify": True,
            "bearer_auth": True,
        }
    port = http_port or 8080
    return {
        "url": f"http://127.0.0.1:{port}"
        f"{constants.OCS_METRICS_EXPORTER_METRICS_PATH}",
        "tls_skip_verify": False,
        "bearer_auth": False,
    }


def create_prometheus_k8s_bearer_token():
    """
    Create a short-lived token for prometheus-k8s SA in openshift-monitoring.

    Returns:
        str: bearer token

    Raises:
        CommandFailed: if token creation fails
    """
    base_cmd = f"oc create token {PROMETHEUS_K8S_SA} -n {OPENSHIFT_MONITORING_NS}"
    last_exc = None
    for suffix in (" --duration=15m", ""):
        cmd = base_cmd + suffix
        try:
            completed = exec_cmd(cmd, secrets=[])
            raw = completed.stdout or b""
            token = raw.decode().strip() if isinstance(raw, bytes) else raw.strip()
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
    Curl /metrics from inside the exporter pod (loopback).

    Args:
        pod_obj (Pod): exporter pod
        bearer_token (str): optional pre-created token
        max_bytes (int): limit response size

    Returns:
        str: Prometheus text exposition sample
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
    Assert payload looks like Prometheus text exposition format.

    Args:
        text (str): body from /metrics

    Raises:
        AssertionError: if format check fails
    """
    assert text and text.strip(), "metrics endpoint returned an empty body"
    stripped = text.lstrip()
    first_line = stripped.split("\n", 1)[0]
    prom_comment = first_line.startswith("# HELP") or first_line.startswith("# TYPE")
    prom_metric = bool(re.match(r"^[a-zA-Z_:][a-zA-Z0-9_:]*(?:\{|\s)", first_line))
    assert prom_comment or prom_metric, (
        "expected Prometheus text format from /metrics (line starting with "
        f"'# HELP', '# TYPE', or metric_name); got: {first_line[:200]!r}"
    )


def assert_single_exporter_container_without_rbac_proxy(pod_obj):
    """
    Assert the pod has exactly one container and no kube-rbac-proxy sidecar.

    Args:
        pod_obj (Pod): ocs-metrics-exporter pod

    Raises:
        AssertionError: if container layout is wrong
    """
    containers = pod_obj.pod_data.get("spec", {}).get("containers", [])
    names = [c.get("name", "") for c in containers]
    assert len(names) == 1, (
        f"ocs-metrics-exporter must run a single container; "
        f"got {len(names)}: {names!r}"
    )
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
        str: response body
    """
    endpoint = resolve_metrics_endpoint(pod_obj)
    url = endpoint["url"].replace(
        constants.OCS_METRICS_EXPORTER_METRICS_PATH,
        constants.OCS_METRICS_EXPORTER_READYZ_PATH,
    )
    secrets = []
    parts = [
        "curl",
        "-sS",
        "--connect-timeout",
        "5",
        "--max-time",
        "10",
        "-f",
    ]
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
    Assert the exporter pod declares HTTPS port 8443 in container spec.

    Args:
        pod_obj (Pod): ocs-metrics-exporter pod

    Raises:
        AssertionError: if port 8443 is not found
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
        f"containers="
        f"{[c.get('name') for c in pod_obj.pod_data.get('spec', {}).get('containers', [])]}"
    )
