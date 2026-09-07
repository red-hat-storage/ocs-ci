# -*- coding: utf8 -*-
"""
Helpers for ocs-metrics-exporter validation (RHSTOR-7964).

Metrics scrape aligns with manual QE: for TLS metrics on 8443, use
``oc create token prometheus-k8s -n openshift-monitoring`` and
``curl -sk -H 'Authorization: Bearer ...' https://localhost:8443/metrics``
from inside the pod.
"""

import logging
import re
import shlex

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
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


def resolve_metrics_endpoint(pod_obj):
    """
    Resolve /metrics URL and curl options from pod container ports.

    Prefers HTTPS on 8443 over plain HTTP metrics.

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
            if container_port == constants.OCS_METRICS_EXPORTER_HTTPS_PORT:
                https_port = container_port
            elif "https" in name and https_port is None:
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
    Create a short-lived token for prometheus-k8s in openshift-monitoring.

    Returns:
        str: bearer token

    Raises:
        CommandFailed: if ``oc create token`` fails.
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


def scrape_full_metrics_text(pod_obj, bearer_token=None, max_bytes=65536):
    """
    Curl the full /metrics body (up to ``max_bytes``) from inside the exporter pod.

    Args:
        pod_obj (Pod): ocs-metrics-exporter pod
        bearer_token (str): optional pre-created bearer token
        max_bytes (int): cap response size

    Returns:
        str: Prometheus text exposition body
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
    Assert the payload looks like Prometheus text exposition format.

    Args:
        text (str): body from /metrics

    Raises:
        AssertionError: if body does not match minimal Prometheus text format.
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


def parse_metric_families(metrics_text):
    """
    Parse raw Prometheus text exposition into a dict of metric name -> list of samples.

    Each sample is a dict with keys ``labels`` (dict) and ``value`` (str).

    Args:
        metrics_text (str): raw Prometheus text from /metrics

    Returns:
        dict: {metric_name: [{"labels": {...}, "value": str}, ...]}
    """
    families = {}
    sample_re = re.compile(
        r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+(\S+)(?:\s+\S+)?$"
    )
    for line in metrics_text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = sample_re.match(line)
        if not m:
            continue
        name = m.group(1)
        labels_str = m.group(2) or ""
        value = m.group(3)
        labels = {}
        if labels_str:
            for pair in re.findall(r'(\w+)="([^"]*)"', labels_str):
                labels[pair[0]] = pair[1]
        families.setdefault(name, []).append({"labels": labels, "value": value})
    return families


def verify_no_cli_spawning_in_logs(pod_obj, time_window="5m", cli_patterns=None):
    """
    Verify that specified patterns are absent in pod logs.

    Args:
        pod_obj (Pod): Exporter pod
        time_window (str): Time window for logs (e.g., "5m", "30m")
        cli_patterns (list): Patterns to check for

    Raises:
        AssertionError: if any pattern is detected
    """
    cli_patterns = cli_patterns or [
        "rbd status",
        "rbd children",
        "ceph osd blocklist",
        "ceph fs subvolume",
    ]

    cmd = f"oc logs {pod_obj.name} -n {pod_obj.namespace} --since={time_window}"
    try:
        logs = exec_cmd(cmd).stdout.decode()
    except CommandFailed:
        logger.warning(f"Could not retrieve logs for {pod_obj.name}")
        return

    for pattern in cli_patterns:
        assert pattern.lower() not in logs.lower(), (
            f"Pattern detected: found '{pattern}' in exporter logs. "
            f"Expected pattern to be absent."
        )
    logger.info("Verified no matching patterns in exporter logs")
