# -*- coding: utf8 -*-
"""
Helpers for RHSTOR-7964 / ocs-metrics-exporter validation (internal and provider paths).

Metrics scrape aligns with manual QE: for TLS metrics on 8443, use
``oc create token prometheus-k8s -n openshift-monitoring`` and
``curl -sk -H 'Authorization: Bearer …' https://localhost:8443/metrics`` from inside the pod.
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


def scrape_metrics_text_sample(pod_obj, bearer_token=None, max_bytes=8192):
    """
    Curl /metrics from inside the exporter pod (loopback), matching manual QE.

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


def should_expect_consumer_name_in_metrics():
    """
    Whether RHSTOR-7964 multicluster metrics should expose a consumer_name label.

    Returns:
        bool: True if multicluster config includes a consumer-type cluster.
    """
    if not getattr(config, "multicluster", False):
        return False
    return bool(config.is_consumer_exist() or config.hci_client_exist())


def skip_if_no_provider_with_consumers():
    """
    Skip the calling test if not running on a provider with onboarded consumers.

    Raises:
        pytest.skip: if single-cluster or no consumer/client in multicluster config.
    """
    import pytest

    if not getattr(config, "multicluster", False):
        pytest.skip("single-cluster deployment; requires provider+consumer")
    if not (config.is_consumer_exist() or config.hci_client_exist()):
        pytest.skip("no consumer/client clusters in multicluster config")


def assert_consumer_name_in_metrics(metrics_body):
    """
    Assert ``consumer_name`` appears in the scraped /metrics body (manual QA grep).

    Args:
        metrics_body (str): raw Prometheus text

    Raises:
        AssertionError: if consumer_name is missing when required.
    """
    assert "consumer_name" in metrics_body, (
        "expected consumer_name in /metrics for provider+consumer topology (RHSTOR-7964); "
        "see manual: curl ... | grep consumer_name"
    )


def scrape_full_metrics_text(pod_obj, bearer_token=None, max_bytes=65536):
    """
    Curl the full /metrics body (up to ``max_bytes``) from inside the exporter pod.

    Unlike ``scrape_metrics_text_sample`` this fetches a larger payload suitable for
    metric-level assertions.

    Args:
        pod_obj (Pod): ocs-metrics-exporter pod
        bearer_token (str): optional pre-created bearer token
        max_bytes (int): cap response size

    Returns:
        str: Prometheus text exposition body
    """
    return scrape_metrics_text_sample(
        pod_obj, bearer_token=bearer_token, max_bytes=max_bytes
    )


def parse_metric_families(metrics_text):
    """
    Parse raw Prometheus text exposition into a dict of metric name → list of samples.

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


def assert_metric_present(families, metric_name):
    """
    Assert that a named metric has at least one sample in the parsed families.

    Args:
        families (dict): output of ``parse_metric_families``
        metric_name (str): e.g. ``ocs_rbd_pv_metadata``

    Raises:
        AssertionError: if metric is missing
    """
    assert (
        metric_name in families and families[metric_name]
    ), f"metric {metric_name!r} not found in /metrics output"


def assert_metric_has_consumer_name(families, metric_name):
    """
    Assert every sample of ``metric_name`` carries a ``consumer_name`` label.

    Args:
        families (dict): output of ``parse_metric_families``
        metric_name (str): metric to check

    Raises:
        AssertionError
    """
    assert_metric_present(families, metric_name)
    for sample in families[metric_name]:
        assert (
            "consumer_name" in sample["labels"]
        ), f"metric {metric_name!r} sample missing consumer_name label: {sample!r}"


def assert_metric_no_consumer_name(families, metric_name):
    """
    Assert no sample of ``metric_name`` carries a ``consumer_name`` label.

    Args:
        families (dict): output of ``parse_metric_families``
        metric_name (str): metric to check

    Raises:
        AssertionError
    """
    assert_metric_present(families, metric_name)
    for sample in families[metric_name]:
        assert (
            "consumer_name" not in sample["labels"]
        ), f"cluster-level metric {metric_name!r} must not have consumer_name: {sample!r}"


def get_consumer_names_from_metrics(families, metric_name):
    """
    Extract the set of distinct ``consumer_name`` label values from a metric.

    Args:
        families (dict): output of ``parse_metric_families``
        metric_name (str): metric to inspect

    Returns:
        set: unique consumer_name values
    """
    assert_metric_present(families, metric_name)
    return {
        s["labels"]["consumer_name"]
        for s in families[metric_name]
        if "consumer_name" in s["labels"]
    }


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


def assert_exporter_ceph_auth_secret_exists(namespace=None):
    """
    Assert that the dedicated Ceph auth secret for ocs-metrics-exporter exists.

    Args:
        namespace (str): storage namespace; defaults from config.

    Raises:
        AssertionError: if the secret is not found.
    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    ocp_secret = OCP(kind=constants.SECRET, namespace=namespace)
    secrets = ocp_secret.get(
        resource_name=constants.OCS_METRICS_EXPORTER_CEPH_AUTH_SECRET
    )
    assert secrets, (
        f"Ceph auth secret {constants.OCS_METRICS_EXPORTER_CEPH_AUTH_SECRET!r} "
        f"not found in namespace {namespace}"
    )
    logger.info(
        "Verified Ceph auth secret %s exists in %s",
        constants.OCS_METRICS_EXPORTER_CEPH_AUTH_SECRET,
        namespace,
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


def get_consumer_names_from_storage_consumers(namespace=None):
    """
    Return the set of StorageConsumer CR names from the provider cluster.

    Args:
        namespace (str): storage namespace; defaults from config.

    Returns:
        set: StorageConsumer names (e.g. ``{"consumer-1", "consumer-2"}``)
    """
    from ocs_ci.ocs.resources.storageconsumer import get_ready_consumers_names

    return set(get_ready_consumers_names())


# ============================================================================
# PVC and Volume Operations Helpers
# ============================================================================


def create_test_rbd_pvc(
    namespace, pvc_name, size="5Gi", storage_class=None, access_mode="ReadWriteOnce"
):
    """
    Create a test RBD PVC for metrics validation.

    Args:
        namespace (str): Namespace to create PVC in
        pvc_name (str): Name of the PVC
        size (str): Size of the PVC (default: "5Gi")
        storage_class (str): StorageClass name (default: ocs-storagecluster-ceph-rbd)
        access_mode (str): Access mode (default: "ReadWriteOnce")

    Returns:
        dict: Created PVC object
    """
    from ocs_ci.ocs.resources import pvc

    storage_class = storage_class or constants.CEPHBLOCKPOOL_SC
    pvc_obj = pvc.create_pvc(
        sc_name=storage_class,
        pvc_name=pvc_name,
        namespace=namespace,
        size=size,
        access_mode=access_mode,
    )
    pvc.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND, timeout=300)
    logger.info(f"Created and bound RBD PVC {pvc_name} in namespace {namespace}")
    return pvc_obj


def create_test_cephfs_pvc(
    namespace, pvc_name, size="5Gi", storage_class=None, access_mode="ReadWriteMany"
):
    """
    Create a test CephFS PVC for metrics validation.

    Args:
        namespace (str): Namespace to create PVC in
        pvc_name (str): Name of the PVC
        size (str): Size of the PVC (default: "5Gi")
        storage_class (str): StorageClass name (default: ocs-storagecluster-cephfs)
        access_mode (str): Access mode (default: "ReadWriteMany")

    Returns:
        dict: Created PVC object
    """
    from ocs_ci.ocs.resources import pvc

    storage_class = storage_class or constants.CEPHFILESYSTEM_SC
    pvc_obj = pvc.create_pvc(
        sc_name=storage_class,
        pvc_name=pvc_name,
        namespace=namespace,
        size=size,
        access_mode=access_mode,
    )
    pvc.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND, timeout=300)
    logger.info(f"Created and bound CephFS PVC {pvc_name} in namespace {namespace}")
    return pvc_obj


def create_pod_with_pvc(namespace, pod_name, pvc_name, image=None):
    """
    Create a pod that mounts a PVC for testing.

    Args:
        namespace (str): Namespace to create pod in
        pod_name (str): Name of the pod
        pvc_name (str): Name of the PVC to mount
        image (str): Container image (default: ubi9/ubi-minimal)

    Returns:
        Pod: Created pod object
    """
    from ocs_ci.ocs.resources import pod as pod_helpers

    image = image or "registry.access.redhat.com/ubi9/ubi-minimal"
    pod_dict = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": pod_name, "namespace": namespace},
        "spec": {
            "containers": [
                {
                    "name": "app",
                    "image": image,
                    "command": ["/bin/sh", "-c", "sleep 3600"],
                    "volumeMounts": [{"mountPath": "/data", "name": "pvc-vol"}],
                }
            ],
            "volumes": [
                {"name": "pvc-vol", "persistentVolumeClaim": {"claimName": pvc_name}}
            ],
        },
    }
    pod_obj = pod_helpers.create_pod(**pod_dict)
    pod_helpers.wait_for_resource_state(pod_obj, constants.STATUS_RUNNING, timeout=300)
    logger.info(f"Created pod {pod_name} with PVC {pvc_name} in namespace {namespace}")
    return pod_obj


def create_snapshot_from_pvc(namespace, snapshot_name, pvc_name, snapshot_class=None):
    """
    Create a VolumeSnapshot from a PVC.

    Args:
        namespace (str): Namespace
        snapshot_name (str): Name of the snapshot
        pvc_name (str): Source PVC name
        snapshot_class (str): VolumeSnapshotClass (default: ocs-storagecluster-rbdplugin-snapclass)

    Returns:
        dict: Created VolumeSnapshot object
    """
    from ocs_ci.ocs.resources import snapshot

    snapshot_class = snapshot_class or constants.DEFAULT_VOLUMESNAPSHOTCLASS_RBD
    snap_obj = snapshot.create_snapshot(
        pvc_name=pvc_name,
        snapshot_name=snapshot_name,
        namespace=namespace,
        snapshot_class=snapshot_class,
    )
    snapshot.wait_for_snapshot_ready(snap_obj, timeout=300)
    logger.info(
        f"Created snapshot {snapshot_name} from PVC {pvc_name} in namespace {namespace}"
    )
    return snap_obj


def create_clone_from_snapshot(
    namespace, clone_name, snapshot_name, size="5Gi", storage_class=None
):
    """
    Create a clone PVC from a VolumeSnapshot.

    Args:
        namespace (str): Namespace
        clone_name (str): Name of the clone PVC
        snapshot_name (str): Source snapshot name
        size (str): Size of the clone (default: "5Gi")
        storage_class (str): StorageClass (default: ocs-storagecluster-ceph-rbd)

    Returns:
        dict: Created clone PVC object
    """
    from ocs_ci.ocs.resources import pvc

    storage_class = storage_class or constants.CEPHBLOCKPOOL_SC
    clone_obj = pvc.create_restore_pvc(
        sc_name=storage_class,
        snap_name=snapshot_name,
        namespace=namespace,
        size=size,
        pvc_name=clone_name,
        restore_pvc_yaml=None,
    )
    pvc.wait_for_resource_state(clone_obj, constants.STATUS_BOUND, timeout=300)
    logger.info(
        f"Created clone PVC {clone_name} from snapshot {snapshot_name} in namespace {namespace}"
    )
    return clone_obj


# ============================================================================
# Ceph Toolbox Operations Helpers
# ============================================================================


def get_ceph_toolbox_pod(namespace=None):
    """
    Get the Ceph toolbox pod for running Ceph commands.

    Args:
        namespace (str): Storage namespace (default: from config)

    Returns:
        Pod: Ceph toolbox pod object

    Raises:
        AssertionError: if toolbox pod not found
    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    pods = get_pods_having_label("app=rook-ceph-tools", namespace=namespace)
    running = [
        p for p in pods if p.get("status", {}).get("phase") == constants.STATUS_RUNNING
    ]
    assert running, f"Ceph toolbox pod not found in namespace {namespace}"
    return Pod(**running[0])


def exec_ceph_command(toolbox_pod, command, timeout=300):
    """
    Execute a Ceph command in the toolbox pod.

    Args:
        toolbox_pod (Pod): Ceph toolbox pod
        command (str): Ceph command to execute
        timeout (int): Command timeout in seconds

    Returns:
        str: Command output

    Raises:
        CommandFailed: if command fails
    """
    logger.info(f"Executing Ceph command: {command}")
    result = toolbox_pod.exec_cmd_on_pod(
        command, out_yaml_format=False, timeout=timeout
    )
    return result


def verify_rbd_image_watchers(toolbox_pod, pool, image):
    """
    Get RBD image watchers from Ceph toolbox.

    Args:
        toolbox_pod (Pod): Ceph toolbox pod
        pool (str): Pool name
        image (str): Image name

    Returns:
        list: List of watcher addresses
    """
    cmd = f"rbd status {pool}/{image}"
    output = exec_ceph_command(toolbox_pod, cmd)
    watchers = []
    for line in output.splitlines():
        if "watcher=" in line:
            watchers.append(line.strip())
    logger.info(f"Found {len(watchers)} watchers for {pool}/{image}")
    return watchers


def verify_rbd_children_count(toolbox_pod, pool, image):
    """
    Get RBD children count from Ceph toolbox.

    Args:
        toolbox_pod (Pod): Ceph toolbox pod
        pool (str): Pool name
        image (str): Image name

    Returns:
        int: Number of children
    """
    cmd = f"rbd children {pool}/{image}"
    output = exec_ceph_command(toolbox_pod, cmd)
    children = [line.strip() for line in output.splitlines() if line.strip()]
    count = len(children)
    logger.info(f"Found {count} children for {pool}/{image}")
    return count


def get_ceph_blocklist(toolbox_pod):
    """
    Get the current Ceph OSD blocklist.

    Args:
        toolbox_pod (Pod): Ceph toolbox pod

    Returns:
        list: List of blocklisted addresses
    """
    cmd = "ceph osd blocklist ls"
    output = exec_ceph_command(toolbox_pod, cmd)
    blocklist = []
    for line in output.splitlines():
        if ":" in line and "listed" not in line.lower():
            blocklist.append(line.strip().split()[0])
    logger.info(f"Current blocklist has {len(blocklist)} entries")
    return blocklist


def add_to_ceph_blocklist(toolbox_pod, ip_address):
    """
    Add an IP address to the Ceph OSD blocklist.

    Args:
        toolbox_pod (Pod): Ceph toolbox pod
        ip_address (str): IP address to blocklist

    Returns:
        str: Command output
    """
    cmd = f"ceph osd blocklist add {ip_address}:0/0"
    logger.info(f"Adding {ip_address} to Ceph blocklist")
    return exec_ceph_command(toolbox_pod, cmd)


def remove_from_ceph_blocklist(toolbox_pod, ip_address):
    """
    Remove an IP address from the Ceph OSD blocklist.

    Args:
        toolbox_pod (Pod): Ceph toolbox pod
        ip_address (str): IP address to remove

    Returns:
        str: Command output
    """
    cmd = f"ceph osd blocklist rm {ip_address}:0/0"
    logger.info(f"Removing {ip_address} from Ceph blocklist")
    return exec_ceph_command(toolbox_pod, cmd)


# ============================================================================
# Log Verification Helpers
# ============================================================================


def verify_no_cli_spawning_in_logs(pod_obj, time_window="5m", cli_patterns=None):
    """
    Verify that no CLI processes are spawned by checking pod logs.

    Args:
        pod_obj (Pod): Exporter pod
        time_window (str): Time window for logs (e.g., "5m", "10m")
        cli_patterns (list): List of CLI command patterns to check for

    Raises:
        AssertionError: if CLI spawning is detected
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
            f"CLI spawning detected: found '{pattern}' in exporter logs. "
            f"RHSTOR-7964 requires go-ceph API usage only, no CLI spawning."
        )
    logger.info("Verified no CLI spawning in exporter logs")


def verify_go_ceph_usage_in_logs(pod_obj, time_window="5m"):
    """
    Verify that go-ceph library is being used by checking logs.

    Args:
        pod_obj (Pod): Exporter pod
        time_window (str): Time window for logs

    Returns:
        bool: True if go-ceph usage is detected
    """
    cmd = f"oc logs {pod_obj.name} -n {pod_obj.namespace} --since={time_window}"
    try:
        logs = exec_cmd(cmd).stdout.decode()
        go_ceph_indicators = ["go-ceph", "rados", "rbd.Image", "cephfs"]
        found = any(indicator in logs for indicator in go_ceph_indicators)
        if found:
            logger.info("Detected go-ceph usage in exporter logs")
        return found
    except CommandFailed:
        logger.warning(f"Could not retrieve logs for {pod_obj.name}")
        return False


def check_for_errors_in_logs(pod_obj, error_patterns=None, time_window="5m"):
    """
    Check for error patterns in pod logs.

    Args:
        pod_obj (Pod): Pod to check
        error_patterns (list): List of error patterns to search for
        time_window (str): Time window for logs

    Returns:
        list: List of found error lines
    """
    error_patterns = error_patterns or ["error", "panic", "fatal", "failed"]
    cmd = f"oc logs {pod_obj.name} -n {pod_obj.namespace} --since={time_window}"

    try:
        logs = exec_cmd(cmd).stdout.decode()
    except CommandFailed:
        logger.warning(f"Could not retrieve logs for {pod_obj.name}")
        return []

    errors = []
    for line in logs.splitlines():
        if any(pattern.lower() in line.lower() for pattern in error_patterns):
            errors.append(line.strip())

    if errors:
        logger.warning(f"Found {len(errors)} error lines in logs")
    return errors


# ============================================================================
# Metric Validation Helpers
# ============================================================================


def verify_consumer_name_empty_or_absent(families, metric_name):
    """
    Verify that consumer_name label is empty or absent for internal workloads.

    Args:
        families (dict): Parsed metric families
        metric_name (str): Metric name to check

    Raises:
        AssertionError: if consumer_name is present with non-empty value
    """
    assert_metric_present(families, metric_name)
    for sample in families[metric_name]:
        consumer_name = sample["labels"].get("consumer_name", "")
        assert not consumer_name, (
            f"Internal workload metric {metric_name!r} should have empty consumer_name; "
            f"got: {consumer_name!r} in sample: {sample!r}"
        )
    logger.info(f"Verified {metric_name} has empty/absent consumer_name for internal")


def verify_rados_namespace_value(families, metric_name, expected_value=None):
    """
    Verify rados_namespace label value in metrics.

    Args:
        families (dict): Parsed metric families
        metric_name (str): Metric name to check
        expected_value (str): Expected rados_namespace value (None for any)

    Returns:
        set: Set of found rados_namespace values
    """
    assert_metric_present(families, metric_name)
    values = set()
    for sample in families[metric_name]:
        rados_ns = sample["labels"].get("rados_namespace", "")
        values.add(rados_ns)
        if expected_value is not None:
            assert rados_ns == expected_value, (
                f"Expected rados_namespace={expected_value!r}, "
                f"got {rados_ns!r} in {metric_name}"
            )
    logger.info(f"Found rados_namespace values in {metric_name}: {values}")
    return values


def get_metric_value(families, metric_name, label_filters=None):
    """
    Get metric value(s) matching label filters.

    Args:
        families (dict): Parsed metric families
        metric_name (str): Metric name
        label_filters (dict): Label key-value pairs to filter by

    Returns:
        list: List of matching metric values
    """
    assert_metric_present(families, metric_name)
    label_filters = label_filters or {}

    values = []
    for sample in families[metric_name]:
        match = all(sample["labels"].get(k) == v for k, v in label_filters.items())
        if match:
            values.append(sample["value"])

    return values


def count_metric_samples(families, metric_name, label_filters=None):
    """
    Count metric samples matching label filters.

    Args:
        families (dict): Parsed metric families
        metric_name (str): Metric name
        label_filters (dict): Label key-value pairs to filter by

    Returns:
        int: Count of matching samples
    """
    assert_metric_present(families, metric_name)
    label_filters = label_filters or {}

    count = 0
    for sample in families[metric_name]:
        match = all(sample["labels"].get(k) == v for k, v in label_filters.items())
        if match:
            count += 1

    return count


# ============================================================================
# Memory and Performance Helpers
# ============================================================================


def measure_pod_memory_usage(pod_obj):
    """
    Measure current memory usage of a pod.

    Args:
        pod_obj (Pod): Pod to measure

    Returns:
        str: Memory usage (e.g., "128Mi")
    """
    cmd = f"oc adm top pod {pod_obj.name} -n {pod_obj.namespace} --no-headers"
    try:
        output = exec_cmd(cmd).stdout
        # Output format: NAME CPU(cores) MEMORY(bytes)
        parts = output.split()
        if len(parts) >= 3:
            memory = parts[2]
            logger.info(f"Pod {pod_obj.name} memory usage: {memory}")
            return memory
    except CommandFailed:
        logger.warning(f"Could not measure memory for {pod_obj.name}")
    return "0Mi"


def measure_scrape_latency(pod_obj, bearer_token=None):
    """
    Measure the time taken to scrape /metrics endpoint.

    Args:
        pod_obj (Pod): Exporter pod
        bearer_token (str): Optional bearer token

    Returns:
        float: Scrape latency in seconds
    """
    import time

    start_time = time.time()
    try:
        scrape_full_metrics_text(pod_obj, bearer_token=bearer_token)
        latency = time.time() - start_time
        logger.info(f"Metrics scrape latency: {latency:.2f} seconds")
        return latency
    except Exception as e:
        logger.error(f"Failed to measure scrape latency: {e}")
        return -1.0
