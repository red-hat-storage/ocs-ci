"""
Helper for TLSProfile custom resources (ocs.openshift.io/v1) and in-cluster TLS
scanning via :func:`scan_cluster`. The scan logic lives in
``scripts/bash/tls_scan_endpoints.sh`` (loaded at runtime).

References (DF 4.22+): ``TLSProfile`` centralizes TLS version, ciphers, and groups
for NooBaa, RGW, ocs-metrics-exporter, csi-snapshot-metadata, and
ocs-client-operator; CR name ``ocs-tls-profile`` in the operator namespace;
``ocs-tls-profiles`` is an OLM dependency (include in disconnected mirroring).
Cipher/group sets follow the product-supported lists (Mozilla Intermediate/Modern
plus PQC groups). On FIPS-enabled clusters, PQ hybrids and ChaCha are not
FIPS 140-2 approved; use the ``skipif_fips_enabled`` pytest mark on tests that
rely on those algorithms.
"""

import copy
import csv
import io
import json
import logging
import os
import re
import uuid

from ocs_ci.framework import config
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.exceptions import CommandFailed, TimeoutExpiredError
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility.utils import TimeoutSampler, exec_cmd

log = logging.getLogger(__name__)

# Centralized TLSProfile cipher/group sets — API-compatible with tlsprofiles.ocs.openshift.io
# (DF 4.22 supported config; verify enums via `oc get crd tlsprofiles.ocs.openshift.io -oyaml`).
TLS_PROFILE_V13_CIPHERS = [
    "TLS_AES_128_GCM_SHA256",
    "TLS_AES_256_GCM_SHA384",
    "TLS_CHACHA20_POLY1305_SHA256",
]
TLS_PROFILE_V13_GROUPS = [
    "secp256r1",
    "secp384r1",
    "secp521r1",
    "X25519",
    "X25519MLKEM768",
    "SecP256r1MLKEM768",
    "SecP384r1MLKEM1024",
]
# csi-snapshot-metadata sidecar (DF 5.0) accepts only CurveP256/P384/P521, X25519,
# and X25519MLKEM768. Passing SecP256r1MLKEM768 / SecP384r1MLKEM1024 makes it
# crashloop: unsupported curve "... must be one of: CurveP256, ... X25519MLKEM768".
CSI_SNAPSHOT_METADATA_V13_GROUPS = [
    "secp256r1",
    "secp384r1",
    "secp521r1",
    "X25519",
    "X25519MLKEM768",
]
TLS_PROFILE_V12_CIPHERS = [
    "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
    "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
    "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256",
    "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
    "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
    "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256",
]
TLS_PROFILE_V12_GROUPS = [
    "secp256r1",
    "secp384r1",
    "secp521r1",
    "X25519",
]

# Selector strings for rook Object Gateway TLS (domain form domain or domain/server).
# DF docs list ``noobaa.io``, ``rook.io``, and ``*``; RGW reconciliation uses the
# ceph object store gateway domain ``ceph.rook.io`` in practice.
# ocs-metrics-exporter is selected with ``ocs.openshift.io/metrics-exporter``.
# csi-snapshot-metadata (Changed Block Tracking gRPC) uses ``cbt.storage.k8s.io``.
# ocs-client-operator uses ``ocs.openshift.io/webhook`` and ``ocs.openshift.io/metrics``.
TLS_PROFILE_SELECTOR_NOOBAA_DOMAIN = "noobaa.io"
TLS_PROFILE_SELECTOR_RGW_DOMAIN = "ceph.rook.io"
TLS_PROFILE_SELECTOR_METRICS_EXPORTER = "ocs.openshift.io/metrics-exporter"
TLS_PROFILE_SELECTOR_CSI_SNAPSHOT_METADATA = "cbt.storage.k8s.io"
TLS_PROFILE_SELECTOR_CLIENT_OPERATOR_WEBHOOK = "ocs.openshift.io/webhook"
TLS_PROFILE_SELECTOR_CLIENT_OPERATOR_METRICS = "ocs.openshift.io/metrics"
# ocs-metrics-exporter HTTPS listeners: https-main (metrics scrape) and
# https-self (exporter process metrics). TLSProfile applies to both.
METRICS_EXPORTER_HTTPS_PORTS = (
    constants.OCS_METRICS_EXPORTER_PORT,
    constants.OCS_METRICS_EXPORTER_LEGACY_PORT,
)
CSI_SNAPSHOT_METADATA_HTTPS_PORTS = (constants.CSI_SNAPSHOT_METADATA_PORT,)
# ocs-client-operator HTTPS listeners: metrics (:8443) and webhook (:7443).
CLIENT_OPERATOR_HTTPS_PORTS = (
    constants.OCS_CLIENT_OPERATOR_METRICS_PORT,
    constants.OCS_CLIENT_OPERATOR_WEBHOOK_PORT,
)
CLIENT_OPERATOR_HTTPS_PORT_ROLES = {
    constants.OCS_CLIENT_OPERATOR_METRICS_PORT: "metrics",
    constants.OCS_CLIENT_OPERATOR_WEBHOOK_PORT: "webhook",
}

# IANA names from TLSProfile spec -> OpenSSL names produced by scantls.
TLS_PROFILE_IANA_TO_OPENSSL_CIPHER = {
    "TLS_AES_128_GCM_SHA256": "TLS_AES_128_GCM_SHA256",
    "TLS_AES_256_GCM_SHA384": "TLS_AES_256_GCM_SHA384",
    "TLS_CHACHA20_POLY1305_SHA256": "TLS_CHACHA20_POLY1305_SHA256",
    "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256": "ECDHE-ECDSA-AES128-GCM-SHA256",
    "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384": "ECDHE-ECDSA-AES256-GCM-SHA384",
    "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256": "ECDHE-ECDSA-CHACHA20-POLY1305",
    "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256": "ECDHE-RSA-AES128-GCM-SHA256",
    "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384": "ECDHE-RSA-AES256-GCM-SHA384",
    "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256": "ECDHE-RSA-CHACHA20-POLY1305",
}
TLS_PROFILE_IANA_TO_OPENSSL_GROUP = {
    "secp256r1": "prime256v1",
    "secp384r1": "secp384r1",
    "secp521r1": "secp521r1",
    "X25519": "X25519",
    "X25519MLKEM768": "X25519MLKEM768",
    "SecP256r1MLKEM768": "SecP256r1MLKEM768",
    "SecP384r1MLKEM1024": "SecP384r1MLKEM1024",
}

# Heuristic: log lines that likely indicate TLS/handshake/cert/TLSProfile handling failures.
# Use re.IGNORECASE: inline (?i) after "|" is invalid in Python 3.11+.
# Avoid bare "handshake" — it matches WebSocket "handshake request" (not TLS protocol errors).
_TLS_LOG_ERROR_RE = re.compile(
    r".*(\berror\b|\bfatal\b|\bpanic\b).*("
    r"\btls\b|\bssl\b|x509|\bcipher\b|TLSProfile|certificate|"
    r"handshake failure|\btls\s+handshake\b|\bssl\s+handshake\b"
    r")"
    r"|.*(\btls\b|TLSProfile|x509|handshake failure).*(\berror\b|\bfatal\b|failed|failure|invalid|reject)",
    re.IGNORECASE,
)

# Transient / non-TLS errors that sometimes mention "tls" or "handshake" in unrelated contexts.
_TLS_LOG_EXCLUDE_RE = re.compile(
    r"(?i)connection refused|dial tcp|websocket dial|handshake request|"
    r"RPC:\s*Reconnect|reconnect\s*-\s*got error|context deadline exceeded|"
    r"i/o timeout|no route to host|broken pipe|\bEOF\b|temporary failure",
)


def gather_tls_relevant_pod_names(namespace, component):
    """
    Pod names to scan for TLS-related log errors based on test parametrization.

    Always includes ocs-operator and rook-ceph-operator; adds NooBaa / RGW /
    ocs-metrics-exporter / csi-snapshot-metadata / ocs-client-operator pods
    when those paths are under test.
    """
    from ocs_ci.ocs.resources.pod import get_pods_having_label

    selectors = [
        constants.OCS_OPERATOR_LABEL,
        constants.OPERATOR_LABEL,
    ]
    if component in ("noobaa", "all"):
        selectors.extend(
            [
                constants.NOOBAA_OPERATOR_POD_LABEL,
                constants.NOOBAA_CORE_POD_LABEL,
            ]
        )
    if component in ("rgw", "all"):
        selectors.append(constants.RGW_APP_LABEL)
    if component == "metrics-exporter":
        selectors.append(constants.OCS_METRICS_EXPORTER)
    if component == "ocs-client-operator":
        selectors.append(constants.OCS_CLIENT_OPERATOR_LABEL)

    names = set()
    for label in selectors:
        items = get_pods_having_label(label, namespace) or []
        for item in items:
            name = item.get("metadata", {}).get("name")
            if name:
                names.add(name)
    if component == "csi-snapshot-metadata":
        for item in _list_csi_snapshot_metadata_pod_items(namespace):
            name = item.get("metadata", {}).get("name")
            if name:
                names.add(name)
    return sorted(names)


def scan_pod_logs_for_tls_errors(
    pod_name,
    namespace,
    since="30m",
    tail=800,
    unreadable_pods=None,
):
    """
    Return log lines that match TLS-related error heuristics for a single pod.

    If ``unreadable_pods`` is None, failures from :func:`get_pod_logs` propagate.
    When a list is passed, the pod name and exception are appended and an empty
    hit list is returned so callers can fail after scanning other pods.
    """
    from ocs_ci.ocs.resources.pod import get_pod_logs

    try:
        raw = get_pod_logs(
            pod_name=pod_name,
            namespace=namespace,
            since=since,
            tail=str(tail),
        )
    except Exception as exc:
        log.warning("Could not read logs for pod %s: %s", pod_name, exc)
        if unreadable_pods is not None:
            unreadable_pods.append((pod_name, exc))
            return []
        raise

    bad = []
    for line in raw.splitlines():
        if not line.strip():
            continue
        if (
            _TLS_LOG_ERROR_RE.search(line)
            and not _TLS_LOG_EXCLUDE_RE.search(line)
            and not re.search(r"(?i)no error|error.?0|errors:? ?0", line)
        ):
            bad.append(line)
    return bad


def assert_no_tls_errors_in_relevant_pod_logs(
    namespace,
    component,
    since="45m",
    tail=800,
    max_lines_per_pod=30,
):
    """
    Fail the test if recent operator / workload logs contain likely TLS error lines.

    Args:
        namespace (str): Storage namespace (e.g. openshift-storage).
        component (str): Test parametrization key: ``all``, ``noobaa``,
            ``rgw``, ``metrics-exporter``, ``csi-snapshot-metadata``, or
            ``ocs-client-operator``.
        since (str): Passed to ``oc logs --since`` (recent window for this run).
        tail (str|int): Max tail lines per pod.
        max_lines_per_pod (int): Cap lines included in failure output.
    """
    findings = {}
    unreadable_pods = []
    for pod_name in gather_tls_relevant_pod_names(namespace, component):
        hits = scan_pod_logs_for_tls_errors(
            pod_name,
            namespace,
            since=since,
            tail=tail,
            unreadable_pods=unreadable_pods,
        )
        if hits:
            findings[pod_name] = hits[:max_lines_per_pod]

    if unreadable_pods:
        blocks = [f"{pname}: {exc!r}" for pname, exc in unreadable_pods]
        raise AssertionError(
            "Could not read logs for one or more pods (TLS log scan incomplete):\n"
            + "\n".join(blocks)
        )

    if findings:
        blocks = []
        for pname, lines in findings.items():
            blocks.append(pname + ":\n" + "\n".join(f"  {ln}" for ln in lines))
        raise AssertionError(
            "TLS-related errors found in pod logs (heuristic grep):\n"
            + "\n".join(blocks)
        )


# --- In-cluster TLS scanner (openssl s_client probes on pod IPs) -------------

SCAN_CLUSTER_DEFAULT_TIMEOUT = 5
SCAN_CLUSTER_DEFAULT_SKIP_PORTS = "22,53"
SCAN_CLUSTER_DEFAULT_TLS_VERSIONS = "tls1.2,tls1.3"

SCAN_CLUSTER_DEFAULT_TLS12_CIPHERS = (
    "ECDHE-ECDSA-AES128-GCM-SHA256,"
    "ECDHE-ECDSA-AES256-GCM-SHA384,"
    "ECDHE-ECDSA-CHACHA20-POLY1305,"
    "ECDHE-RSA-AES128-GCM-SHA256,"
    "ECDHE-RSA-AES256-GCM-SHA384,"
    "ECDHE-RSA-CHACHA20-POLY1305"
)

SCAN_CLUSTER_DEFAULT_TLS12_GROUPS = "prime256v1,secp384r1,secp521r1,X25519"

SCAN_CLUSTER_DEFAULT_TLS13_CIPHERS = (
    "TLS_AES_128_GCM_SHA256," "TLS_AES_256_GCM_SHA384," "TLS_CHACHA20_POLY1305_SHA256"
)

SCAN_CLUSTER_DEFAULT_TLS13_GROUPS = (
    "prime256v1,secp384r1,secp521r1,X25519,"
    "X25519MLKEM768,SecP256r1MLKEM768,SecP384r1MLKEM1024"
)

TLS_SCANNER_IMAGE = "ghcr.io/leelavg/scantls@sha256:5e80dd5576812f3c8248fad7cbf19a74b74384aafd14614ccd53ef6b4e1f40d1"
TLS_SCANNER_NAMESPACE = "scantls-system"
# Extra NetworkPolicy (not StorageCluster-owned) so the scanner can reach
# ocs-metrics-exporter HTTPS. The operator reverts patches to the owned NP.
TLS_SCAN_METRICS_EXPORTER_SCANNER_NP = "ocs-metrics-exporter-scantls"
# Seconds between ``oc get pod … jsonpath={.status.phase}`` samples (scanner pod startup).
TLS_SCAN_POD_PHASE_POLL_SLEEP = 2

RGW_HTTP_PORT = 8080
RGW_SSL_PORT = 443

TLS_SCAN_COMPONENT_SELECTORS = {
    "noobaa": {"label": "app=noobaa"},
    "rgw": {
        "label": "app=rook-ceph-rgw",
        "fallback_ports": [RGW_HTTP_PORT, RGW_SSL_PORT],
    },
    "ceph": {"label": "rook_cluster=openshift-storage"},
    "csi": {"name_filter": "csi"},
    "metrics-exporter": {"label": constants.OCS_METRICS_EXPORTER},
    "csi-snapshot-metadata": {
        "name_filter": constants.CSI_SNAPSHOT_METADATA_NAME_SUBSTRING,
        "container_name_filter": constants.CSI_SNAPSHOT_METADATA_NAME_SUBSTRING,
    },
    "ocs-client-operator": {
        "label": constants.OCS_CLIENT_OPERATOR_LABEL,
        "name_filter": constants.OCS_CLIENT_OPERATOR_CONTROLLER_MANAGER_PREFIX,
        "extra_ports": CLIENT_OPERATOR_HTTPS_PORTS,
    },
    "all": {},
}

TLS_SCAN_BASH_SCRIPT_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        os.pardir,
        os.pardir,
        "scripts",
        "bash",
        "tls_scan_endpoints.sh",
    )
)
_tls_scan_bash_script_cache = None


def _get_tls_scan_bash_script():
    """Load the in-cluster TLS probe script from ``scripts/bash/tls_scan_endpoints.sh``."""
    global _tls_scan_bash_script_cache
    if _tls_scan_bash_script_cache is None:
        try:
            with open(TLS_SCAN_BASH_SCRIPT_PATH, encoding="utf-8") as fh:
                _tls_scan_bash_script_cache = fh.read()
        except OSError as exc:
            raise RuntimeError(
                f"TLS scan: cannot read bash script {TLS_SCAN_BASH_SCRIPT_PATH}: {exc}"
            ) from exc
    return _tls_scan_bash_script_cache


def _resolve_tls_scan_kubeconfig(kubeconfig):
    """Return explicit kubeconfig path, or None to use current oc context."""
    if kubeconfig:
        return kubeconfig
    kc = config.RUN.get("kubeconfig")
    if kc:
        return kc
    cluster_path = config.ENV_DATA.get("cluster_path")
    if cluster_path:
        loc = config.RUN.get("kubeconfig_location") or defaults.KUBECONFIG_LOCATION
        return os.path.join(cluster_path, loc)
    return None


def _tls_scan_run_oc(args, kubeconfig=None, timeout=60):
    cmd = ["oc"] + list(args)
    if kubeconfig:
        cmd.extend(["--kubeconfig", kubeconfig])
    completed = exec_cmd(cmd, timeout=timeout)
    return completed.stdout.decode()


def _tls_scan_include_container(
    pod_name, container_name, containers, name_filter, container_name_filter
):
    """
    Return True if this container should be scanned for ``component`` filters.

    Pod matches if ``name_filter`` is in the pod name or ``container_name_filter``
    is in any container name. When a matching sidecar exists, only that sidecar
    is scanned; if only the pod name matches, all of its containers are scanned.
    """
    if not name_filter and not container_name_filter:
        return True
    name_ok = bool(name_filter and name_filter in pod_name)
    this_container_ok = bool(
        container_name_filter and container_name_filter in container_name
    )
    if this_container_ok:
        return True
    if name_ok and not any(
        container_name_filter and container_name_filter in (c.get("name") or "")
        for c in containers
    ):
        return True
    return False


def _find_fallback_ports(selector, component, pod):
    fallback_ports = selector.get("fallback_ports", [])
    if fallback_ports or component != "all":
        return fallback_ports
    pod_labels = pod.get("metadata", {}).get("labels", {})
    for comp_sel in TLS_SCAN_COMPONENT_SELECTORS.values():
        comp_label = comp_sel.get("label", "")
        comp_fb = comp_sel.get("fallback_ports", [])
        if comp_fb and "=" in comp_label:
            key, val = comp_label.split("=", 1)
            if pod_labels.get(key) == val:
                return comp_fb
    return []


def _build_container_endpoints(container, pod_name, pod_ns, pod_ip, fallback_ports):
    c_name = container["name"]
    cmd_parts = container.get("command", []) + container.get("args", [])
    process = ""
    if cmd_parts:
        process = cmd_parts[0].rsplit("/", 1)[-1][:15]
    if not process:
        process = container.get("image", "").split("/")[-1].split(":")[0][:15]
    declared_ports = container.get("ports", [])
    port_numbers = [
        p.get("containerPort") for p in declared_ports if p.get("containerPort")
    ]
    if not port_numbers and fallback_ports:
        log.info(
            "TLS scan: pod %s container %s has no declared ports, "
            "using fallback ports %s",
            pod_name,
            c_name,
            fallback_ports,
        )
        port_numbers = fallback_ports
    return [
        {
            "pod_namespace": pod_ns,
            "pod_name": pod_name,
            "pod_ip": pod_ip,
            "container_name": c_name,
            "port": str(port),
            "process": process,
        }
        for port in port_numbers
    ]


def _tls_scan_discover_endpoints(kubeconfig, namespaces, component="all"):
    selector = TLS_SCAN_COMPONENT_SELECTORS.get(component, {})
    label = selector.get("label")
    name_filter = selector.get("name_filter")
    container_name_filter = selector.get("container_name_filter")
    extra_ports = tuple(int(p) for p in (selector.get("extra_ports") or ()))

    endpoints = []
    for ns in namespaces:
        log.info("TLS scan: discovering %s pods in namespace %s", component, ns)
        cmd = [
            "get",
            "pods",
            "-n",
            ns,
            "-o",
            "json",
            "--field-selector=status.phase=Running",
        ]
        if label:
            cmd.extend(["-l", label])

        out = _tls_scan_run_oc(cmd, kubeconfig=kubeconfig, timeout=30)
        data = json.loads(out)

        for pod in data.get("items", []):
            pod_name = pod["metadata"]["name"]
            pod_ns = pod["metadata"]["namespace"]
            pod_ip = pod["status"].get("podIP", "")
            if not pod_ip:
                continue
            containers = pod["spec"]["containers"]
            fallback_ports = _find_fallback_ports(selector, component, pod)
            declared_ports = set()
            extra_template = None
            snapshot_fallback_added = False
            for container in containers:
                c_name = container["name"]
                if not _tls_scan_include_container(
                    pod_name,
                    c_name,
                    containers,
                    name_filter,
                    container_name_filter,
                ):
                    continue
                new_eps = _build_container_endpoints(
                    container, pod_name, pod_ns, pod_ip, fallback_ports
                )
                cmd_parts = container.get("command", []) + container.get("args", [])
                process = ""
                if cmd_parts:
                    process = cmd_parts[0].rsplit("/", 1)[-1][:15]
                if not process:
                    process = (
                        container.get("image", "").split("/")[-1].split(":")[0][:15]
                    )
                # Prefer the manager container so extra-port probes are not
                # labeled with a later sidecar.
                if extra_template is None or c_name == "manager":
                    extra_template = {
                        "pod_namespace": pod_ns,
                        "pod_name": pod_name,
                        "pod_ip": pod_ip,
                        "container_name": c_name,
                        "process": process,
                    }
                for ep in new_eps:
                    declared_ports.add(int(ep["port"]))
                    endpoints.append(ep)
                if (
                    not new_eps
                    and component == "csi-snapshot-metadata"
                    and not snapshot_fallback_added
                ):
                    snapshot_port = int(constants.CSI_SNAPSHOT_METADATA_PORT)
                    if snapshot_port not in declared_ports:
                        snapshot_fallback_added = True
                        declared_ports.add(snapshot_port)
                        endpoints.append(
                            {
                                "pod_namespace": pod_ns,
                                "pod_name": pod_name,
                                "pod_ip": pod_ip,
                                "container_name": c_name,
                                "process": process,
                                "port": str(snapshot_port),
                            }
                        )
            if extra_template:
                for extra in extra_ports:
                    if extra not in declared_ports:
                        endpoints.append(
                            {
                                **extra_template,
                                "port": str(extra),
                            }
                        )

    log.info(
        "TLS scan: discovered %d endpoints for component %r in %d namespace(s)",
        len(endpoints),
        component,
        len(namespaces),
    )
    return endpoints


def _tls_scan_build_endpoints_file(endpoints):
    lines = []
    for ep in endpoints:
        lines.append(
            f"{ep['pod_namespace']}|{ep['pod_name']}|{ep['pod_ip']}|"
            f"{ep['container_name']}|{ep['port']}|{ep['process']}"
        )
    return "\n".join(lines) + "\n"


def _tls_scan_setup_namespace(kubeconfig):
    try:
        _tls_scan_run_oc(
            ["get", "namespace", TLS_SCANNER_NAMESPACE],
            kubeconfig=kubeconfig,
            timeout=10,
        )
        log.info("TLS scan: namespace %s exists", TLS_SCANNER_NAMESPACE)
    except CommandFailed:
        log.info("TLS scan: creating namespace %s", TLS_SCANNER_NAMESPACE)
        _tls_scan_run_oc(
            ["create", "namespace", TLS_SCANNER_NAMESPACE],
            kubeconfig=kubeconfig,
            timeout=10,
        )
        # Ensure namespaceSelector matchLabels used by NetworkPolicy resolve.
        try:
            _tls_scan_run_oc(
                [
                    "label",
                    "namespace",
                    TLS_SCANNER_NAMESPACE,
                    f"kubernetes.io/metadata.name={TLS_SCANNER_NAMESPACE}",
                    "--overwrite",
                ],
                kubeconfig=kubeconfig,
                timeout=10,
            )
        except CommandFailed:
            log.warning(
                "TLS scan: could not label namespace %s for NetworkPolicy selection",
                TLS_SCANNER_NAMESPACE,
            )


def _tls_scan_wait_for_pod_ready(
    kubeconfig,
    pod_name,
    timeout=120,
    sleep=TLS_SCAN_POD_PHASE_POLL_SLEEP,
):
    def _pod_phase():
        out = _tls_scan_run_oc(
            [
                "get",
                "pod",
                pod_name,
                "-n",
                TLS_SCANNER_NAMESPACE,
                "-o",
                "jsonpath={.status.phase}",
            ],
            kubeconfig=kubeconfig,
            timeout=10,
        )
        return out.strip()

    try:
        for phase in TimeoutSampler(timeout, sleep, _pod_phase):
            if phase == "Running":
                return
            if phase in ("Failed", "Error"):
                raise CommandFailed(f"TLS scan: scanner pod failed: {phase}")
    except TimeoutExpiredError:
        raise CommandFailed(
            f"TLS scan: scanner pod not ready after {timeout}s"
        ) from None


def _tls_scan_run_in_pod(kubeconfig, pod_name, endpoints_data, timeout=600):
    _tls_scan_run_oc(
        [
            "exec",
            "-n",
            TLS_SCANNER_NAMESPACE,
            pod_name,
            "--",
            "bash",
            "-c",
            f"cat > /tmp/endpoints.txt << 'ENDOFDATA'\n{endpoints_data}ENDOFDATA",
        ],
        kubeconfig=kubeconfig,
        timeout=30,
    )

    _tls_scan_run_oc(
        [
            "exec",
            "-n",
            TLS_SCANNER_NAMESPACE,
            pod_name,
            "--",
            "bash",
            "-c",
            f"cat > /tmp/scan.sh << 'ENDOFSCRIPT'\n{_get_tls_scan_bash_script()}ENDOFSCRIPT",
        ],
        kubeconfig=kubeconfig,
        timeout=30,
    )

    log.info("TLS scan: running openssl probes (may take several minutes)")
    return _tls_scan_run_oc(
        [
            "exec",
            "-n",
            TLS_SCANNER_NAMESPACE,
            pod_name,
            "--",
            "bash",
            "/tmp/scan.sh",
        ],
        kubeconfig=kubeconfig,
        timeout=timeout,
    )


def _tls_scan_space_separated_to_list(value):
    if not value or value == "NA":
        return []
    return value.split()


def _tls_scan_parse_csv(csv_text):
    results = []
    reader = csv.DictReader(io.StringIO(csv_text))
    for row in reader:
        port_str = row.get("port", "0")
        try:
            port_val = int(port_str)
        except (ValueError, TypeError):
            port_val = 0

        results.append(
            {
                "pod_namespace": row.get("pod_namespace", ""),
                "pod_name": row.get("pod_name", ""),
                "pod_ip": row.get("pod_ip", ""),
                "container_name": row.get("container_name", ""),
                "port": port_val,
                "process": row.get("process", ""),
                "status": row.get("status", ""),
                "tls_versions": _tls_scan_space_separated_to_list(
                    row.get("tlsversions")
                ),
                "tls12_ciphers": _tls_scan_space_separated_to_list(
                    row.get("tls12ciphers")
                ),
                "tls12_groups": _tls_scan_space_separated_to_list(
                    row.get("tls12groups")
                ),
                "tls13_ciphers": _tls_scan_space_separated_to_list(
                    row.get("tls13ciphers")
                ),
                "tls13_groups": _tls_scan_space_separated_to_list(
                    row.get("tls13groups")
                ),
                "reason": row.get("reason", ""),
            }
        )
    return results


def _tls_scan_cleanup_pod(kubeconfig, pod_name):
    log.info("TLS scan: deleting pod %s", pod_name)
    try:
        _tls_scan_run_oc(
            [
                "delete",
                "pod",
                pod_name,
                "-n",
                TLS_SCANNER_NAMESPACE,
                "--grace-period=0",
                "--force",
                "--ignore-not-found",
            ],
            kubeconfig=kubeconfig,
            timeout=30,
        )
    except CommandFailed as e:
        log.warning("TLS scan: cleanup failed: %s", e)


def _tls_scan_delete_scanner_namespace(kubeconfig):
    """
    Delete ``scantls-system`` and all resources in it (``oc delete namespace``).
    """
    log.info(
        "TLS scan: deleting namespace %s (removes all resources in it)",
        TLS_SCANNER_NAMESPACE,
    )
    try:
        _tls_scan_run_oc(
            [
                "delete",
                "namespace",
                TLS_SCANNER_NAMESPACE,
                "--ignore-not-found",
                "--timeout=5m",
            ],
            kubeconfig=kubeconfig,
            timeout=360,
        )
    except CommandFailed as e:
        log.warning("TLS scan: namespace cleanup failed: %s", e)


def _tls_scan_metrics_exporter_scanner_ingress():
    """Ingress rule allowing ``scantls-system`` to HTTPS ports 8443 and 9443."""
    return {
        "from": [
            {
                "namespaceSelector": {
                    "matchLabels": {
                        "kubernetes.io/metadata.name": TLS_SCANNER_NAMESPACE,
                    }
                }
            }
        ],
        "ports": [
            {"port": int(constants.OCS_METRICS_EXPORTER_PORT), "protocol": "TCP"},
            {
                "port": int(constants.OCS_METRICS_EXPORTER_LEGACY_PORT),
                "protocol": "TCP",
            },
        ],
    }


def _tls_scan_np_ingress_allows_scanner(ingress):
    """Return True if any ingress rule already selects ``scantls-system``."""
    for rule in ingress or []:
        for src in rule.get("from") or []:
            labels = (src.get("namespaceSelector") or {}).get("matchLabels") or {}
            if labels.get("kubernetes.io/metadata.name") == TLS_SCANNER_NAMESPACE:
                return True
    return False


def _tls_scan_metrics_exporter_np_allow_scanner(kubeconfig, namespace):
    """
    Temporarily allow scantls-system to reach ocs-metrics-exporter HTTPS ports.

    StorageCluster owns NetworkPolicy ``ocs-metrics-exporter`` (ingress only
    from openshift-monitoring). ocs-operator reverts patches to that object, so
    this creates a separate additive policy instead of mutating the owned one.

    Returns:
        callable | None: Restore function, or None if there is nothing to restore.
    """
    try:
        raw = _tls_scan_run_oc(
            [
                "get",
                "networkpolicy",
                "ocs-metrics-exporter",
                "-n",
                namespace,
                "-o",
                "json",
            ],
            kubeconfig=kubeconfig,
            timeout=30,
        )
    except CommandFailed:
        log.info(
            "TLS scan: no NetworkPolicy ocs-metrics-exporter in %s; "
            "not adding scanner ingress",
            namespace,
        )
        return None
    data = json.loads(raw)
    spec = data.get("spec") or {}
    original_ingress = spec.get("ingress") or []
    if _tls_scan_np_ingress_allows_scanner(original_ingress):
        log.info(
            "TLS scan: ocs-metrics-exporter NetworkPolicy already allows %s",
            TLS_SCANNER_NAMESPACE,
        )
        return None
    pod_selector = copy.deepcopy(spec.get("podSelector") or {})
    if not pod_selector:
        pod_selector = {
            "matchLabels": {"app.kubernetes.io/name": "ocs-metrics-exporter"}
        }
    np_manifest = {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {
            "name": TLS_SCAN_METRICS_EXPORTER_SCANNER_NP,
            "namespace": namespace,
            "labels": {"app": "tls-scanner"},
        },
        "spec": {
            "podSelector": pod_selector,
            "policyTypes": ["Ingress"],
            "ingress": [_tls_scan_metrics_exporter_scanner_ingress()],
        },
    }
    apply_cmd = ["oc", "apply", "-f", "-"]
    if kubeconfig:
        apply_cmd.extend(["--kubeconfig", kubeconfig])
    exec_cmd(apply_cmd, timeout=30, input=json.dumps(np_manifest).encode())
    log.info(
        "TLS scan: created NetworkPolicy %s so %s can scrape "
        "ocs-metrics-exporter HTTPS in %s",
        TLS_SCAN_METRICS_EXPORTER_SCANNER_NP,
        TLS_SCANNER_NAMESPACE,
        namespace,
    )

    def _restore():
        try:
            _tls_scan_run_oc(
                [
                    "delete",
                    "networkpolicy",
                    TLS_SCAN_METRICS_EXPORTER_SCANNER_NP,
                    "-n",
                    namespace,
                    "--ignore-not-found",
                ],
                kubeconfig=kubeconfig,
                timeout=30,
            )
            log.info(
                "TLS scan: deleted NetworkPolicy %s",
                TLS_SCAN_METRICS_EXPORTER_SCANNER_NP,
            )
        except CommandFailed:
            log.exception(
                "TLS scan: failed to delete NetworkPolicy %s",
                TLS_SCAN_METRICS_EXPORTER_SCANNER_NP,
            )

    return _restore


def scan_cluster(
    component="all",
    kubeconfig=None,
    namespaces=None,
    timeout=SCAN_CLUSTER_DEFAULT_TIMEOUT,
    skip_ports=None,
    tls_versions=None,
    tls12_ciphers=None,
    tls12_groups=None,
    tls13_ciphers=None,
    tls13_groups=None,
    scanner_image=None,
    scan_timeout=600,
    cleanup=True,
):
    """
    Discover pod container ports in the storage namespace(s), run a short-lived
    scanner pod in ``scantls-system``, and return per-endpoint TLS probe results.

    For ``metrics-exporter`` (and ``all``), a temporary NetworkPolicy
    ``ocs-metrics-exporter-scantls`` is applied so ``scantls-system`` may reach
    ports 8443/9443 (the StorageCluster-owned policy is left unchanged).

    Args:
        component: ``noobaa``, ``rgw``, ``ceph``, ``csi``, ``metrics-exporter``,
            ``csi-snapshot-metadata``, ``ocs-client-operator``, or ``all``.
        kubeconfig: Path to kubeconfig; defaults from RUN / ENV_DATA (see
            :func:`_resolve_tls_scan_kubeconfig`).
        namespaces: Namespaces to scan; default
            ``cluster_namespace`` or openshift-storage.
        timeout: Per-openssl-probe timeout (seconds).
        skip_ports: Comma-separated ports to skip.
        tls_versions: Comma-separated versions to test (default tls1.2,tls1.3).
        tls12_ciphers: Comma-separated OpenSSL cipher names for TLS 1.2.
        tls12_groups: Comma-separated groups for TLS 1.2.
        tls13_ciphers: Comma-separated ciphersuites for TLS 1.3.
        tls13_groups: Comma-separated groups for TLS 1.3.
        scanner_image: Scanner container image (default ``TLS_SCANNER_IMAGE``).
        scan_timeout: Max seconds for the remote ``scan.sh`` run.
        cleanup: When True, delete the scanner pod and remove the
            ``scantls-system`` namespace (and all objects in it) when finished.

    Returns:
        list: One dict per endpoint with keys pod_namespace, pod_name, pod_ip,
        container_name, port (int), process, status (OK|NO_TLS|SKIPPED),
        tls_versions, tls12_ciphers, tls12_groups, tls13_ciphers,
        tls13_groups, reason.
    """
    if component not in TLS_SCAN_COMPONENT_SELECTORS:
        raise ValueError(
            f"Unknown component {component!r}; must be one of: "
            f"{', '.join(TLS_SCAN_COMPONENT_SELECTORS)}"
        )

    kubeconfig = _resolve_tls_scan_kubeconfig(kubeconfig)

    if namespaces is None:
        ns = (
            config.ENV_DATA.get("cluster_namespace")
            or constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        namespaces = [ns]

    if scanner_image is None:
        scanner_image = TLS_SCANNER_IMAGE

    env_vars = {
        "TIMEOUT": str(timeout),
        "SKIP_PORTS": skip_ports or SCAN_CLUSTER_DEFAULT_SKIP_PORTS,
        "TLS_VERSIONS": tls_versions or SCAN_CLUSTER_DEFAULT_TLS_VERSIONS,
        "TLS12_CIPHERS": tls12_ciphers or SCAN_CLUSTER_DEFAULT_TLS12_CIPHERS,
        "TLS12_GROUPS": tls12_groups or SCAN_CLUSTER_DEFAULT_TLS12_GROUPS,
        "TLS13_CIPHERS": tls13_ciphers or SCAN_CLUSTER_DEFAULT_TLS13_CIPHERS,
        "TLS13_GROUPS": tls13_groups or SCAN_CLUSTER_DEFAULT_TLS13_GROUPS,
    }

    endpoints = _tls_scan_discover_endpoints(kubeconfig, namespaces, component)
    if not endpoints:
        log.warning(
            "TLS scan: no endpoints for component %r in %s",
            component,
            namespaces,
        )
        return []

    endpoints_data = _tls_scan_build_endpoints_file(endpoints)

    pod_name = f"tls-scanner-{uuid.uuid4().hex[:8]}"
    pod_manifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": pod_name,
            "namespace": TLS_SCANNER_NAMESPACE,
            "labels": {"app": "tls-scanner"},
        },
        "spec": {
            "restartPolicy": "Never",
            "terminationGracePeriodSeconds": 0,
            "containers": [
                {
                    "name": "scanner",
                    "image": scanner_image,
                    "command": ["sleep", "3600"],
                    "env": [{"name": k, "value": v} for k, v in env_vars.items()],
                    "securityContext": {
                        "allowPrivilegeEscalation": False,
                        "capabilities": {"drop": ["ALL"]},
                        "runAsNonRoot": True,
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                }
            ],
        },
    }

    log.info(
        "TLS scan: deploying scanner pod %s in %s",
        pod_name,
        TLS_SCANNER_NAMESPACE,
    )
    manifest_json = json.dumps(pod_manifest)
    apply_cmd = ["oc", "apply", "-f", "-"]
    if kubeconfig:
        apply_cmd.extend(["--kubeconfig", kubeconfig])

    restore_np = []
    try:
        _tls_scan_setup_namespace(kubeconfig)
        if component in ("metrics-exporter", "all"):
            for ns in namespaces:
                restore_fn = _tls_scan_metrics_exporter_np_allow_scanner(kubeconfig, ns)
                if restore_fn:
                    restore_np.append(restore_fn)
        exec_cmd(apply_cmd, timeout=30, input=manifest_json.encode())
        _tls_scan_wait_for_pod_ready(kubeconfig, pod_name)
        csv_output = _tls_scan_run_in_pod(
            kubeconfig, pod_name, endpoints_data, timeout=scan_timeout
        )
        results = _tls_scan_parse_csv(csv_output)
        log.info(
            "TLS scan: complete %d endpoints (%d OK, %d NO_TLS, %d other)",
            len(results),
            sum(1 for r in results if r["status"] == "OK"),
            sum(1 for r in results if r["status"] == "NO_TLS"),
            sum(1 for r in results if r["status"] not in ("OK", "NO_TLS")),
        )
        return results
    finally:
        for restore_fn in restore_np:
            restore_fn()
        if cleanup:
            _tls_scan_cleanup_pod(kubeconfig, pod_name)
            _tls_scan_delete_scanner_namespace(kubeconfig)


# Maps TLSProfile ``spec.rules[].config.version`` to tokens produced by the in-cluster
# scanner (entries in ``tls_versions`` from :func:`scan_cluster`).
TLS_PROFILE_VERSION_TO_SCAN_TOKEN = {
    "TLSv1.2": "tls1.2",
    "TLSv1.3": "tls1.3",
}


def tls_profile_api_version_to_scan_token(api_version):
    """Return scanner ``tls_versions`` token (e.g. ``tls1.3``) for a TLSProfile version."""
    token = TLS_PROFILE_VERSION_TO_SCAN_TOKEN.get(api_version)
    if not token:
        raise ValueError(
            f"Unsupported TLSProfile API version {api_version!r}; "
            f"expected one of: {', '.join(TLS_PROFILE_VERSION_TO_SCAN_TOKEN)}"
        )
    return token


def assert_tls_scan_results_include_version(
    results,
    api_tls_version,
    *,
    min_matching_endpoints=1,
    context="",
):
    """
    Fail unless at least ``min_matching_endpoints`` scan rows with ``status == "OK"``
    list the TLS version matching ``api_tls_version`` (see :func:`scan_cluster`).

    Other ``OK`` rows may negotiate only other versions (e.g. TLS 1.2 on some ports
    while the profile allows 1.3); mixed ports on the same workload do not fail the
    check as long as the minimum number of matches is met.

    Rows with ``NO_TLS`` or ``SKIPPED`` are ignored.

    Args:
        results: Return value of :func:`scan_cluster`.
        api_tls_version: e.g. ``TLSv1.2`` or ``TLSv1.3``.
        min_matching_endpoints: Minimum count of ``OK`` rows that must include the
            expected version (default 1).
        context: Short string appended to failure messages for debugging.

    On success with ``api_tls_version`` ``TLSv1.3``, each matching endpoint is logged at
    INFO (pod IP, port, namespace, pod, container, process, ``tls_versions``).
    """
    token = tls_profile_api_version_to_scan_token(api_tls_version)
    ok_rows = [r for r in results if r["status"] == "OK"]
    suffix = f" ({context})" if context else ""

    matching = [r for r in ok_rows if token in (r.get("tls_versions") or [])]

    if len(matching) < min_matching_endpoints:
        sample_other = [
            f"{r['pod_namespace']}/{r['pod_name']}:{r['port']} "
            f"tls_versions={(r.get('tls_versions') or [])!r}"
            for r in ok_rows
            if token not in (r.get("tls_versions") or [])
        ][:15]
        other_msg = (
            "\nOther OK endpoints (no %r): %s"
            % (
                token,
                "\n".join(sample_other) if sample_other else "none",
            )
            if sample_other or ok_rows
            else ""
        )
        raise AssertionError(
            f"TLS scan: expected at least {min_matching_endpoints} OK endpoint(s) "
            f"with {api_tls_version} ({token!r}){suffix}; "
            f"found {len(matching)} matching, {len(ok_rows)} OK total "
            f"(rows in scan: {len(results)}).{other_msg}"
        )

    if api_tls_version == "TLSv1.3" and matching:
        log.info(
            "TLS scan: TLSProfile targets TLS 1.3; %d scanner row(s) negotiated tls1.3%s",
            len(matching),
            suffix,
        )
        for r in matching:
            pod_ip = r.get("pod_ip") or ""
            port = r.get("port") or ""
            endpoint = f"{pod_ip}:{port}" if pod_ip else f":{port}"
            log.info(
                "TLS 1.3 found: endpoint=%s namespace=%s pod=%s container=%s "
                "port=%s process=%s tls_versions=%s",
                endpoint,
                r.get("pod_namespace"),
                r.get("pod_name"),
                r.get("container_name"),
                r.get("port"),
                r.get("process"),
                r.get("tls_versions"),
            )


class TLSProfile:
    """
    Manage TLSProfile CRs in the ODF namespace.
    """

    API_VERSION = "ocs.openshift.io/v1"
    KIND = "TLSProfile"

    def __init__(
        self,
        name="ocs-tls-profile",
        namespace=None,
    ):
        """
        Args:
            name (str): TLSProfile metadata.name
            namespace (str): Namespace for the resource; defaults to cluster_namespace
                from config, then openshift-storage.
        """
        self.name = name
        self.namespace = (
            namespace
            or config.ENV_DATA.get("cluster_namespace")
            or (constants.OPENSHIFT_STORAGE_NAMESPACE)
        )
        self._ocp = OCP(
            api_version=self.API_VERSION,
            kind=self.KIND,
            namespace=self.namespace,
            resource_name=self.name,
        )

    def create_tls_profile(
        self,
        selectors=None,
        tls_version="TLSv1.3",
        ciphers=None,
        groups=None,
        do_reload=True,
    ):
        """
        Create a TLSProfile with one rule; selectors, TLS version, ciphers, and
        groups are configurable.

        Args:
            selectors (list | str): Rule selectors; default is a single wildcard.
            tls_version (str): spec.rules[].config.version
            ciphers (list): spec.rules[].config.ciphers
            groups (list): spec.rules[].config.groups
            do_reload (bool): Reload OCS object after create.

        Returns:
            OCS: The created TLSProfile object.
        """
        if selectors is None:
            selectors = ["*"]
        elif isinstance(selectors, str):
            selectors = [selectors]

        if ciphers is None:
            ciphers = (
                list(TLS_PROFILE_V13_CIPHERS)
                if tls_version == "TLSv1.3"
                else list(TLS_PROFILE_V12_CIPHERS)
            )
        if groups is None:
            groups = (
                list(TLS_PROFILE_V13_GROUPS)
                if tls_version == "TLSv1.3"
                else list(TLS_PROFILE_V12_GROUPS)
            )

        tls_resource = {
            "apiVersion": self.API_VERSION,
            "kind": self.KIND,
            "metadata": {"name": self.name, "namespace": self.namespace},
            "spec": {
                "rules": [
                    {
                        "selectors": list(selectors),
                        "config": {
                            "version": tls_version,
                            "ciphers": list(ciphers),
                            "groups": list(groups),
                        },
                    }
                ]
            },
        }
        ocs_obj = OCS(**tls_resource)
        log.info(
            f"Creating {self.KIND} {self.name} in namespace {self.namespace} "
            f"(version={tls_version})"
        )
        ocs_obj.create(do_reload=do_reload)
        return ocs_obj

    def is_tls_profile_available(self, silent=True):
        """
        Return True if the TLSProfile exists on the cluster.

        Args:
            silent (bool): If True, suppress warnings on failed get attempts.
        """
        data = self._ocp.get(
            resource_name=self.name,
            dont_raise=True,
            silent=silent,
            retry=0,
        )
        if not data:
            return False
        return data.get("kind") == self.KIND

    def get_tls_profile(self, out_yaml_format=True):
        """
        Fetch the TLSProfile from the cluster.

        Args:
            out_yaml_format (bool): Return parsed YAML dict when True.

        Returns:
            dict or str: Resource from oc get.
        """
        return self._ocp.get(resource_name=self.name, out_yaml_format=out_yaml_format)

    def delete_tls_profile(self, wait=True, force=False):
        """
        Delete the TLSProfile from the cluster.

        Args:
            wait (bool): Wait for deletion to complete.
            force (bool): Force delete with grace period 0.

        Returns:
            dict: Parsed oc delete output when YAML; depends on OCP.delete behavior.
        """
        log.info(f"Deleting {self.KIND} {self.name} in namespace {self.namespace}")
        return self._ocp.delete(resource_name=self.name, wait=wait, force=force)

    def get_rule_config(self):
        """Return the first rule's config dict or None."""
        data = self.get_tls_profile()
        rules = data.get("spec", {}).get("rules") or []
        if not rules:
            return None
        return (rules[0].get("config") or {}).copy()

    def get_config_version(self):
        """Return spec.rules[0].config.version if present."""
        cfg = self.get_rule_config()
        return cfg.get("version") if cfg else None

    def replace_rules(self, selectors, tls_version, ciphers, groups):
        """
        Merge-patch the full rules list (single rule) on this TLSProfile.

        Args:
            selectors (list): Rule selectors.
            tls_version (str): e.g. TLSv1.2, TLSv1.3
            ciphers (list): Cipher suite names for that version.
            groups (list): Group names for that version.
        """
        patch = {
            "spec": {
                "rules": [
                    {
                        "selectors": list(selectors),
                        "config": {
                            "version": tls_version,
                            "ciphers": list(ciphers),
                            "groups": list(groups),
                        },
                    }
                ]
            }
        }
        patched = self._ocp.patch(
            resource_name=self.name,
            params=json.dumps(patch),
            format_type="merge",
        )
        if not patched:
            log.warning(
                "oc patch for %s did not report success; validating via get",
                self.name,
            )

    def apply_rules(self, rules, do_reload=True):
        """
        Replace spec.rules, creating the TLSProfile if it is absent.

        Args:
            rules (list): spec.rules value to apply.
            do_reload (bool): Reload OCS object after create when the CR is missing.
        """
        rules = copy.deepcopy(list(rules))
        if not self.is_tls_profile_available(silent=True):
            tls_resource = {
                "apiVersion": self.API_VERSION,
                "kind": self.KIND,
                "metadata": {"name": self.name, "namespace": self.namespace},
                "spec": {"rules": rules},
            }
            log.info(
                "Recreating %s %s in namespace %s to restore rules",
                self.KIND,
                self.name,
                self.namespace,
            )
            OCS(**tls_resource).create(do_reload=do_reload)
            return
        patch = {"spec": {"rules": rules}}
        patched = self._ocp.patch(
            resource_name=self.name,
            params=json.dumps(patch),
            format_type="merge",
        )
        if not patched:
            log.warning(
                "oc patch for %s did not report success; validating via get",
                self.name,
            )


def snapshot_tlsprofile_state(tls):
    """
    Capture whether ``ocs-tls-profile`` exists and a copy of spec.rules.

    Returns:
        tuple: ``(existed_before, original_rules)``. ``original_rules`` is None
        when the profile is absent.
    """
    existed_before = tls.is_tls_profile_available(silent=True)
    if not existed_before:
        return False, None
    data = tls.get_tls_profile()
    original_rules = copy.deepcopy((data.get("spec") or {}).get("rules") or [])
    return True, original_rules


def teardown_tlsprofile(tls, existed_before, original_rules):
    """
    Restore a pre-existing TLSProfile or delete one this test created.

    No-op when the profile is still absent after a skip, or when pre-existing
    spec.rules are unchanged.
    """
    present = tls.is_tls_profile_available(silent=True)
    if not existed_before:
        if present:
            log.info("Teardown: deleting ocs-tls-profile created by this test")
            tls.delete_tls_profile(wait=True, force=True)
        return
    current_rules = None
    if present:
        data = tls.get_tls_profile()
        current_rules = (data.get("spec") or {}).get("rules") or []
    if present and current_rules == original_rules:
        return
    if original_rules is None:
        return
    log.info("Teardown: restoring pre-existing ocs-tls-profile rules")
    tls.apply_rules(original_rules)


def tlsprofile_crd_exists():
    """Return True if tlsprofiles.ocs.openshift.io CRD is installed."""
    crd = OCP(
        api_version="apiextensions.k8s.io/v1",
        kind="CustomResourceDefinition",
        resource_name="tlsprofiles.ocs.openshift.io",
    )
    data = crd.get(dont_raise=True, silent=True)
    return bool(data and data.get("metadata", {}).get("name"))


def wait_for_tlsprofile_config_version(
    tls_profile, expected_version, timeout=600, sleep=15
):
    """Wait until TLSProfile spec shows the given TLS version string."""

    def _version():
        return tls_profile.get_config_version()

    TimeoutSampler(timeout, sleep, _version).wait_for_func_value(expected_version)


def get_noobaa_api_server_security(namespace):
    """Return NooBaa spec.security.apiServerSecurity dict or None."""
    nb = OCP(kind="noobaa", namespace=namespace, resource_name="noobaa")
    data = nb.get()
    return data.get("spec", {}).get("security", {}).get("apiServerSecurity")


def wait_for_noobaa_api_server_security_absent(namespace, timeout=600, sleep=15):
    """Wait until NooBaa has no apiServerSecurity (TLSProfile-based config cleared)."""

    def _cleared():
        return get_noobaa_api_server_security(namespace) is None

    TimeoutSampler(timeout, sleep, _cleared).wait_for_func_value(True)


def wait_for_noobaa_tls_min_version_substring(
    namespace, expected_substring, timeout=600, sleep=15
):
    """
    Wait until NooBaa apiServerSecurity.tlsMinVersion contains expected_substring
    (e.g. '1.2' or '1.3').
    """

    def _match():
        sec = get_noobaa_api_server_security(namespace)
        if sec is None:
            return False
        ver = sec.get("tlsMinVersion")
        return ver is not None and expected_substring in str(ver).lower()

    TimeoutSampler(timeout, sleep, _match).wait_for_func_value(True)


def get_first_cephobjectstore_name(namespace):
    """
    Return the name of the first CephObjectStore in namespace, or None.
    """
    cos = OCP(
        api_version=defaults.ROOK_API_VERSION,
        kind="CephObjectStore",
        namespace=namespace,
    )
    items = cos.get().get("items") or []
    if not items:
        return None
    return items[0]["metadata"]["name"]


def get_cephobjectstore_security(namespace, name):
    """Return CephObjectStore spec.security dict or None."""
    cos = OCP(
        api_version=defaults.ROOK_API_VERSION,
        kind="CephObjectStore",
        namespace=namespace,
        resource_name=name,
    )
    data = cos.get()
    return data.get("spec", {}).get("security")


def wait_for_cephobjectstore_tls_ciphers_substring(
    namespace, cos_name, cipher_substr, timeout=600, sleep=15
):
    """Wait until RGW object's spec.security.ciphers mentions cipher_substr (TLS 1.2 probe)."""

    def _match():
        sec = get_cephobjectstore_security(namespace, cos_name)
        if not sec:
            return False
        ciphers = sec.get("ciphers") or []
        joined = " ".join(ciphers).lower()
        return cipher_substr.lower() in joined

    TimeoutSampler(timeout, sleep, _match).wait_for_func_value(True)


def wait_for_cephobjectstore_security_cleared(
    namespace, cos_name, timeout=600, sleep=15
):
    """
    Wait until spec.security carries no TLSProfile-propagated cipher/group lists
    (empty or security omitted).
    """

    def _cleared():
        sec = get_cephobjectstore_security(namespace, cos_name)
        if sec is None:
            return True
        ciphers = sec.get("ciphers") or []
        groups = sec.get("tlsGroups") or []
        return len(ciphers) == 0 and len(groups) == 0

    TimeoutSampler(timeout, sleep, _cleared).wait_for_func_value(True)


def _tls_container_fingerprints(pods, container_name_substr=None):
    """
    Return a frozenset identifying ready/running containers for rollout waits.

    Each item is ``(pod_uid, pod_name, container_name, startedAt, restartCount)``.
    """
    fingerprints = []
    for pod in pods or []:
        uid = pod.get("metadata", {}).get("uid") or ""
        pname = pod.get("metadata", {}).get("name") or ""
        for cs in pod.get("status", {}).get("containerStatuses") or []:
            cname = cs.get("name") or ""
            if container_name_substr and container_name_substr not in cname:
                continue
            started = ((cs.get("state") or {}).get("running") or {}).get(
                "startedAt"
            ) or ""
            restarts = int(cs.get("restartCount") or 0)
            fingerprints.append((uid, pname, cname, started, restarts))
    return frozenset(fingerprints)


def _tls_workload_containers_ready(pods, container_name_substr=None):
    """Return True if matching containers exist and all of them are ready."""
    running = [
        p
        for p in (pods or [])
        if p.get("status", {}).get("phase") == constants.STATUS_RUNNING
    ]
    if not running:
        return False
    saw_match = False
    for pod in running:
        statuses = pod.get("status", {}).get("containerStatuses") or []
        if not statuses:
            return False
        for cs in statuses:
            cname = cs.get("name") or ""
            if container_name_substr and container_name_substr not in cname:
                continue
            saw_match = True
            if not cs.get("ready"):
                return False
        if not container_name_substr and not all(cs.get("ready") for cs in statuses):
            return False
    if container_name_substr:
        return saw_match
    return True


def _wait_for_tls_workload_ready(
    list_pods_fn,
    previous_fingerprints,
    container_name_substr=None,
    timeout=600,
    sleep=15,
    workload_name="workload",
):
    """
    Wait until the workload is ready. When ``previous_fingerprints`` is set,
    also wait until none of those fingerprints remain (new pod UID, restart,
    or container start time).
    """
    log.info(
        "Waiting for %s to be ready after TLSProfile change "
        "(previous fingerprints=%d)",
        workload_name,
        len(previous_fingerprints or ()),
    )

    def _done():
        pods = list_pods_fn() or []
        if not _tls_workload_containers_ready(pods, container_name_substr):
            return False
        if previous_fingerprints is None:
            return True
        current = _tls_container_fingerprints(pods, container_name_substr)
        if not current:
            return False
        return previous_fingerprints.isdisjoint(current)

    TimeoutSampler(timeout, sleep, _done).wait_for_func_value(True)


def snapshot_metrics_exporter_roll_state(namespace):
    """Capture ocs-metrics-exporter container fingerprints before a TLSProfile apply."""
    from ocs_ci.ocs.resources.pod import get_pods_having_label

    pods = (
        get_pods_having_label(constants.OCS_METRICS_EXPORTER, namespace=namespace) or []
    )
    return _tls_container_fingerprints(pods)


def snapshot_csi_snapshot_metadata_roll_state(namespace):
    """Capture csi-snapshot-metadata sidecar fingerprints before a TLSProfile apply."""
    return _tls_container_fingerprints(
        _list_csi_snapshot_metadata_pod_items(namespace),
        constants.CSI_SNAPSHOT_METADATA_NAME_SUBSTRING,
    )


def snapshot_ocs_client_operator_roll_state(namespace):
    """Capture ocs-client-operator manager fingerprints before a TLSProfile apply."""
    from ocs_ci.ocs.resources.pod import get_pods_having_label

    pods = (
        get_pods_having_label(constants.OCS_CLIENT_OPERATOR_LABEL, namespace=namespace)
        or []
    )
    return _tls_container_fingerprints(pods, "manager")


def metrics_exporter_is_deployed(namespace):
    """Return True if at least one ocs-metrics-exporter pod exists in namespace."""
    from ocs_ci.ocs.resources.pod import get_pods_having_label

    return bool(
        get_pods_having_label(constants.OCS_METRICS_EXPORTER, namespace=namespace)
    )


def wait_for_metrics_exporter_ready(
    namespace, timeout=600, sleep=15, previous_fingerprints=None
):
    """
    Wait until ocs-metrics-exporter is Running and ready.

    When ``previous_fingerprints`` is set (from
    :func:`snapshot_metrics_exporter_roll_state` taken before applying a
    TLSProfile), also wait until every remaining fingerprint is new so the
    scan does not hit a pre-rollout pod.
    """
    from ocs_ci.ocs.resources.pod import get_pods_having_label

    def _list_pods():
        return (
            get_pods_having_label(
                constants.OCS_METRICS_EXPORTER,
                namespace=namespace,
                statuses=[constants.STATUS_RUNNING],
            )
            or []
        )

    _wait_for_tls_workload_ready(
        _list_pods,
        previous_fingerprints,
        workload_name="ocs-metrics-exporter",
        timeout=timeout,
        sleep=sleep,
    )


def _format_tls_scan_row(row):
    """Short one-line summary of a :func:`scan_cluster` row for assertions."""
    return (
        f"{row.get('pod_namespace')}/{row.get('pod_name')}:"
        f"{row.get('port')} status={row.get('status')!r} "
        f"tls_versions={row.get('tls_versions')!r} "
        f"tls12_ciphers={row.get('tls12_ciphers')!r} "
        f"tls13_ciphers={row.get('tls13_ciphers')!r} "
        f"reason={row.get('reason')!r}"
    )


def filter_tls_scan_results_by_ports(results, ports):
    """Return scan rows whose ``port`` is in ``ports`` (int-compared)."""
    port_set = {int(p) for p in ports}
    return [r for r in results if int(r.get("port") or 0) in port_set]


def _tls_port_label(port, port_roles=None):
    """Return ``(label, role)`` for assertion messages. ``role`` is None unless mapped."""
    if port_roles:
        role = port_roles.get(int(port), "unknown")
        return f"port {port} ({role})", role
    return f"port {port}", None


def _assert_https_tls_applied(
    results,
    api_tls_version,
    ports,
    component_label,
    error_header,
    context="",
    expected_ciphers=None,
    expected_groups=None,
    port_roles=None,
    verify_ciphers_groups=False,
    success_log_includes_role=False,
    exact_version=True,
):
    """
    Fail unless each required port is serving HTTPS with the TLSProfile version.

    Every OK scan row for a port must negotiate ``api_tls_version``. When
    ``exact_version`` is True, the row must not also offer the other of
    tls1.2 / tls1.3. When False (min-version semantics), a higher version such
    as tls1.3 may still be offered for a TLSv1.2 profile. When
    ``verify_ciphers_groups`` is True, scantls cipher/group names for the
    requested version must stay within the TLSProfile set.
    """
    token = tls_profile_api_version_to_scan_token(api_tls_version)
    other_token = "tls1.2" if token == "tls1.3" else "tls1.3"
    suffix = f" ({context})" if context else ""
    problems = []

    if verify_ciphers_groups:
        if expected_ciphers is None:
            expected_ciphers = (
                TLS_PROFILE_V13_CIPHERS
                if token == "tls1.3"
                else TLS_PROFILE_V12_CIPHERS
            )
        if expected_groups is None:
            expected_groups = (
                TLS_PROFILE_V13_GROUPS if token == "tls1.3" else TLS_PROFILE_V12_GROUPS
            )
        allowed_ciphers = set(openssl_ciphers_for_tls_profile(expected_ciphers))
        allowed_groups = set(openssl_groups_for_tls_profile(expected_groups))
        cipher_field = "tls13_ciphers" if token == "tls1.3" else "tls12_ciphers"
        group_field = "tls13_groups" if token == "tls1.3" else "tls12_groups"
        other_cipher_field = "tls12_ciphers" if token == "tls1.3" else "tls13_ciphers"

    for port in ports:
        port_label, role = _tls_port_label(port, port_roles)
        rows = filter_tls_scan_results_by_ports(results, (port,))
        if not rows:
            problems.append(
                f"{port_label}: no scan row (HTTPS listener not discovered on "
                f"{component_label})"
            )
            continue

        ok_rows = [r for r in rows if r.get("status") == "OK"]
        if not ok_rows:
            problems.append(
                f"{port_label}: not serving HTTPS/TLS "
                f"(rows: {'; '.join(_format_tls_scan_row(r) for r in rows)})"
            )
            continue

        missing_token = [
            r for r in ok_rows if token not in (r.get("tls_versions") or [])
        ]
        if missing_token:
            problems.append(
                f"{port_label}: HTTPS is up but {api_tls_version} ({token!r}) "
                f"was not negotiated "
                f"(rows: {'; '.join(_format_tls_scan_row(r) for r in missing_token)})"
            )
            continue

        leaked = [r for r in ok_rows if other_token in (r.get("tls_versions") or [])]
        if exact_version and leaked:
            problems.append(
                f"{port_label}: TLSProfile {api_tls_version} should be exact, "
                f"but {other_token!r} was still offered "
                f"(rows: {'; '.join(_format_tls_scan_row(r) for r in leaked)})"
            )
            continue

        if verify_ciphers_groups:
            for r in ok_rows:
                scanned_ciphers = r.get(cipher_field) or []
                other_ciphers = r.get(other_cipher_field) or []
                scanned_groups = r.get(group_field) or []
                if not scanned_ciphers:
                    problems.append(
                        f"{port_label}: {api_tls_version} negotiated but scantls "
                        f"reported no {cipher_field} "
                        f"({_format_tls_scan_row(r)})"
                    )
                    continue
                unexpected_ciphers = [
                    c for c in scanned_ciphers if c not in allowed_ciphers
                ]
                if unexpected_ciphers:
                    problems.append(
                        f"{port_label}: scantls reported ciphers not in TLSProfile "
                        f"{api_tls_version}: {unexpected_ciphers}; "
                        f"allowed={sorted(allowed_ciphers)} "
                        f"({_format_tls_scan_row(r)})"
                    )
                if exact_version and other_ciphers:
                    problems.append(
                        f"{port_label}: TLSProfile {api_tls_version} should not offer "
                        f"{other_cipher_field}={other_ciphers} "
                        f"({_format_tls_scan_row(r)})"
                    )
                unexpected_groups = [
                    g for g in scanned_groups if g not in allowed_groups
                ]
                if unexpected_groups:
                    problems.append(
                        f"{port_label}: scantls reported groups not in TLSProfile "
                        f"{api_tls_version}: {unexpected_groups}; "
                        f"allowed={sorted(allowed_groups)} "
                        f"({_format_tls_scan_row(r)})"
                    )

        if success_log_includes_role:
            log.info(
                "%s HTTPS TLSProfile applied: port=%s role=%s version=%s%s",
                component_label,
                port,
                role,
                api_tls_version,
                suffix,
            )
        else:
            log.info(
                "%s HTTPS TLSProfile applied: port=%s version=%s%s",
                component_label,
                port,
                api_tls_version,
                suffix,
            )
        for r in ok_rows:
            log.info(
                "HTTPS %s:%s pod=%s container=%s tls_versions=%s "
                "tls12_ciphers=%s tls13_ciphers=%s tls12_groups=%s tls13_groups=%s",
                r.get("pod_ip"),
                r.get("port"),
                r.get("pod_name"),
                r.get("container_name"),
                r.get("tls_versions"),
                r.get("tls12_ciphers"),
                r.get("tls13_ciphers"),
                r.get("tls12_groups"),
                r.get("tls13_groups"),
            )

    if problems:
        raise AssertionError(
            f"{error_header} {api_tls_version}{suffix}:\n"
            + "\n".join(f"  - {p}" for p in problems)
        )


def assert_metrics_exporter_https_tls_applied(
    results,
    api_tls_version,
    ports=None,
    context="",
):
    """
    Fail unless ocs-metrics-exporter HTTPS ports (default 8443 and 9443) are
    serving TLS and the TLSProfile version is in effect on each port.

    TLSProfile version is exact (min == max), so a matching port must negotiate
    the expected version and must not still offer the other of tls1.2 / tls1.3.

    Args:
        results: Return value of :func:`scan_cluster` for component
            ``metrics-exporter``.
        api_tls_version: ``TLSv1.2`` or ``TLSv1.3``.
        ports: HTTPS ports to require (default ``METRICS_EXPORTER_HTTPS_PORTS``).
        context: Short string appended to failure messages.
    """
    ports = tuple(ports) if ports is not None else METRICS_EXPORTER_HTTPS_PORTS
    _assert_https_tls_applied(
        results,
        api_tls_version,
        ports,
        component_label="ocs-metrics-exporter",
        error_header="ocs-metrics-exporter HTTPS ports 8443/9443 did not apply",
        context=context,
    )


def openssl_ciphers_for_tls_profile(iana_ciphers):
    """Map TLSProfile IANA cipher names to OpenSSL names used by scantls."""
    mapped = []
    for name in iana_ciphers:
        openssl_name = TLS_PROFILE_IANA_TO_OPENSSL_CIPHER.get(name, name)
        mapped.append(openssl_name)
    return mapped


def openssl_groups_for_tls_profile(iana_groups):
    """Map TLSProfile IANA group names to OpenSSL names used by scantls."""
    mapped = []
    for name in iana_groups:
        openssl_name = TLS_PROFILE_IANA_TO_OPENSSL_GROUP.get(name, name)
        mapped.append(openssl_name)
    return mapped


def _list_csi_snapshot_metadata_pod_items(namespace):
    """
    Return pod dicts for csi-snapshot-metadata (dedicated Deployment or CSI
    sidecar). Matches label ``app=csi-snapshot-metadata``, pod name, or
    container name containing ``snapshot-metadata``.
    """
    from ocs_ci.ocs.resources.pod import get_pods_having_label

    needle = constants.CSI_SNAPSHOT_METADATA_NAME_SUBSTRING
    seen = set()
    matched = []

    def _add(pod):
        name = pod.get("metadata", {}).get("name")
        if name and name not in seen:
            seen.add(name)
            matched.append(pod)

    for pod in get_pods_having_label(constants.CSI_SNAPSHOT_METADATA, namespace) or []:
        _add(pod)

    ocp_pods = OCP(kind=constants.POD, namespace=namespace)
    for pod in ocp_pods.get().get("items") or []:
        name = pod.get("metadata", {}).get("name", "")
        if needle in name:
            _add(pod)
            continue
        for container in pod.get("spec", {}).get("containers", []):
            if needle in (container.get("name") or ""):
                _add(pod)
                break
    return matched


def csi_snapshot_metadata_is_deployed(namespace):
    """Return True if a csi-snapshot-metadata pod or sidecar exists."""
    return bool(_list_csi_snapshot_metadata_pod_items(namespace))


def wait_for_csi_snapshot_metadata_ready(
    namespace, timeout=600, sleep=15, previous_fingerprints=None
):
    """
    Wait until csi-snapshot-metadata sidecars are Running and ready.

    When ``previous_fingerprints`` is set, wait until those sidecars have
    rolled (new UID/start/restart) so scantls does not hit pre-TLSProfile
    listeners that still offer both TLS 1.2 and 1.3.
    """

    def _list_pods():
        return _list_csi_snapshot_metadata_pod_items(namespace)

    _wait_for_tls_workload_ready(
        _list_pods,
        previous_fingerprints,
        container_name_substr=constants.CSI_SNAPSHOT_METADATA_NAME_SUBSTRING,
        workload_name="csi-snapshot-metadata",
        timeout=timeout,
        sleep=sleep,
    )


def assert_csi_snapshot_metadata_https_tls_applied(
    results,
    api_tls_version,
    ports=None,
    context="",
    expected_ciphers=None,
    expected_groups=None,
):
    """
    Fail unless csi-snapshot-metadata port 50051 is serving HTTPS with the
    TLSProfile version, and scantls reports only the configured ciphers/groups.

    Cipher/group names from the TLSProfile spec are mapped to OpenSSL names
    used by scantls.

    ``TLSv1.3`` is exact (no tls1.2). ``TLSv1.2`` is a minimum: the sidecar
    sets ``--tls-min-version=VersionTLS12`` with no max, so tls1.3 may still
    be offered.

    Args:
        results: Return value of :func:`scan_cluster` for component
            ``csi-snapshot-metadata``.
        api_tls_version: ``TLSv1.2`` or ``TLSv1.3``.
        ports: HTTPS/gRPC ports to require (default port 50051).
        context: Short string appended to failure messages.
        expected_ciphers: TLSProfile IANA cipher list (defaults from version).
        expected_groups: TLSProfile IANA group list (defaults from version).
    """
    ports = tuple(ports) if ports is not None else CSI_SNAPSHOT_METADATA_HTTPS_PORTS
    _assert_https_tls_applied(
        results,
        api_tls_version,
        ports,
        component_label="csi-snapshot-metadata",
        error_header="csi-snapshot-metadata HTTPS port 50051 did not apply",
        context=context,
        expected_ciphers=expected_ciphers,
        expected_groups=expected_groups,
        verify_ciphers_groups=True,
        exact_version=api_tls_version != "TLSv1.2",
    )


def ocs_client_operator_is_deployed(namespace):
    """Return True if at least one ocs-client-operator pod exists in namespace."""
    from ocs_ci.ocs.resources.pod import get_pods_having_label

    return bool(
        get_pods_having_label(constants.OCS_CLIENT_OPERATOR_LABEL, namespace=namespace)
    )


def wait_for_ocs_client_operator_ready(
    namespace, timeout=600, sleep=15, previous_fingerprints=None
):
    """
    Wait until ocs-client-operator manager is Running and ready.

    TLSProfile changes often restart the manager in place (same pod UID,
    new startedAt). When ``previous_fingerprints`` is set, wait until that
    restart is visible.
    """
    from ocs_ci.ocs.resources.pod import get_pods_having_label

    def _list_pods():
        return (
            get_pods_having_label(
                constants.OCS_CLIENT_OPERATOR_LABEL,
                namespace=namespace,
                statuses=[constants.STATUS_RUNNING],
            )
            or []
        )

    _wait_for_tls_workload_ready(
        _list_pods,
        previous_fingerprints,
        container_name_substr="manager",
        workload_name="ocs-client-operator",
        timeout=timeout,
        sleep=sleep,
    )


def assert_ocs_client_operator_https_tls_applied(
    results,
    api_tls_version,
    ports=None,
    context="",
    expected_ciphers=None,
    expected_groups=None,
):
    """
    Fail unless ocs-client-operator webhook and metrics HTTPS ports are
    serving TLS with the TLSProfile version, and scantls reports only the
    configured ciphers/groups.

    TLSProfile version is exact (min == max). Cipher/group names from the
    TLSProfile spec are mapped to OpenSSL names used by scantls.

    Args:
        results: Return value of :func:`scan_cluster` for component
            ``ocs-client-operator``.
        api_tls_version: ``TLSv1.2`` or ``TLSv1.3``.
        ports: HTTPS ports to require (default metrics 8443 and webhook 7443).
        context: Short string appended to failure messages.
        expected_ciphers: TLSProfile IANA cipher list (defaults from version).
        expected_groups: TLSProfile IANA group list (defaults from version).
    """
    ports = tuple(ports) if ports is not None else CLIENT_OPERATOR_HTTPS_PORTS
    _assert_https_tls_applied(
        results,
        api_tls_version,
        ports,
        component_label="ocs-client-operator",
        error_header=("ocs-client-operator webhook/metrics HTTPS ports did not apply"),
        context=context,
        expected_ciphers=expected_ciphers,
        expected_groups=expected_groups,
        port_roles=CLIENT_OPERATOR_HTTPS_PORT_ROLES,
        verify_ciphers_groups=True,
        success_log_includes_role=True,
    )
