"""
Helper for TLSProfile custom resources (ocs.openshift.io/v1) and in-cluster TLS
scanning via :func:`scan_cluster`. The scan logic lives in
``scripts/bash/tls_scan_endpoints.sh`` (loaded at runtime).

References (DF 4.22+): ``TLSProfile`` centralizes TLS version, ciphers, and groups
for NooBaa and RGW; CR name ``ocs-tls-profile`` in the operator namespace;
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
TLS_PROFILE_SELECTOR_NOOBAA_DOMAIN = "noobaa.io"
TLS_PROFILE_SELECTOR_RGW_DOMAIN = "ceph.rook.io"

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

    Always includes ocs-operator and rook-ceph-operator; adds NooBaa / RGW pods when
    those paths are under test.
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

    names = set()
    for label in selectors:
        items = get_pods_having_label(label, namespace) or []
        for item in items:
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
        component (str): Test parametrization key: ``all``, ``noobaa``, or ``rgw``.
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

TLS_SCANNER_IMAGE = "ghcr.io/leelavg/scantls@sha256:f9b6547c7285b28539b23d2135108b57ac8bbac0c51a82c5a274a2674a6eff70"
TLS_SCANNER_NAMESPACE = "scantls-system"
# Seconds between ``oc get pod … jsonpath={.status.phase}`` samples (scanner pod startup).
TLS_SCAN_POD_PHASE_POLL_SLEEP = 2

TLS_SCAN_COMPONENT_SELECTORS = {
    "noobaa": {"label": "app=noobaa"},
    "rgw": {"label": "app=rook-ceph-rgw"},
    "ceph": {"label": "rook_cluster=openshift-storage"},
    "csi": {"name_filter": "csi"},
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


def _tls_scan_discover_endpoints(kubeconfig, namespaces, component="all"):
    selector = TLS_SCAN_COMPONENT_SELECTORS.get(component, {})
    label = selector.get("label")
    name_filter = selector.get("name_filter")

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
            if name_filter and name_filter not in pod_name:
                continue
            for container in pod["spec"]["containers"]:
                c_name = container["name"]
                cmd_parts = container.get("command", []) + container.get("args", [])
                process = ""
                if cmd_parts:
                    process = cmd_parts[0].rsplit("/", 1)[-1][:15]
                if not process:
                    process = (
                        container.get("image", "").split("/")[-1].split(":")[0][:15]
                    )
                for port_info in container.get("ports", []):
                    port = port_info.get("containerPort")
                    if port:
                        endpoints.append(
                            {
                                "pod_namespace": pod_ns,
                                "pod_name": pod_name,
                                "pod_ip": pod_ip,
                                "container_name": c_name,
                                "port": str(port),
                                "process": process,
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

    Args:
        component: ``noobaa``, ``rgw``, ``ceph``, ``csi``, or ``all``.
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

    try:
        _tls_scan_setup_namespace(kubeconfig)
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

    def apply_rules(self, rules, do_reload=True, metadata=None, spec=None):
        """
        Replace spec.rules, creating the TLSProfile if it is absent.

        When recreating a missing CR, ``metadata`` and ``spec`` restore
        user-managed fields (labels, annotations, and non-rules spec) from a
        prior snapshot. ``rules`` always wins over ``spec["rules"]``.

        Args:
            rules (list): spec.rules value to apply.
            do_reload (bool): Reload OCS object after create when the CR is missing.
            metadata (dict): Optional user-managed metadata to restore on create
                or merge-patch (labels, annotations, and other non-system keys).
            spec (dict): Optional full spec to restore; ``rules`` overwrites
                ``spec["rules"]``.
        """
        rules = copy.deepcopy(list(rules))
        spec_body = copy.deepcopy(spec) if spec is not None else {}
        spec_body["rules"] = rules
        meta = {"name": self.name, "namespace": self.namespace}
        if metadata:
            extra_meta = copy.deepcopy(metadata)
            extra_meta.pop("name", None)
            extra_meta.pop("namespace", None)
            meta.update(extra_meta)
            meta["name"] = self.name
            meta["namespace"] = self.namespace
        if not self.is_tls_profile_available(silent=True):
            tls_resource = {
                "apiVersion": self.API_VERSION,
                "kind": self.KIND,
                "metadata": meta,
                "spec": spec_body,
            }
            log.info(f"Creating {self.KIND} {self.name} in namespace {self.namespace}")
            OCS(**tls_resource).create(do_reload=do_reload)
            return
        patch = {"spec": spec_body}
        if metadata:
            patch_meta = copy.deepcopy(metadata)
            patch_meta.pop("name", None)
            patch_meta.pop("namespace", None)
            if patch_meta:
                patch["metadata"] = patch_meta
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


_TLS_PROFILE_SYSTEM_METADATA_KEYS = (
    "uid",
    "resourceVersion",
    "generation",
    "creationTimestamp",
    "deletionTimestamp",
    "deletionGracePeriodSeconds",
    "managedFields",
    "selfLink",
    "ownerReferences",
)


def _tlsprofile_user_managed_fields(data):
    """
    Return user-managed metadata and spec from a TLSProfile object.

    Drops controller/system metadata so a later create/patch can restore labels,
    annotations, and the full spec without resourceVersion/uid conflicts.
    """
    metadata = copy.deepcopy(data.get("metadata") or {})
    for key in _TLS_PROFILE_SYSTEM_METADATA_KEYS:
        metadata.pop(key, None)
    return {
        "metadata": metadata,
        "spec": copy.deepcopy(data.get("spec") or {}),
    }


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


# ocs-operator sets this on every successful StorageCluster reconcile.
# https://github.com/red-hat-storage/ocs-operator/blob/main/api/v1/storagecluster_types.go
_RECONCILE_COMPLETE_CONDITION = "ReconcileComplete"


def _optional_int(value):
    """Return ``value`` as int, or None when it is missing or not numeric."""
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _generation_changed(before, after):
    """
    Return True when metadata.generation moved because of this patch.

    A missing baseline (the object was created by the patch) counts as a change.
    A missing current generation does not.
    """
    after_generation = _optional_int(after)
    if after_generation is None:
        return False
    before_generation = _optional_int(before)
    if before_generation is None:
        return True
    return after_generation != before_generation


def _observed_generation_current(generation, observed):
    """Return True when status.observedGeneration has caught metadata.generation."""
    generation_i = _optional_int(generation)
    observed_i = _optional_int(observed)
    if generation_i is None or observed_i is None:
        return False
    return observed_i >= generation_i


def _reconcile_complete_condition(status):
    """Return the ReconcileComplete heartbeat fields, or None when absent."""
    for condition in (status or {}).get("conditions") or []:
        if condition.get("type") == _RECONCILE_COMPLETE_CONDITION:
            return {
                "status": condition.get("status"),
                "lastHeartbeatTime": condition.get("lastHeartbeatTime"),
                "lastTransitionTime": condition.get("lastTransitionTime"),
            }
    return None


def _reconcile_complete_updated(before, current):
    """
    Return True when ReconcileComplete was written again after ``before``.

    openshift/custom-resource-status documents ``lastHeartbeatTime`` as the
    last time that condition was updated. ``SetStatusCondition`` refreshes it
    on every write, including when status, reason, and message stay the same.
    ocs-operator ``SetCompleteCondition`` sets ReconcileComplete=True at the
    end of a successful reconcile, and ``Reconcile`` persists that status.

    ``lastTransitionTime`` moves only when the condition status changes. An
    already Ready StorageCluster stays Ready through a TLSProfile reconcile, so
    phase and lastTransitionTime are not a reliable signal by themselves.
    """
    if not current:
        return False
    before = before or {}
    heartbeat = current.get("lastHeartbeatTime")
    if heartbeat and heartbeat != before.get("lastHeartbeatTime"):
        return True
    transition = current.get("lastTransitionTime")
    if transition and transition != before.get("lastTransitionTime"):
        return True
    return False


def _storage_cluster_reconcile_reached(baseline, current):
    """
    Return True when ``current`` shows the reconcile started by this patch.

    Use ``status.observedGeneration`` only when the patch changed that object's
    metadata.generation and the field is present. Neither StorageCluster nor
    TLSProfile publishes observedGeneration today; then require ReconcileComplete
    to be written again. A phase change alone is not success.
    """
    if (
        _generation_changed(
            baseline.get("profile_generation"), current.get("profile_generation")
        )
        and current.get("profile_observed_generation") is not None
    ):
        return _observed_generation_current(
            current.get("profile_generation"),
            current.get("profile_observed_generation"),
        )
    if (
        _generation_changed(
            baseline.get("cluster_generation"), current.get("cluster_generation")
        )
        and current.get("cluster_observed_generation") is not None
    ):
        return _observed_generation_current(
            current.get("cluster_generation"),
            current.get("cluster_observed_generation"),
        )
    return _reconcile_complete_updated(
        baseline.get("reconcile_complete"), current.get("reconcile_complete")
    )


def _tls_profile_blocked_by_fips(fips_value):
    """
    Return True only for boolean True and the string ``\"true\"``.

    Other values, including ``\"false\"``, ``\"True\"``, ``False``, and ``1``,
    keep the previous behavior and do not block TLSProfile apply.
    """
    return fips_value is True or fips_value == "true"


def _read_reconcile_snapshot(storage_cluster, tls_profile):
    """Read StorageCluster and TLSProfile fields used to detect reconcile."""
    storage_cluster.reload_data()
    data = storage_cluster.data or {}
    status = data.get("status") or {}
    if tls_profile.is_tls_profile_available(silent=True):
        profile = tls_profile.get_tls_profile() or {}
        profile_generation = (profile.get("metadata") or {}).get("generation")
        profile_observed = (profile.get("status") or {}).get("observedGeneration")
    else:
        profile_generation = None
        profile_observed = None
    return {
        "cluster_generation": (data.get("metadata") or {}).get("generation"),
        "cluster_observed_generation": status.get("observedGeneration"),
        "phase": status.get("phase"),
        "reconcile_complete": _reconcile_complete_condition(status),
        "profile_generation": profile_generation,
        "profile_observed_generation": profile_observed,
    }


def _capture_storage_cluster_reconcile_baseline(storage_cluster, tls_profile):
    """Snapshot reconcile signals immediately before the TLSProfile patch."""
    storage_cluster_name = config.ENV_DATA["storage_cluster_name"]
    baseline = _read_reconcile_snapshot(storage_cluster, tls_profile)
    log.info(
        f"StorageCluster {storage_cluster_name} reconcile baseline before "
        f"TLSProfile apply: phase={baseline.get('phase')} "
        f"generation={baseline.get('cluster_generation')} "
        f"observedGeneration={baseline.get('cluster_observed_generation')} "
        f"ReconcileComplete={baseline.get('reconcile_complete')} "
        f"tlsProfileGeneration={baseline.get('profile_generation')} "
        f"tlsProfileObservedGeneration={baseline.get('profile_observed_generation')}"
    )
    return baseline


def _wait_for_storage_cluster_reconcile_to_start(
    baseline, tls_profile, storage_cluster, timeout=300, sleep=10
):
    """
    Wait until a reconcile signal captured before the patch moves.

    When the patch changes TLSProfile or StorageCluster generation and that
    object reports ``status.observedGeneration``, wait for observedGeneration
    to catch the new generation. Otherwise wait until the ReconcileComplete
    condition heartbeat or transition differs from ``baseline``.

    ``status.phase`` is logged and is not a success condition. An already
    Ready StorageCluster stays Ready for a TLSProfile reconcile. Timeout is a
    failure.

    Args:
        baseline (dict): Snapshot from
            :func:`_capture_storage_cluster_reconcile_baseline`.
        tls_profile (TLSProfile): Profile that was just applied.
        storage_cluster (StorageCluster): Cluster whose reconcile is being watched.
        timeout (int): Seconds to wait for the signal to move.
        sleep (int): Seconds between reads. Must be smaller than ``timeout``.

    Raises:
        UnexpectedBehaviour: If the reconcile signal does not move in time.
    """
    from ocs_ci.ocs.exceptions import UnexpectedBehaviour

    storage_cluster_name = config.ENV_DATA["storage_cluster_name"]
    log.info(
        f"Waiting up to {timeout}s for StorageCluster {storage_cluster_name} "
        "to reconcile after TLSProfile apply"
    )
    last_summary = {"value": None}

    def _reconcile_started():
        current = _read_reconcile_snapshot(storage_cluster, tls_profile)
        summary = (
            f"phase={current.get('phase')} "
            f"generation={current.get('cluster_generation')} "
            f"observedGeneration={current.get('cluster_observed_generation')} "
            f"ReconcileComplete={current.get('reconcile_complete')} "
            f"tlsProfileGeneration={current.get('profile_generation')} "
            f"tlsProfileObservedGeneration="
            f"{current.get('profile_observed_generation')}"
        )
        if summary != last_summary["value"]:
            log.info(
                f"StorageCluster {storage_cluster_name} reconcile status: {summary}"
            )
            last_summary["value"] = summary
        return _storage_cluster_reconcile_reached(baseline, current)

    try:
        for started in TimeoutSampler(timeout, sleep, _reconcile_started):
            if started:
                break
    except TimeoutExpiredError as exc:
        raise UnexpectedBehaviour(
            f"StorageCluster {storage_cluster_name} did not record a reconcile "
            f"within {timeout}s after TLSProfile apply "
            f"(last status: {last_summary['value']})"
        ) from exc
    log.info(
        f"StorageCluster {storage_cluster_name} reconcile recorded "
        f"({last_summary['value']})"
    )


def apply_tls_profile_after_deployment():
    """
    Apply the configured TLSProfile after a successful ODF deployment.

    No-op unless ``ENV_DATA.enable_tls_profile`` is true. ODF versions older
    than 4.22, and FIPS-enabled deployments (``fips`` is boolean True or
    ``\"true\"``), are skipped. Otherwise load
    ``ENV_DATA.tls_profile_config_path``
    and apply it with :meth:`TLSProfile.apply_rules`. Wait for that version with
    :func:`wait_for_tlsprofile_config_version`. Before the patch, snapshot
    StorageCluster generation, ``status.observedGeneration`` when present, and
    the ReconcileComplete condition. After the patch, wait for observedGeneration
    to catch a generation change, or for ReconcileComplete to be written again.
    Timeout is a failure. Then wait for Ready again with
    :func:`ocs_ci.ocs.resources.storage_cluster.verify_storage_cluster`. After
    reconcile, ``oc get`` must still show the TLSProfile at that version.
    """
    if not config.ENV_DATA.get("enable_tls_profile"):
        return
    if _tls_profile_blocked_by_fips(config.ENV_DATA.get("fips")):
        log.warning(
            "Skipping TLSProfile apply: enable_tls_profile is set, but "
            "TLSProfile is not supported on FIPS-enabled deployments"
        )
        return

    from ocs_ci.ocs.exceptions import (
        CommandFailed,
        ConfigurationError,
        ResourceNotFoundError,
        UnexpectedBehaviour,
    )
    from ocs_ci.ocs.resources.storage_cluster import (
        StorageCluster,
        verify_storage_cluster,
    )
    from ocs_ci.utility import templating, version

    ocs_version = version.get_semantic_ocs_version_from_config()
    if ocs_version < version.VERSION_4_22:
        log.warning(
            "Skipping TLSProfile apply: enable_tls_profile is set, but "
            f"ODF {ocs_version} is older than 4.22"
        )
        return

    config_path = config.ENV_DATA.get("tls_profile_config_path")
    if not config_path:
        raise ConfigurationError(
            "enable_tls_profile is true but ENV_DATA.tls_profile_config_path is not set"
        )
    resolved_path = (
        config_path
        if os.path.isabs(config_path)
        else os.path.join(constants.TOP_DIR, config_path)
    )
    if not os.path.isfile(resolved_path):
        raise ConfigurationError(f"TLSProfile config file not found: {resolved_path}")
    if not tlsprofile_crd_exists():
        raise ConfigurationError(
            "enable_tls_profile is true, but CRD tlsprofiles.ocs.openshift.io "
            "is not installed"
        )

    tls_data = templating.load_yaml(resolved_path) or {}
    if tls_data.get("kind") != TLSProfile.KIND:
        raise ConfigurationError(
            f"TLSProfile config {resolved_path} has kind {tls_data.get('kind')!r}, "
            f"expected {TLSProfile.KIND}"
        )
    managed = _tlsprofile_user_managed_fields(tls_data)
    rules = (managed.get("spec") or {}).get("rules") or []
    if not isinstance(rules, list) or not rules:
        raise ConfigurationError(f"TLSProfile config {resolved_path} has no spec.rules")
    expected_version = (rules[0].get("config") or {}).get("version")
    if not expected_version:
        raise ConfigurationError(
            f"TLSProfile config {resolved_path} has no spec.rules[0].config.version"
        )

    file_namespace = (managed.get("metadata") or {}).get("namespace")
    namespace = config.ENV_DATA.get("cluster_namespace") or file_namespace
    if file_namespace and namespace and file_namespace != namespace:
        log.info(
            f"TLSProfile manifest namespace {file_namespace} does not match "
            f"cluster namespace {namespace}; applying in {namespace}"
        )
    name = (managed.get("metadata") or {}).get("name") or "ocs-tls-profile"
    tls_profile = TLSProfile(name=name, namespace=namespace)
    log.info(
        f"Applying TLSProfile {tls_profile.name} in namespace "
        f"{tls_profile.namespace} from {resolved_path}"
    )
    storage_cluster = StorageCluster(
        resource_name=config.ENV_DATA["storage_cluster_name"],
        namespace=config.ENV_DATA["cluster_namespace"],
    )
    reconcile_baseline = _capture_storage_cluster_reconcile_baseline(
        storage_cluster, tls_profile
    )
    tls_profile.apply_rules(
        rules,
        metadata=managed.get("metadata"),
        spec=managed.get("spec"),
    )
    wait_for_tlsprofile_config_version(tls_profile, expected_version)
    log.info(
        f"TLSProfile {tls_profile.name} is applied with version {expected_version}"
    )
    _wait_for_storage_cluster_reconcile_to_start(
        reconcile_baseline, tls_profile, storage_cluster
    )
    log.info("Waiting for StorageCluster to reach Ready after TLSProfile reconcile")
    verify_storage_cluster()
    log.info(
        f"Verifying TLSProfile {tls_profile.name} in namespace "
        f"{tls_profile.namespace} with oc get"
    )
    try:
        tls_profile.get_tls_profile()
    except CommandFailed as exc:
        raise ResourceNotFoundError(
            f"TLSProfile {tls_profile.name} in namespace {tls_profile.namespace} "
            "is not showing after StorageCluster reconcile"
        ) from exc
    actual_version = tls_profile.get_config_version()
    if actual_version != expected_version:
        raise UnexpectedBehaviour(
            f"TLSProfile {tls_profile.name} get shows version {actual_version!r}, "
            f"expected {expected_version!r}"
        )
    log.info(
        f"TLSProfile {tls_profile.name} get shows version {actual_version} "
        "after StorageCluster reconcile"
    )
