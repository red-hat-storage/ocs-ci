"""
Verify nodes can still reach the kube-apiserver ClusterIP after chaos.

Network chaos (``NetworkFaults`` netem and Krkn node-network Jobs) can leave
OVN-Kubernetes in a state where the *underlay* is healthy but ClusterIP
service translation is broken on some nodes: ``ovnkube-node`` reprograms the
``br-ex`` ``table=2`` service flow with a next-hop MAC that matches no
interface, so every SYN to ``172.30.0.1:443`` is dropped in the OVS datapath.

That failure is invisible to a Ceph health check -- MONs, OSDs and MDS stay
healthy -- but every pod that talks to the apiserver on those nodes
CrashLoopBackOffs with ``dial tcp 172.30.0.1:443: i/o timeout``.

This module asserts recovery; it deliberately does **not** repair anything.
Restarting ``ovnkube-node`` here would convert a product bug into a silent
teardown step and every future run would pass.

CLI::

    python -m ocs_ci.resiliency.service_connectivity \\
        --kubeconfig /path/to/kubeconfig

    python -m ocs_ci.resiliency.service_connectivity \\
        --cluster-path /path/to/cluster --tries 1
"""

import argparse
import logging
import os
import re
import subprocess
import sys
import time

from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import ClusterIPServiceUnreachableError, CommandFailed

log = logging.getLogger(__name__)

# Fallback when the ``kubernetes`` Service cannot be read.
DEFAULT_APISERVER_CLUSTER_IP = "172.30.0.1"
APISERVER_CLUSTER_PORT = 443

PER_NODE_TIMEOUT = 120
CURL_TIMEOUT = 10

# Recovery after faults are removed is not instant; retry before failing.
DEFAULT_TRIES = 6
DEFAULT_DELAY = 20

_HTTP_CODE_RE = re.compile(r"APISERVER_CODE=(\d+)")
_BREX_MAC_RE = re.compile(r"link/ether\s+([0-9a-f:]{17})")
_FLOW_MAC_RE = re.compile(r"mod_dl_dst:([0-9a-f:]{17})")


def build_probe_command(cluster_ip, port=APISERVER_CLUSTER_PORT, timeout=CURL_TIMEOUT):
    """
    Build the single-shot host probe run under ``oc debug``.

    ``exec_oc_debug_cmd`` wraps the command in double quotes, so the probe
    must avoid ``$vars``, ``$(...)``, backticks and embedded double quotes.

    Args:
        cluster_ip (str): ClusterIP of the ``kubernetes`` Service.
        port (int): Service port.
        timeout (int): Per-curl timeout in seconds.

    Returns:
        str: Shell command emitting ``APISERVER_CODE=``, ``link/ether`` and
            ``mod_dl_dst:`` markers.
    """
    url = f"https://{cluster_ip}:{port}/healthz"
    return (
        f"curl -sk -m {timeout} -o /dev/null "
        f"-w 'APISERVER_CODE=%{{http_code}}' {url}; "
        "echo; "
        "ip link show br-ex 2>/dev/null | grep -o 'link/ether [0-9a-f:]*' "
        "| head -1; "
        "ovs-ofctl dump-flows br-ex table=2 2>/dev/null "
        f"| grep {cluster_ip} | grep -o 'mod_dl_dst:[0-9a-f:]*' | head -1"
    )


def parse_probe_output(probe_output):
    """
    Parse probe stdout into its three signals.

    Args:
        probe_output (str): Raw stdout from :func:`build_probe_command`.

    Returns:
        dict: ``http_code`` (str or None), ``brex_mac`` (str or None),
            ``flow_mac`` (str or None).
    """
    result = {"http_code": None, "brex_mac": None, "flow_mac": None}
    if not probe_output:
        return result
    text = probe_output.lower()
    code = _HTTP_CODE_RE.search(probe_output)
    if code:
        result["http_code"] = code.group(1)
    brex = _BREX_MAC_RE.search(text)
    if brex:
        result["brex_mac"] = brex.group(1)
    flow = _FLOW_MAC_RE.search(text)
    if flow:
        result["flow_mac"] = flow.group(1)
    return result


def evaluate_probe(node_name, parsed):
    """
    Turn parsed probe signals into a failure dict, or ``None`` when healthy.

    A missing ``flow_mac`` is not a failure: ``ovs-ofctl`` is absent on some
    platforms and the curl result is the authoritative signal.

    Args:
        node_name (str): Node the probe ran on.
        parsed (dict): Output of :func:`parse_probe_output`.

    Returns:
        dict: ``{"node", "reason"}`` when unhealthy, otherwise ``None``.
    """
    code = parsed.get("http_code")
    brex_mac = parsed.get("brex_mac")
    flow_mac = parsed.get("flow_mac")

    if code is None:
        return {
            "node": node_name,
            "reason": "probe produced no APISERVER_CODE (oc debug failed?)",
        }
    if code == "000":
        reason = (
            f"ClusterIP {DEFAULT_APISERVER_CLUSTER_IP}:{APISERVER_CLUSTER_PORT} "
            f"timed out (curl code 000)"
        )
        if brex_mac and flow_mac and brex_mac != flow_mac:
            reason += (
                f"; br-ex table=2 rewrites service traffic to {flow_mac} "
                f"but br-ex MAC is {brex_mac} -- OVN programmed a bogus "
                f"next-hop MAC"
            )
        return {"node": node_name, "reason": reason}
    if not code.startswith("2"):
        return {
            "node": node_name,
            "reason": f"apiserver /healthz returned HTTP {code}",
        }
    if brex_mac and flow_mac and brex_mac != flow_mac:
        return {
            "node": node_name,
            "reason": (
                f"apiserver reachable but br-ex table=2 next-hop MAC "
                f"{flow_mac} does not match br-ex MAC {brex_mac}"
            ),
        }
    return None


def get_apiserver_cluster_ip(ocp_obj=None):
    """Return the ClusterIP of the ``kubernetes`` Service in ``default``."""
    try:
        svc = ocp.OCP(
            kind=constants.SERVICE,
            namespace="default",
            resource_name="kubernetes",
            cluster_kubeconfig=getattr(ocp_obj, "cluster_kubeconfig", "") or "",
        )
        cluster_ip = (svc.get().get("spec") or {}).get("clusterIP")
        if cluster_ip:
            return cluster_ip
    except Exception:
        log.exception("Service connectivity: could not read kubernetes Service IP")
    log.warning(
        "Service connectivity: falling back to %s", DEFAULT_APISERVER_CLUSTER_IP
    )
    return DEFAULT_APISERVER_CLUSTER_IP


def list_cluster_node_names(ocp_obj=None):
    """Return names of all cluster nodes (workers, masters, infra)."""
    node_api = ocp.OCP(
        kind=constants.NODE,
        cluster_kubeconfig=getattr(ocp_obj, "cluster_kubeconfig", "") or "",
    )
    data = node_api.get()
    return [
        item.get("metadata", {}).get("name")
        for item in (data.get("items") or [])
        if item.get("metadata", {}).get("name")
    ]


def probe_node(ocp_obj, node_name, cluster_ip, timeout=PER_NODE_TIMEOUT):
    """
    Probe one node; return a failure dict or ``None`` when healthy.

    Never raises: an unreachable node is reported as a failure so one bad
    node cannot abort the sweep.
    """
    cmd = build_probe_command(cluster_ip)
    try:
        output = ocp_obj.exec_oc_debug_cmd(
            node=node_name, cmd_list=[cmd], timeout=timeout
        )
    except (CommandFailed, subprocess.TimeoutExpired) as ex:
        return {
            "node": node_name,
            "reason": f"could not run connectivity probe: {ex}",
        }
    parsed = parse_probe_output(output)
    log.debug("Service connectivity: %s probe -> %s", node_name, parsed)
    return evaluate_probe(node_name, parsed)


def inspect_cluster_service_connectivity(
    ocp_obj=None,
    node_names=None,
    cluster_ip=None,
    per_node_timeout=PER_NODE_TIMEOUT,
):
    """
    Probe every node; never abort the loop because one node failed.

    Returns:
        list: Failure dicts (empty when every node can reach the ClusterIP).
    """
    ocp_obj = ocp_obj or ocp.OCP()
    if node_names is None:
        node_names = list_cluster_node_names(ocp_obj)
    cluster_ip = cluster_ip or get_apiserver_cluster_ip(ocp_obj)
    failures = []
    for node_name in node_names:
        try:
            failure = probe_node(
                ocp_obj, node_name, cluster_ip, timeout=per_node_timeout
            )
        except Exception:
            log.exception("Service connectivity: probe crashed on %s", node_name)
            failure = {"node": node_name, "reason": "probe raised unexpectedly"}
        if failure:
            failures.append(failure)
    return failures


def assert_cluster_service_connectivity(
    ocp_obj=None,
    node_names=None,
    tries=DEFAULT_TRIES,
    delay=DEFAULT_DELAY,
    per_node_timeout=PER_NODE_TIMEOUT,
    context="service connectivity",
):
    """
    Raise :class:`ClusterIPServiceUnreachableError` if any node stays broken.

    Tolerates slow reconvergence by retrying, but fails on no recovery --
    that is the whole point. Only the nodes that failed the previous attempt
    are re-probed, so a healthy cluster costs one pass.

    Args:
        ocp_obj: :class:`ocs_ci.ocs.ocp.OCP` instance (created if omitted).
        node_names (list): Node names; default is all cluster nodes.
        tries (int): Probe attempts before failing.
        delay (int): Seconds between attempts.
        per_node_timeout (int): Seconds for each ``oc debug``.
        context (str): Included in the raised error.

    Returns:
        list: Empty list when every node recovered.

    Raises:
        ClusterIPServiceUnreachableError: When nodes remain unreachable.
    """
    ocp_obj = ocp_obj or ocp.OCP()
    if node_names is None:
        node_names = list_cluster_node_names(ocp_obj)
    cluster_ip = get_apiserver_cluster_ip(ocp_obj)
    log.info(
        "Service connectivity: verifying %s:%s reachable from %s",
        cluster_ip,
        APISERVER_CLUSTER_PORT,
        node_names,
    )

    pending = list(node_names)
    failures = []
    for attempt in range(1, tries + 1):
        failures = inspect_cluster_service_connectivity(
            ocp_obj=ocp_obj,
            node_names=pending,
            cluster_ip=cluster_ip,
            per_node_timeout=per_node_timeout,
        )
        if not failures:
            log.info(
                "Service connectivity: all nodes reach %s (attempt %s/%s)",
                cluster_ip,
                attempt,
                tries,
            )
            return []
        pending = [item["node"] for item in failures]
        if attempt < tries:
            log.warning(
                "Service connectivity: %s still unreachable (attempt %s/%s); "
                "retrying in %ss",
                pending,
                attempt,
                tries,
                delay,
            )
            time.sleep(delay)

    raise ClusterIPServiceUnreachableError(failures, context=context)


def _configure_access(kubeconfig=None, cluster_path=None):
    from ocs_ci.framework import config

    if cluster_path:
        cluster_path = os.path.expanduser(cluster_path)
        config.ENV_DATA["cluster_path"] = cluster_path
    if kubeconfig:
        kubeconfig = os.path.expanduser(kubeconfig)
        os.environ["KUBECONFIG"] = kubeconfig
        config.RUN["kubeconfig"] = kubeconfig
        config.RUN["custom_kubeconfig_location"] = kubeconfig
        # ocp.OCP.exec_oc_cmd reads ENV_DATA["cluster_path"] unconditionally,
        # so --kubeconfig on its own must still populate it.
        config.ENV_DATA.setdefault("cluster_path", os.path.dirname(kubeconfig))
        return kubeconfig
    if cluster_path:
        default_kc = os.path.join(
            cluster_path, config.RUN.get("kubeconfig_location", "auth/kubeconfig")
        )
        if os.path.exists(default_kc):
            os.environ["KUBECONFIG"] = default_kc
            config.RUN["kubeconfig"] = default_kc
            return default_kc
    return os.environ.get("KUBECONFIG")


def main(argv=None):
    """CLI entry point to verify ClusterIP reachability from every node."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    from ocs_ci.framework.logger_factory import set_log_record_factory

    set_log_record_factory()
    parser = argparse.ArgumentParser(
        description=(
            "Verify every OpenShift node can reach the kube-apiserver "
            "ClusterIP. Detects OVN-Kubernetes blackholing ClusterIP traffic "
            "after network chaos, which a Ceph health check cannot see. "
            "Reports only; never repairs."
        )
    )
    parser.add_argument(
        "--kubeconfig",
        help="Path to kubeconfig (default: $KUBECONFIG or <cluster-path>/auth/kubeconfig)",
    )
    parser.add_argument(
        "--cluster-path",
        help="ocs-ci cluster directory (used to locate kubeconfig if --kubeconfig omitted)",
    )
    parser.add_argument(
        "--tries",
        type=int,
        default=DEFAULT_TRIES,
        help=f"Probe attempts before failing (default {DEFAULT_TRIES})",
    )
    parser.add_argument(
        "--delay",
        type=int,
        default=DEFAULT_DELAY,
        help=f"Seconds between attempts (default {DEFAULT_DELAY})",
    )
    parser.add_argument(
        "--per-node-timeout",
        type=int,
        default=PER_NODE_TIMEOUT,
        help=f"Seconds for each oc debug (default {PER_NODE_TIMEOUT})",
    )
    args = parser.parse_args(argv)
    kc = _configure_access(kubeconfig=args.kubeconfig, cluster_path=args.cluster_path)
    if not kc or not os.path.exists(os.path.expanduser(kc)):
        parser.error("kubeconfig not found; pass --kubeconfig or --cluster-path")
    log.info("Using kubeconfig %s", kc)
    try:
        assert_cluster_service_connectivity(
            ocp_obj=ocp.OCP(),
            tries=args.tries,
            delay=args.delay,
            per_node_timeout=args.per_node_timeout,
            context="cli check",
        )
        log.info("All nodes can reach the kube-apiserver ClusterIP")
        return 0
    except ClusterIPServiceUnreachableError as ex:
        log.error("%s", ex)
        return 1


if __name__ == "__main__":
    sys.exit(main())
