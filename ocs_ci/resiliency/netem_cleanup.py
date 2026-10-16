"""
Idempotent cleanup of leftover ``tc netem`` qdiscs on OpenShift nodes.

Chaos injection (``NetworkFaults`` and Krkn node-network Jobs) can leave
``delay`` / ``loss`` netem on host interfaces when teardown is skipped.
This module does not depend on state captured at injection time.

CLI::

    python -m ocs_ci.resiliency.netem_cleanup \\
        --kubeconfig /path/to/kubeconfig

    python -m ocs_ci.resiliency.netem_cleanup \\
        --cluster-path /path/to/cluster --verify-only
"""

import argparse
import logging
import os
import re
import subprocess
import sys

from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import CommandFailed, NetemResidueError, TimeoutExpiredError

log = logging.getLogger(__name__)

# Always considered even when ``ip link`` / interface discovery fails.
DEFAULT_NETEM_INTERFACES = (
    "ens3",
    "ovs-system",
    "br-ex",
    "br-int",
    "ovn-k8s-mp0",
    "genev_sys_6081",
)

PER_NODE_TIMEOUT = 60
_NETEM_DEV_RE = re.compile(r"qdisc\s+netem\s+\S+\s+dev\s+(\S+)")
_NETEM_LINE_RE = re.compile(r"qdisc\s+netem\s+")
_IFACE_MARK_RE = re.compile(r"^NETEM_IFACE=(\S+)")


def parse_netem_qdiscs(tc_qdisc_show_output, default_iface=None):
    """
    Parse ``tc qdisc show`` text into residue dicts.

    Full ``tc qdisc show`` lines include ``dev <iface>``. Per-iface
    ``tc qdisc show dev X`` omits that token; pass ``default_iface`` or
    ``NETEM_IFACE=<name>`` marker lines so those still map to a device.

    Args:
        tc_qdisc_show_output (str): Raw ``tc qdisc show`` output.
        default_iface (str): Iface to use when a netem line has no ``dev``.

    Returns:
        list: Dicts with keys ``iface`` and ``qdisc``.
    """
    residue = []
    if not tc_qdisc_show_output:
        return residue
    current_iface = default_iface
    for line in tc_qdisc_show_output.splitlines():
        stripped = line.strip()
        mark = _IFACE_MARK_RE.match(stripped)
        if mark:
            current_iface = mark.group(1)
            continue
        match = _NETEM_DEV_RE.search(stripped)
        if match:
            iface = match.group(1).split("@", 1)[0]
            residue.append({"iface": iface, "qdisc": stripped})
            continue
        if current_iface and _NETEM_LINE_RE.search(stripped):
            residue.append({"iface": current_iface, "qdisc": stripped})
    return residue


def _debug(ocp_obj, node_name, cmd, timeout):
    return ocp_obj.exec_oc_debug_cmd(node=node_name, cmd_list=[cmd], timeout=timeout)


def _probe_unreachable(ex):
    """Return True when oc debug did not reach the node."""
    if isinstance(ex, (subprocess.TimeoutExpired, TimeoutExpiredError)):
        return True
    message = str(ex).lower()
    return "timed out" in message or "timeout expired" in message


def _unreachable_item(node_name):
    return {
        "node": node_name,
        "iface": "<unreachable>",
        "qdisc": "oc debug timed out or failed; netem status unknown",
    }


def _only_unreachable(residue):
    return bool(residue) and all(
        item.get("iface") == "<unreachable>" for item in residue
    )


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


def inspect_node_netem(ocp_obj, node_name, timeout=PER_NODE_TIMEOUT):
    """
    Return netem residue on one node.

    Each item is ``{"node", "iface", "qdisc"}``. On total inspect failure a
    single ``iface="<unreachable>"`` item is returned.
    """
    try:
        output = _debug(ocp_obj, node_name, "tc qdisc show", timeout)
        items = parse_netem_qdiscs(output)
        return [
            {"node": node_name, "iface": item["iface"], "qdisc": item["qdisc"]}
            for item in items
        ]
    except (CommandFailed, subprocess.TimeoutExpired, TimeoutExpiredError) as ex:
        if _probe_unreachable(ex):
            log.warning(
                "Netem cleanup: %s unreachable (%s); not probing further",
                node_name,
                ex,
            )
            return [_unreachable_item(node_name)]
        log.warning(
            "Netem cleanup: tc qdisc show failed on %s (%s); trying per-iface",
            node_name,
            ex,
        )
    residue = []
    inspected = 0
    for iface in DEFAULT_NETEM_INTERFACES:
        try:
            output = _debug(ocp_obj, node_name, f"tc qdisc show dev {iface}", timeout)
            inspected += 1
        except (CommandFailed, subprocess.TimeoutExpired, TimeoutExpiredError) as ex:
            if _probe_unreachable(ex):
                log.warning(
                    "Netem cleanup: %s unreachable while inspecting %s (%s); "
                    "not probing further",
                    node_name,
                    iface,
                    ex,
                )
                return [_unreachable_item(node_name)]
            log.warning("Netem cleanup: cannot inspect %s/%s: %s", node_name, iface, ex)
            continue
        for item in parse_netem_qdiscs(output, default_iface=iface):
            residue.append(
                {
                    "node": node_name,
                    "iface": item["iface"],
                    "qdisc": item["qdisc"],
                }
            )
    if residue:
        return residue
    if inspected == len(DEFAULT_NETEM_INTERFACES):
        return []
    return [_unreachable_item(node_name)]


def _delete_root_netem(ocp_obj, node_name, iface, timeout):
    cmd = f"tc qdisc del dev {iface} root"
    log.info("Netem cleanup: deleting root qdisc on %s/%s", node_name, iface)
    try:
        _debug(ocp_obj, node_name, cmd, timeout)
    except (CommandFailed, subprocess.TimeoutExpired) as ex:
        log.warning("Netem cleanup: delete failed on %s/%s: %s", node_name, iface, ex)


def _conditional_delete_default_ifaces(ocp_obj, node_name, timeout):
    """
    One ``oc debug``: delete root qdisc only if that iface currently has netem.

    Used when inspect could not confirm status. Explicit iface names (no
    shell ``$vars``) so ``exec_oc_debug_cmd`` double-quoting is safe.
    """
    parts = [
        (
            f"tc qdisc show dev {iface} | grep -q netem && "
            f"tc qdisc del dev {iface} root"
        )
        for iface in DEFAULT_NETEM_INTERFACES
    ]
    log.info("Netem cleanup: conditional delete of default ifaces on %s", node_name)
    try:
        _debug(ocp_obj, node_name, "; ".join(parts), timeout)
    except (CommandFailed, subprocess.TimeoutExpired) as ex:
        log.warning(
            "Netem cleanup: conditional default-iface delete failed on %s: %s",
            node_name,
            ex,
        )


def sweep_node_netem(ocp_obj, node_name, timeout=PER_NODE_TIMEOUT):
    """
    Delete any root netem qdisc found on ``node_name``.

    Returns:
        list: Inspect residue. An unreachable node is not probed again.
    """
    residue = inspect_node_netem(ocp_obj, node_name, timeout=timeout)
    if _only_unreachable(residue):
        log.warning(
            "Netem cleanup: %s unreachable; skipping further oc debug", node_name
        )
        return residue
    ifaces = {item["iface"] for item in residue if item["iface"] != "<unreachable>"}
    for iface in ifaces:
        _delete_root_netem(ocp_obj, node_name, iface, timeout)
    if residue and not ifaces:
        _conditional_delete_default_ifaces(ocp_obj, node_name, timeout)
    return residue


def inspect_cluster_netem(
    ocp_obj=None, node_names=None, per_node_timeout=PER_NODE_TIMEOUT
):
    """Inspect every node; never abort the loop because one node failed."""
    ocp_obj = ocp_obj or ocp.OCP()
    if node_names is None:
        node_names = list_cluster_node_names(ocp_obj)
    residue = []
    for node_name in node_names:
        try:
            residue.extend(
                inspect_node_netem(ocp_obj, node_name, timeout=per_node_timeout)
            )
        except Exception:
            log.exception("Netem cleanup: inspect crashed on %s", node_name)
            residue.append(
                {
                    "node": node_name,
                    "iface": "<unreachable>",
                    "qdisc": "inspect raised unexpectedly",
                }
            )
    return residue


def assert_cluster_free_of_netem(
    ocp_obj=None,
    node_names=None,
    per_node_timeout=PER_NODE_TIMEOUT,
    context="pre-flight",
):
    """
    Raise :class:`NetemResidueError` if any node already has netem.

    Does not delete anything. Use this as a gate before injection so a
    leftover from a previous run is not stacked with more netem.
    """
    residue = inspect_cluster_netem(
        ocp_obj=ocp_obj,
        node_names=node_names,
        per_node_timeout=per_node_timeout,
    )
    if residue:
        raise NetemResidueError(residue, context=context)
    log.info("Netem pre-flight: no leftover netem on %s", node_names or "all nodes")


def sweep_cluster_netem(
    ocp_obj=None,
    node_names=None,
    fail_on_residue=True,
    per_node_timeout=PER_NODE_TIMEOUT,
    context="netem sweep",
):
    """
    Delete leftover netem on every node, then re-inspect.

    Args:
        ocp_obj: :class:`ocs_ci.ocs.ocp.OCP` instance (created if omitted).
        node_names (list): Node names; default is all cluster nodes.
        fail_on_residue (bool): Raise if netem remains after the sweep.
        per_node_timeout (int): Seconds for each ``oc debug``.
        context (str): Included in :class:`NetemResidueError`.

    Returns:
        list: Remaining residue (empty when clean).
    """
    ocp_obj = ocp_obj or ocp.OCP()
    if node_names is None:
        node_names = list_cluster_node_names(ocp_obj)
    log.info("Netem cleanup: sweeping nodes %s", node_names)
    unreachable_residue = []
    reachable = []
    for node_name in node_names:
        try:
            node_residue = sweep_node_netem(
                ocp_obj, node_name, timeout=per_node_timeout
            )
        except Exception:
            log.exception(
                "Netem cleanup: sweep failed on %s; continuing with remaining nodes",
                node_name,
            )
            unreachable_residue.append(_unreachable_item(node_name))
            continue
        if _only_unreachable(node_residue):
            unreachable_residue.extend(node_residue)
        else:
            reachable.append(node_name)
    residue = inspect_cluster_netem(
        ocp_obj=ocp_obj,
        node_names=reachable,
        per_node_timeout=per_node_timeout,
    )
    residue.extend(unreachable_residue)
    if residue and fail_on_residue:
        raise NetemResidueError(residue, context=context)
    if residue:
        log.error("Netem cleanup: residue remains after sweep: %s", residue)
    else:
        log.info("Netem cleanup: no netem qdiscs remain on %s", node_names)
    return residue


def cleanup_session_netem_and_chaos_pods(log_context, sweep_context):
    """
    Sweep leftover tc netem, then leftover Krkn hog, network-chaos, and debug pods.

    Args:
        log_context (str): Prefix for failure logs.
        sweep_context (str): Context included in netem residue errors.
    """
    try:
        sweep_cluster_netem(fail_on_residue=True, context=sweep_context)
    except Exception:
        log.exception("%s: netem sweep failed", log_context)
        raise
    try:
        from ocs_ci.krkn_chaos.krkn_helpers import cleanup_krkn_hog_pods

        cleanup_krkn_hog_pods()
    except Exception:
        log.exception("%s: leftover chaos/debug pod cleanup failed", log_context)


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
    """CLI entry point to sweep or inspect leftover netem."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    from ocs_ci.framework.logger_factory import set_log_record_factory

    set_log_record_factory()
    parser = argparse.ArgumentParser(
        description=(
            "Remove leftover tc netem qdiscs from OpenShift node host network "
            "interfaces (workers and masters). Use this to recover a cluster "
            "poisoned by interrupted chaos runs."
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
        "--verify-only",
        action="store_true",
        help="Only inspect; do not delete. Exit 1 if netem is present.",
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
    ocp_obj = ocp.OCP()
    try:
        if args.verify_only:
            assert_cluster_free_of_netem(
                ocp_obj=ocp_obj,
                per_node_timeout=args.per_node_timeout,
                context="verify-only",
            )
            log.info("Cluster is free of leftover netem")
            return 0
        sweep_cluster_netem(
            ocp_obj=ocp_obj,
            fail_on_residue=True,
            per_node_timeout=args.per_node_timeout,
            context="cli cleanup",
        )
        log.info("Netem sweep completed; cluster is clean")
        return 0
    except NetemResidueError as ex:
        log.error("%s", ex)
        return 1
    finally:
        if not args.verify_only:
            try:
                from ocs_ci.krkn_chaos.krkn_helpers import cleanup_krkn_hog_pods

                cleanup_krkn_hog_pods()
            except Exception:
                log.exception(
                    "Netem cleanup: could not delete leftover debug/chaos pods"
                )


if __name__ == "__main__":
    sys.exit(main())
