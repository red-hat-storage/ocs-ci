# -*- coding: utf8 -*-

"""
This module contains primary interface of network split module, expected to be
used by test setup and teardown fixtures, or manually via command line tool.

Until arbiter network split fixtures are implemented, API here is tentative.
"""

from datetime import datetime, timedelta
import argparse
import logging
import os
import sys

import yaml

from ocs_ci.framework import config
from ocs_ci.utility.networksplit import machineconfig
from ocs_ci.utility.networksplit.zone import ZoneConfig
from ocs_ci.utility.utils import run_cmd
from ocs_ci.ocs.ocp import exec_oc_debug_cmd


logger = logging.getLogger(name=__file__)


VALID_NETWORK_SPLITS = ("ab", "bc", "ab-bc", "ab-ac")


def _list_cluster_nodes(zone_name=None):
    """
    Get cluster nodes of a whole cluster or from given zone only.

    Args:
        zone_name (str): name of k8s topology zone to list nodes within, if not
            specified, nodes from whole cluster will be listed

    Returns:
        list: node ip addressess (as strings)

    """
    oc_cmd = [
        "oc",
        "--kubeconfig",
        os.getenv("KUBECONFIG"),
        "get",
        "nodes",
        "-o",
        "name",
    ]
    if zone_name is not None:
        oc_cmd.extend(["-l", "topology.kubernetes.io/zone=" + zone_name])
        logger.debug("trying to list nodes in %s zone", zone_name)
    else:
        logger.debug("trying to list all nodes")
    nodes_str = run_cmd(cmd=oc_cmd, timeout=600)
    return nodes_str.splitlines()


def _get_all_node_ip_addrs(node):
    """
    Get all ip addresses (both internal and external) of given node.

    Args:
        node (str): name of OCP node

    Returns:
        list: node ip addressess (as strings)

    """
    ip_addrs = []
    oc_cmd = ["oc", "--kubeconfig", os.getenv("KUBECONFIG"), "get", node, "-o", "yaml"]
    logger.debug("trying to get details about %s", node)
    node_str = run_cmd(cmd=oc_cmd, timeout=600)
    node_dict = yaml.safe_load(node_str)
    for addr_d in node_dict["status"]["addresses"]:
        if addr_d["type"] not in ("ExternalIP", "InternalIP"):
            continue
        ip_addrs.append(addr_d["address"])
    return ip_addrs


def get_zone_config(zone_names=None):
    """
    Detect and create zone configuration based on ocs-ci config file, unless
    zone_names are specified to override ocs-ci config.

    Args:
        zone_names (list): lislt of zone names

    Returns:
        ZoneConfig: object with list of node ip addresses for each zone

    """
    zn = {}
    if zone_names is not None:
        zn["a"] = zone_names[0]
        zn["b"] = zone_names[1]
        zn["c"] = zone_names[2]
    else:
        zn["a"] = config.DEPLOYMENT["arbiter_zone"]
        zn["b"] = config.DEPLOYMENT["worker_zones"][0]
        zn["c"] = config.DEPLOYMENT["worker_zones"][1]
    zc = ZoneConfig()
    for zone, name in zn.items():
        cluster_nodes = _list_cluster_nodes(name)
        for node in cluster_nodes:
            zc.add_nodes(zone, _get_all_node_ip_addrs(node))
    return zc


def get_networksplit_mc_spec(zone_env):
    """
    Create MachineConfig spec (to be deployed on via ``ObjectConfFile`` object)
    to install network split firewall tweaking script and unit files on all
    cluster nodes.

    Args:
        zone_env (str): content of firewall zone env file specifying node ip
            addressess for each cluster zone

    Returns:
        machineconfig_spec: list of dictrionaries with MachineConfig spec

    """
    mc_spec = []
    for role in "master", "worker":
        mc_spec.append(machineconfig.create_mc_dict(role, zone_env))
    return mc_spec


def schedule_split(split_name, target_dt, target_length):
    """
    Schedule start and stop of network split on all nodes of the cluster.

    Args:
        split_name (str): network split configuration specification, eg.
            "ab", see VALID_NETWORK_SPLITS constant
        target_dt (datetime): requested start time of the network split
        target_lenght (int): number of minutes specifying how long the network
            split configuration should be active

    Raises:
        ValueError: in case invalid ``split_name`` or ``target_dt`` is
            specified.

    """
    # input validation
    if split_name not in VALID_NETWORK_SPLITS:
        raise ValueError(f"invalid split_name specified: '{split_name}'")
    now_dt = datetime.now()
    # scheduling could take about 30 seconds for a cluster with 9 machines
    if target_dt - now_dt <= timedelta(minutes=1):
        msg = (
            "target start time is not at least 1 minute in the future, "
            "and it's not possible to guarantee that start timers will be "
            "scheduled across all nodes in time"
        )
        logger.error(msg)
        raise ValueError(msg)
    # convert start timestamp into unix time (number of seconds since epoch)
    start_ts = int(target_dt.timestamp())
    # compute target stop timestamp
    stop_ts = start_ts + (target_length * 60)
    # generate systemd timer unit names
    start_unit = f"network-split-{split_name}-setup@{start_ts}.timer"
    stop_unit = f"network-split-teardown@{stop_ts}.timer"
    # schedule both timers on every node of the cluster
    for node in _list_cluster_nodes():
        node_name = node[5:]
        cmd_str = f"systemctl start {start_unit} {stop_unit}"
        exec_oc_debug_cmd(node_name, cmd_str, timeout=300)


def main_setup():
    """
    Simple command line interface to generate MachineConfig yaml to deploy to
    make scheduling network splits possible.

    Example usage::

         $ ocs-network-split-setup --zone-label-names arbiter,d1,d2 -o mc.yaml
         $ oc create -f mc.yaml
         $ oc get mcp

    """
    ap = argparse.ArgumentParser(description="network split setup helper")
    ap.add_argument(
        "--print-env-only",
        action="store_true",
        default=False,
        help="just show firewall zone env file and exit",
    )
    ap.add_argument(
        "--output",
        "-o",
        metavar="FILE",
        type=argparse.FileType("w"),
        default=sys.stdout,
        help="name of yaml file with MachineConfig to deploy on OCP cluster",
    )
    ap.add_argument(
        "--zone-label-names",
        required=True,
        help="comma separated list of zone names 'a,b,c' where a is an arbiter",
    )
    ap.add_argument(
        "-d",
        "--debug",
        action="store_true",
        help="set log level to DEBUG",
    )
    args = ap.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)

    # get node ip addresses of each zone via zone config
    zone_names = args.zone_label_names.split(",")
    zone_config = get_zone_config(zone_names)
    zone_env = zone_config.get_env_file()

    if args.print_env_only:
        print(zone_env)
        return

    # get MachineConfig spec (ready to deploy list of dics)
    mc = get_networksplit_mc_spec(zone_env)
    args.output.write(yaml.dump_all(mc))


def main_schedule():
    """
    Simple command line interface to schedule given cluster network split.

    Example usage::

         $ ocs-network-split-sched ab-bc 2021-03-18T18:45 --split-len 30

    """
    ap = argparse.ArgumentParser(description="network split scheduler")
    ap.add_argument(
        "split_name",
        choices=VALID_NETWORK_SPLITS,
        help="which split configuration to schedule",
    )
    ap.add_argument(
        "timestamp", help="moment when to schedule the network split (in ISO format)"
    )
    ap.add_argument(
        "--split-len",
        metavar="MIN",
        default=15,
        help="how long the network split should take (in minutes)",
    )
    ap.add_argument(
        "-d",
        "--debug",
        action="store_true",
        help="set log level to DEBUG",
    )
    args = ap.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    try:
        start_dt = datetime.fromisoformat(args.timestamp)
    except ValueError as ex:
        print(ex)
        return 1
    schedule_split(args.split_name, start_dt, args.split_len)
