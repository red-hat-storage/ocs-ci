import argparse
import logging
import pickle

import os

from ocs_ci.framework import config
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs import platform_nodes
from ocs_ci.ocs.node import get_node_objs
from ocs_ci.utility.retry import retry

from ocs_ci.ocs.constants import (
    NODE_OBJ_FILE, NODE_FILE, INSTANCE_FILE
)

FORMAT = (
    '%(asctime)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s'
)
logging.basicConfig(format=FORMAT, level=logging.DEBUG)
log = logging.getLogger(__name__)


def cycle_nodes(cluster_path, action):
    """
    Start/Stop AWS nodes to save costs when not in use.

    Args:
        cluster_path(str): location of cluster path that has auth files
        action (str): action to perform either start or stop

    """
    node_obj_file = os.path.join(cluster_path, NODE_OBJ_FILE)
    nodes_file = os.path.join(cluster_path, NODE_FILE)
    instance_file = os.path.join(cluster_path, INSTANCE_FILE)
    if action == 'stop':
        ceph = CephCluster()
        ceph.set_noout()
        node_objs = get_node_objs()
        kls = platform_nodes.PlatformNodesFactory()
        nodes = kls.get_nodes_platform()
        with open(instance_file, "wb") as instance_file:
            log.info("Storing ocs instances objects")
            pickle.dump(nodes.get_ec2_instances(nodes=node_objs), instance_file)
        with open(nodes_file, "wb") as node_file:
            log.info("Storing ocp nodes objects")
            pickle.dump(nodes, node_file)
        with open(node_obj_file, "wb") as node_obj_file:
            log.info("Stopping all nodes")
            pickle.dump(node_objs, node_obj_file)
            nodes.stop_nodes(nodes=node_objs)
    elif action == 'start':
        with open(instance_file, "rb") as instance_file:
            log.info("Reading instance objects")
            instances = pickle.load(instance_file)
        with open(nodes_file, "rb") as node_file:
            log.info("Reading ocp nodes object")
            nodes = pickle.load(node_file)
        with open(node_obj_file, "rb") as node_obj_file:
            log.info("Starting ocs nodes")
            node_objs = pickle.load(node_obj_file)
            nodes.start_nodes(instances=instances, nodes=node_objs)
            unset_noout()


@retry((CommandFailed), tries=10, delay=10, backoff=1)
def unset_noout():
    """
    unset_noout with 10 retries and delay of 10 seconds.
    """
    ceph = CephCluster()
    ceph.unset_noout()


def cluster_pause():
    """
    Entry point to start/stop cluster nodes - AWS only

    """
    parser = argparse.ArgumentParser(description='Start/Stop Cluster Nodes - AWS Only')
    parser.add_argument(
        '--cluster-path',
        action='store',
        required=True,
        help="Location of cluster path that was used during installation "
    )
    parser.add_argument(
        '--action',
        nargs='?',
        required=True,
        choices=('start', 'stop'),
        help=""
    )
    logging.basicConfig(level=logging.INFO)
    args = parser.parse_args()
    cluster_path = os.path.expanduser(args.cluster_path)
    config.ENV_DATA['cluster_path'] = cluster_path
    os.environ["KUBECONFIG"] = os.path.join(cluster_path, config.RUN['kubeconfig_location'])
    cycle_nodes(cluster_path, args.action)
