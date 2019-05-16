"""
A test program for checking Rook and RookCluster classes
and pod objects.
"""


import os
import logging

os.sys.path.append(os.path.dirname(os.getcwd()))

from ocs import rook


logger = logging.getLogger(__name__)


def run():
    """
    Assumes rook cluster has already been deployed.
    """
    rook_obj = rook.Rook()  # Everything with default
    cluster = rook_obj.cluster
    cluster_name = cluster.cluster_name
    logger.info(f"Cluster name is {cluster_name} ")

    logger.info("Pod info for Pods in the cluster are")
    for pod in cluster.pods:
        logger.info(f'{pod.name}')
        logger.info(f'{pod.labels}')

    client = [pod for pod in cluster.pods
              if pod.labels['app'] == 'rook-ceph-tools'][0]
    if not client:
        logger.error("Failed to get client")
        return False

    out, err, ret = client.exec_command(cmd="ceph -s")
    if not ret:
        logger.info(out)
    else:
        logger.info(err)
