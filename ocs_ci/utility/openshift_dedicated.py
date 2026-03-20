# -*- coding: utf8 -*-
"""
Module for interactions with Openshift Dedciated Cluster.

"""


import logging
import os
import json

from ocs_ci.framework import config
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility.utils import run_cmd, exec_cmd
from ocs_ci.utility import utils

logger = logging.getLogger(__name__)
openshift_dedicated = config.AUTH.get("openshiftdedicated", {})
_credentials_cache = {}


def login():
    """
    Login to OCM client
    """
    token = openshift_dedicated["token"]
    ms_env = config.ENV_DATA.get("ms_env_type", "staging")
    cmd = f"ocm login --token={token}"
    if ms_env != "production":
        # default MS environment consider is staging
        cmd += " --url=staging"

    logger.info("Logging in to OCM cli")
    run_cmd(cmd, secrets=[token])
    logger.info("Successfully logged in to OCM")


def create_cluster(cluster_name):
    """
    Create OCP cluster.

    Args:
        cluster_name (str): Cluster name.

    """
    configs = config.ENV_DATA["configs"]
    cmd = (
        f"podman run -e ADDON_IDS -e NUM_WORKER_NODES -e OCM_COMPUTE_MACHINE_TYPE"
        f" -e OCM_TOKEN -e CLUSTER_NAME -e CLUSTER_EXPIRY_IN_MINUTES"
        f" quay.io/app-sre/osde2e test --configs {configs}"
    )
    exec_cmd(cmd, timeout=9000)
    cluster_info = get_cluster_details(cluster_name)
    # Create metadata file to store the cluster name
    cluster_info["clusterName"] = cluster_name
    cluster_info["clusterID"] = cluster_info["id"]
    cluster_path = config.ENV_DATA["cluster_path"]
    metadata_file = os.path.join(cluster_path, "metadata.json")
    with open(metadata_file, "w+") as f:
        json.dump(cluster_info, f)


def get_cluster_details(cluster):
    """
    Returns info about the cluster which is taken from the OCM command.

    Args:
        cluster (str): Cluster name.

    """
    cmd = f"ocm describe cluster {cluster} --json=true"
    out = run_cmd(cmd)
    return json.loads(out)


def download_ocm_cli():
    """
    Method to download OCM cli

    Returns:
        str: path to the installer
    """
    force_download = (
        config.RUN["cli_params"].get("deploy")
        and config.DEPLOYMENT["force_download_ocm_cli"]
    )
    return utils.get_ocm_cli(
        config.DEPLOYMENT["ocm_cli_version"], force_download=force_download
    )


def get_cached_credentials(cluster):
    """
    Get cached credentials for the cluster or fetch if not cached.

    Args:
        cluster (str): Cluster name.

    Returns:
        dict: Cluster credentials.
    """
    if cluster not in _credentials_cache:
        _credentials_cache[cluster] = get_credentials(cluster)
    return _credentials_cache[cluster]


def update_cached_credentials(cluster):
    """
    Update cached credentials for the cluster.

    Args:
        cluster (str): Cluster name.

    """
    _credentials_cache[cluster] = get_credentials(cluster)


def get_credentials(cluster):
    """
    Get json with cluster credentials

    Args:
        cluster (str): Cluster name.

    Returns:
        json: cluster credentials

    """
    cluster_details = get_cluster_details(cluster)
    cluster_id = cluster_details.get("id")
    cmd = f"ocm get /api/clusters_mgmt/v1/clusters/{cluster_id}/credentials"
    out = run_cmd(cmd)
    return json.loads(out)


def get_kubeconfig(cluster, path):
    """
    Export kubeconfig to provided path.
    To get fresh credentials run first update_cached_credentials(cluster)

    Args:
        cluster (str): Cluster name.
        path (str): Path where to create kubeconfig file.
    """
    path = os.path.expanduser(path)
    basepath = os.path.dirname(path)
    os.makedirs(basepath, exist_ok=True)
    credentials = get_cached_credentials(cluster)
    with open(path, "w+") as fd:
        fd.write(credentials.get("kubeconfig"))


def get_kubeadmin_password(cluster, path):
    """
    Export password for kubeadmin to provided path.
    To get fresh credentials run first update_cached_credentials(cluster)

    Args:
        cluster (str): Cluster name.
        path (str): Path where to create kubeadmin-password file.
    """
    path = os.path.expanduser(path)
    basepath = os.path.dirname(path)
    os.makedirs(basepath, exist_ok=True)
    credentials = get_cached_credentials(cluster)
    with open(path, "w+") as fd:
        fd.write(credentials.get("admin").get("password"))


def get_admin_name(cluster):
    """
    Get admin name for the cluster.
    To get fresh credentials run first update_cached_credentials(cluster)

    Args:
        cluster (str): Cluster name.

    Returns:
        str: Admin name.
    """
    credentials = get_cached_credentials(cluster)
    return credentials.get("admin").get("username")


def destroy_cluster(cluster):
    """
    Destroy the cluster on Openshift Dedicated.

    Args:
        cluster (str): Cluster name or ID.

    """
    try:
        cluster_details = get_cluster_details(cluster)
    except CommandFailed as e:
        if "Cluster was Deprovisioned" in str(e):
            logger.warning(f"Cluster {cluster} is already deprovisioned, {e}")
            return
        raise
    cluster_id = cluster_details.get("id")
    cmd = f"ocm delete /api/clusters_mgmt/v1/clusters/{cluster_id}"
    run_cmd(cmd, timeout=900)


def list_cluster():
    """
    Returns info about the openshift dedciated clusters which is taken from the OCM command.

    """
    cmd = "ocm list clusters --columns name,state"
    out = run_cmd(cmd)
    result = out.strip().split("\n")
    cluster_list = []
    for each_line in result[1:]:
        name, state = each_line.split()
        cluster_list.append([name, state])
    return cluster_list
