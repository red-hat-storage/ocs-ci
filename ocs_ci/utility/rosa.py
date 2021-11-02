# -*- coding: utf8 -*-
"""
Module for interactions with Openshift Dedciated Cluster.
"""


import logging
import os
import json

from ocs_ci.framework import config
from ocs_ci.utility import openshift_dedicated as ocm
from ocs_ci.utility.utils import run_cmd, exec_cmd
from ocs_ci.utility import utils

logger = logging.getLogger(name=__file__)
rosa = config.AUTH.get("rosa", {})


def login():
    """
    Login to ROSA client
    """
    token = ocm["token"]
    cmd = f"rosa login --token={token}"
    logger.info("Logging in to ROSA cli")
    run_cmd(cmd, secrets=[token])
    logger.info("Successfully logged in to ROSA")


def create_cluster(cluster_name, version):
    """
    Create OCP cluster.

    Args:
        cluster_name (str): Cluster name.
        version (str): cluster version

    """
    configs = config.ENV_DATA["configs"]
    create_account_roles(configs["ocp_version"])
    create_operator_roles(cluster_name)
    cmd = (
        f"rosa create cluster --cluster-name {cluster_name} --region {configs['region']} "
        f"--compute-nodes {configs['worker_replicas']} --compute-machine-type "
        f"{configs['worker_instance_type']}  --version {configs['ocp_version']} --yes"
    )
    exec_cmd(cmd, timeout=9000)
    create_oidc_provider(cluster_name)
    cluster_info = ocm.get_cluster_details(cluster_name)
    # Create metadata file to store the cluster name
    cluster_info["clusterName"] = cluster_name
    cluster_info["clusterID"] = cluster_info["id"]
    cluster_path = config.ENV_DATA["cluster_path"]
    metadata_file = os.path.join(cluster_path, "metadata.json")
    with open(metadata_file, "w+") as f:
        json.dump(cluster_info, f)


def create_account_roles(version, prefix="ManagedOpenShift"):
    """
    Create the required account-wide roles and policies, including Operator policies.

    Args:
        version (str): cluster version
        prefix (str): role prefix

    """
    cmd = f"rosa create account-roles --version {version} --prefix {prefix} --yes"
    run_cmd(cmd)


def create_operator_roles(cluster):
    """
    Create the cluster-specific Operator IAM roles. The roles created include the
    relevant prefix for the cluster name

    Args:
        cluster (str): cluster name or cluster id

    """
    cmd = f"rosa create operator-roles --cluster {cluster} --yes"
    run_cmd(cmd)


def create_oidc_provider(cluster):
    """
    Create the OpenID Connect (OIDC) provider that the Operators will use to
    authenticate

    Args:
        cluster (str): cluster name or cluster id

    """
    cmd = f"rosa create oidc-provider --cluster {cluster} --yes"
    run_cmd(cmd)


def download_rosa_cli():
    """
    Method to download OCM cli

    Returns:
        str: path to the installer

    """
    force_download = (
        config.RUN["cli_params"].get("deploy")
        and config.DEPLOYMENT["force_download_rosa_cli"]
    )
    return utils.get_rosa_cli(
        config.DEPLOYMENT["rosa_cli_version"], force_download=force_download
    )


def install_odf_addon(cluster):
    """
    Install ODF Managed Service addon to cluster.

    Args:
        cluster (str): cluster name or cluster id

    """
    addon_name = config.DEPLOYMENT["addon_name"]
    cmd = f"rosa install addon --cluster={cluster} {addon_name}"
    run_cmd(cmd)
