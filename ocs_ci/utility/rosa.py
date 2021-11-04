# -*- coding: utf8 -*-
"""
Module for interactions with Openshift Dedciated Cluster.
"""


import json
import logging
import os
import re
import time

from ocs_ci.framework import config
from ocs_ci.utility import openshift_dedicated as ocm
from ocs_ci.utility import utils
from ocs_ci.utility.version import get_semantic_version

logger = logging.getLogger(name=__file__)
rosa = config.AUTH.get("rosa", {})


def login():
    """
    Login to ROSA client
    """
    token = ocm["token"]
    cmd = f"rosa login --token={token}"
    logger.info("Logging in to ROSA cli")
    utils.run_cmd(cmd, secrets=[token])
    logger.info("Successfully logged in to ROSA")


def create_cluster(cluster_name, version):
    """
    Create OCP cluster.

    Args:
        cluster_name (str): Cluster name.
        version (str): cluster version

    """
    create_account_roles(version)
    region = config.DEPLOYMENT["region"]
    compute_nodes = config.ENV_DATA["worker_replicas"]
    compute_machine_type = config.ENV_DATA["worker_instance_type"]
    cmd = (
        f"rosa create cluster --cluster-name {cluster_name} --region {region} "
        f"--compute-nodes {compute_nodes} --compute-machine-type "
        f"{compute_machine_type}  --version {version} --sts --yes"
    )
    utils.run_cmd(cmd)
    create_operator_roles(cluster_name)
    create_oidc_provider(cluster_name)
    logger.info("Waiting for installation of ROSA cluster")
    for cluster_info in utils.TimeoutSampler(
        10000, 30, ocm.get_cluster_details, cluster_name
    ):
        status = cluster_info["status"]["state"]
        logger.info(f"Current installation status: {status}")
        if status == "ready":
            logger.info("Cluster was installed")
            break
    cluster_info = ocm.get_cluster_details(cluster_name)
    # Create metadata file to store the cluster name
    cluster_info["clusterName"] = cluster_name
    cluster_info["clusterID"] = cluster_info["id"]
    cluster_path = config.ENV_DATA["cluster_path"]
    metadata_file = os.path.join(cluster_path, "metadata.json")
    with open(metadata_file, "w+") as f:
        json.dump(cluster_info, f)


def wait_for_rosa_input(spawn, acceptable_duration=5):
    """
    pass
    """
    sleep_count = 0
    while spawn.isalive():
        if sleep_count > acceptable_duration:
            break
        sleep_count = sleep_count + 1
        time.sleep(1)


def create_account_roles(version, prefix="ManagedOpenShift"):
    """
    Create the required account-wide roles and policies, including Operator policies.

    Args:
        version (str): cluster version
        prefix (str): role prefix

    """
    version = get_semantic_version(version, only_major_minor=True)
    cmd = (
        f"rosa create account-roles --version {version} --mode auto"
        f' --permissions-boundary "" --prefix {prefix}  --yes'
    )
    utils.run_cmd(cmd)


def create_operator_roles(cluster, prefix='""'):
    """
    Create the cluster-specific Operator IAM roles. The roles created include the
    relevant prefix for the cluster name

    Args:
        cluster (str): cluster name or cluster id
        prefix (str): role prefix

    """
    cmd = (
        f"rosa create operator-roles --cluster {cluster} --prefix {prefix}"
        f' --mode auto --permissions-boundary "" --yes'
    )
    utils.run_cmd(cmd)


def create_oidc_provider(cluster):
    """
    Create the OpenID Connect (OIDC) provider that the Operators will use to
    authenticate

    Args:
        cluster (str): cluster name or cluster id

    """
    cmd = f"rosa create oidc-provider --cluster {cluster} --mode auto --yes"
    utils.run_cmd(cmd)


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


def get_addon_info(cluster, addon_name):
    """
    Get line related to addon from rosa `list addons` command.

    Args:
        cluster (str): cluster name
        addon_name (str): addon name

    Returns:
        str: line of the command for relevant addon

    """
    cmd = "rosa list addons -c cluster"
    output = utils.run_cmd(cmd)
    line = [line for line in output.splitlines() if re.match(f"^{addon_name} ", line)]
    return line


def install_odf_addon(cluster):
    """
    Install ODF Managed Service addon to cluster.

    Args:
        cluster (str): cluster name or cluster id

    """
    addon_name = config.DEPLOYMENT["addon_name"]
    size = config.ENV_DATA["size"]
    notification_email_0 = config.ENV_DATA.get("notification_email_0")
    notification_email_1 = config.ENV_DATA.get("notification_email_1")
    notification_email_2 = config.ENV_DATA.get("notification_email_2")
    cmd = (
        f"rosa install addon --cluster={cluster} --size {size} {addon_name}"
        f" --notification-email-0 {notification_email_0}"
        f" --notification-email-1 {notification_email_1}"
        f" --notification-email-2 {notification_email_2} --yes"
    )
    utils.run_cmd(cmd)
    for addon_info in utils.TimeoutSampler(
        10000, 30, get_addon_info, cluster, addon_name
    ):
        logger.info(f"Current addon installation info: " f"{addon_info}")
        if "installed" in addon_info and "not installed" not in addon_info:
            logger.info(f"Addon {addon_name} was installed")
            break
