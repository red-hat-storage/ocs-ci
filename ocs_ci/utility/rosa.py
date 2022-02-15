# -*- coding: utf8 -*-
"""
Module for interactions with Openshift Dedciated Cluster.
"""


import json
import logging
import os
import re

from ocs_ci.framework import config
from ocs_ci.ocs.exceptions import (
    ManagedServiceAddonDeploymentError,
    UnsupportedPlatformVersionError,
)
from ocs_ci.utility import openshift_dedicated as ocm
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
    utils.run_cmd(cmd, secrets=[token])
    logger.info("Successfully logged in to ROSA")


def create_cluster(cluster_name, version):
    """
    Create OCP cluster.

    Args:
        cluster_name (str): Cluster name
        version (str): cluster version

    """
    rosa_ocp_version = get_latest_rosa_version(version)
    create_account_roles(version)
    region = config.DEPLOYMENT["region"]
    compute_nodes = config.ENV_DATA["worker_replicas"]
    compute_machine_type = config.ENV_DATA["worker_instance_type"]
    multi_az = "--multi-az " if config.ENV_DATA["multi_availability_zones"] else ""
    cmd = (
        f"rosa create cluster --cluster-name {cluster_name} --region {region} "
        f"--compute-nodes {compute_nodes} --mode auto --compute-machine-type "
        f"{compute_machine_type}  --version {rosa_ocp_version} {multi_az}--sts --yes"
    )
    utils.run_cmd(cmd)
    logger.info("Waiting for installation of ROSA cluster")
    for cluster_info in utils.TimeoutSampler(
        4500, 30, ocm.get_cluster_details, cluster_name
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


def get_latest_rosa_version(version):
    """
    Returns latest available z-stream version available for ROSA.

    Args:
        version (str): OCP version in format `x.y`

    Returns:
        str: Latest available z-stream version

    """
    cmd = "rosa list versions"
    output = utils.run_cmd(cmd)
    logger.info(f"Looking for z-stream version of {version}")
    rosa_version = None
    for line in output.splitlines():
        match = re.search(f"^{version}\\.(\\d+) ", line)
        if match:
            rosa_version = match.group(0).rstrip()
            break
    if rosa_version is None:
        logger.error(f"Could not find any version of {version} available for ROSA")
        logger.info("Try providing an older version of OCP with --ocp-version")
        logger.info("Latest OCP versions available for ROSA are:")
        for i in range(3):
            logger.info(f"{output.splitlines()[i + 1]}")
        raise UnsupportedPlatformVersionError
    return rosa_version


def create_account_roles(version, prefix="ManagedOpenShift"):
    """
    Create the required account-wide roles and policies, including Operator policies.

    Args:
        version (str): cluster version
        prefix (str): role prefix

    """
    cmd = (
        f"rosa create account-roles --mode auto"
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
    cmd = f"rosa list addons -c {cluster}"
    output = utils.run_cmd(cmd)
    line = [line for line in output.splitlines() if re.match(f"^{addon_name} ", line)]
    return line[0]


def install_odf_addon(cluster):
    """
    Install ODF Managed Service addon to cluster.

    Args:
        cluster (str): cluster name or cluster id

    """
    addon_name = config.DEPLOYMENT["addon_name"]
    size = config.ENV_DATA["size"]
    notification_email_0 = config.REPORTING.get("notification_email_0")
    notification_email_1 = config.REPORTING.get("notification_email_1")
    notification_email_2 = config.REPORTING.get("notification_email_2")
    cmd = f"rosa install addon --cluster={cluster} --size {size} {addon_name}" f" --yes"
    if notification_email_0:
        cmd = cmd + f" --notification-email-0 {notification_email_0}"
    if notification_email_1:
        cmd = cmd + f" --notification-email-1 {notification_email_1}"
    if notification_email_2:
        cmd = cmd + f" --notification-email-2 {notification_email_2}"

    utils.run_cmd(cmd)
    for addon_info in utils.TimeoutSampler(
        4000, 30, get_addon_info, cluster, addon_name
    ):
        logger.info(f"Current addon installation info: " f"{addon_info}")
        if "ready" in addon_info:
            logger.info(f"Addon {addon_name} was installed")
            break
        if "failed" in addon_info:
            raise ManagedServiceAddonDeploymentError(
                f"Addon {addon_name} failed to be installed"
            )


def delete_odf_addon(cluster):
    """
    Delete ODF Managed Service addon from cluster.

    Args:
        cluster (str): cluster name or cluster id

    """
    addon_name = config.DEPLOYMENT["addon_name"]
    cmd = f"rosa uninstall addon --cluster={cluster} {addon_name} --yes"
    utils.run_cmd(cmd)
    for addon_info in utils.TimeoutSampler(
        4000, 30, get_addon_info, cluster, addon_name
    ):
        logger.info(f"Current addon installation info: " f"{addon_info}")
        if "not installed" in addon_info:
            logger.info(f"Addon {addon_name} was uninstalled")
            break
        if "failed" in addon_info:
            raise ManagedServiceAddonDeploymentError(
                f"Addon {addon_name} failed to be uninstalled"
            )


def delete_operator_roles(cluster_id):
    """
    Delete operator roles of the given cluster

    Args:
        cluster_id (str): the id of the cluster
    """
    cmd = f"rosa delete operator-roles -c {cluster_id}"
    utils.run_cmd(cmd)


def delete_oidc_provider(cluster_id):
    """
    Delete oidc provider of the given cluster

    Args:
        cluster_id (str): the id of the cluster
    """
    cmd = f"rosa delete oidc-provider -c {cluster_id}"
    utils.run_cmd(cmd)
