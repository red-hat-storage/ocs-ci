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
    ConfigurationError,
)
from ocs_ci.utility import openshift_dedicated as ocm
from ocs_ci.utility import utils

from ocs_ci.utility.aws import AWS as AWSUtil
from ocs_ci.utility.managedservice import (
    remove_header_footer_from_key,
    generate_onboarding_token,
    get_storage_provider_endpoint,
)


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


def create_cluster(cluster_name, version, region):
    """
    Create OCP cluster.

    Args:
        cluster_name (str): Cluster name
        version (str): cluster version
        region (str): Cluster region

    """

    rosa_ocp_version = config.DEPLOYMENT["installer_version"]
    # Validate ocp version with rosa ocp supported version
    # Select the valid version if given version is invalid
    if not validate_ocp_version(rosa_ocp_version):
        logger.warning(
            f"Given OCP version {rosa_ocp_version} "
            f"is not valid ROSA OCP version. "
            f"Selecting latest rosa version for deployment"
        )
        rosa_ocp_version = get_latest_rosa_version(version)
        logger.info(f"Using OCP version {rosa_ocp_version}")

    create_account_roles(version)
    compute_nodes = config.ENV_DATA["worker_replicas"]
    compute_machine_type = config.ENV_DATA["worker_instance_type"]
    multi_az = "--multi-az " if config.ENV_DATA.get("multi_availability_zones") else ""
    cluster_type = config.ENV_DATA.get("cluster_type", "")
    provider_name = config.ENV_DATA.get("provider_name", "")
    rosa_mode = config.ENV_DATA.get("rosa_mode", "")
    cmd = (
        f"rosa create cluster --cluster-name {cluster_name} --region {region} "
        f"--compute-nodes {compute_nodes} --compute-machine-type "
        f"{compute_machine_type}  --version {rosa_ocp_version} {multi_az}--sts --yes"
    )
    if rosa_mode == "auto":
        cmd += " --mode auto"
    if cluster_type.lower() == "consumer" and config.ENV_DATA.get("provider_name", ""):
        aws = AWSUtil()
        subnet_id = ",".join(aws.get_cluster_subnet_ids(provider_name))
        cmd = f"{cmd} --subnet-ids {subnet_id}"

    utils.run_cmd(cmd, timeout=1200)
    if rosa_mode != "auto":
        logger.info(
            "Waiting for ROSA cluster status changed to waiting or pending state"
        )
        for cluster_info in utils.TimeoutSampler(
            4500, 30, ocm.get_cluster_details, cluster_name
        ):
            status = cluster_info["status"]["state"]
            logger.info(f"Current installation status: {status}")
            if status == "waiting" or status == "pending":
                logger.info(f"Cluster is in {status} state")
                break
        create_operator_roles(cluster_name)
        create_oidc_provider(cluster_name)

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


def validate_ocp_version(version):
    """
    Validate the version whether given version is z-stream version available for ROSA.

    Args:
        version (str): OCP version string

    Returns:
        bool: True if given version is available in z-stream version for ROSA
              else False
    """
    cmd = "rosa list versions -o json"
    out = utils.run_cmd(cmd)
    output = json.loads(out)
    available_versions = [info["raw_id"] for info in output]
    if version in available_versions:
        logger.info(f"OCP versions {version} is available for ROSA")
        return True
    else:
        logger.info(
            f"Given OCP versions {version} is not available for ROSA. "
            f"Valid OCP versions supported on ROSA are : {available_versions}"
        )
        return False


def create_account_roles(version, prefix="ManagedOpenShift"):
    """
    Create the required account-wide roles and policies, including Operator policies.

    Args:
        version (str): cluster version
        prefix (str): role prefix

    """
    cmd = f"rosa create account-roles --mode auto" f" --prefix {prefix}  --yes"
    utils.run_cmd(cmd, timeout=1200)


def create_operator_roles(cluster):
    """
    Create the cluster-specific Operator IAM roles. The roles created include the
    relevant prefix for the cluster name

    Args:
        cluster (str): cluster name or cluster id

    """
    cmd = f"rosa create operator-roles --cluster {cluster}" f" --mode auto --yes"
    utils.run_cmd(cmd, timeout=1200)


def create_oidc_provider(cluster):
    """
    Create the OpenID Connect (OIDC) provider that the Operators will use to
    authenticate

    Args:
        cluster (str): cluster name or cluster id

    """
    cmd = f"rosa create oidc-provider --cluster {cluster} --mode auto --yes"
    utils.run_cmd(cmd, timeout=1200)


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
        str: line of the command for relevant addon. If not found, it returns None.

    """
    cmd = f"rosa list addons -c {cluster}"
    output = utils.run_cmd(cmd)
    line = [line for line in output.splitlines() if re.match(f"^{addon_name} ", line)]
    addon_info = line[0] if line else None
    return addon_info


def install_odf_addon(cluster):
    """
    Install ODF Managed Service addon to cluster.

    Args:
        cluster (str): cluster name or cluster id

    """
    addon_name = config.ENV_DATA["addon_name"]
    size = config.ENV_DATA["size"]
    cluster_type = config.ENV_DATA.get("cluster_type", "")
    provider_name = config.ENV_DATA.get("provider_name", "")
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

    if cluster_type.lower() == "provider":
        public_key = config.AUTH.get("managed_service", {}).get("public_key", "")
        if not public_key:
            raise ConfigurationError(
                "Public key for Managed Service not defined.\n"
                "Expected following configuration in auth.yaml file:\n"
                "managed_service:\n"
                '  private_key: "..."\n'
                '  public_key: "..."'
            )
        public_key_only = remove_header_footer_from_key(public_key)
        cmd += f' --onboarding-validation-key "{public_key_only}"'

    if cluster_type.lower() == "consumer" and provider_name:
        unit = config.ENV_DATA.get("unit", "Ti")
        storage_provider_endpoint = get_storage_provider_endpoint(provider_name)
        cmd += f' --unit "{unit}" --storage-provider-endpoint "{storage_provider_endpoint}"'
        onboarding_ticket = config.DEPLOYMENT.get("onboarding_ticket", "")
        if not onboarding_ticket:
            onboarding_ticket = generate_onboarding_token()
        if onboarding_ticket:
            cmd += f' --onboarding-ticket "{onboarding_ticket}"'
        else:
            raise ValueError(" Invalid onboarding ticket configuration")

    utils.run_cmd(cmd, timeout=1200)
    for addon_info in utils.TimeoutSampler(
        7200, 30, get_addon_info, cluster, addon_name
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
    addon_name = config.ENV_DATA["addon_name"]
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
    cmd = f"rosa delete operator-roles -c {cluster_id} --mode auto --yes"
    utils.run_cmd(cmd, timeout=1200)


def delete_oidc_provider(cluster_id):
    """
    Delete oidc provider of the given cluster

    Args:
        cluster_id (str): the id of the cluster
    """
    cmd = f"rosa delete oidc-provider -c {cluster_id} --mode auto --yes"
    utils.run_cmd(cmd, timeout=1200)


def is_odf_addon_installed(cluster_name=None):
    """
    Check if the odf addon is installed

    Args:
        cluster_name (str): The cluster name. The default value is 'config.ENV_DATA["cluster_name"]'

    Returns:
        bool: True, if the odf addon is installed. False, otherwise

    """
    cluster_name = cluster_name or config.ENV_DATA["cluster_name"]
    addon_name = config.ENV_DATA.get("addon_name")
    addon_info = get_addon_info(cluster_name, addon_name)

    if addon_info and "ready" in addon_info:
        return True
    else:
        return False
