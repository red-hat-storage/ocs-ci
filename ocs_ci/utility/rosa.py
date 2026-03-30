# -*- coding: utf8 -*-
"""
Module for interactions with Openshift Dedciated Cluster.
"""

import argparse
import json
import logging
import os
import re
import sys
import yaml

from ocs_ci import framework
from ocs_ci.framework import config
from ocs_ci.framework.logger_helper import log_step
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    ManagedServiceAddonDeploymentError,
    ConfigurationError,
    ResourceWrongStatusException,
    TimeoutExpiredError,
)
from ocs_ci.ocs.machinepool import MachinePools, NodeConf
from ocs_ci.utility import openshift_dedicated as ocm
from ocs_ci.utility import utils

from ocs_ci.utility.aws import AWS as AWSUtil
from ocs_ci.utility.managedservice import (
    remove_header_footer_from_key,
    generate_onboarding_token,
    get_storage_provider_endpoint,
)
from ocs_ci.utility.openshift_dedicated import get_cluster_details
from ocs_ci.utility.retry import catch_exceptions
from ocs_ci.utility.utils import exec_cmd, TimeoutSampler
from ocs_ci.utility import version

logger = logging.getLogger(__name__)
rosa = config.AUTH.get("rosa", {})
rosa_hcp = config.ENV_DATA.get("platform") == constants.ROSA_HCP_PLATFORM
# to trace the leftovers of aws resources - use the date + letters for every role, config, etc.
date_in_minimal_format = utils.date_in_minimal_format()
random_letters = utils.get_random_letters(3)


def login():
    """
    Login to ROSA client
    """
    token = config.AUTH.get("openshiftdedicated", {})["token"]
    ms_env = config.ENV_DATA.get("ms_env_type", "staging")
    cmd = f"rosa login --token={token}"
    if ms_env != "production":
        # default MS environment consider is staging
        cmd += " --env=staging"
    logger.info("Logging in to ROSA cli")
    utils.run_cmd(cmd, secrets=[token])
    logger.info("Successfully logged in to ROSA")


def create_cluster(cluster_name, version_str, region):
    """
    Create OCP cluster.

    Args:
        cluster_name (str): Cluster name
        version_str (str): cluster version
        region (str): Cluster region

    """
    # AWS does not guarantee cluster creation time, and machinepool desired replicas.
    # we need to wait cmd finish execution up to 3 hours (recorded time during observation)
    create_timeout = 60 * 60 * 3
    aws = AWSUtil()
    rosa_ocp_version = config.DEPLOYMENT["installer_version"]
    # Validate ocp version with rosa ocp supported version
    # Select the valid version if given version is invalid
    log_step("Get OCP version matched with configuration")
    if not validate_ocp_version(rosa_ocp_version):
        logger.warning(
            f"Given OCP version {rosa_ocp_version} "
            f"is not valid ROSA OCP version. "
            f"Selecting latest rosa version for deployment"
        )
        logger.info(f"Looking for z-stream version of {version_str}")
        rosa_ocp_version = version.get_latest_rosa_ocp_version(version_str)
        logger.info(f"Using OCP version {rosa_ocp_version}")

    if rosa_hcp:
        account_roles_prefix = (
            f"{constants.ACCOUNT_ROLE_PREFIX_ROSA_HCP}-{cluster_name}"
        )
    else:
        account_roles_prefix = "ManagedOpenShift"
    log_step("Creating account roles")
    create_account_roles(account_roles_prefix)

    oidc_config_id = None
    if rosa_hcp:
        log_step("Creating OIDC config")
        oidc_config_id = create_oidc_config()

    compute_nodes = config.ENV_DATA["worker_replicas"]
    compute_machine_type = config.ENV_DATA["worker_instance_type"]
    multi_az = "--multi-az " if config.ENV_DATA.get("multi_availability_zones") else ""
    rosa_mode = config.ENV_DATA.get("rosa_mode", "")
    private_link = config.ENV_DATA.get("private_link", False)
    machine_cidr = config.ENV_DATA.get("machine_cidr", "10.0.0.0/16")
    subnet_section_name = "ms_subnet_ids_per_region_" + config.ENV_DATA.get(
        "subnet_type", "default"
    )
    if rosa_hcp:
        # For ROSA HCP we have only one subnet id's pair. Hence we can use default subnet id's.
        subnet_section_name = "rosahcp_subnet_ids_per_region_default"
    cmd = (
        f"rosa create cluster --cluster-name {cluster_name} --region {region} "
        f"--machine-cidr {machine_cidr} --replicas {compute_nodes} "
        f"--compute-machine-type {compute_machine_type} "
        f"--version {rosa_ocp_version} {multi_az} --sts --yes --watch"
    )

    if oidc_config_id:
        cmd += f" --oidc-config-id {oidc_config_id}"
    if rosa_mode == "auto":
        cmd += " --mode auto"

    # OCM has a check for byo-vpc and now onwards use of osd vpc is result into
    # https://issues.redhat.com/browse/ODFMS-262
    # hence Addon based and appliance mode provider consumer must use byo-vpc.
    # For quick fix we can use existing byo-vpc for all deployment as default
    # The implementation is still flexi to provide subnets or vpc name from env parameter
    # if parameters are not defined then existing byo-vpc will be used
    if config.ENV_DATA.get("subnet_ids", ""):
        subnet_ids = config.ENV_DATA.get("subnet_ids")
    elif rosa_hcp:
        # we have only one subnet id's pair, for ROSA HCP we need a pair of public and private subnet-ids
        # ROSA CLI identifies public vs. private subnets based on routing
        # in future we may want to change indexes and pick-up approach
        public_subnet = config.ENV_DATA["rosahcp_subnet_ids_per_region_default"][
            "us-west-2"
        ]["public_subnet"].split(",")[0]
        private_subnet = config.ENV_DATA["rosahcp_subnet_ids_per_region_default"][
            "us-west-2"
        ]["private_subnet"].split(",")[0]
        subnet_ids = f"{public_subnet},{private_subnet}"
    elif config.ENV_DATA.get("vpc_name", ""):
        subnet_ids = ",".join(
            aws.get_cluster_subnet_ids(config.ENV_DATA.get("vpc_name"))
        )
        # TODO: improve this for enabling it for selecting private and public subnets
        #  separately. This will enable to create private link cluster using byo-vpc name
    else:
        subnet_ids = config.ENV_DATA[subnet_section_name][region]["private_subnet"]
        if not private_link:
            subnet_ids += (
                f",{config.ENV_DATA[subnet_section_name][region]['public_subnet']}"
            )
    cmd = f"{cmd} --subnet-ids {subnet_ids}"

    if private_link:
        cmd += " --private-link "

    if rosa_hcp:
        # with rosa hcp we need operator roles to be created before cluster creation
        prefix = f"{constants.OPERATOR_ROLE_PREFIX_ROSA_HCP}-{cluster_name}"
        aws_account_id = aws.get_caller_identity()
        log_step("Creating operator roles and waiting them to be created")
        create_operator_roles(
            prefix=prefix,
            oidc_config_id=oidc_config_id,
            aws_account_id=aws_account_id,
            account_roles_prefix=account_roles_prefix,
        )
        wait_operator_roles(prefix)
        cmd += f" --operator-roles-prefix {prefix} "
        cmd += " --hosted-cp "

    log_step("Running create rosa cluster command")
    utils.run_cmd(cmd, timeout=create_timeout)
    if rosa_mode != "auto" and not rosa_hcp:
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
        log_step("Creating operator roles")
        create_operator_roles(cluster_name)
        log_step("Creating OIDC provider")
        create_oidc_provider(cluster_name)

    logger.info("Waiting for installation of ROSA cluster")
    for cluster_info in utils.TimeoutSampler(
        create_timeout, 30, ocm.get_cluster_details, cluster_name
    ):
        status = cluster_info["status"]["state"]
        logger.info(f"Current installation status: {status}")
        if status == "ready":
            logger.info("Cluster was installed")
            break
    log_step("Retrieving cluster details and storing in metadata.json file")
    cluster_info = ocm.get_cluster_details(cluster_name)
    # Create metadata file to store the cluster name
    cluster_info["clusterName"] = cluster_name
    cluster_info["clusterID"] = cluster_info["id"]
    cluster_path = config.ENV_DATA["cluster_path"]
    metadata_file = os.path.join(cluster_path, "metadata.json")
    with open(metadata_file, "w+") as f:
        json.dump(cluster_info, f)


def appliance_mode_cluster(cluster_name):
    """
    Create appliance mode provider cluster

    Args:
        cluster_name (str): Cluster name

    """
    addon_name = config.ENV_DATA.get("addon_name", "")
    size = config.ENV_DATA["size"]
    public_key = config.AUTH.get("managed_service", {}).get("public_key", "")
    notification_email_0 = config.REPORTING.get("notification_email_0")
    notification_email_1 = config.REPORTING.get("notification_email_1")
    notification_email_2 = config.REPORTING.get("notification_email_2")
    region = config.ENV_DATA.get("region", "")
    private_link = config.ENV_DATA.get("private_link", False)
    machine_cidr = config.ENV_DATA.get("machine_cidr", "10.0.0.0/16")
    subnet_section_name = "ms_subnet_ids_per_region_" + config.ENV_DATA.get(
        "subnet_type", "default"
    )
    if not public_key:
        raise ConfigurationError(
            "Public key for Managed Service not defined.\n"
            "Expected following configuration in auth.yaml file:\n"
            "managed_service:\n"
            '  private_key: "..."\n'
            '  public_key: "..."'
        )
    public_key_only = remove_header_footer_from_key(public_key)

    if config.ENV_DATA.get("subnet_ids", ""):
        subnet_ids = config.ENV_DATA.get("subnet_ids")
    else:
        subnet_ids = config.ENV_DATA[subnet_section_name][region]["private_subnet"]
        if not private_link:
            subnet_ids += (
                f",{config.ENV_DATA[subnet_section_name][region]['public_subnet']}"
            )

    cmd = (
        f"rosa create service --type {addon_name} --name {cluster_name} "
        f"--machine-cidr {machine_cidr} --size {size} "
        f"--onboarding-validation-key {public_key_only} "
        f"--subnet-ids {subnet_ids}"
    )
    if private_link:
        cmd += " --private-link "
    if notification_email_0:
        cmd = cmd + f" --notification-email-0 {notification_email_0}"
    if notification_email_1:
        cmd = cmd + f" --notification-email-1 {notification_email_1}"
    if notification_email_2:
        cmd = cmd + f" --notification-email-2 {notification_email_2}"
    if region:
        cmd = cmd + f" --region {region}"

    utils.run_cmd(cmd, timeout=1200)
    logger.info("Waiting for ROSA cluster status changed to waiting or pending state")
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
        logger.info(f"Cluster installation status: {status}")
        if status == "ready":
            logger.info("Cluster is installed")
            break
    if cluster_info["status"]["state"] == "ready":
        for addon_info in utils.TimeoutSampler(
            7200, 30, get_addon_info, cluster_name, addon_name
        ):
            logger.info(f"Current addon installation info: " f"{addon_info}")
            if "ready" in addon_info:
                logger.info(f"Addon {addon_name} is installed")
                break
            if "failed" in addon_info:
                logger.warning(f"Addon {addon_name} failed to be installed")
        addon_info = get_addon_info(cluster_name, addon_name)
        if "failed" in addon_info:
            raise ManagedServiceAddonDeploymentError(
                f"Addon {addon_name} failed to be installed"
            )
        logger.info("Waiting for ROSA service ready status")
    for service_status in utils.TimeoutSampler(
        7200, 30, get_rosa_service_details, cluster_name
    ):
        if "ready" in service_status:
            logger.info(f"service {cluster_name} is ready")
            break
        elif "failed" in service_status:
            logger.info(f"service {cluster_name} is failed")
            break
        else:
            logger.info(f"Current service creation status: {service_status}")
    # Create metadata file to store the cluster name
    cluster_info = ocm.get_cluster_details(cluster_name)
    cluster_info["clusterName"] = cluster_name
    cluster_info["clusterID"] = cluster_info["id"]
    cluster_path = config.ENV_DATA["cluster_path"]
    metadata_file = os.path.join(cluster_path, "metadata.json")
    with open(metadata_file, "w+") as f:
        json.dump(cluster_info, f)


def get_rosa_service_details(cluster):
    """
    Returns info about the rosa service cluster.

    Args:
        cluster (str): Cluster name.

    """
    cmd = "rosa list services"
    # cmd = f"rosa list services -o json --region {region}"
    services_details = utils.run_cmd(cmd, timeout=1200)
    # services_details = json.loads(out)
    for service_info in services_details.splitlines():
        if cluster in service_info:
            return service_info
    # Todo : update this function when -o json get supported in rosa services command
    # TODO : need exception handling
    return json.loads(service_info)


def validate_ocp_version(version_str):
    """
    Validate the version whether given version is z-stream version available for ROSA.

    Args:
        version_str (str): OCP version string

    Returns:
        bool: True if given version is available in z-stream version for ROSA
              else False
    """
    cmd = "rosa list versions -o json"
    out = utils.run_cmd(cmd)
    output = json.loads(out)
    available_versions = [info["raw_id"] for info in output]
    if version_str in available_versions:
        logger.info(f"OCP versions {version_str} is available for ROSA")
        return True
    else:
        logger.info(
            f"Given OCP versions {version_str} is not available for ROSA. "
            f"Valid OCP versions supported on ROSA are : {available_versions}"
        )
        return False


def create_account_roles(prefix="ManagedOpenShift"):
    """
    Create the necessary account-wide roles and policies, including operator-specific policies.

    **Important:**
    - Each cluster should have a unique prefix for its account roles, rather than using a common prefix across the
    entire account.
    - If multiple clusters are deployed with the same role prefix, deleting account roles during the cluster
    destruction stage could lead to:
    - Loss of Red Hat (RH) support.
    - Disruption of communication with worker nodes.

    Ensure that role prefixes are uniquely assigned per cluster to maintain cluster integrity and supportability.

    Args:
        prefix (str): role prefix

    """
    if rosa_hcp:
        hosted_cp_param = "--hosted-cp"
    else:
        hosted_cp_param = ""

    cmd = f"rosa create account-roles {hosted_cp_param} --mode auto --prefix {prefix} --yes"
    exec_cmd(cmd, timeout=1200)


def create_operator_roles(
    cluster="", prefix="", oidc_config_id="", aws_account_id="", account_roles_prefix=""
):
    """
    Create the cluster-specific Operator IAM roles. The roles created include the
    relevant prefix for the cluster name

    Args:
        cluster (str): cluster name
        prefix (str): role prefix
        oidc_config_id (str): OIDC config id
        aws_account_id (str): AWS account id
        account_roles_prefix (str): account roles prefix
    """
    prefix = (
        f"{constants.OPERATOR_ROLE_PREFIX_ROSA_HCP}-{cluster}"
        if prefix == ""
        else prefix
    )
    cmd = f"rosa create operator-roles --cluster {cluster} --mode auto --yes"
    # command with prefix should look another way, to avoid error:
    # ERR: A cluster key for STS cluster and an operator roles prefix cannot be specified alongside each other
    if rosa_hcp:
        cmd = (
            "rosa create operator-roles "
            "--hosted-cp "
            f"--prefix={prefix} "
            f"--oidc-config-id={oidc_config_id} "
            f"--installer-role-arn=arn:aws:iam::{aws_account_id}:role/{account_roles_prefix}-HCP-ROSA-Installer-Role "
            "--mode auto --yes"
        )
    utils.run_cmd(cmd, timeout=1200)


def get_operator_roles_data(prefix):
    """
    Get the operator roles with the given prefix

    Args:
        prefix (str): role prefix

    Returns:
        dict: JSON data of operator roles

    """
    cmd = f"rosa list operator-roles --prefix {prefix} -o json"
    if "No operator roles available" in utils.exec_cmd(cmd).stdout.decode():
        return
    return json.loads(utils.exec_cmd(cmd).stdout)


def wait_operator_roles(prefix, wait_minutes=10):
    """
    Wait for the operator roles to be created

    Args:
        prefix (str): role prefix
        wait_minutes (int): Time in minutes to wait for operator roles to be created

    Returns:
        bool: True if operator roles are created, False otherwise
    """
    for sample in TimeoutSampler(
        timeout=60 * wait_minutes,
        sleep=10,
        func=get_operator_roles_data,
        prefix=prefix,
    ):
        if sample:
            return True


def create_oidc_provider(cluster):
    """
    Create the OpenID Connect (OIDC) provider that the Operators will use to
    authenticate

    Args:
        cluster (str): cluster name or cluster id

    """
    cmd = f"rosa create oidc-provider --cluster {cluster} --mode auto --yes"
    utils.run_cmd(cmd, timeout=1200)


def create_oidc_config():
    """
    Create OIDC config and wait for it to appear in the list
    ! In a very extreme case, other OIDC config can be created in the same time failing TimeoutSampler and
    raising TimeoutExpiredError exception

    Returns:
        str: OIDC config id

    Raises:
        TimeoutExpiredError: If OIDC config is not created in time
    """
    cmd = "rosa create oidc-config --managed --mode=auto --yes"
    proc = exec_cmd(cmd, timeout=1200)
    if proc.returncode != 0:
        raise CommandFailed(
            f"Failed to create oidc config: {proc.stderr.decode().strip()}"
        )

    for sample in TimeoutSampler(
        timeout=300,
        sleep=10,
        func=get_oidc_config_ids,
        latest=True,
    ):
        if len(sample) and sample[0] in proc.stdout.decode().strip():
            logger.info("OIDC config created successfully")
            return sample[0]


def get_oidc_endpoint_url(oidc_config_id):
    """
    Get OIDC provider endpoint URL for the given OIDC config id

    Args:
        oidc_config_id (str): OIDC config id

    Returns:
        str: OIDC provider id
    """
    cmd = f"rosa list oidc-config -o json | jq -r '.[] | select(.id == \"{oidc_config_id}\") | .issuer_url'"
    proc = exec_cmd(cmd, shell=True)
    if proc.returncode != 0:
        raise CommandFailed(
            f"Failed to get oidc provider id: {proc.stderr.decode().strip()}"
        )
    issuer_url = proc.stdout.decode().strip()
    logger.info(f"OIDC issuer url: {issuer_url}")
    return issuer_url


def delete_oidc_config(oidc_config_id):
    """
    Delete OIDC config

    Args:
        oidc_config_id (str): OIDC config id

    """
    # check if requested oidc config persisted
    if oidc_config_id not in get_oidc_config_ids():
        logger.warning(
            f"OIDC config {oidc_config_id} is not found in the list of available configs"
        )
        return

    cmd = f"rosa delete oidc-config --oidc-config-id {oidc_config_id} --mode auto --yes"
    utils.exec_cmd(cmd, timeout=1200)
    for sample in TimeoutSampler(
        timeout=300,
        sleep=10,
        func=get_oidc_config_ids,
        latest=False,
    ):
        if oidc_config_id not in sample:
            logger.info("OIDC config deleted successfully")
            return


def get_latest_oidc_config_id():
    cmd = "rosa list oidc-config -o json | jq -r 'max_by(.creation_timestamp) | .id'"
    proc = exec_cmd(cmd, shell=True)
    if proc.returncode != 0:
        raise CommandFailed(
            f"Failed to get latest oidc config id: {proc.stderr.decode().strip()}"
        )
    oidc_config_latest = proc.stdout.decode().strip()
    logger.info(f"Latest OIDC config id: {oidc_config_latest}")
    return oidc_config_latest


def get_oidc_config_ids(latest=False):
    """
    Get OIDC config ids. If latest is True, return only the latest OIDC config id.

    Args:
        latest (bool): If True, return only the latest OIDC config id

    Returns:
        list: List of OIDC config ids
    """
    if latest:
        cmd = (
            "rosa list oidc-config -o json | jq -r 'max_by(.creation_timestamp) | .id'"
        )
    else:
        cmd = (
            "rosa list oidc-config -o json | jq -r 'map(select(has(\"id\"))) | .[].id'"
        )

    proc = exec_cmd(cmd, shell=True)
    if proc.returncode != 0:
        raise CommandFailed(
            f"Failed to get OIDC config ids: {proc.stderr.decode().strip()}"
        )

    oidc_configs = proc.stdout.decode().strip()
    logger.info(f"OIDC config id(s), latest='{latest}': {oidc_configs}")
    return oidc_configs.splitlines()


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
    cluster_type = config.ENV_DATA.get("cluster_type", "")
    provider_name = config.ENV_DATA.get("provider_name", "")
    notification_email_0 = config.REPORTING.get("notification_email_0")
    notification_email_1 = config.REPORTING.get("notification_email_1")
    notification_email_2 = config.REPORTING.get("notification_email_2")
    cmd = f"rosa install addon --cluster={cluster} {addon_name} --yes"
    # TODO(fbalak) it needs to be determined if this option is needed
    # This option was introduced in new rosa cli version and it seems that
    # it is a mandatory parameter (although rosa install addon --help says
    # that there is a default value)
    billing_model = config.ENV_DATA.get("rosa_billing_model")
    cmd = cmd + f" --billing-model {billing_model}"
    if notification_email_0:
        cmd = cmd + f" --notification-email-0 {notification_email_0}"
    if notification_email_1:
        cmd = cmd + f" --notification-email-1 {notification_email_1}"
    if notification_email_2:
        cmd = cmd + f" --notification-email-2 {notification_email_2}"

    if cluster_type.lower() == "provider":
        size = config.ENV_DATA.get("size", "")
        cmd += f" --size {size}"
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

    if cluster_type.lower() == "consumer" and (
        provider_name or config.ENV_DATA.get("appliance_mode", False)
    ):
        storage_provider_endpoint = get_storage_provider_endpoint(provider_name)
        cmd += f' --storage-provider-endpoint "{storage_provider_endpoint}"'
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
        logger.info(f"Current addon installation info: {addon_info}")
        if "ready" in addon_info:
            logger.info(f"Addon {addon_name} was installed")
            break
        if "failed" in addon_info:
            logger.warning(f"Addon {addon_name} is failed")

    addon_info = get_addon_info(cluster, addon_name)
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
    cluster_type = config.ENV_DATA.get("cluster_type", "")
    if cluster_type.lower() == "provider" and config.ENV_DATA.get("appliance_mode"):
        logger.info(
            "Addon uninstallation is not allowed for appliance mode"
            " managed service. It can be changed after fix of "
            "https://issues.redhat.com/browse/SDA-6011"
        )
        # TODO : Update rosa delete service addon command after completion of jira SDA-6011
        return

    addon_name = config.ENV_DATA["addon_name"]
    cmd = f"rosa uninstall addon --cluster={cluster} {addon_name} --yes"
    utils.run_cmd(cmd)
    for addon_info in utils.TimeoutSampler(
        7200, 300, get_addon_info, cluster, addon_name
    ):
        logger.info(f"Current addon installation info: " f"{addon_info}")
        if "not installed" in addon_info:
            logger.info(f"Addon {addon_name} was uninstalled")
            break
        if "failed" in addon_info:
            raise ManagedServiceAddonDeploymentError(
                f"Addon {addon_name} failed to be uninstalled"
            )


def delete_operator_roles(prefix):
    """
    Delete operator roles with prefix

    Args:
        prefix (str): prefix. Usually it is cluster name set during 'rosa create operator-roles' command
    """
    cmd = f"rosa delete operator-roles --prefix {prefix} --mode auto --yes"
    proc = exec_cmd(cmd, timeout=1200)
    if proc.returncode != 0:
        raise CommandFailed(
            f"Failed to delete operator roles: {proc.stderr.decode().strip()}"
        )
    logger.info(f"{proc.stdout.decode().strip()}")


def delete_account_roles(prefix):
    """
    Delete account roles
    ! Important to not delete account roles if there are any clusters in the account using this prefix

    Args:
        prefix (str): role prefix

    """
    cmd = f"rosa delete account-roles -p {prefix} --mode auto --yes"
    proc = exec_cmd(cmd, timeout=1200)
    if proc.returncode != 0:
        raise CommandFailed(
            f"Failed to delete account roles: {proc.stderr.decode().strip()}"
        )
    logger.info(f"{proc.stdout.decode().strip()}")


def get_rosa_cluster_service_id(cluster):
    """
    Get service id of cluster

    Args:
        cluster (str): cluster name

    Returns:
        str: service id of cluster. If not found, it returns None.

    """
    cmd = "rosa list service"
    cmd_out = utils.run_cmd(cmd)
    line = [line for line in cmd_out.splitlines() if re.search(f"{cluster}$", line)]
    cluster_service_info = line[0].split()[0] if line else None
    return cluster_service_info


# with 1.2.53 rosa list service or delete service command return 404, we catch this exception and later deprecate
@catch_exceptions(CommandFailed)
def destroy_appliance_mode_cluster(cluster):
    """
    Delete rosa cluster if appliance mode

    Args:
        cluster: name of the cluster

    Returns:
        bool: True if appliance mode and cluster delete initiated
              else False
    """
    service_id = get_rosa_cluster_service_id(cluster)
    if not service_id:
        logger.info(
            f"Cluster does not exist in rosa list service. "
            f"The cluster '{cluster}' is not appliance mode cluster. "
        )
        return False

    cluster_type = config.ENV_DATA.get("cluster_type", "")
    if cluster_type.lower() == "provider":
        if ocm.get_cluster_details(cluster)["status"]["state"] == "ready":
            # Check that no consumer is connected
            storageconsumers = ocp.OCP(
                kind="storageconsumer",
                namespace=config.ENV_DATA["cluster_namespace"],
            )
            for sample in utils.TimeoutSampler(
                timeout=3600,
                sleep=300,
                func=storageconsumers.get,
            ):
                if len(sample.get("items")) == 0:
                    logger.info(
                        "No consumer cluster connected, we can delete this provider cluster"
                    )
                    break

    delete_service_cmd = f"rosa delete service --id={service_id} --yes"
    try:
        utils.run_cmd(delete_service_cmd, timeout=1200)
    except CommandFailed as err:
        if "service is already deleting" in str(err):
            logger.info(f"Cluster {cluster} deletion was already triggered.")
        else:
            raise

    logger.info("Waiting for ROSA cluster state changed to uninstalling")
    for cluster_info in utils.TimeoutSampler(
        3600, 90, ocm.get_cluster_details, cluster
    ):
        status = cluster_info["status"]["state"]
        logger.info(f"Cluster uninstalling status: {status}")
        if status == "uninstalling":
            logger.info(f"Cluster '{cluster}' is uninstalling")
            break
    for service_status in utils.TimeoutSampler(
        3600, 30, get_rosa_service_details, cluster
    ):
        if "deleting service" in service_status:
            logger.info("Rosa service status is 'deleting service'")
            break
    return True


def destroy_rosa_cluster(cluster, best_effort=True):
    """
    Delete rosa cluster

    Parameters:
        cluster (str): name of the cluster
        best_effort (bool): If True (true), ignore errors and continue with the deletion of the cluster
    """
    try:
        external_id = get_cluster_details(cluster)["id"]
    except CommandFailed as e:
        if "Cluster was Deprovisioned" in str(e):
            logger.warning(f"Cluster {cluster} is already deprovisioned, {e}")
            return True
        raise
    cmd = f"ocm delete cluster {external_id} -p best_effort={str(best_effort).lower()}"
    proc = exec_cmd(cmd, timeout=1200)
    if proc.returncode != 0:
        raise CommandFailed(f"Failed to delete cluster: {proc.stderr.decode().strip()}")
    logger.info(f"{proc.stdout.decode().strip()}")
    return True


def delete_oidc_provider(cluster_name):
    """
    Delete oidc provider of the given cluster

    Args:
        cluster_name (str): the cluster name
    """
    cmd = f"rosa delete oidc-provider -c {cluster_name} --mode auto --yes"
    proc = exec_cmd(cmd, timeout=1200)
    if proc.returncode != 0:
        raise CommandFailed(
            f"Failed to delete oidc provider: {proc.stderr.decode().strip()}"
        )
    logger.info(f"{proc.stdout.decode().strip()}")


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


def post_onboarding_verification():
    """
    Check that after onboarding consumer the relevant resources
    were created in the provider cluster and then switch back to the original cluster:
    1. StorageConsumer with the correct id exists and all its CephResources
    are in Ready status
    2. CephBlockPool and Subvolumegroup with the correct id are in Ready status
    """
    logger.info("Starting post-onboarding verification")
    if not config.multicluster:
        logger.info("No post-onboarding checks are run on non-multicluster deployment")
        return
    restore_ctx_index = config.cur_index
    consumer_ids = []
    consumer_indexes = config.get_consumer_indexes_list()
    for cluster_index in consumer_indexes:
        config.switch_ctx(cluster_index)
        clusterversion_yaml = ocp.OCP(
            kind="ClusterVersion",
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name="version",
        )
        current_consumer = clusterversion_yaml.get()["spec"]["clusterID"]
        logger.info(f"Current consumer's ID is {current_consumer}")
        consumer_ids.append(f"storageconsumer-{current_consumer}")
    logger.info(f"Consumer ids from consumer clusters: {consumer_ids}")
    config.switch_to_provider()
    for consumer in consumer_ids:
        consumer_yaml = ocp.OCP(
            kind="StorageConsumer",
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=consumer,
        )
        ceph_resources = consumer_yaml.get().get("status")["cephResources"]
        for resource in ceph_resources:
            if resource["status"] != constants.STATUS_READY:
                raise ResourceWrongStatusException(
                    f"{resource['name']} of {consumer} is in status {resource['status']}. Status should be Ready"
                )
        for resource in {
            constants.CEPHBLOCKPOOL.lower(),
            constants.CEPHFILESYSTEMSUBVOLUMEGROUP,
        }:
            resource_name = resource + "-" + consumer
            resource_yaml = ocp.OCP(
                kind=resource,
                namespace=config.ENV_DATA["cluster_namespace"],
                resource_name=resource_name,
            )
            if resource_yaml.get()["status"]["phase"] != constants.STATUS_READY:
                raise ResourceWrongStatusException(
                    f"{resource_name} is in Status {resource_yaml.get()['status']['phase']}. Status should be Ready"
                )
    config.switch_ctx(restore_ctx_index)


def wait_for_addon_to_be_ready(
    cluster_name=None, addon_name=None, timeout=300, sleep=20
):
    """
    Wait for the addon of a cluster to be ready

    Args:
        cluster_name (str): The cluster name. The default value is 'config.ENV_DATA["cluster_name"]'
        addon_name (str): The addon name. The default value is 'addon_name = config.ENV_DATA["addon_name"]'
        timeout (int): Timeout to wait for the addon to be ready
        sleep (int): Time in seconds to sleep between attempts

    Raise:
        TimeoutExpiredError: In case the addon is not ready in the given timeout

    """
    cluster_name = cluster_name or config.ENV_DATA["cluster_name"]
    addon_name = addon_name or config.ENV_DATA["addon_name"]

    for addon_info in utils.TimeoutSampler(
        timeout=timeout,
        sleep=sleep,
        func=get_addon_info,
        cluster=cluster_name,
        addon_name=addon_name,
    ):
        if addon_info and "ready" in addon_info:
            logger.info(
                f"The addon {addon_name} of the cluster {cluster_name} is in a ready state"
            )
            break


def edit_addon_installation(
    addon_param_key, addon_param_value, cluster_name=None, addon_name=None, wait=True
):
    """
    Edit a specific parameter of the odf addon installation of a cluster

    Args:
        addon_param_key (str): The addon key param to modify
        addon_param_value (str): The addon value param to modify
        cluster_name (str): The cluster name. The default value is 'config.ENV_DATA["cluster_name"]'
        addon_name (str): The addon name. The default value is 'addon_name = config.ENV_DATA["addon_name"]'
        wait (bool): If true, wait for the addon to be ready. False, otherwise.

    """
    cluster_name = cluster_name or config.ENV_DATA["cluster_name"]
    addon_name = addon_name or config.ENV_DATA["addon_name"]
    cmd = f"rosa edit addon {addon_name} -c {cluster_name} --{addon_param_key} '{addon_param_value}'"
    utils.run_cmd(cmd)
    if wait:
        wait_for_addon_to_be_ready(cluster_name, addon_name)


def get_console_url(cluster_name):
    """
    Get the console URL of the given cluster

    Args:
        cluster_name (str): The cluster name

    Returns:
        str: The console URL

    """
    cmd = (
        f"rosa describe cluster --cluster {cluster_name} -o json | jq -r '.console.url'"
    )
    proc = exec_cmd(cmd, shell=True)
    if proc.returncode != 0:
        raise CommandFailed(
            f"Failed to get console URL: {proc.stderr.decode().strip()}"
        )
    return proc.stdout.decode().strip()


@catch_exceptions((CommandFailed, TimeoutExpiredError))
def wait_console_url(cluster_name, timeout=600, sleep=10):
    """
    Wait for the console URL of the cluster to be ready

    Args:
        cluster_name (str): The cluster name
        timeout (int): Timeout to wait for the console URL to be ready
        sleep (int): Time in seconds to sleep between attempts

    Returns:
        str: The console URL

    Raises:
        TimeoutExpiredError: In case the console URL is not ready in the given timeout

    """
    for console_url in TimeoutSampler(
        timeout=timeout,
        sleep=sleep,
        func=get_console_url,
        cluster_name=cluster_name,
    ):
        if console_url and "https" in console_url:
            logger.info(f"Console URL: {console_url}")
            return console_url


def get_associated_oidc_config_id(cluster_name):
    """
    Get the associated OIDC config id of the given cluster

    Args:
        cluster_name (str): The cluster name

    Returns:
        str: The OIDC config id

    """
    cmd = (
        f"rosa describe cluster --cluster {cluster_name} -o json "
        "| jq -r '.aws.sts.oidc_endpoint_url? "
        '| split("/") | .[-1] // ""\''
    )
    proc = exec_cmd(cmd, shell=True)
    if proc.returncode != 0:
        logger.warning(f"Failed to get OIDC config id: {proc.stderr.decode().strip()}")
        return ""
    return proc.stdout.decode().strip()


def label_nodes(cluster_name, machinepool_id, labels, rewrite=False):
    """
    Label nodes of the given cluster.
    ! Important
    This method rewrites existing behavior of labeling nodes in the cluster, it appends the labels to the existing
    labels, but not rewrite them. This prevents the issue of accidental overwriting the existing labels.

    Args:
        cluster_name (str): The cluster name
        machinepool_id (str): The machinepool id
        labels (str): The labels to apply
        rewrite (bool): If True, rewrite the labels. False, otherwise.

    Returns:
        str: The output of the command
    """
    machine_pools = MachinePools(cluster_name)
    machine_pool = machine_pools.filter(machinepool_id="workers", pick_first=True)
    if not rewrite:
        labels_dict = machine_pool.labels
        logger.info(f"Existing labels: {labels_dict}")
        # convert to comma separated string
        if labels_dict:
            labels = (
                ",".join([f"{key}={value}" for key, value in labels_dict.items()])
                + ","
                + labels
            )
        else:
            labels = labels
    machine_pools.edit_machine_pool(
        NodeConf(**{"machinepool_id": machinepool_id, "labels": labels}),
        wait_ready=False,
    )
    machine_pool.refresh()
    return machine_pool.labels


def rosa_create_htpasswd_idp(
    htpasswd_path, cluster_name=config.ENV_DATA["cluster_name"], idp_name="my_htpasswd"
):
    """
    Creates HTPasswd IDP from htpasswd file

    Args:
        htpasswd_path (str): Path to htpasswd file
        cluster_name (str): Cluster name
        idp_name (str): Name of the IDP

    """
    cmd = f"rosa create idp --cluster {cluster_name} --type htpasswd --name {idp_name} --from-file {htpasswd_path}"
    resp = utils.exec_cmd(cmd)
    if resp.returncode != 0:
        raise CommandFailed(f"Failed to create IDP from htpasswd file {htpasswd_path}")
    else:
        logger.info(f"response\n: {resp.stdout.decode('utf-8').splitlines()}")


def rosa_list_idps(cluster_name=config.ENV_DATA["cluster_name"]):
    """
    List IDPs

    Args:
        cluster_name (str): Cluster name

    Returns:
        dict: Dictionary with IDP names as keys and IDP types as values
    """
    cmd = f"rosa list idps --cluster {cluster_name}"
    resp = utils.exec_cmd(cmd)

    if resp.returncode != 0:
        raise CommandFailed("Failed to list IDPs")
    else:
        out_decoded = resp.stdout.decode("utf-8").splitlines()
        logger.info(f"response\n: {out_decoded}")
    # at least one line is always returned, so we can safely skip the header - out_decoded[1:]
    names_to_idp_dict = {item.split()[0]: item.split()[1] for item in out_decoded[1:]}
    return names_to_idp_dict


def rosa_delete_htpasswd_idp(
    cluster_name=config.ENV_DATA["cluster_name"], idp_name="my_htpasswd"
):
    """
    Deletes IDP

    Args:
        cluster_name (str): Cluster name
        idp_name (str): Name of the IDP

    """
    cmd = f"rosa delete idp {idp_name} --cluster {cluster_name} --yes"
    resp = utils.exec_cmd(cmd)
    if resp.returncode != 0:
        raise CommandFailed("Failed to delete IDP")
    else:
        logger.info(f"response\n: {resp.stdout.decode('utf-8').splitlines()}")


def upgrade_rosa_cluster(cluster_name, version):
    """
    Upgrade the ROSA cluster to the given version
    ! important ! rosa cli version drops error in case if --control-plane parameter is not used
    ! important ! upgrade is not performed automatically in case of ROSA clusters, especially in case of HCP;
    Upgrade is controlled by the Hive Operator; schedule depends on a Control Plane Queue

    Args:
        cluster_name (str): The cluster name
        version (str): The version to upgrade the cluster

    """
    cmd = f"rosa upgrade cluster --cluster {cluster_name} --control-plane --version {version} --mode auto --yes"
    proc = exec_cmd(cmd, timeout=2400)
    logger.info(f"Upgrade cluster command output:\n {proc.stdout.decode().strip()}")


def rosa_ocp_version_endpoint():
    """
    Endpoint for getting available OCP versions form ROSA.
    """
    parser = argparse.ArgumentParser(
        description="Get information about available OCP versions from ROSA"
    )
    parser.add_argument(
        "--debug",
        "-d",
        action="store_true",
        default=False,
        help="Print logging messages (mainly useful for debugging).",
    )
    parser.add_argument(
        "--get-available-versions",
        "-a",
        action="store_true",
        default=False,
        help="Get all available OCP versions from ROSA",
    )
    parser.add_argument(
        "--check-available-version",
        "-c",
        action="store",
        default=None,
        metavar="OCP_VERSION",
        help=(
            "Check if provided OCP version is available in ROSA, "
            "if available, returns latest z-stream version"
        ),
    )
    parser.add_argument(
        "--ocsci-conf",
        action="store",
        required=True,
        type=argparse.FileType("r", encoding="UTF-8"),
        help="""OCM Credentials configuration file in yaml format.
            Example file:
                ---
                AUTH:
                  openshiftdedicated:
                    token: '<TOKEN>'
            """,
    )
    args = parser.parse_args()

    if args.debug:
        FORMAT = "%(asctime)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s"
        logging.basicConfig(format=FORMAT, level=logging.DEBUG)

    # load auth data to config
    rosa_conf = args.ocsci_conf
    config_data = yaml.safe_load(rosa_conf)
    framework.config.update(config_data)
    rosa_conf.close()

    utils.add_path_to_env_path(os.path.expanduser(config.RUN["bin_dir"]))
    download_rosa_cli()
    login()

    versions = version.get_ocp_versions_rosa(yaml_format=True)
    if args.get_available_versions:
        for v in versions:
            print(v["raw_id"])
        sys.exit(0)

    if args.check_available_version:
        for v in versions:
            if v["raw_id"].startswith(args.check_available_version):
                print(v["raw_id"])
                sys.exit(0)
        else:
            print(
                f"Version {args.check_available_version} not available in ROSA.",
                file=sys.stderr,
            )
            sys.exit(255)
