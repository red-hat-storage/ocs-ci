# -*- coding: utf8 -*-
"""
Module for interactions with Openshift Dedciated Cluster.
"""

import json
import logging
import os
import re

from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    ManagedServiceAddonDeploymentError,
    UnsupportedPlatformVersionError,
    ConfigurationError,
    ResourceWrongStatusException,
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

    create_account_roles()
    compute_nodes = config.ENV_DATA["worker_replicas"]
    compute_machine_type = config.ENV_DATA["worker_instance_type"]
    multi_az = "--multi-az " if config.ENV_DATA.get("multi_availability_zones") else ""
    rosa_mode = config.ENV_DATA.get("rosa_mode", "")
    private_link = config.ENV_DATA.get("private_link", False)
    machine_cidr = config.ENV_DATA.get("machine_cidr", "10.0.0.0/16")
    subnet_section_name = "ms_subnet_ids_per_region_" + config.ENV_DATA.get(
        "subnet_type", "default"
    )
    cmd = (
        f"rosa create cluster --cluster-name {cluster_name} --region {region} "
        f"--machine-cidr {machine_cidr} --replicas {compute_nodes} "
        f"--compute-machine-type {compute_machine_type} "
        f"--version {rosa_ocp_version} {multi_az}--sts --yes"
    )
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
    elif config.ENV_DATA.get("vpc_name", ""):
        aws = AWSUtil()
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


def get_latest_rosa_version(version):
    """
    Returns latest available z-stream version available for ROSA.

    Args:
        version (str): OCP version in format `x.y`

    Returns:
        str: Latest available z-stream version

    """
    cmd = "rosa list versions"
    output = utils.run_cmd(cmd, timeout=1800)
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


def create_account_roles(prefix="ManagedOpenShift"):
    """
    Create the required account-wide roles and policies, including Operator policies.

    Args:
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
        config.ENV_DATA["rosa_cli_version"], force_download=force_download
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


def delete_operator_roles(cluster_id):
    """
    Delete operator roles of the given cluster

    Args:
        cluster_id (str): the id of the cluster
    """
    cmd = f"rosa delete operator-roles -c {cluster_id} --mode auto --yes"
    utils.run_cmd(cmd, timeout=1200)


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
