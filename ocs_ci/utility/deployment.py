"""
Utility functions that are used as a part of OCP or OCS deployments
"""

import logging
import os
import re
import tempfile
from datetime import datetime

import yaml

import requests

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed, ExternalClusterDetailsException
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility import templating
from ocs_ci.utility.utils import (
    create_directory_path,
    exec_cmd,
    run_cmd,
    wait_for_machineconfigpool_status,
)

logger = logging.getLogger(__name__)


def get_ocp_ga_version(channel):
    """
    Retrieve the latest GA version for

    Args:
        channel (str): the OCP version channel to retrieve GA version for

    Returns:
        str: latest GA version for the provided channel.
            An empty string is returned if no version exists.


    """
    logger.debug("Retrieving GA version for channel: %s", channel)
    url = "https://api.openshift.com/api/upgrades_info/v1/graph"
    headers = {"Accept": "application/json"}
    payload = {"channel": f"stable-{channel}"}
    r = requests.get(url, headers=headers, params=payload)
    nodes = r.json()["nodes"]
    if nodes:
        versions = [node["version"] for node in nodes]
        versions.sort()
        ga_version = versions[-1]
        logger.debug("Found GA version: %s", ga_version)
        return ga_version
    logger.debug("No GA version found")
    return ""


def create_external_secret(ocs_version=None, apply=False):
    """
    Creates secret data for external cluster

    Args:
         ocs_version (str): OCS version
         apply (bool): True if want to use apply instead of create command

    """
    ocs_version = ocs_version or config.ENV_DATA["ocs_version"]
    secret_data = templating.load_yaml(constants.EXTERNAL_CLUSTER_SECRET_YAML)
    external_cluster_details = config.EXTERNAL_MODE.get("external_cluster_details", "")
    if not external_cluster_details:
        raise ExternalClusterDetailsException("No external cluster data found")
    secret_data["data"]["external_cluster_details"] = external_cluster_details
    if config.DEPLOYMENT.get("multi_storagecluster"):
        secret_data["metadata"][
            "namespace"
        ] = constants.OPENSHIFT_STORAGE_EXTENDED_NAMESPACE
    secret_data_yaml = tempfile.NamedTemporaryFile(
        mode="w+", prefix="external_cluster_secret", delete=False
    )
    templating.dump_data_to_temp_yaml(secret_data, secret_data_yaml.name)
    logger.info(f"Creating external cluster secret for OCS version: {ocs_version}")
    oc_type = "apply" if apply else "create"
    run_cmd(f"oc {oc_type} -f {secret_data_yaml.name}")


def get_cluster_prefix(cluster_name, special_rules):
    """
    Parse out the "prefix" of a cluster name. Note this is not the same thing as the
    CLUSTER_PREFIX in jenkins. In fact we will parse that value out. This  "cluster
    prefix" is used to check cloud providers to see if a particular user already has
    a cluster created. This is to stop people from using too many cloud resources at
    one time.

    Args:
        cluster_name (str): name of the cluster
        special_rules (dict): dictionary containing special prefix rules that allow
            clusters to remain alive longer than our default value

    Returns:
        str: cluster name prefix

    """
    prefix, _, tier = cluster_name.rpartition("-")
    for pattern in special_rules.keys():
        if bool(re.match(pattern, prefix, re.I)):
            logger.debug("%s starts with %s", cluster_name, pattern)
            prefix = re.sub(pattern, "", prefix)
            break
    # If `prefix` is an empty string we should assume that there was no hyphen
    # in the cluster name and that the value for `tier` is what we should use.
    prefix = prefix or tier
    # Remove potential leading hyphen
    if prefix.startswith("-"):
        prefix = prefix[1:]
    return prefix


def get_ocp_release_image():
    """
    Get the url of ocp release image
    * from DEPLOYMENT["custom_ocp_image"] or
    * from openshift-install version command output

    Returns:
        str: Release image of the openshift installer

    """
    if not config.DEPLOYMENT.get("ocp_image"):
        if config.DEPLOYMENT.get("custom_ocp_image"):
            config.DEPLOYMENT["ocp_image"] = config.DEPLOYMENT.get("custom_ocp_image")
        else:
            config.DEPLOYMENT["ocp_image"] = get_ocp_release_image_from_installer()
    return config.DEPLOYMENT["ocp_image"]


def get_ocp_release_image_from_installer():
    """
    Retrieve release image using the openshift installer.

    Returns:
        str: Release image of the openshift installer

    """
    logger.info("Retrieving release image from openshift installer")
    installer_path = config.ENV_DATA["installer_path"]
    cmd = f"{installer_path} version"
    proc = exec_cmd(cmd)
    for line in proc.stdout.decode().split("\n"):
        if "release image" in line:
            return line.split(" ")[2].strip()


def workaround_mark_disks_as_ssd():
    """
    This function creates MachineConfig defining new service `workaround-ssd`, which configures all disks as SSD
    (not rotational).
    This is useful for example on some Bare metal servers where are SSD disks not properly recognized as SSD, because of
    wrong RAID controller configuration or issue.
    """
    try:
        logger.info("WORKAROUND: mark disks as ssd (non rotational)")
        mc_yaml_file = templating.load_yaml(constants.MC_WORKAROUND_SSD)
        mc_yaml = OCS(**mc_yaml_file)
        mc_yaml.create()
        wait_for_machineconfigpool_status("all")
        logger.info("WORKAROUND: disks marked as ssd (non rotational)")
    except CommandFailed as err:
        if "AlreadyExists" in str(err):
            logger.info("Workaround already applied.")
        else:
            raise err


def create_openshift_install_log_file(cluster_path, console_url):
    """
    Workaround.
    Create .openshift_install.log file containing URL to OpenShift console.
    It is used by our CI jobs to show the console URL in build description.

    Args:
        cluster_path (str): The path to the cluster directory.
        console_url (str): The address of the OpenShift cluster management-console
    """
    installer_log_file = os.path.join(cluster_path, ".openshift_install.log")
    formatted_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    logger.info(f"Cluster URL: {console_url}")
    with open(installer_log_file, "a") as fd:
        fd.writelines(
            [
                "W/A for our CI to get URL to the cluster in jenkins job. "
                "Cluster is deployed via some kind of managed deployment (Assisted Installer API or ROSA). "
                "OpenShift Installer (IPI or UPI deployment) were not used!\n"
                f'time="{formatted_time}" level=info msg="Access the OpenShift web-console here: '
                f"{console_url}\"\n'",
            ]
        )
    logger.info("Created '.openshift_install.log' file")


def get_and_apply_idms_from_catalog(image, apply=True, insecure=False):
    """
    Get IDMS from catalog image (if exists) and apply it on the cluster (if
    requested).

    Args:
        image (str): catalog image of ocs registry.
        apply (bool): controls if the IDMS should be applied or not
            (default: true)
        insecure (bool): If True, it allows push and pull operations to registries to be made over HTTP

    Returns:
        str: path to the idms.yaml file or empty string, if idms not available
            in the catalog image

    """
    stage_testing = config.DEPLOYMENT.get("stage_rh_osbs")
    konflux_build = config.DEPLOYMENT.get("konflux_build")
    if stage_testing and konflux_build:
        logger.info("Skipping applying IDMS rules from image for konflux stage testing")
        return ""
    idms_file_location = "/idms.yaml"
    idms_file_dest_dir = os.path.join(
        config.ENV_DATA["cluster_path"], f"idms-{config.RUN['run_id']}"
    )
    idms_file_dest_location = os.path.join(idms_file_dest_dir, "idms.yaml")
    pull_secret_path = os.path.join(constants.DATA_DIR, "pull-secret")
    create_directory_path(idms_file_dest_dir)
    cmd = (
        f"oc image extract --filter-by-os linux/amd64 --registry-config {pull_secret_path} "
        f"{image} --confirm "
        f"--path {idms_file_location}:{idms_file_dest_dir}"
    )
    if insecure:
        cmd = f"{cmd} --insecure"
    exec_cmd(cmd)
    if not os.path.exists(idms_file_dest_location):
        return ""

    # make idms name unique - append run_id
    with open(idms_file_dest_location) as f:
        idms_content = yaml.safe_load(f)
    idms_content["metadata"]["name"] += f"-{config.RUN['run_id']}"
    with open(idms_file_dest_location, "w") as f:
        yaml.dump(idms_content, f)

    if apply and not config.DEPLOYMENT.get("disconnected"):
        exec_cmd(f"oc apply -f {idms_file_dest_location}")
        managed_ibmcloud = (
            config.ENV_DATA["platform"] == constants.IBMCLOUD_PLATFORM
            and config.ENV_DATA["deployment_type"] == "managed"
        )
        if not managed_ibmcloud:
            num_nodes = (
                config.ENV_DATA["worker_replicas"]
                + config.ENV_DATA["master_replicas"]
                + config.ENV_DATA.get("infra_replicas", 0)
            )
            timeout = 2800 if num_nodes > 6 else 1900
            wait_for_machineconfigpool_status(node_type="all", timeout=timeout)

    return idms_file_dest_location
