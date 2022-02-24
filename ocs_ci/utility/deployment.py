"""
Utility functions that are used as a part of OCP or OCS deployments
"""
import logging
import re
import tempfile

import requests

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import ExternalClusterDetailsException
from ocs_ci.utility import templating
from ocs_ci.utility.utils import run_cmd

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
