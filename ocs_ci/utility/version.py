# -*- coding: utf8 -*-
"""
Module for version related util functions.
"""
import logging
import re
from semantic_version import Version
import yaml

from ocs_ci.framework import config
from ocs_ci.ocs import defaults
from ocs_ci.ocs.exceptions import (
    WrongVersionExpression,
    UnsupportedPlatformVersionError,
)
from ocs_ci.ocs import constants

log = logging.getLogger(__name__)


def get_semantic_version(
    version, only_major_minor=False, ignore_pre_release=False, only_major=False
):
    """
    Returning semantic version from provided version as string.

    Args:
        version (str): String version (e.g. 4.6)
        only_major_minor (bool): If True, only major and minor will be parsed.
        ignore_pre_release (bool): If True, the pre release version will be ignored
        only_major(bool): If True, only major will be parsed.

    Returns:
       semantic_version.base.Version: Object of semantic version.

    """
    version = Version.coerce(version)
    if only_major:
        version.minor = None
        version.patch = None
        version.prerelease = None
    elif only_major_minor:
        version.patch = None
        version.prerelease = None
        version.build = None
        version.partial = None
    elif ignore_pre_release:
        version.prerelease = None
    return version


# Version constants
VERSION_4_2 = get_semantic_version("4.2", True)
VERSION_4_3 = get_semantic_version("4.3", True)
VERSION_4_4 = get_semantic_version("4.4", True)
VERSION_4_5 = get_semantic_version("4.5", True)
VERSION_4_6 = get_semantic_version("4.6", True)
VERSION_4_7 = get_semantic_version("4.7", True)
VERSION_4_8 = get_semantic_version("4.8", True)
VERSION_4_9 = get_semantic_version("4.9", True)
VERSION_4_10 = get_semantic_version("4.10", True)
VERSION_4_11 = get_semantic_version("4.11", True)
VERSION_4_12 = get_semantic_version("4.12", True)
VERSION_4_13 = get_semantic_version("4.13", True)
VERSION_4_14 = get_semantic_version("4.14", True)
VERSION_4_15 = get_semantic_version("4.15", True)
VERSION_4_16 = get_semantic_version("4.16", True)
VERSION_4_17 = get_semantic_version("4.17", True)
VERSION_4_18 = get_semantic_version("4.18", True)
VERSION_4_19 = get_semantic_version("4.19", True)
VERSION_4_20 = get_semantic_version("4.20", True)


def get_semantic_ocs_version_from_config(cluster_config=None):
    """
    Returning OCS semantic version from config.

    Returns:
       semantic_version.base.Version: Object of semantic version for OCS.
       cluster_config (MultiClusterConfig): config related to specific cluster in case of multicluster

    """
    if not cluster_config:
        cluster_config = config
    return get_semantic_version(cluster_config.ENV_DATA["ocs_version"], True)


def get_semantic_ocp_version_from_config():
    """
    Returning OCP semantic version from config.

    Returns:
       semantic_version.base.Version: Object of semantic version for OCP.

    """
    return get_semantic_version(config.DEPLOYMENT["installer_version"], True)


def get_semantic_ocp_running_version(separator=None):
    """
    Returning running OCP semantic version from cluster.

    Args:
        separator (str): String that would separate major and
            minor version numbers

    Returns:
       semantic_version.base.Version: Object of semantic version for OCP.

    """
    # Importing here to avoid circular import
    from ocs_ci.utility.utils import get_running_ocp_version

    return get_semantic_version(get_running_ocp_version(separator), True)


def get_ocs_version_from_csv(only_major_minor=False, ignore_pre_release=False):
    """
    Returns semantic OCS Version from the CSV (ODF if version >= 4.9, OCS otherwise)

    Args:
        only_major_minor (bool): If True, only major and minor will be parsed.
        ignore_pre_release (bool): If True, the pre release version will be ignored

    Returns:
        semantic_version.base.Version: Object of semantic version for OCS.

    """
    # Import ocp here to avoid circular dependency issue
    from ocs_ci.ocs import ocp

    csvs = ocp.OCP(
        namespace=config.ENV_DATA["cluster_namespace"], kind="", resource_name="csv"
    )
    if get_semantic_ocs_version_from_config() >= VERSION_4_9:
        operator_name = defaults.ODF_OPERATOR_NAME
    else:
        operator_name = defaults.OCS_OPERATOR_NAME
    for item in csvs.get()["items"]:
        if item["metadata"]["name"].startswith(operator_name):
            return get_semantic_version(
                item["spec"]["version"], only_major_minor, ignore_pre_release
            )


def compare_versions(expression):
    """
    Evaluate version comparison expression

    Args:
        expression (str): expression string like '4.11>=4.2',
            supported operators are: >,<,=>,=<,==,!=

    Returns:
        Boolean: evaluated comparison expression

    """
    pattern = r" *([\d.]+) *([=!<>]{1,2}) *([\d.]+) *"
    m = re.fullmatch(pattern, expression)
    if not m:
        raise WrongVersionExpression(
            f"Expression '{expression}' doesn't match pattern '{pattern}'."
        )
    v1, op, v2 = m.groups()
    return eval(f"get_semantic_version(v1, True){op}get_semantic_version(v2, True)")


def get_previous_version(version, count=1):
    """
    Fetches the nth previous version

    Args:
        version (str): Version ( eg: 4.16, 4.16.0-0.nightly-2024-06-25-194629)
        count (int): previous version count. if count is 1, it will get 1st previous version.
            if count is 2, it will get 2nd previous version.

    Returns:
        str: Previous version ( returns only major and minor version, eg: 4.15 )

    """
    version = get_semantic_version(version, only_major_minor=True)
    new_minor = version.minor - count
    previous_version = f"{version.major}.{new_minor}"
    return previous_version


def get_dr_hub_operator_version(namespace=constants.OPENSHIFT_NAMESPACE):
    """
    Get DR Hub Operator Version

    Returns:
        str: returns version string

    """
    # Importing here to avoid circular dependency
    from ocs_ci.ocs.resources.csv import get_csvs_start_with_prefix

    csv_list = get_csvs_start_with_prefix(
        constants.ACM_ODR_HUB_OPERATOR_RESOURCE, namespace=namespace
    )
    for csv in csv_list:
        if constants.ACM_ODR_HUB_OPERATOR_RESOURCE in csv["metadata"]["name"]:
            # extract version string
            return csv["spec"]["version"]


def get_dr_cluster_operator_version(namespace=constants.OPENSHIFT_NAMESPACE):
    """
    Get DR Cluster Operator Version

    Returns:
        str: returns version string

    """
    # Importing here to avoid circular dependency
    from ocs_ci.ocs.resources.csv import get_csvs_start_with_prefix

    csv_list = get_csvs_start_with_prefix("odr-cluster-operator", namespace=namespace)
    for csv in csv_list:
        if "odr-cluster-operator" in csv["metadata"]["name"]:
            # extract version string
            return csv["spec"]["version"]


def get_odf_multicluster_orchestrator_version(namespace=constants.ACM_HUB_NAMESPACE):
    """
    Get ODF Multicluster Orchestrator Version

    Returns:
        str: returns version string

    """
    # Importing here to avoid circular dependency
    from ocs_ci.ocs.resources.csv import get_csvs_start_with_prefix

    csv_list = get_csvs_start_with_prefix(
        constants.ACM_ODF_MULTICLUSTER_ORCHESTRATOR_RESOURCE,
        namespace=namespace,
    )
    for csv in csv_list:
        if (
            constants.ACM_ODF_MULTICLUSTER_ORCHESTRATOR_RESOURCE
            in csv["metadata"]["name"]
        ):
            # extract version string
            return csv["spec"]["version"]


def get_ocp_gitops_operator_version(namespace=constants.OPENSHIFT_NAMESPACE):
    """
    Get OCP Gitops Operator Version

    Returns:
        str: returns version string

    """
    # Importing here to avoid circular dependency
    from ocs_ci.ocs.resources.csv import get_csvs_start_with_prefix

    csv_list = get_csvs_start_with_prefix(
        constants.GITOPS_OPERATOR_NAME, namespace=namespace
    )
    for csv in csv_list:
        if constants.GITOPS_OPERATOR_NAME in csv["metadata"]["name"]:
            # extract version string
            return csv["spec"]["version"]


def get_submariner_operator_version(namespace=constants.SUBMARINER_OPERATOR_NAMESPACE):
    """
    Get Submariner Operator Version

    Returns:
        str: returns version string

    """
    # Importing here to avoid circular dependency
    from ocs_ci.ocs.resources.csv import get_csvs_start_with_prefix

    csv_list = get_csvs_start_with_prefix("submariner", namespace=namespace)
    for csv in csv_list:
        if "submariner" in csv["metadata"]["name"]:
            # extract version string
            return csv["spec"]["version"]


def get_volsync_operator_version(namespace=constants.SUBMARINER_OPERATOR_NAMESPACE):
    """
    Get VolSync Operator Version

    Returns:
        str: returns version string

    """
    # Importing here to avoid circular dependency
    from ocs_ci.ocs.resources.csv import get_csvs_start_with_prefix

    csv_list = get_csvs_start_with_prefix("volsync", namespace=namespace)
    for csv in csv_list:
        if "volsync" in csv["metadata"]["name"]:
            # extract version string
            return csv["spec"]["version"]


def get_ocp_versions_rosa(yaml_format=False):
    """
    Get the list of available versions for ROSA.

    Args:
        yaml_format (bool): Use yaml output from rosa command and parse it as yaml.

    Returns:
        str: a list of available versions for ROSA in string format
    """
    from ocs_ci.utility.utils import exec_cmd

    yaml_arg = "-o yaml" if yaml_format else ""

    cmd = f"rosa list versions {yaml_arg}"
    output = exec_cmd(cmd, timeout=1800).stdout.decode()
    if yaml_format:
        return yaml.safe_load(output)
    else:
        return output


def ocp_version_available_on_rosa(version):
    """
    Check if requested version is available on ROSA for upgrade

    Args:
        version (str): OCP version in format `x.y.z`

    Returns:
        bool: True if version is supported, False otherwise
    """
    output = get_ocp_versions_rosa()
    return True if version in output else False


def get_next_ocp_version_rosa(version):
    """
    Get the next available minor version for ROSA.

    Args:
        version (str): OCP version in format `x.y.z`

    Returns:
        str: Next available version for ROSA

    """
    # This should return a list of versions in `x.y.z` format in string representation
    output = get_ocp_versions_rosa()

    current_version = Version(version)
    next_version = None

    for line in output.splitlines():
        try:
            available_version = Version(line.split()[0])
            if available_version > current_version:
                next_version = available_version
                break
        except ValueError:
            # Skipping invalid version
            pass

    if next_version is None:
        raise UnsupportedPlatformVersionError(
            f"Could not find any next version after {version} available for ROSA"
        )

    return str(next_version)


def get_latest_rosa_ocp_version(version):
    """
    Returns latest z-stream version available for ROSA.

    Args:
        version (str): OCP version in format `x.y`

    Returns:
        str: Latest available z-stream version

    """
    output = get_ocp_versions_rosa()
    rosa_version = None
    for line in output.splitlines():
        match = re.search(f"^{version}\\.(\\d+) ", line)
        if match:
            rosa_version = match.group(0).rstrip()
            break
    if rosa_version is None:
        error_msg = (
            f"Could not find any version of {version} available for ROSA. "
            f"Try providing an older version of OCP with --ocp-version. "
            f"Latest OCP versions available for ROSA are: \n"
        )
        for i in range(3):
            error_msg += f"{output.splitlines()[i + 1]}"
        raise UnsupportedPlatformVersionError(error_msg)
    return rosa_version


def drop_z_version(version_str):
    """
    Drops the z (patch) version from a semantic version string.

    Args:
        version_str (str): Version string in the format `x.y.z` or `x.y`

    Returns:
        str: Version string in the format `x.y`
    """
    version = Version.coerce(version_str)
    return f"{version.major}.{version.minor}"


def get_running_odf_version():
    """
    Get current running ODF version

    Returns:
        string: ODF version

    """
    # Importing here to avoid circular imports
    from ocs_ci.ocs.resources import csv

    namespace = config.ENV_DATA["cluster_namespace"]
    odf_csv = csv.get_csvs_start_with_prefix(
        csv_prefix=defaults.ODF_OPERATOR_NAME, namespace=namespace
    )
    odf_full_version = odf_csv[0]["metadata"]["labels"]["full_version"]
    log.info(f"ODF full version is {odf_full_version}")
    return odf_full_version


def get_semantic_running_odf_version():
    """
    Get current running ODF semantic version

    Returns:
        semantic_version.base.Version: Object of semantic ODF running version.

    """
    odf_full_version = get_running_odf_version()
    odf_version = get_semantic_version(version=odf_full_version)
    return odf_version
