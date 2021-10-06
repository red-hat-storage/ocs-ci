# -*- coding: utf8 -*-
"""
Module for version related util functions.
"""

from semantic_version import Version

from ocs_ci.framework import config
from ocs_ci.ocs import defaults


def get_semantic_version(version, only_major_minor=False, ignore_pre_release=False):
    """
    Returning semantic version from provided version as string.

    Args:
        version (str): String version (e.g. 4.6)
        only_major_minor (bool): If True, only major and minor will be parsed.
        ignore_pre_release (bool): If True, the pre release version will be ignored

    Retruns
       semantic_version.base.Version: Object of semantic version.

    """
    version = Version.coerce(version)
    if only_major_minor:
        version.patch = None
        version.prerelease = None
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


def get_semantic_ocs_version_from_config():
    """
    Returning OCS semantic version from config.

    Retruns
       semantic_version.base.Version: Object of semantic version for OCS.

    """
    return get_semantic_version(config.ENV_DATA["ocs_version"], True)


def get_semantic_ocp_version_from_config():
    """
    Returning OCP semantic version from config.

    Retruns
       semantic_version.base.Version: Object of semantic version for OCP.

    """
    return get_semantic_version(config.DEPLOYMENT["installer_version"], True)


def get_ocs_version_from_csv(only_major_minor=False, ignore_pre_release=False):
    """
    Returns semantic OCS Version from the CSV (ODF if version >= 4.9, OCS otherwise)

    Args:
        only_major_minor (bool): If True, only major and minor will be parsed.
        ignore_pre_release (bool): If True, the pre release version will be ignored

    Retruns:
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
