"""
This module contains helpers functions needed for
ODF deployment.
"""

import logging

from ocs_ci.ocs import defaults
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod, get_osd_pods, get_osd_pod_id
from ocs_ci.utility import version
from ocs_ci.ocs.constants import (
    MCLOCK_HIGH_CLIENT_OPS,
    MCLOCK_BALANCED,
    MCLOCK_HIGH_RECOVERY_OPS,
)

logger = logging.getLogger(__name__)


def get_required_csvs():
    """
    Get the mandatory CSVs needed for the ODF cluster

    Returns:
        list: list of CSVs needed

    """
    ocs_version = version.get_semantic_ocs_version_from_config()
    ocs_operator_names = [
        defaults.ODF_CSI_ADDONS_OPERATOR,
        defaults.ODF_OPERATOR_NAME,
        defaults.OCS_OPERATOR_NAME,
        defaults.MCG_OPERATOR,
    ]
    if ocs_version >= version.VERSION_4_16:
        operators_4_16_additions = [
            defaults.ROOK_CEPH_OPERATOR,
            defaults.ODF_PROMETHEUS_OPERATOR,
            defaults.ODF_CLIENT_OPERATOR,
            defaults.RECIPE_OPERATOR,
        ]
        ocs_operator_names.extend(operators_4_16_additions)
    if ocs_version >= version.VERSION_4_17:
        operators_4_17_additions = [defaults.CEPHCSI_OPERATOR]
        ocs_operator_names.extend(operators_4_17_additions)
    if ocs_version >= version.VERSION_4_18:
        operators_4_18_additions = [defaults.ODF_DEPENDENCIES]
        ocs_operator_names.extend(operators_4_18_additions)
    return ocs_operator_names


def set_ceph_config(entity, config_name, value):
    """
    Sets the ceph config values

    Args:
        entity (str): The Ceph entity like "osd", "mon", "mds", etc. but can be "global" as well.
        config_name (str): Name of the Ceph config option (e.g., "bluestore_slow_ops_warn_lifetime").
        value (str): The value to set for the config.

    """
    cmd = f"ceph config set {entity} {config_name} {value}"
    toolbox = get_ceph_tools_pod()
    toolbox.exec_ceph_cmd(cmd)


def is_storage_system_needed():
    """
    Checks whether creation of storage system is needed or not

    Returns:
        bool: True if storage system is need, otherwise False

    """
    storage_system_needed = True
    odf_running_version = version.get_ocs_version_from_csv(only_major_minor=True)
    if odf_running_version >= version.VERSION_4_19:
        storage_system_needed = False
    else:
        logger.debug("Storage system is needed")
    return storage_system_needed


def set_ceph_mclock_config_profile(profile_name, osd_ids=None):
    """
    Set the Ceph mClock profile for the specified OSDs.

    If no OSDs are specified, the profile is applied to all OSDs in the cluster.

    Args:
        profile_name (str): The mClock profile to set ('balanced', 'high_client_ops', 'high_recovery_ops').
        osd_ids (list): List of OSD IDs. If None, the profile is applied to all OSDs.

    Raises:
        ValueError: If the given profile name is invalid.
        CommandFailed: If enabling override or setting the profile fails.

    """
    valid_profiles = {MCLOCK_HIGH_CLIENT_OPS, MCLOCK_BALANCED, MCLOCK_HIGH_RECOVERY_OPS}
    if profile_name not in valid_profiles:
        raise ValueError(
            f"Invalid mClock profile: {profile_name}. Must be one of {valid_profiles}"
        )

    if not osd_ids:
        osd_pods = get_osd_pods()
        osd_ids = [get_osd_pod_id(p) for p in osd_pods]

    toolbox = get_ceph_tools_pod()

    logger.info("Enabling mClock override recovery settings")
    toolbox.exec_ceph_cmd(
        "ceph config set osd osd_mclock_override_recovery_settings true"
    )

    logger.info(f"Setting mClock profile '{profile_name}' for OSDs: {osd_ids}")
    for osd_id in osd_ids:
        set_profile_cmd = (
            f"ceph config set osd.{osd_id} osd_mclock_profile {profile_name}"
        )
        toolbox.exec_ceph_cmd(set_profile_cmd)


def set_ceph_mclock_high_client_recovery_profile(osd_ids=None):
    """
    Apply the 'high_client_ops' mClock profile to specified OSDs (or all OSDs if not provided).

    Args:
        osd_ids (list): List of OSD IDs. If None, the profile is applied to all OSDs.

    Raises:
        CommandFailed: If enabling override or setting the profile fails.

    """
    set_ceph_mclock_config_profile(MCLOCK_HIGH_CLIENT_OPS, osd_ids)


def set_ceph_mclock_balanced_profile(osd_ids=None):
    """
    Apply the 'balanced' mClock profile to specified OSDs (or all OSDs if not provided).

    Args:
        osd_ids (list): List of OSD IDs. If None, the profile is applied to all OSDs.

    Raises:
        CommandFailed: If enabling override or setting the profile fails.

    """
    set_ceph_mclock_config_profile(MCLOCK_BALANCED, osd_ids)


def set_ceph_mclock_high_recovery_profile(osd_ids=None):
    """
    Apply the 'high_recovery_ops' mClock profile to specified OSDs (or all OSDs if not provided).

    Args:
        osd_ids (list): List of OSD IDs. If None, the profile is applied to all OSDs.

    Raises:
        CommandFailed: If enabling override or setting the profile fails.

    """
    set_ceph_mclock_config_profile(MCLOCK_HIGH_RECOVERY_OPS, osd_ids)
