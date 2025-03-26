"""
This module contains helpers functions needed for
ODF deployment.
"""

import logging

from ocs_ci.ocs import defaults
from ocs_ci.utility import version

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


def is_storage_system_needed():
    """
    Checks whether creation of storage system is needed or not

    Returns:
        bool: True if storage system is need, otherwise False

    """
    # Build 4.19.0-59 is stable build where we can create storage system ( normal flow )
    version_for_storage_system = "4.19.0-59"
    odf_version = version.get_semantic_running_odf_version()
    semantic_version_for_storage_system = version.get_semantic_version(
        version_for_storage_system
    )
    return odf_version <= semantic_version_for_storage_system
