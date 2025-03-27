"""
This module contains helpers functions needed for
ODF deployment.
"""

import logging

from ocs_ci.ocs import defaults
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod
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
