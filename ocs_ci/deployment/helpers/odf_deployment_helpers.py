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
        ]
        ocs_operator_names.extend(operators_4_16_additions)
    return ocs_operator_names
