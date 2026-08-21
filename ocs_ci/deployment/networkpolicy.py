"""
Module for applying NetworkPolicy overrides during ODF deployment and upgrade.
"""

import logging
import os
import tempfile

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.utility import templating
from ocs_ci.utility.utils import exec_cmd

logger = logging.getLogger(__name__)


def apply_allow_all_network_policy():
    """
    Apply an allow-all NetworkPolicy to the storage namespace.
    This overrides any default-deny policies that may block ODF operator
    traffic during installation or upgrade.

    The policy is applied only when DEPLOYMENT['disable_networkpolicy'] is True.
    """
    if not config.DEPLOYMENT.get("disable_networkpolicy"):
        logger.debug(
            "disable_networkpolicy is not set or False, skipping "
            "allow-all NetworkPolicy override."
        )
        return

    namespace = config.ENV_DATA.get(
        "cluster_namespace", constants.OPENSHIFT_STORAGE_NAMESPACE
    )
    logger.info(f"Applying allow-all NetworkPolicy override to namespace '{namespace}'")

    network_policy_data = templating.load_yaml(
        constants.NETWORK_POLICY_ALLOW_ALL_OVERRIDE_TEMPLATE
    )
    network_policy_data["metadata"]["namespace"] = namespace

    network_policy_file = tempfile.NamedTemporaryFile(
        mode="w+", prefix="allow_all_network_policy_", suffix=".yaml", delete=False
    )
    network_policy_file.close()
    try:
        templating.dump_data_to_temp_yaml(network_policy_data, network_policy_file.name)
        exec_cmd(["oc", "apply", "-f", network_policy_file.name])
        logger.info(
            f"Successfully applied allow-all-override NetworkPolicy "
            f"in namespace '{namespace}'"
        )
    finally:
        os.remove(network_policy_file.name)
