"""
This module contains functions needed to install IBM Fusion
"""

import logging
import tempfile

from ocs_ci.framework import config
from ocs_ci.ocs.constants import FUSION_SUBSCRIPTION_YAML, ISF_CATALOG_SOURCE_NAME
from ocs_ci.utility import templating
from ocs_ci.utility.utils import run_cmd

logger = logging.getLogger(__name__)


def deploy_fusion():
    """
    Installs IBM Fusion
    """
    logger.info("Installing IBM Fusion")
    # create subscription
    subscription_fusion_file = FUSION_SUBSCRIPTION_YAML
    subscription_fusion_yaml_data = templating.load_yaml(subscription_fusion_file)
    subscription_fusion_yaml_data["spec"]["channel"] = config.DEPLOYMENT["channel"]
    fusion_pre_ga = config.DEPLOYMENT.get("fusion_pre_ga", False)
    if fusion_pre_ga:
        subscription_fusion_yaml_data["spec"]["source"] = ISF_CATALOG_SOURCE_NAME
    subscription_fusion_manifest = tempfile.NamedTemporaryFile(
        mode="w+", prefix="subscription_fusion_manifest", delete=False
    )
    templating.dump_data_to_temp_yaml(
        subscription_fusion_yaml_data, subscription_fusion_manifest.name
    )
    run_cmd(f"oc create -f {subscription_fusion_manifest.name}")
