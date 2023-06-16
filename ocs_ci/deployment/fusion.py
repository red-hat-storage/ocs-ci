"""
This module contains functions needed to install IBM Fusion
"""

import logging
import tempfile

from ocs_ci.framework import config
from ocs_ci.ocs.constants import FUSION_SUBSCRIPTION_YAML
from ocs_ci.utility import templating
from ocs_ci.utility.utils import run_cmd

logger = logging.getLogger(__name__)


def deploy_fusion():
    """
    Installs cert-manager
    """
    logger.info("Installing IBM Fusion")
    # create subscription
    subscription_fusion_file = FUSION_SUBSCRIPTION_YAML
    subscription_fusion_yaml_data = templating.load_yaml(subscription_fusion_file)
    subscription_fusion_yaml_data["spec"]["channel"] = config.DEPLOYMENT["channel"]
    subscription_fusion_manifest = tempfile.NamedTemporaryFile(
        mode="w+", prefix="subscription_fusion_manifest", delete=False
    )
    templating.dump_data_to_temp_yaml(
        subscription_fusion_yaml_data, subscription_fusion_manifest.name
    )
    run_cmd(f"oc create -f {subscription_fusion_manifest.name}")
