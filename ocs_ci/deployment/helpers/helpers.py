"""
This module contains helper functions which is independent of platform
"""

import logging
import tempfile

from ocs_ci.ocs import constants
from ocs_ci.utility import templating
from ocs_ci.utility.utils import run_cmd

logger = logging.getLogger(__name__)


def mcg_only_deployment():
    """
    Creates cluster with MCG only deployment
    """
    logger.info("Creating storage cluster with MCG only deployment")
    cluster_data = templating.load_yaml(constants.STORAGE_CLUSTER_YAML)
    cluster_data["spec"]["multiCloudGateway"] = {}
    cluster_data["spec"]["multiCloudGateway"]["reconcileStrategy"] = "standalone"
    del cluster_data["spec"]["storageDeviceSets"]
    cluster_data_yaml = tempfile.NamedTemporaryFile(
        mode="w+", prefix="cluster_storage", delete=False
    )
    templating.dump_data_to_temp_yaml(cluster_data, cluster_data_yaml.name)
    run_cmd(f"oc create -f {cluster_data_yaml.name}", timeout=1200)
