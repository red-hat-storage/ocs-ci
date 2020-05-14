"""
This module contains local-storage related methods
"""
import logging
import os
import shutil
import yaml

from ocs_ci.utility.utils import clone_repo, run_cmd
from ocs_ci.ocs import constants

logger = logging.getLogger(__name__)


def fetch_all_device_paths():
    """
    Return all device paths inside worker nodes

    Returns:
        list : List containing all device paths

    """
    path = os.path.join(constants.EXTERNAL_DIR, "device-by-id-ocp")
    clone_repo(constants.OCP_QE_DEVICEPATH_REPO, path)
    os.chdir(path)
    logger.info("Running script to fetch device paths...")
    run_cmd("ansible-playbook devices_by_id.yml")
    with open("local-storage-block.yaml") as local_storage_block:
        local_block = yaml.load(local_storage_block, Loader=yaml.FullLoader)
        dev_paths = local_block["spec"]["storageClassDevices"][0]["devicePaths"]
    logger.info(f"All devices are {dev_paths}")
    os.chdir(constants.TOP_DIR)
    shutil.rmtree(path)
    return dev_paths
