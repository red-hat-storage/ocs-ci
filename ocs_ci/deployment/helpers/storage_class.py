import logging
import yaml

from ocs_ci.framework import config
from ocs_ci.framework.logger_helper import log_step
from ocs_ci.ocs import constants
from ocs_ci.utility.utils import run_cmd


logger = logging.getLogger(__name__)

DEFAULT_STORAGE_CLASS_MAP = {
    constants.AWS_PLATFORM: "gp2-csi",
    constants.IBMCLOUD_PLATFORM: "ibmc-vpc-block-10iops-tier",
    constants.VSPHERE_PLATFORM: "thin-csi-odf",
    constants.AZURE_PLATFORM: "managed-csi",
    constants.GCP_PLATFORM: None,
    constants.ROSA_HCP_PLATFORM: None,
    constants.RHV_PLATFORM: "ovirt-csi-sc",
    constants.HCI_BAREMETAL: None,
    constants.BAREMETAL_PLATFORM: None,
    constants.FUSIONAAS_PLATFORM: None,
    constants.IBM_POWER_PLATFORM: None,
}


def get_storageclass() -> str:
    """
    Retrieve the storageclass to use from the config or based on platform

    Returns:
        str: Name of the storageclass

    """
    logger.info("Getting storageclass")
    platform = config.ENV_DATA.get("platform")
    customized_deployment_storage_class = config.DEPLOYMENT.get(
        "customized_deployment_storage_class"
    )

    if customized_deployment_storage_class:
        storage_class = customized_deployment_storage_class
    else:
        storage_class = DEFAULT_STORAGE_CLASS_MAP.get(platform)

    logger.info(f"Using storage class: {storage_class}")
    return storage_class


def create_custom_storageclass(storage_class_path: str) -> str:
    """
    Create a custom storageclass using the yaml file defined at the storage_class_path

    Args:
        storage_class_path (str): Filepath to storageclass yaml definition

    Returns:
        str: Name of the storageclass

    """
    with open(storage_class_path, "r") as custom_sc_fo:
        custom_sc = yaml.load(custom_sc_fo, Loader=yaml.SafeLoader)

    storage_class_name = custom_sc["metadata"]["name"]
    log_step(f"Creating custom storage class {storage_class_name}")
    run_cmd(f"oc create -f {storage_class_path}")

    return storage_class_name
