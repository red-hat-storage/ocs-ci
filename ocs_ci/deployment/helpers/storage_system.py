import logging

from ocs_ci.deployment.helpers.odf_deployment_helpers import is_storage_system_needed
from ocs_ci.framework.logger_helper import log_step
from ocs_ci.ocs import constants
from ocs_ci.utility import templating
from ocs_ci.utility.utils import exec_cmd


logger = logging.getLogger(__name__)


def create_storage_system(namespace):
    """
    Create storagesystem if needed.

    Args:
        namespace (str): storagesystem namespace

    """
    if is_storage_system_needed():
        logger.info("Creating StorageSystem")
        # change namespace of storage system if needed
        storage_system_data = templating.load_yaml(constants.STORAGE_SYSTEM_ODF_YAML)
        storage_system_data["metadata"]["namespace"] = namespace
        storage_system_data["spec"]["namespace"] = namespace

        # create storage system
        templating.dump_data_to_temp_yaml(
            storage_system_data, constants.STORAGE_SYSTEM_ODF_YAML
        )
        log_step("Apply StorageSystem CR")
        exec_cmd(f"oc apply -f {constants.STORAGE_SYSTEM_ODF_YAML}")
