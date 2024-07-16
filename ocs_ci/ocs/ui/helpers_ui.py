import logging

from selenium.webdriver.common.by import By
from ocs_ci.ocs.ui.views import locators
from ocs_ci.utility.utils import get_ocp_version
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.ui.base_ui import login_ui, close_browser
from ocs_ci.ocs.ui.add_replace_device_ui import AddReplaceDeviceUI
from ocs_ci.ocs.resources.storage_cluster import get_deviceset_count, get_osd_size

logger = logging.getLogger(__name__)


def ui_deployment_conditions():
    """
    Conditions for installing the OCS operator via UI

    return:
        bool: True if support UI deployment, False otherwise
    """
    platform = config.ENV_DATA["platform"]
    ocp_version = get_ocp_version()
    ocs_version = config.ENV_DATA.get("ocs_version")
    is_arbiter = config.DEPLOYMENT.get("arbiter_deployment")
    is_lso = config.DEPLOYMENT.get("local_storage")
    is_external = config.DEPLOYMENT["external_mode"]
    is_disconnected = config.DEPLOYMENT.get("disconnected")
    is_kms = config.DEPLOYMENT.get("kms_deployment")
    is_proxy = config.DEPLOYMENT.get("proxy")
    is_infra_nodes = config.DEPLOYMENT.get("infra_nodes")

    try:
        locators[ocp_version]["deployment"]
    except KeyError as e:
        logger.info(
            f"OCS deployment via UI is not supported on ocp version {ocp_version}"
        )
        logger.error(e)
        return False

    if platform.lower() not in (
        constants.AWS_PLATFORM,
        constants.VSPHERE_PLATFORM,
        constants.AZURE_PLATFORM,
        constants.GCP_PLATFORM,
        constants.BAREMETAL_PLATFORM,
    ):
        logger.info(f"OCS deployment via UI is not supported on platform {platform}")
        return False
    elif ocs_version != ocp_version or ocp_version == "4.6":
        logger.info(
            f"OCS deployment via UI is not supported when the OCS version [{ocs_version}]"
            f" is different from the OCP version [{ocp_version}]"
        )
        return False
    elif is_external or is_disconnected or is_proxy or is_kms or is_arbiter:
        logger.info(
            "OCS deployment via UI is not supported on "
            "external/disconnected/proxy/kms/arbiter cluster"
        )
        return False
    elif platform == constants.AWS_PLATFORM and is_lso is True:
        logger.info("OCS deployment via UI is not supported on AWS-LSO")
        return False
    elif platform == constants.AZURE_PLATFORM and is_lso is True:
        logger.info("OCS deployment via UI is not supported on AZURE-LSO")
        return False
    elif ocp_version == "4.6" and is_lso is True:
        logger.info("OCS deployment via UI is not supported on LSO-OCP4.6")
        return False
    elif is_infra_nodes and ocp_version != "4.10":
        logger.info("Infra node checkbox exist only on OCP4.10")
        return False
    else:
        return True


def format_locator(locator, *args):
    """
    Use this function format_locator when working with dynamic locators.

    Args:
        locator (tuple): (GUI element needs to operate on (str), type (By))
        *args (str): Name of the variable (string) which contains the dynamic web element
            when generated on certain action

    return:
        formats the locator using .format() function which takes string to be inserted as an argument

    """
    return locator[0].format(*args), locator[1]


def ui_add_capacity_conditions():
    """
    Conditions for add capacity via UI

    return:
        bool: True if support UI add capacity, False otherwise
    """
    platform = config.ENV_DATA["platform"]
    ocp_version = get_ocp_version()
    is_external = config.DEPLOYMENT["external_mode"]
    is_disconnected = config.DEPLOYMENT.get("disconnected")
    is_proxy = config.DEPLOYMENT.get("proxy")

    try:
        locators[ocp_version]["add_capacity"]
    except KeyError as e:
        logger.info(
            f"Add capacity via UI is not supported on ocp version {ocp_version}"
        )
        logger.error(e)
        return False

    if platform.lower() not in (
        constants.AWS_PLATFORM,
        constants.VSPHERE_PLATFORM,
        constants.AZURE_PLATFORM,
        constants.GCP_PLATFORM,
    ):
        logger.info(f"Add capacity via UI is not supported on platform {platform}")
        return False
    elif ocp_version not in (
        "4.7",
        "4.8",
        "4.9",
        "4.10",
        "4.11",
        "4.12",
        "4.13",
        "4.14",
        "4.15",
        "4.16",
        "4.17",
    ):
        logger.info(
            f"Add capacity via UI is not supported when the OCP version [{ocp_version}]"
        )
        return False
    elif is_external or is_disconnected or is_proxy:
        if is_external:
            logger.info(
                "Add capacity via UI is not automated at the moment on external cluster"
            )
        if is_disconnected:
            logger.info(
                "Add capacity via UI is not automated at the moment on disconnected cluster"
            )
        if is_proxy:
            logger.info(
                "Add capacity via UI is not automated at the moment on proxy cluster"
            )
        return False
    else:
        return True


def ui_add_capacity(osd_size_capacity_requested):
    """
    Add Capacity via UI

    Args:
        osd_size_capacity_requested (int): Requested osd size capacity

    Returns:
        new_storage_devices_sets_count (int) : Returns True if all OSDs are in Running state

    """
    osd_size_existing = get_osd_size()
    device_sets_required = int(osd_size_capacity_requested / osd_size_existing)
    old_storage_devices_sets_count = get_deviceset_count()
    new_storage_devices_sets_count = int(
        device_sets_required + old_storage_devices_sets_count
    )
    logger.info("Add capacity via UI")
    login_ui()
    add_ui_obj = AddReplaceDeviceUI()
    add_ui_obj.add_capacity_ui()
    close_browser()
    return new_storage_devices_sets_count


def get_element_type(element_name):
    """
    This function accepts an element name as a argument and returns the element type by creating XPATH for it.
    This is helpful when we are creating dynamic names for PVC's, Pod's, Namespaces's etc. and want to interact
    with the same on UI.

    """

    return (f"//a[contains(@title,'{element_name}')]", By.XPATH)


def get_element_by_text(text):
    """
    This function accepts a text as an argument and returns the element type by creating XPATH for it.
    This is helpful when we are creating dynamic names for PVC's, Pod's, Namespaces's etc. and want to interact
    with the same on UI.

    """
    return (f"//*[text()= '{text}']", By.XPATH)


def is_ui_deployment():
    """
    This function checks if the current deployment is UI deployment or not.

    """

    if (
        (config.RUN["kubeconfig"] is not None)
        and (config.DEPLOYMENT["ui_deployment"])
        and (ui_deployment_conditions())
    ):
        return True

    return False
