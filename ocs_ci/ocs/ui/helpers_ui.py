import logging

from ocs_ci.ocs.ui.views import locators
from ocs_ci.utility.utils import get_ocp_version
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.helpers.proxy import get_cluster_proxies
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
    http_proxy, https_proxy, no_proxy = get_cluster_proxies()
    is_proxy = True if http_proxy else False

    try:
        locators[ocp_version]["deployment"]
    except KeyError as e:
        logger.info(
            f"OCS deployment via UI is not supported on ocp version {ocp_version}"
        )
        logger.error(e)
        return False

    if platform not in (constants.AWS_PLATFORM, constants.VSPHERE_PLATFORM):
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
    elif ocp_version == "4.6" and is_lso is True:
        logger.info("OCS deployment via UI is not supported on LSO-OCP4.6")
        return False
    else:
        return True


def format_locator(locator, string_to_insert):
    """
    Use this function format_locator when working with dynamic locators.

    Args:
        locator (tuple): (GUI element needs to operate on (str), type (By))
        string_to_insert (str): Name of the variable (string) which contains the dynamic web element
            when generated on certain action

    return:
        formats the locator using .format() function which takes string to be inserted as an argument

    """
    return locator[0].format(string_to_insert), locator[1]


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
    is_lso = config.DEPLOYMENT.get("local_storage")
    http_proxy, https_proxy, no_proxy = get_cluster_proxies()
    is_proxy = True if http_proxy else False

    try:
        locators[ocp_version]["add_capacity"]
    except KeyError as e:
        logger.info(
            f"Add capacity via UI is not supported on ocp version {ocp_version}"
        )
        logger.error(e)
        return False

    if platform.lower() not in (constants.AWS_PLATFORM, constants.VSPHERE_PLATFORM):
        logger.info(f"Add capacity via UI is not supported on platform {platform}")
        return False
    elif ocp_version not in ("4.7", "4.8"):
        logger.info(
            f"Add capacity via UI is not supported when the OCP version [{ocp_version}]"
        )
        return False
    elif is_external or is_disconnected or is_proxy or is_lso:
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
        if is_lso:
            logger.info(
                "Add capacity via UI is not automated at the moment on lso cluster"
            )
        return False
    else:
        return True


def add_capacity_ui(osd_size_capacity_requested):
    """
    Add storage capacity to the cluster

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
    logging.info("Add capacity via UI")
    setup_ui = login_ui()
    add_ui_obj = AddReplaceDeviceUI(setup_ui)
    add_ui_obj.add_capacity_ui()
    close_browser(setup_ui)
    return new_storage_devices_sets_count
