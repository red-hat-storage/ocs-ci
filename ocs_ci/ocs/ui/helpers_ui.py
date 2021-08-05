import logging
import time

from pyautogui import write, press
from webdriver_manager import driver
from selenium.webdriver.common.by import By
from ocs_ci.ocs.ui.views import locators
from ocs_ci.utility.utils import get_ocp_version
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.helpers.proxy import get_cluster_proxies
from ocs_ci.ocs.ui.base_ui import BaseUI, PageNavigator


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

    if platform not in (
        constants.AWS_PLATFORM,
        constants.VSPHERE_PLATFORM,
        constants.AZURE_PLATFORM,
    ):
        logger.info(f"OCS deployment via UI is not supported on platform {platform}")
        return False
    elif ocs_version != ocp_version or ocp_version == "4.6":
        logger.info(
            f"OCS deployment via UI is not supported when the OCS version [{ocs_version}]"
            f" is different from the OCP version [{ocp_version}]"
        )
        return False
    elif (
        is_external
        or is_disconnected
        or is_proxy
        or is_kms
        or is_arbiter
        or is_infra_nodes
    ):
        logger.info(
            "OCS deployment via UI is not supported on "
            "external/disconnected/proxy/kms/arbiter/infra-nodes cluster"
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


def create_storage_class_ui(setup_ui, sc_name="test-storage-class", encryption=False, backend_path=None):
    """
    Test to  creation and deletion of encrypted RBD PVC


    """
    base_ui_obj = PageNavigator(setup_ui)

    ocp_version = get_ocp_version()
    pvc_loc = locators[ocp_version]["storage_class"]

    base_ui_obj.navigate_storageclasses_page()
    logger.info("Create Storage Class")
    base_ui_obj.do_click(pvc_loc["create-sc"])
    logger.info("Storage Class Name")
    base_ui_obj.do_send_keys(pvc_loc["sc-name"], f"{sc_name}")
    logger.info("Storage Class Description")
    base_ui_obj.do_send_keys(pvc_loc["sc-description"], "this is a test storage class")
    logger.info("Storage Class Reclaim Policy")
    base_ui_obj.do_click(pvc_loc["reclaim-policy"])
    base_ui_obj.do_click(pvc_loc["reclaim-policy-delete"])
    logger.info("Storage Class Provisioner")
    base_ui_obj.do_click(pvc_loc["provisioner"])
    base_ui_obj.do_click(pvc_loc["rbd-provisioner"])
    logger.info("Storage Class Storage Pool")
    base_ui_obj.do_click(pvc_loc["storage-pool"])
    base_ui_obj.do_click(pvc_loc["ceph-block-pool"])
    if encryption:
        logger.info("Storage Class with Encryption")
        base_ui_obj.do_click(pvc_loc["encryption"])
        logger.info("Click on Change Connection Details")
        base_ui_obj.do_click(pvc_loc["connections-details"])
        logger.info("Storage Class Service Name")
        base_ui_obj.do_clear(pvc_loc["service-name"])
        base_ui_obj.do_send_keys(pvc_loc["service-name"], "vault")
        logger.info("Storage Class Address")
        base_ui_obj.do_clear(pvc_loc["kms-address"])
        base_ui_obj.do_send_keys(pvc_loc["kms-address"], "http://vault.qe.rh-ocs.com/")
        logger.info("Storage Class Port")
        base_ui_obj.do_clear(pvc_loc["kms-port"])
        base_ui_obj.do_send_keys(pvc_loc["kms-port"], "8200")
        logger.info("Click on Advanced Settings")
        base_ui_obj.do_click(pvc_loc["advanced-settings"])
        logger.info("Enter Backend Path")
        base_ui_obj.do_send_keys(pvc_loc["backend-path"], backend_path)
        logger.info("Enter TLS Server Name")
        base_ui_obj.do_send_keys(pvc_loc["tls-server-name"], "http://vault.qe.rh-ocs.com/")
        logger.info("Enter Vault Enterprise Namespace")
        base_ui_obj.do_send_keys(pvc_loc["vault-enterprise-namespace"], "kms-test-namespace")
        logger.info("Selecting CA Certificate")
        base_ui_obj.do_click(pvc_loc["browse-ca-certificate"])
        time.sleep(2)
        write('/home/amagrawa/kms-cert/cert.pem')
        press('enter')
        logger.info("CA Certificate Selected")
        logger.info("Selecting Client Certificate")
        base_ui_obj.do_click(pvc_loc["browse-client-certificate"])
        write('/home/amagrawa/kms-cert/fullchain.pem')
        time.sleep(2)
        press('enter')
        logger.info("Client Certificate Selected")
        logger.info("Selecting Client Private Key")
        base_ui_obj.do_click(pvc_loc["browse-client-private-key"])
        # write('/home/amagrawa/kms-cert/privkey.pem')
        write("/home/amagrawa/Downloads/Work/ocs-ci/data/vault-privkey.pem")
        time.sleep(2)
        press('enter')
        logger.info("Private Key Selected")
        logger.info("Saving Key Management Service Advanced Settings")
        base_ui_obj.do_click(pvc_loc["save-advanced-settings"])
    logger.info("Creating Storage Class with Encryption")
    base_ui_obj.do_click(pvc_loc["create"])

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
    ):
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
    logging.info("Add capacity via UI")
    setup_ui = login_ui()
    add_ui_obj = AddReplaceDeviceUI(setup_ui)
    add_ui_obj.add_capacity_ui()
    close_browser(setup_ui)
    return new_storage_devices_sets_count


def get_element_type(element_name):
    """
    This function accepts an element name as a argument and returns the element type by creating XPATH for it.
    This is helpful when we are creating dynamic names for PVC's, Pod's, Namespaces's etc. and want to interact
    with the same on UI.

    """

    return (f"//a[contains(@title,'{element_name}')]", By.XPATH)



def delete_storage_class_with_encryption_ui(setup_ui, sc_name="test-storage-class"):
    base_ui_obj = PageNavigator(setup_ui)

    ocp_version = get_ocp_version()
    pvc_loc = locators[ocp_version]["storage_class"]

    base_ui_obj.navigate_storageclasses_page()
    logger.info("Click on Dropdown and Select Name")
    base_ui_obj.do_click(pvc_loc["sc-dropdown"])
    base_ui_obj.do_click(pvc_loc["name-from-dropdown"])

    logger.info("Search Storage Class with Name")
    base_ui_obj.do_send_keys(pvc_loc["sc-search"], text=sc_name)

    logger.info("CLick and Select Storage Class")
    base_ui_obj.do_click(format_locator(pvc_loc["test-project-link"], sc_name))

    logger.info("Click on Actions")
    base_ui_obj.do_click(pvc_loc["sc-actions"])

    logger.info("Deleting Storage Class")
    base_ui_obj.do_click(pvc_loc["delete-storage-class"])
