import logging
import os
import time

from pyautogui import write, press
from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.ocs.ui.views import locators
from ocs_ci.utility.utils import get_ocp_version
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.ui.base_ui import PageNavigator
from selenium.webdriver.common.by import By
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


def create_storage_class_ui(
    setup_ui,
    encryption=False,
    backend_path=None,
    vault_namespace=None,
    reclaim_policy="Delete",
    provisioner="rbd",
    vol_binding_mode="Immediate",
    service_name=None,
    kms_address=None,
    tls_server_name=None,
):
    """
    Test for creation of storage class with or without encryption via UI

    Args:
            setup_ui: login function on conftest file
            encryption (bool): By default set to False, should be True for Encryption enabled Storage Class
            backend_path (str): name of the vault backend path
            vault_namespace (str): name of the vault namespace
            reclaim_policy (str): value of the reclaim policy, it could be 'Delete' or 'Retain'
            provisioner (str): type of provisioner used, it could be 'rbd' or 'cephfs'
            vol_binding_mode (str): value of the volume binding mode, it could be 'WaitForFirstConsumer' or 'Immediate'
            service_name (str): the default value is None which can be changed in the function call
            kms_address (str): the default value is None which can be changed in the function call
            tls_server_name (str): the default value is None which can be changed in the function call

    Returns:
            sc_name (str) if the storage class creation is successful, returns False otherwise
    """
    base_ui_obj = PageNavigator(setup_ui)

    ocp_version = get_ocp_version()
    pvc_loc = locators[ocp_version]["storage_class"]

    base_ui_obj.navigate_storageclasses_page()
    logger.info("Create Storage Class")
    base_ui_obj.do_click(pvc_loc["create-sc"])
    logger.info("Storage Class Name")
    sc_name = create_unique_resource_name(
        resource_description="test", resource_type="storageclass"
    )
    base_ui_obj.do_send_keys(pvc_loc["sc-name"], f"{sc_name}")
    logger.info("Storage Class Description")
    base_ui_obj.do_send_keys(pvc_loc["sc-description"], "this is a test storage class")
    logger.info("Storage Class Reclaim Policy")
    base_ui_obj.do_click(pvc_loc["reclaim-policy"])
    if reclaim_policy == "Delete":
        base_ui_obj.do_click(pvc_loc["reclaim-policy-delete"])
    elif reclaim_policy == "Retain":
        base_ui_obj.do_click(pvc_loc["reclaim-policy-retain"])

    if ocp_version >= "4.9":
        logger.info("Volume binding mode")
        base_ui_obj.do_click(pvc_loc["volume_binding_mode"])
        if vol_binding_mode == "WaitForFirstConsumer":
            logger.info("select WaitForFirstConsumer")
            base_ui_obj.do_click(pvc_loc["wait_for_first_consumer"])
        elif vol_binding_mode == "Immediate":
            logger.info("select Immediate")
            base_ui_obj.do_click(pvc_loc["immediate"])

    logger.info("Storage Class Provisioner")
    base_ui_obj.do_click(pvc_loc["provisioner"])
    if provisioner == "rbd":
        base_ui_obj.do_click(pvc_loc["rbd-provisioner"])
    elif provisioner == "cephfs":
        base_ui_obj.do_click(pvc_loc["cephfs-provisioner"])
    logger.info("Storage Class Storage Pool")
    base_ui_obj.do_click(pvc_loc["storage-pool"])
    base_ui_obj.do_click(pvc_loc["ceph-block-pool"])
    if encryption:
        logger.info("Storage Class with Encryption")
        base_ui_obj.do_click(pvc_loc["encryption"])
        logger.info("Checking if 'Change connection details' option is available")
        conn_details = base_ui_obj.check_element_text(
            expected_text="Change connection details"
        )
        if conn_details:
            logger.info("Click on Change Connection Details")
            base_ui_obj.do_click(pvc_loc["connections-details"])
        if ocp_version >= "4.9":
            logger.info("Click on Create new KMS connection")
            base_ui_obj.do_click(pvc_loc["new_kms"])
        logger.info("Storage Class Service Name")
        base_ui_obj.do_clear(pvc_loc["service-name"])
        base_ui_obj.do_send_keys(pvc_loc["service-name"], service_name)
        logger.info("Storage Class Address")
        base_ui_obj.do_clear(pvc_loc["kms-address"])
        base_ui_obj.do_send_keys(pvc_loc["kms-address"], kms_address)
        logger.info("Storage Class Port")
        base_ui_obj.do_clear(pvc_loc["kms-port"])
        base_ui_obj.do_send_keys(pvc_loc["kms-port"], "8200")
        logger.info("Click on Advanced Settings")
        base_ui_obj.do_click(pvc_loc["advanced-settings"])
        logger.info("Enter Backend Path")
        base_ui_obj.do_clear(pvc_loc["backend-path"])
        base_ui_obj.do_send_keys(pvc_loc["backend-path"], backend_path)
        logger.info("Enter TLS Server Name")
        base_ui_obj.do_clear(pvc_loc["tls-server-name"])
        base_ui_obj.do_send_keys(pvc_loc["tls-server-name"], tls_server_name)
        logger.info("Enter Vault Enterprise Namespace")
        base_ui_obj.do_clear(pvc_loc["vault-enterprise-namespace"])
        base_ui_obj.do_send_keys(pvc_loc["vault-enterprise-namespace"], vault_namespace)
        logger.info("Selecting CA Certificate")
        print(os.path.abspath(constants.VAULT_CA_CERT_PEM))
        logger.info(os.path.abspath(constants.VAULT_CA_CERT_PEM))
        element = base_ui_obj.driver.find_element_by_xpath("//*[@id='modal-container']/div/div/div/form/div/div[2]/div/div/div[4]/div[2]/div/input")
        base_ui_obj.driver.execute_script("arguments[0].value='typed';",element)
        # base_ui_obj.driver.send_keys(locator=("//*[@id='modal-container']/div/div/div/form/div/div[2]/div/div/div[4]/div[2]/div/input",By.XPATH), text="aaruni")
        # base_ui_obj.do_click(pvc_loc["browse-ca-certificate"])
        # time.sleep(1)
        # write(os.path.abspath(constants.VAULT_CA_CERT_PEM))
        # time.sleep(1)
        # press("enter")
        # time.sleep(1)
        logger.info("Selecting Client Certificate")
        base_ui_obj.send_keys(pvc_loc["browse-client-certificate"], text=os.path.abspath(constants.VAULT_CLIENT_CERT_PEM))
        # base_ui_obj.do_click(pvc_loc["browse-client-certificate"])
        # time.sleep(1)
        # write(os.path.abspath(constants.VAULT_CLIENT_CERT_PEM))
        # time.sleep(1)
        # press("enter")
        # time.sleep(1)
        logger.info("Selecting Client Private Key")
        base_ui_obj.send_keys(pvc_loc["browse-client-private-key"],
                              text=os.path.abspath(constants.VAULT_PRIVKEY_PEM))
        # base_ui_obj.do_click(pvc_loc["browse-client-private-key"])
        # time.sleep(1)
        # write(os.path.abspath(constants.VAULT_PRIVKEY_PEM))
        # time.sleep(1)
        # press("enter")
        # time.sleep(1)
        base_ui_obj.take_screenshot()
        logger.info("Saving Key Management Service Advanced Settings")
        base_ui_obj.do_click(pvc_loc["save-advanced-settings"], enable_screenshot=True)
        time.sleep(1)
        logger.info("Save Key Management Service details")
        base_ui_obj.do_click(pvc_loc["save-service-details"], enable_screenshot=True)
        time.sleep(1)
        logger.info("Creating Storage Class with Encryption")
    else:
        logger.info("Creating storage class without Encryption")
    base_ui_obj.do_click(pvc_loc["create"])
    time.sleep(1)
    logger.info("Verifying if Storage Class is created or not")
    base_ui_obj.navigate_storageclasses_page()
    logger.info("Click on Dropdown and Select Name")
    base_ui_obj.do_click(pvc_loc["sc-dropdown"])
    base_ui_obj.do_click(pvc_loc["name-from-dropdown"])
    logger.info("Search Storage Class with Name")
    base_ui_obj.do_send_keys(pvc_loc["sc-search"], text=sc_name)
    logger.info("Click and Select Storage Class")
    base_ui_obj.do_click(format_locator(pvc_loc["select-sc"], sc_name))
    # Verifying Storage Class Details via UI
    logger.info("Verifying Storage Class Details via UI")
    sc_check = base_ui_obj.check_element_text(expected_text=sc_name)
    if sc_check:
        logger.info(f"Storage Class '{sc_name}' Found")
        return sc_name
    else:
        logger.error(f"Storage Class '{sc_name}' Not Found, Creation Failed")
        return False


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
    elif ocp_version not in ("4.7", "4.8", "4.9"):
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


def delete_storage_class_ui(setup_ui, sc_name):
    """
    Test for deletion of storage class via UI

    Args:
            setup_ui: login function on conftest file
            sc_name (str): Storage class name to be deleted via UI

     Returns:
            Returns True if the Storage class name is not found on Storage class Console page, returns False otherwise

    """
    base_ui_obj = PageNavigator(setup_ui)

    ocp_version = get_ocp_version()
    pvc_loc = locators[ocp_version]["storage_class"]

    base_ui_obj.navigate_storageclasses_page()
    logger.info("Click on Dropdown and Select Name")
    base_ui_obj.do_click(pvc_loc["sc-dropdown"])
    base_ui_obj.do_click(pvc_loc["name-from-dropdown"])
    logger.info("Search Storage Class with Name")
    base_ui_obj.do_send_keys(pvc_loc["sc-search"], text=sc_name)
    logger.info("Click and Select Storage Class")
    base_ui_obj.do_click(format_locator(pvc_loc["select-sc"], sc_name))
    logger.info("Click on Actions")
    base_ui_obj.do_click(pvc_loc["sc-actions"])
    logger.info("Deleting Storage Class")
    base_ui_obj.do_click(pvc_loc["delete-storage-class"])
    logger.info("Approving Storage Class Deletion")
    base_ui_obj.do_click(pvc_loc["approve-storage-class-deletion"])
    time.sleep(2)
    # Verifying if Storage Class is Details or not via UI
    logger.info("Verifying if Storage Class is Deleted or not via UI")
    logger.info("Search Storage Class Name on Storage Class Page")
    sc_check = base_ui_obj.check_element_text(expected_text=sc_name)
    if sc_check:
        logger.error(f"Storage Class '{sc_name}' Found, Deletion via UI failed")
        return False
    else:
        logger.info(f"Storage Class '{sc_name}' Not Found, Deletion successful")
        return True
