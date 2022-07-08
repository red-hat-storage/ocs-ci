import logging
import time
from ocs_ci.ocs.ui.base_ui import PageNavigator
from ocs_ci.ocs.ui.views import locators
from ocs_ci.utility.utils import get_ocp_version
from selenium.webdriver.common.by import By
from ocs_ci.helpers.helpers import create_unique_resource_name

logger = logging.getLogger(__name__)


class StorageClassUI(PageNavigator):
    """
    User Interface Selenium

    """

    def __init__(self, driver):
        super().__init__(driver)
        ocp_version = get_ocp_version()
        self.sc_loc = locators[ocp_version]["storageclass"]

    def create_storageclass(self, pool_name):
        """
        Basic function to create RBD based storageclass

        Args:
            pool_name (str): The pool to choose in the storageclass.

        Return:
            sc_name (str): the name of the storageclass created, otherwise return None.

        """

        self.navigate_overview_page()
        self.navigate_storageclasses_page()
        self.page_has_loaded()
        sc_name = create_unique_resource_name("test", "storageclass")
        self.do_click(self.sc_loc["create_storageclass_button"])
        self.do_send_keys(self.sc_loc["input_storageclass_name"], sc_name)
        self.do_click(self.sc_loc["provisioner_dropdown"])
        self.do_click(self.sc_loc["rbd_provisioner"])
        self.do_click(self.sc_loc["pool_dropdown"])
        self.do_click([f"button[data-test={pool_name}", By.CSS_SELECTOR])
        self.do_click(self.sc_loc["save_storageclass"])
        if self.verify_storageclass_existence(sc_name):
            return sc_name
        else:
            return None

    def verify_storageclass_existence(self, sc_name):
        """
        Check if storageclass is existing in the storageclass list page

        Args:
            sc_name (str): The name of storageclass to verify.

        Return:
              True is it exist otherwise False.

        """

        self.navigate_overview_page()
        self.navigate_storageclasses_page()
        self.page_has_loaded()
        sc_existence = self.wait_until_expected_text_is_found(
            (f"a[data-test-id={sc_name}]", By.CSS_SELECTOR), sc_name, 5
        )
        return sc_existence

    def delete_rbd_storage_class(self, sc_name):
        """
        Delete RBD storageclass

        Args:
            sc_name (str): Name of the storageclass to delete.

        Returns:
            (bool): True if deletion succeeded otherwise False.

        """

        self.navigate_overview_page()
        self.navigate_storageclasses_page()
        self.page_has_loaded()
        logger.info(f"sc_name is {sc_name}")
        self.do_click((f"{sc_name}", By.LINK_TEXT))
        self.do_click(self.sc_loc["action_inside_storageclass"])
        self.do_click(self.sc_loc["delete_inside_storageclass"])
        self.do_click(self.sc_loc["confirm_delete_inside_storageclass"])
        # wait for storageclass to be deleted
        time.sleep(2)
        return not self.verify_storageclass_existence(sc_name)

    def create_encrypted_storage_class_ui(
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
            ca_cert_pem = base_ui_obj.driver.find_element(
                By.XPATH, "(//input[@type='file'])[1]"
            )
            ca_cert_pem.send_keys(os.path.abspath(constants.VAULT_CA_CERT_PEM))
            logger.info("Selecting Client Certificate")
            client_cert_pem = base_ui_obj.driver.find_element(
                By.XPATH, "(//input[@type='file'])[2]"
            )
            client_cert_pem.send_keys(os.path.abspath(constants.VAULT_CLIENT_CERT_PEM))
            logger.info("Selecting Client Private Key")
            client_private_key_pem = base_ui_obj.driver.find_element(
                By.XPATH, "(//input[@type='file'])[3]"
            )
            client_private_key_pem.send_keys(os.path.abspath(constants.VAULT_PRIVKEY_PEM))
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
