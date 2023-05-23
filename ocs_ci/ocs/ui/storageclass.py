import logging
import time
import os
from ocs_ci.ocs.ui.base_ui import PageNavigator
from selenium.webdriver.common.by import By
from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.ocs import constants
from ocs_ci.ocs.ui.helpers_ui import format_locator

logger = logging.getLogger(__name__)


class StorageClassUI(PageNavigator):
    """
    User Interface Selenium

    """

    def __init__(self):
        super().__init__()

    def create_storageclass(self, pool_name):
        """
        Basic function to create RBD based storageclass

        Args:
            pool_name (str): The pool to choose in the storageclass.

        Return:
            sc_name (str): the name of the storageclass created, otherwise return None.

        """
        # self.navigate_cluster_overview_page()
        self.navigate_storageclasses_page()
        self.page_has_loaded()
        sc_name = create_unique_resource_name("test", "storageclass")
        self.do_click(self.sc_loc["create_storageclass_button"])
        self.do_send_keys(self.sc_loc["input_storageclass_name"], sc_name)
        self.do_click(self.sc_loc["volume_binding_mode"])
        self.do_click(self.sc_loc["immediate"])
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

        # self.navigate_cluster_overview_page()
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

        # self.navigate_cluster_overview_page()
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
        self,
        backend_path=None,
        reclaim_policy="Delete",
        provisioner=constants.OCS_PROVISIONERS[0],
        vol_binding_mode="Immediate",
        service_name=None,
        kms_address=None,
        tls_server_name=None,
    ):
        """
        Test for creation of storage class with encryption via UI
        Args:
            backend_path (str): name of the vault backend path
            reclaim_policy (str): value of the reclaim policy, it could be 'Delete' or 'Retain'
            provisioner (str): type of provisioner used, it could be 'rbd' or 'cephfs'
            vol_binding_mode (str): value of the volume binding mode, it could be 'WaitForFirstConsumer' or 'Immediate'
            service_name (str): the default value is None which can be changed in the function call
            kms_address (str): the default value is None which can be changed in the function call
            tls_server_name (str): the default value is None which can be changed in the function call
        Returns:
                sc_name (str) if the storage class creation is successful, returns False otherwise
        """
        self.navigate_storageclasses_page()
        logger.info("Create Storage Class")
        self.do_click(self.sc_loc["create-sc"])
        logger.info("Storage Class Name")
        sc_name = create_unique_resource_name(
            resource_description="test", resource_type="storageclass"
        )
        self.do_send_keys(self.sc_loc["sc-name"], f"{sc_name}")
        logger.info("Storage Class Description")
        self.do_send_keys(self.sc_loc["sc-description"], "this is a test storage class")
        logger.info("Storage Class Reclaim Policy")
        self.do_click(self.sc_loc["reclaim-policy"])
        if reclaim_policy == "Delete":
            self.do_click(self.sc_loc["reclaim-policy-delete"])
        elif reclaim_policy == "Retain":
            self.do_click(self.sc_loc["reclaim-policy-retain"])

        if self.ocp_version >= "4.9":
            logger.info("Volume binding mode")
            self.do_click(self.sc_loc["volume_binding_mode"])
            if vol_binding_mode == "WaitForFirstConsumer":
                logger.info("select WaitForFirstConsumer")
                self.do_click(self.sc_loc["wait_for_first_consumer"])
            elif vol_binding_mode == "Immediate":
                logger.info("select Immediate")
                self.do_click(self.sc_loc["immediate"])

        logger.info("Storage Class Provisioner")
        self.do_click(self.sc_loc["provisioner"])
        if provisioner == constants.OCS_PROVISIONERS[0]:
            self.do_click(self.sc_loc["rbd-provisioner"])
        elif provisioner == constants.OCS_PROVISIONERS[1]:
            self.do_click(self.sc_loc["cephfs-provisioner"])
        logger.info("Storage Class Storage Pool")
        self.do_click(self.sc_loc["storage-pool"])
        self.do_click(self.sc_loc["ceph-block-pool"])

        logger.info("Storage Class with Encryption")
        self.do_click(self.sc_loc["encryption"])
        logger.info("Checking if 'Change connection details' option is available")
        conn_details = self.check_element_text(
            expected_text="Change connection details"
        )
        if conn_details:
            logger.info("Click on Change Connection Details")
            self.do_click(self.sc_loc["connections-details"])
        if self.ocp_version >= "4.9":
            logger.info("Click on Create new KMS connection")
            self.do_click(self.sc_loc["new_kms"])
        logger.info("KMS Service Name")
        self.do_clear(self.sc_loc["service-name"])
        self.do_send_keys(self.sc_loc["service-name"], service_name)
        logger.info("Vault Node Address")
        self.do_clear(self.sc_loc["kms-address"])
        self.do_send_keys(self.sc_loc["kms-address"], kms_address)
        logger.info("Vault Port")
        self.do_clear(self.sc_loc["kms-port"])
        self.do_send_keys(self.sc_loc["kms-port"], "8200")
        logger.info("Click on Advanced Settings")
        self.do_click(self.sc_loc["advanced-settings"])
        logger.info("Enter Backend Path")
        self.do_clear(self.sc_loc["backend-path"])
        self.do_send_keys(self.sc_loc["backend-path"], backend_path)
        logger.info("Enter TLS Server Name")
        self.do_clear(self.sc_loc["tls-server-name"])
        self.do_send_keys(self.sc_loc["tls-server-name"], tls_server_name)
        logger.info("Clear Existing Vault Enterprise Namespace if any")
        time.sleep(1)
        self.do_clear(self.sc_loc["vault-enterprise-namespace"])
        # self.do_send_keys(self.sc_loc["vault-enterprise-namespace"], vault_namespace)
        logger.info("Selecting CA Certificate")
        ca_cert_pem = self.driver.find_element(By.XPATH, "(//input[@type='file'])[1]")
        ca_cert_pem.send_keys(os.path.abspath(constants.VAULT_CA_CERT_PEM))
        logger.info("Selecting Client Certificate")
        client_cert_pem = self.driver.find_element(
            By.XPATH, "(//input[@type='file'])[2]"
        )
        client_cert_pem.send_keys(os.path.abspath(constants.VAULT_CLIENT_CERT_PEM))
        logger.info("Selecting Client Private Key")
        client_private_key_pem = self.driver.find_element(
            By.XPATH, "(//input[@type='file'])[3]"
        )
        client_private_key_pem.send_keys(os.path.abspath(constants.VAULT_PRIVKEY_PEM))
        self.take_screenshot()
        logger.info("Saving Key Management Service Advanced Settings")
        self.do_click(self.sc_loc["save-advanced-settings"], enable_screenshot=True)
        time.sleep(1)
        logger.info("Save Key Management Service details")
        self.do_click(self.sc_loc["save-service-details"], enable_screenshot=True)
        time.sleep(1)
        logger.info("Creating Storage Class with Encryption")
        self.do_click(self.sc_loc["create"])
        time.sleep(1)
        logger.info("Verifying if Storage Class is created or not")
        self.navigate_storageclasses_page()
        logger.info("Click on Dropdown and Select Name")
        self.do_click(self.sc_loc["sc-dropdown"])
        self.do_click(self.sc_loc["name-from-dropdown"])
        logger.info("Search Storage Class with Name")
        self.do_send_keys(self.sc_loc["sc-search"], text=sc_name)
        logger.info("Click and Select Storage Class")
        self.do_click(format_locator(self.sc_loc["select-sc"], sc_name))
        # Verifying Storage Class Details via UI
        logger.info("Verifying Storage Class Details via UI")
        sc_check = self.check_element_text(expected_text=sc_name)
        if sc_check:
            logger.info(f"Storage Class '{sc_name}' Found")
            return sc_name
        else:
            logger.error(f"Storage Class '{sc_name}' Not Found, Creation Failed")
            return False
