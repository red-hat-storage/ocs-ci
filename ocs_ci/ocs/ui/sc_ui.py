import logging
import time

import pyautogui
from pyautogui import write, press
# from seleniumbase import BaseCase

from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait

from ocs_ci.ocs.ui.base_ui import PageNavigator
from ocs_ci.ocs.ui.views import locators
from ocs_ci.utility.utils import get_ocp_version, get_running_ocp_version
from ocs_ci.ocs.ui.base_ui import PageNavigator
from ocs_ci.ocs.ui.views import locators
from ocs_ci.utility.utils import get_ocp_version, get_running_ocp_version
from ocs_ci.ocs import constants
from selenium.webdriver.support import expected_conditions as ec
from ocs_ci.ocs.ui.ui_utils import format_locator

logger = logging.getLogger(__name__)


class PVEncryptionUI(PageNavigator):

    def __init__(self, driver):
        super().__init__(driver)
        ocp_version = get_ocp_version()
        self.pvc_loc = locators[ocp_version]["storage_class"]

    def create_storage_class_with_encryption_ui(self, sc_name="test-storage-class"):
        self.navigate_storageclasses_page()
        logger.info("Create Storage Class")
        self.do_click(self.pvc_loc["create-sc"])
        logger.info("Storage Class Name")
        self.do_send_keys(self.pvc_loc["sc-name"], f"{sc_name}")
        logger.info("Storage Class Description")
        self.do_send_keys(self.pvc_loc["sc-description"], "this is a test storage class")
        logger.info("Storage Class Reclaim Policy")
        self.do_click(self.pvc_loc["reclaim-policy"])
        reclaim_policy_delete = self.pvc_loc["reclaim-policy-delete"]
        # if not reclaim_policy_delete.is_selected():
        self.do_click(self.pvc_loc["reclaim-policy-delete"])
        logger.info("Storage Class Provisioner")
        self.do_click(self.pvc_loc["provisioner"])
        self.do_click(self.pvc_loc["rbd-provisioner"])
        logger.info("Storage Class Storage Pool")
        self.do_click(self.pvc_loc["storage-pool"])
        self.do_click(self.pvc_loc["ceph-block-pool"])
        logger.info("Storage Class with Encryption")
        self.do_click(self.pvc_loc["encryption"])
        logger.info("Storage Class Connection Details")
        self.do_click(self.pvc_loc["connections-details"])
        logger.info("Storage Class Service Name")
        self.do_clear(self.pvc_loc["service-name"])
        self.do_send_keys(self.pvc_loc["service-name"], "test-service")
        logger.info("Storage Class Address")
        self.do_clear(self.pvc_loc["kms-address"])
        self.do_send_keys(self.pvc_loc["kms-address"], "https://www.test-service.com")
        logger.info("Storage Class Port")
        self.do_clear(self.pvc_loc["kms-port"])
        self.do_send_keys(self.pvc_loc["kms-port"], "007")
        logger.info("Click on Advanced Settings")
        self.do_click(self.pvc_loc["advanced-settings"])
        logger.info("Enter Backend Path")
        self.do_send_keys(self.pvc_loc["backend-path"], "IDon'tKnow")
        logger.info("Enter TLS Server Name")
        self.do_send_keys(self.pvc_loc["tls-server-name"], "http://vault.qe.rh-ocs.com/")
        logger.info("Enter Vault Enterprise Namespace")
        self.do_send_keys(self.pvc_loc["vault-enterprise-namespace"], "kms-test-namespace")
        logger.info("Selecting CA Certificate")
        self.do_click(self.pvc_loc["browse-ca-certificate"])
        time.sleep(2)
        write('/home/amagrawa/kms-cert/cert.pem')
        press('enter')
        logger.info("CA Certificate Selected")
        logger.info("Selecting Client Certificate")
        self.do_click(self.pvc_loc["browse-client-certificate"])
        write('/home/amagrawa/kms-cert/fullchain.pem')
        time.sleep(2)
        press('enter')
        logger.info("Client Certificate Selected")
        logger.info("Selecting Client Private Key")
        self.do_click(self.pvc_loc["browse-client-private-key"])
        write('/home/amagrawa/kms-cert/privkey.pem')
        time.sleep(2)
        press('enter')
        logger.info("Private Key Selected")
        logger.info("Saving Key Management Service Advanced Settings")
        self.do_click(self.pvc_loc["save-advanced-settings"])
        logger.info("Creating Storage Class with Encryption")
        self.do_click(self.pvc_loc["create"])
