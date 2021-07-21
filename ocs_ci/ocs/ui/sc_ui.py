import logging
import time

from ocs_ci.ocs.ui.base_ui import PageNavigator
from ocs_ci.ocs.ui.views import locators
from ocs_ci.utility.utils import get_ocp_version, get_running_ocp_version

logger = logging.getLogger(__name__)


class PVEncryptionUI(PageNavigator):

    def __init__(self, driver):
        super().__init__(driver)
        ocp_version = get_ocp_version()
        self.pvc_loc = locators[ocp_version]["pvc"]

    def create_storage_class_with_encryption_ui(self):

        self.navigate_storageclasses_page()

        logger.info("Create Storage Class")
        self.do_click(self.pvc_loc["create-sc"])
        logger.info("Storage Class Name")
        self.do_send_keys(self.pvc_loc["sc-name"], "test-storage-class")
        logger.info("Storage Class Description")
        self.do_send_keys(self.pvc_loc["sc-description"], "this is a test storage class")
        logger.info("Storage Class Reclaim Policy")
        self.do_click(self.pvc_loc["reclaim-policy"])
        reclaim_policy_delete = self.pvc_loc["reclaim-policy-delete"]
        if not reclaim_policy_delete.is_selected():
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
        self.do_send_keys(self.pvc_loc["service-name"], "test-service")
        logger.info("Storage Class Address")
        self.do_clear(self.pvc_loc["kms-address"])
        self.do_send_keys(self.pvc_loc["kms-address"], "https://www.test-service.com")
        logger.info("Storage Class Port")
        self.do_send_keys(self.pvc_loc["kms-port"], "007")
        logger.info("Click on Save")
        self.do_click(self.pvc_loc["save-btn"])
        logger.info("Checking Selection of PVC Expansion")
        pvc_expansion_check = self.pvc_loc["reclaim-policy-delete"]
        if not pvc_expansion_check.is_selected():
            self.do_click(self.pvc_loc["pvc-expansion-check"])
        logger.info("Creating Storage Class with Encryption")
        self.do_click(self.pvc_loc["create"])
