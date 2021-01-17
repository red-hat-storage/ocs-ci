import logging

from selenium.webdriver.common.by import By

from ocs_ci.ocs.ui.views import deployment
from ocs_ci.ocs.ui.base_ui import BaseUI


logger = logging.getLogger(__name__)


class DeploymentUI(BaseUI):
    """
    Deployment OCS Operator via User Interface

    """

    def __init__(self, driver):
        super().__init__(driver)

    @property
    def select_mode(self):
        return self.mode

    @select_mode.setter
    def select_mode(self, mode):
        if not isinstance(mode, str):
            raise ValueError("mode arg must be a string")
        self.mode = mode

    @property
    def select_storage_class(self):
        return self.storage_class_type

    @select_storage_class.setter
    def select_storage_class(self, storage_class):
        if not isinstance(storage_class, str):
            raise ValueError("storage class arg must be a string")
        self.storage_class = storage_class

    @property
    def select_osd_size(self):
        return self.osd_size

    @select_osd_size.setter
    def select_osd_size(self, osd_size):
        if not isinstance(osd_size, str):
            raise ValueError("osd size arg must be a string")
        self.osd_size = osd_size

    @property
    def select_encryption(self):
        return self.is_encryption

    @select_encryption.setter
    def select_encryption(self, is_encryption):
        if not isinstance(is_encryption, bool):
            raise ValueError("is_encryption arg must be a bool")
        self.is_encryption = is_encryption

    def navigate_operatorhub(self):
        """
        Navigate to OperatorHub Page

        """
        logger.info("Click On Operators Tab")
        self.choose_expanded_mode(
            mode="true", by_locator=deployment["Operators Tab"], type=By.XPATH
        )
        logger.info("Click On OperatorHub Tab")
        self.do_click(deployment["OperatorHub Tab"], type=By.LINK_TEXT)

    def navigate_installed_operators(self):
        """
        Navigate to Installed Operators page

        """
        logger.info("Click On Installed Operators Tab")
        self.choose_expanded_mode(
            mode="true", by_locator=deployment["Operators Tab"], type=By.XPATH
        )
        logger.info("Click On OperatorHub Tab")
        self.do_click(deployment["Installed Operators Tab"], type=By.LINK_TEXT)

    def install_ocs_opeartor(self):
        """
        Install OCS Opeartor

        """
        self.navigate_operatorhub()

        logger.info("Search OCS Operator")
        self.do_send_keys(
            deployment["Search Operators"],
            text="OpenShift Container Storage",
            type=By.CSS_SELECTOR,
        )

        logger.info("Choose OCS")
        self.do_click(deployment["Choose OCS"])

        logger.info("Click Install OCS")
        self.do_click(deployment["Click Install OCS"], type=By.CSS_SELECTOR)

    def install_storage_cluster(self):
        """
        Install Storage Cluster

        """
        self.navigate_installed_operators()

        logger.info("Click On OCS Installed")
        self.do_click(deployment["OCS Installed"], type=By.CSS_SELECTOR)

        logger.info("Storage Cluster Tab")
        self.do_click(deployment["Storage Cluster Tab"], type=By.CSS_SELECTOR)

        logger.info("Click On 'Create Storage Cluster'")
        self.do_click(deployment["Create Storage Cluster"], type=By.CSS_SELECTOR)

        if self.mode == "internal":
            self.install_internal_cluster()
        elif self.mode == "lso":
            self.install_lso_cluster()

    def install_internal_cluster(self):
        """
        Install Dynamic Cluster

        """
        logger.info("Click On 'Internal'")
        self.do_click(deployment["Internal"], type=By.XPATH)

        logger.info("Select Storage Class")
        self.do_click(deployment["Storage Class Dropdown"], type=By.CSS_SELECTOR)
        self.do_click(deployment["thin"], type=By.CSS_SELECTOR)

        logger.info("Choose OSD Size")
        self.do_click(deployment["OSD Size Dropdown"], type=By.CSS_SELECTOR)
        self.do_click(deployment[self.osd_size], type=By.XPATH)

        if self.is_encryption:
            self.do_click(deployment["Enable Encryption"], type=By.XPATH)

        logger.info("Create Storage Cluster Page")
        self.do_click(deployment["Create Storage Cluster Page"], type=By.XPATH)

    def install_lso_cluster(self):
        """
        Install LSO Cluster

        """
        pass
