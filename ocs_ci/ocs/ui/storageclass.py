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
