import time
import logging
from selenium.webdriver.common.by import By

from ocs_ci.ocs.ui.base_ui import BaseUI
from ocs_ci.ocs.ui.views import pvc


logger = logging.getLogger(__name__)


class PvcUI(BaseUI):
    """
    User Interface Selenium

    """

    def __init__(self, driver):
        super().__init__(driver)

    def navigate_pvc_page(self):
        """
        Navigate to Persistent Volume Claims page

        """
        logger.info("Go to PVC Page")
        self.choose_expanded_mode(
            mode="true", by_locator=pvc["Storage Tab"], type=By.XPATH
        )
        self.do_click(by_locator=pvc["PVC Page"], type=By.LINK_TEXT)

    def create_pvc_ui(self, sc_type, pvc_name, access_mode, pvc_size):
        """
        Create PVC via UI.

        sc_type (str): storage class type
        pvc_name (str): the name of pvc
        access_mode (str): access mode
        pvc_size (str): the size of pvc (GB)

        """
        self.navigate_pvc_page()

        logger.info("Select openshift-storage project")
        self.do_click(pvc["PVC Project Selector"], type=By.CSS_SELECTOR)
        self.do_click(pvc["PVC Select Project openshift-storage"], type=By.CSS_SELECTOR)

        logger.info("Click on 'Create Persistent Volume Claim'")
        self.do_click(pvc["PVC Create Button"])

        logger.info("Select Storage Class")
        self.do_click(pvc["PVC Storage Class Selector"])
        self.do_click(pvc[sc_type])

        logger.info("Select PVC name")
        self.do_send_keys(pvc["PVC Name"], pvc_name)

        logger.info("Select Access Mode")
        self.do_click(pvc[access_mode])

        logger.info("Select PVC size")
        self.do_send_keys(pvc["PVC Size"], text=pvc_size)

        logger.info("Create PVC")
        self.do_click(pvc["PVC Create"])

    def delete_pvc_ui(self, pvc_name):
        """
        Delete pvc via UI

        pvc_name (str): Name of the pvc

        """
        self.navigate_pvc_page()

        logger.info("Select openshift-storage project")
        self.do_click(pvc["PVC Project Selector"], type=By.CSS_SELECTOR)
        self.do_click(pvc["PVC Select Project openshift-storage"], type=By.CSS_SELECTOR)

        self.do_send_keys(
            by_locator='input[data-test-id="item-filter"]',
            type=By.CSS_SELECTOR,
            text=pvc_name,
        )

        logger.info(f"Go to PVC {pvc_name} Page")
        for i in range(2):
            try:
                time.sleep(2)
                self.do_click(pvc["PVC Test"], type=By.CSS_SELECTOR)
            except Exception:
                pass
        logger.info("Click on Actions")
        self.do_click(pvc["PVC Actions"], type=By.CSS_SELECTOR)

        logger.info("Click on 'Delete PVC'")
        self.do_click(pvc["PVC Delete"], type=By.CSS_SELECTOR)

        logger.info("Confirm PVC Deletion")
        self.do_click(pvc["Confirm PVC Deletion"], type=By.CSS_SELECTOR)
