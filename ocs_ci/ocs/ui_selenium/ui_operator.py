import time
import logging
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException

from ocs_ci.utility.utils import run_cmd, get_kubeadmin_password
from ocs_ci.ocs.ui_selenium.const_xpath import xpath_pvc_page

logger = logging.getLogger(__name__)


class UserInterfaceSelenium(object):
    """
    User Interface Selenium

    """

    def __init__(self):
        self.driver = None

    def ui_login(self):
        """
        Login to Openshift Console

        """
        console_url = run_cmd(
            "oc get consoles.config.openshift.io cluster -o"
            "jsonpath='{.status.consoleURL}'"
        )
        password = get_kubeadmin_password()
        password = password.rstrip()
        self.driver = webdriver.Chrome()
        self.driver.get(console_url)
        time.sleep(30)
        self.driver.find_element_by_id("inputUsername").send_keys("kubeadmin")
        self.driver.find_element_by_id("inputPassword").send_keys(password)
        self.click_xpath("/html/body/div/div/main/div/form/div[4]/button")
        time.sleep(30)
        self.driver.get(console_url + "/dashboards")
        time.sleep(20)

    def create_pvc_ui(self, sc_type, pvc_name, access_mode, pvc_size):
        """
        Create PVC via UI.

        sc_type (str): storage class type
        pvc_name (str): the name of pvc
        access_mode (str): access mode
        pvc_size (str): the size of pvc (GB)

        """
        logger.info("Click on Storage Tab")
        self.click_xpath(xpath_pvc_page["Storage Tab"])

        logger.info("Go to PVC Page")
        self.click_xpath(xpath_pvc_page["PVC Page"])

        logger.info("Select openshift-storage project")
        self.click_xpath(xpath_pvc_page["PVC Project Selector"])
        self.click_xpath(xpath_pvc_page["PVC Select Project openshift-storage"])

        logger.info("Click on 'Create Persistent Volume Claim'")
        self.click_xpath(xpath_pvc_page["PVC Create Button"])

        logger.info("Select Storage Class")
        self.click_xpath(xpath_pvc_page["PVC Storage Class Selector"])
        self.click_xpath(xpath_pvc_page[sc_type])

        logger.info("Select PVC name")
        self.send_keys_xpath(xpath_pvc_page["PVC Name"], pvc_name)

        logger.info("Select Access Mode")
        self.click_xpath(xpath_pvc_page[access_mode])

        logger.info("Select PVC size")
        self.send_keys_xpath(xpath_pvc_page["PVC Size"], pvc_size)

        logger.info("Create PVC")
        self.click_xpath(xpath_pvc_page["PVC Create"])

        logger.info("Click on Storage Tab")
        self.click_xpath(xpath_pvc_page["Storage Tab"])

    def delete_pvc_ui(self, pvc_name):
        """
        Delete pvc via UI

        pvc_name (str): Name of the pvc

        """
        logger.info("Click on Storage Tab")
        self.click_xpath(xpath_pvc_page["Storage Tab"])

        logger.info("Go to PVC Page")
        self.click_xpath(xpath_pvc_page["PVC Page"])

        logger.info("Select openshift-storage project")
        self.click_xpath(xpath_pvc_page["PVC Project Selector"])
        self.click_xpath(xpath_pvc_page["PVC Select Project openshift-storage"])

        logger.info(f"Go to PVC {pvc_name} Page")
        for i in range(2):
            try:
                self.driver.find_element_by_partial_link_text(pvc_name).click()
            except NoSuchElementException:
                pass

        logger.info("Click on Actions")
        self.click_xpath(xpath_pvc_page["PVC Actions"])

        logger.info("Click on 'Delete PVC'")
        self.click_xpath(xpath_pvc_page["PVC Delete"])

        logger.info("Confirm PVC Deletion")
        self.click_xpath("//*[@id='confirm-action']")

    def click_xpath(self, xpath):
        """
        Click on Relevant area (xpath)

        xpath (str): XML path used for navigation through the HTML.

        """
        self.driver.find_element_by_xpath(xpath).click()
        time.sleep(3)

    def send_keys_xpath(self, xpath, keys):
        """
        Send keys (strings) to UI.

        xpath (str): XML path used for navigation through the HTML.
        keys (str): Send key (string) to UI

        """
        self.driver.find_element_by_xpath(xpath).send_keys(keys)
        time.sleep(3)

    def cleanup(self):
        """
        Close Web Browser

        """
        self.driver.close()
