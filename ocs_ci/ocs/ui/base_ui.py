import logging
import os
import time
import datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

from ocs_ci.framework import config as ocsci_config
from ocs_ci.utility.utils import run_cmd, get_kubeadmin_password
from ocs_ci.ocs.ui.views import login


logger = logging.getLogger(__name__)


class BaseUI:
    """
    Base Class for UI Tests

    """

    def __init__(self, driver):
        self.driver = driver
        self.screenshots_folder = os.path.join(
            os.path.expanduser(ocsci_config.RUN["log_dir"]),
            f"screenshots_ui_{ocsci_config.RUN['run_id']}",
        )
        os.mkdir(self.screenshots_folder)
        logger.info(f"screenshots pictures:{self.screenshots_folder}")

    def do_click(self, by_locator, type=By.XPATH, timeout=30):
        """
        Click on Button/link on OpenShift Console

        by_locator (str): GUI element needs to operate on.
        type (By): Set of supported locator strategies.
        timeout (int): Looks for a web element repeatedly until timeout (sec) happens.

        """
        screenshot = ocsci_config.UI_SELENIUM.get("screenshot")
        if screenshot:
            self.take_screenshot()
        wait = WebDriverWait(self.driver, timeout)
        element = wait.until(ec.element_to_be_clickable((type, by_locator)))
        element.click()

    def do_send_keys(self, by_locator, text, type=By.XPATH, timeout=30):
        """
        Send text to element on OpenShift Console

        by_locator (str): GUI element needs to operate on.
        text (str): Send text to element
        type (By): Set of supported locator strategies
        timeout (int): Looks for a web element repeatedly until timeout (sec) happens.

        """
        wait = WebDriverWait(self.driver, timeout)
        element = wait.until(ec.element_to_be_clickable((type, by_locator)))
        element.send_keys(text)
        screenshot = ocsci_config.UI_SELENIUM.get("screenshot")
        if screenshot:
            self.take_screenshot()

    def is_expanded(self, by_locator, type=By.XPATH, timeout=30):
        """
        Check whether an element is in an expanded or collapsed state

        Args:
            by_locator (str): GUI element needs to operate on.
            type (By): Set of supported locator strategies
            timeout (int): Looks for a web element repeatedly until timeout (sec) happens.

        return:
            bool: True if element expended, False otherwise

        """
        wait = WebDriverWait(self.driver, timeout)
        element = wait.until(ec.element_to_be_clickable((type, by_locator)))
        return True if element.get_attribute("aria-expanded") == "true" else False

    def choose_expanded_mode(self, mode, by_locator, type):
        """
        Select the element mode (expanded or collapsed)

        mode (bool): True if element expended, False otherwise
        by_locator (str): GUI element needs to operate on.
        type (By): Set of supported locator strategies

        """
        current_mode = self.is_expanded(by_locator=by_locator, type=type)
        if mode != current_mode:
            self.do_click(by_locator=by_locator, type=type)

    def take_screenshot(self):
        """
        Take screenshot using python code

        """
        time.sleep(1)
        current_date_and_time = datetime.datetime.now()
        filename = os.path.join(
            self.screenshots_folder,
            f"{(str(current_date_and_time)).replace(' ','_')}.png",
        )
        logger.info(f"Creating snapshot:{filename}")
        self.driver.save_screenshot(filename)


def login_ui():
    """
    Login to OpenShift Console

    return:
        driver (Selenium WebDriver)

    """
    logger.info("Get URL of OCP console")
    console_url = run_cmd(
        "oc get consoles.config.openshift.io cluster -o"
        "jsonpath='{.status.consoleURL}'"
    )
    logger.info("Get password of OCP console")
    password = get_kubeadmin_password()
    password = password.rstrip()
    browser = ocsci_config.UI_SELENIUM.get("browser_type")
    if browser == "chrome":
        logger.info("chrome browser")
        chrome_options = Options()

        ignore_ssl = ocsci_config.UI_SELENIUM.get("ignore_ssl")
        if ignore_ssl:
            chrome_options.add_argument("--ignore-ssl-errors=yes")
            chrome_options.add_argument("--ignore-certificate-errors")

        # headless browsers are web browsers without a GUI
        headless = ocsci_config.UI_SELENIUM.get("headless")
        if headless:
            chrome_options.add_argument("--headless")

        chrome_browser_type = ocsci_config.UI_SELENIUM.get("chrome_type")
        driver = webdriver.Chrome(
            ChromeDriverManager(chrome_type=chrome_browser_type).install(),
            chrome_options=chrome_options,
        )
    else:
        raise ValueError(f"Not Support on {browser}")

    wait = WebDriverWait(driver, 30)
    driver.maximize_window()
    driver.get(console_url)
    element = wait.until(ec.element_to_be_clickable((By.ID, "inputUsername")))
    element.send_keys("kubeadmin")
    element = wait.until(ec.element_to_be_clickable((By.ID, "inputPassword")))
    element.send_keys(password)
    element = wait.until(
        ec.element_to_be_clickable(
            (By.XPATH, "/html/body/div/div/main/div/form/div[4]/button")
        )
    )
    element.click()
    WebDriverWait(driver, 30).until(ec.title_is(login["OCP Page"]))
    return driver


def close_browser(driver):
    """
    Close Selenium WebDriver

    Args:
        driver (Selenium WebDriver)

    """
    driver.close()
