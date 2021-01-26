import logging
import os
import time
import datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from webdriver_manager.chrome import ChromeDriverManager

from ocs_ci.framework import config as ocsci_config
from ocs_ci.utility.utils import run_cmd, get_kubeadmin_password, get_ocp_version
from ocs_ci.ocs.ui.views import locators


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

    def do_click(self, locator, timeout=30):
        """
        Click on Button/link on OpenShift Console

        locator (set): (GUI element needs to operate on (str), type (By))
        timeout (int): Looks for a web element repeatedly until timeout (sec) happens.

        """
        wait = WebDriverWait(self.driver, timeout)
        element = wait.until(ec.element_to_be_clickable((locator[1], locator[0])))
        screenshot = ocsci_config.UI_SELENIUM.get("screenshot")
        if screenshot:
            self.take_screenshot()
        element.click()

    def do_send_keys(self, locator, text, timeout=30):
        """
        Send text to element on OpenShift Console

        locator (set): (GUI element needs to operate on (str), type (By))
        text (str): Send text to element
        timeout (int): Looks for a web element repeatedly until timeout (sec) happens.

        """
        wait = WebDriverWait(self.driver, timeout)
        element = wait.until(ec.element_to_be_clickable((locator[1], locator[0])))
        element.send_keys(text)
        screenshot = ocsci_config.UI_SELENIUM.get("screenshot")
        if screenshot:
            self.take_screenshot()

    def is_expanded(self, locator, timeout=30):
        """
        Check whether an element is in an expanded or collapsed state

        Args:
            locator (set): (GUI element needs to operate on (str), type (By))
            timeout (int): Looks for a web element repeatedly until timeout (sec) happens.

        return:
            bool: True if element expended, False otherwise

        """
        wait = WebDriverWait(self.driver, timeout)
        element = wait.until(ec.element_to_be_clickable((locator[1], locator[0])))
        return True if element.get_attribute("aria-expanded") == "true" else False

    def choose_expanded_mode(self, mode, locator):
        """
        Select the element mode (expanded or collapsed)

        mode (bool): True if element expended, False otherwise
        locator (set): (GUI element needs to operate on (str), type (By))

        """
        current_mode = self.is_expanded(locator=locator)
        if mode != current_mode:
            self.do_click(locator=locator)

    def get_checkbox_status(self, locator, timeout=30):
        """
        Checkbox Status

        Args:
            locator (set): (GUI element needs to operate on (str), type (By))
            timeout (int): Looks for a web element repeatedly until timeout (sec) happens.

        return:
            bool: True if element is Enabled, False otherwise

        """
        wait = WebDriverWait(self.driver, timeout)
        element = wait.until(ec.element_to_be_clickable((locator[1], locator[0])))
        return element.is_selected()

    def select_checkbox_status(self, status, locator):
        """
        Select checkbox status (enable or disable)

        status (bool): True if checkbox enable, False otherwise
        locator (set): (GUI element needs to operate on (str), type (By))

        """
        current_status = self.get_checkbox_status(locator=locator)
        if status != current_status:
            self.do_click(locator=locator)

    def take_screenshot(self):
        """
        Take screenshot using python code

        """
        time.sleep(1)
        filename = os.path.join(
            self.screenshots_folder,
            f"{datetime.datetime.now().strftime('%Y-%m-%dT%H-%M-%S.%f')}.png",
        )
        logger.info(f"Creating snapshot: {filename}")
        self.driver.save_screenshot(filename)

    def get_text(self, locator, timeout=30, expected_text=None):
        """
        Check if the text matches the expected text.

        Args:
            locator (set): (GUI element needs to operate on (str), type (By))
            timeout (int): Looks for a web element repeatedly until timeout (sec) happens.
            expected_text (string): The expected text.

        return:
            bool: True if the text matches the expected text, False otherwise

        """
        wait = WebDriverWait(self.driver, timeout)
        element = wait.until(ec.element_to_be_clickable((locator[1], locator[0])))
        return element.text == expected_text


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

    ocp_version = get_ocp_version()
    login_loc = locators[ocp_version]["login"]

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
        if not headless:
            chrome_options.add_argument("--headless")

        # chrome_browser_type = ocsci_config.UI_SELENIUM.get("chrome_type")
        chrome_browser_type = "google-chrome"
        driver = webdriver.Chrome(
            ChromeDriverManager(chrome_type=chrome_browser_type).install(),
            chrome_options=chrome_options,
        )
    else:
        raise ValueError(f"Not Support on {browser}")

    wait = WebDriverWait(driver, 30)
    driver.maximize_window()
    driver.get(console_url)
    element = wait.until(
        ec.element_to_be_clickable((login_loc["username"][1], login_loc["username"][0]))
    )
    element.send_keys("kubeadmin")
    element = wait.until(
        ec.element_to_be_clickable((login_loc["password"][1], login_loc["password"][0]))
    )
    element.send_keys(password)
    element = wait.until(
        ec.element_to_be_clickable(
            (login_loc["click_login"][1], login_loc["click_login"][0])
        )
    )
    element.click()
    WebDriverWait(driver, 30).until(ec.title_is(login_loc["ocp_page"]))
    return driver


def close_browser(driver):
    """
    Close Selenium WebDriver

    Args:
        driver (Selenium WebDriver)

    """
    driver.close()
