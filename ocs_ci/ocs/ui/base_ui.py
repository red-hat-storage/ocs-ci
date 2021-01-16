import logging
import tempfile
import os

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.utils import ChromeType


from ocs_ci.utility.utils import run_cmd, get_kubeadmin_password
from ocs_ci.ocs.ui.views import login


logger = logging.getLogger(__name__)


class BaseUI:
    """
    Base Class for UI Tests

    """

    def __init__(self, driver):
        self.driver = driver
        self.screenshots_folder = tempfile.mkdtemp()
        logger.info(f"screenshots pictures:{self.screenshots_folder}")
        self.cnt_screenshot = 0

    def do_click(self, by_locator, type=By.XPATH, timeout=30, screenshot=True):
        """
        Click on Button/link on OpenShift Console

        by_locator(str): Command that tells Selenium IDE which
        GUI element needs to operate on.
        type(By): Set of supported locator strategies.
        timeout(int): Looks for a web element repeatedly until timeout(sec) happens.
        screenshot(bool): True- take screenshot, False-no take screenshot

        """
        wait = WebDriverWait(self.driver, timeout)
        element = wait.until(ec.element_to_be_clickable((type, by_locator)))
        element.click()
        if screenshot:
            self.screenshot()

    def do_send_keys(self, by_locator, text, type=By.XPATH, timeout=30):
        """
        Send text to element on OpenShift Console

        by_locator(str): Command that tells Selenium IDE which
        GUI element needs to operate on.
        text(str): Send text to element
        type(By): Set of supported locator strategies
        timeout(int): Looks for a web element repeatedly until timeout(sec) happens.

        """
        wait = WebDriverWait(self.driver, timeout)
        element = wait.until(ec.element_to_be_clickable((type, by_locator)))
        element.send_keys(text)

    def is_expanded(self, by_locator, type=By.XPATH, timeout=30):
        """
        Check whether an element is in an expanded or collapsed state

        Args:
            by_locator(str): Command that tells Selenium IDE which
            GUI element needs to operate on.
            type(By): Set of supported locator strategies
            timeout(int): Looks for a web element repeatedly until timeout(sec) happens.

        return:
            (str): "true"-element expanded, "false"-element collapsed

        """
        wait = WebDriverWait(self.driver, timeout)
        element = wait.until(ec.element_to_be_clickable((type, by_locator)))
        return element.get_attribute("aria-expanded")

    def choose_expanded_mode(self, mode, by_locator, type):
        """
        Select the element mode (expanded or collapsed)

        by_locator(str): Command that tells Selenium IDE which
        GUI element needs to operate on.
        type(By): Set of supported locator strategies
        mode(str): Element mode (expanded or collapsed)

        """
        current_mode = self.is_expanded(by_locator=by_locator, type=type)
        if mode != current_mode:
            self.do_click(by_locator=by_locator, type=type)

    def screenshot(self):
        """
        Take screenshot using python code

        """
        self.cnt_screenshot += 1
        filename = os.path.join(self.screenshots_folder, str(self.cnt_screenshot))
        self.driver.save_screenshot(filename)


def login_ui(browser, headless=True, chrome_type=ChromeType.CHROMIUM):
    """
    Login to OpenShift Console

    Args:
        browser(str): type of browser (chrome, firefox..)
        headless(bool): True- without GUI, False- with GUI
        chrome_type (ChromeType): Select chrome type (chrome, chromium..)

    return:
        driver(Selenium WebDriver)

    """
    logger.info("Get URL of OCP console")
    console_url = run_cmd(
        "oc get consoles.config.openshift.io cluster -o"
        "jsonpath='{.status.consoleURL}'"
    )
    logger.info("Get password of OCP console")
    password = get_kubeadmin_password()
    password = password.rstrip()
    if browser == "chrome":
        logger.info("chrome browser")
        chrome_options = Options()
        chrome_options.add_argument("--ignore-ssl-errors=yes")
        chrome_options.add_argument("--ignore-certificate-errors")
        # headless browsers are web browsers without a GUI
        if headless:
            chrome_options.add_argument("--headless")
        chrome_browser = chrome_type
        driver = webdriver.Chrome(
            ChromeDriverManager(chrome_type=chrome_browser).install(),
            chrome_options=chrome_options,
        )
    if browser == "firefox":
        logger.info("firefox browser")
        driver = webdriver.Firefox()

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
        driver(Selenium WebDriver)

    """
    driver.close()
