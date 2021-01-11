import logging
import time

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By


from ocs_ci.utility.utils import run_cmd, get_kubeadmin_password
from ocs_ci.ocs.ui.views import login


logger = logging.getLogger(__name__)


class BaseUI:
    """
    Base Class for UI Tests

    """

    def __init__(self, driver):
        self.driver = driver

    def do_click(self, by_locator, type=By.XPATH, timeout=30):
        wait = WebDriverWait(self.driver, timeout)
        element = wait.until(ec.element_to_be_clickable((type, by_locator)))
        element.click()

    def do_click_visibility(self, by_locator, type=By.PARTIAL_LINK_TEXT, timeout=30):
        wait = WebDriverWait(self.driver, timeout=timeout)
        element = wait.until(ec.visibility_of_element_located((type, by_locator)))
        element.click()

    def do_send_keys(self, by_locator, text, type=By.XPATH, timeout=30):
        wait = WebDriverWait(self.driver, timeout)
        element = wait.until(ec.element_to_be_clickable((type, by_locator)))
        element.send_keys(text)

    def get_element_text(self, by_locator, type=By.XPATH, timeout=30):
        wait = WebDriverWait(self.driver, timeout)
        element = wait.until(ec.visibility_of_element_located((type, by_locator)))
        return element.text

    def is_enabled(self, by_locator, type=By.XPATH, timeout=30):
        wait = WebDriverWait(self.driver, timeout)
        element = wait.until(ec.visibility_of_element_located((type, by_locator)))
        return bool(element)

    def get_title(self, title, type=By.XPATH, timeout=30):
        wait = WebDriverWait(self.driver, timeout)
        wait.until(ec.title_is(type, title))
        return self.driver.title


def login_ui(browser):
    """
    Login to OpenShift Console

    Args:
        browser(str): type of browser (chrome, firefox..)

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
        driver = webdriver.Chrome()
    if browser == "firefox":
        logger.info("firefox browser")
        driver = webdriver.Firefox()
    wait = WebDriverWait(driver, 30)
    driver.get(console_url)
    try:
        logger.info("1")
        time.sleep(10)
        driver.find_element_by_xpath('//*[@id="details-button"]').click()
        logger.info("2")
        time.sleep(10)
        driver.find_element_by_xpath('//*[@id="proceed-link"]').click()
        logger.info("3")
        time.sleep(10)
        driver.find_element_by_xpath('//*[@id="details-button"]').click()
        logger.info("4")
        time.sleep(10)
        driver.find_element_by_xpath('//*[@id="proceed-link"]').click()
        logger.info("5")
        time.sleep(10)
    except Exception:
        pass
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
