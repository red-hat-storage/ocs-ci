import logging
import os
import time
import datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException, WebDriverException

from ocs_ci.framework import config as ocsci_config
from ocs_ci.utility.utils import run_cmd, get_kubeadmin_password, get_ocp_version
from ocs_ci.ocs.ui.views import locators
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.utility.retry import retry


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
        if not os.path.isdir(self.screenshots_folder):
            os.mkdir(self.screenshots_folder)
        logger.info(f"screenshots pictures:{self.screenshots_folder}")
        if config.ENV_DATA["platform"].lower() == constants.VSPHERE_PLATFORM:
            self.storage_class = "thin_sc"
        elif config.ENV_DATA["platform"].lower() == constants.AWS_PLATFORM:
            self.storage_class = "gp2_sc"

    def do_click(self, locator, timeout=30):
        """
        Click on Button/link on OpenShift Console

        locator (set): (GUI element needs to operate on (str), type (By))
        timeout (int): Looks for a web element repeatedly until timeout (sec) happens.

        """
        try:
            wait = WebDriverWait(self.driver, timeout)
            element = wait.until(ec.element_to_be_clickable((locator[1], locator[0])))
            screenshot = ocsci_config.UI_SELENIUM.get("screenshot")
            if screenshot:
                self.take_screenshot()
            element.click()
        except TimeoutException as e:
            self.take_screenshot()
            logger.error(e)
            raise TimeoutException

    def do_send_keys(self, locator, text, timeout=30):
        """
        Send text to element on OpenShift Console

        locator (set): (GUI element needs to operate on (str), type (By))
        text (str): Send text to element
        timeout (int): Looks for a web element repeatedly until timeout (sec) happens.

        """
        try:
            wait = WebDriverWait(self.driver, timeout)
            element = wait.until(ec.element_to_be_clickable((locator[1], locator[0])))
            element.send_keys(text)
            screenshot = ocsci_config.UI_SELENIUM.get("screenshot")
            if screenshot:
                self.take_screenshot()
        except TimeoutException as e:
            self.take_screenshot()
            logger.error(e)
            raise TimeoutException

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

    def check_element_text(self, expected_text, element="*"):
        """
        Check if the text matches the expected text.

        Args:
            expected_text (string): The expected text.

        return:
            bool: True if the text matches the expected text, False otherwise

        """
        element_list = self.driver.find_elements_by_xpath(
            f"//{element}[contains(text(), '{expected_text}')]"
        )
        return len(element_list) > 0

    def refresh_page(self):
        """
        Refresh Web Page

        """
        self.driver.refresh()

    def scroll_into_view(self, locator):
        """
        Scroll element into view

        """
        actions = ActionChains(self.driver)
        element = self.driver.find_element(locator[1], locator[0])
        actions.move_to_element(element).perform()

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
        time.sleep(0.5)


class PageNavigator(BaseUI):
    """
    Page Navigator Class

    """

    def __init__(self, driver):
        super().__init__(driver)
        ocp_version = get_ocp_version()
        self.page_nav = locators[ocp_version]["page"]

    def navigate_overview_page(self):
        """
        Navigate to Overview Page

        """
        logger.info("Navigate to Overview Page")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Home"])
        self.do_click(locator=self.page_nav["overview_page"])

    def navigate_quickstarts_page(self):
        """
        Navigate to Quickstarts Page

        """
        self.navigate_overview_page()
        logger.info("Navigate to Quickstarts Page")
        self.scroll_into_view(self.page_nav["quickstarts"])
        self.do_click(locator=self.page_nav["quickstarts"])

    def navigate_projects_page(self):
        """
        Navigate to Projects Page

        """
        logger.info("Navigate to Projects Page")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Home"])
        self.do_click(locator=self.page_nav["projects_page"])

    def navigate_search_page(self):
        """
        Navigate to Search Page

        """
        logger.info("Navigate to Projects Page")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Home"])
        self.do_click(locator=self.page_nav["search_page"])

    def navigate_explore_page(self):
        """
        Navigate to Explore Page

        """
        logger.info("Navigate to Explore Page")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Home"])
        self.do_click(locator=self.page_nav["explore_page"])

    def navigate_events_page(self):
        """
        Navigate to Events Page

        """
        logger.info("Navigate to Events Page")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Home"])
        self.do_click(locator=self.page_nav["events_page"])

    def navigate_operatorhub_page(self):
        """
        Navigate to OperatorHub Page

        """
        logger.info("Navigate to OperatorHub Page")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Operators"])
        self.do_click(locator=self.page_nav["operatorhub_page"])

    def navigate_installed_operators_page(self):
        """
        Navigate to Installed Operators Page

        """
        logger.info("Navigate to Installed Operators Page")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Operators"])
        self.do_click(self.page_nav["installed_operators_page"])

    def navigate_persistentvolumes_page(self):
        """
        Navigate to Persistent Volumes Page

        """
        logger.info("Navigate to Persistent Volumes Page")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Storage"])
        self.do_click(locator=self.page_nav["persistentvolumes_page"])

    def navigate_persistentvolumeclaims_page(self):
        """
        Navigate to Persistent Volume Claims Page

        """
        logger.info("Navigate to Persistent Volume Claims Page")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Storage"])
        self.do_click(locator=self.page_nav["persistentvolumeclaims_page"])

    def navigate_storageclasses_page(self):
        """
        Navigate to Storage Classes Page

        """
        logger.info("Navigate to Storage Classes Page")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Storage"])
        self.do_click(locator=self.page_nav["storageclasses_page"])

    def navigate_volumesnapshots_page(self):
        """
        Navigate to Storage Volume Snapshots Page

        """
        logger.info("Navigate to Storage Volume Snapshots Page")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Storage"])
        self.do_click(locator=self.page_nav["volumesnapshots_page"])

    def navigate_volumesnapshotclasses_page(self):
        """
        Navigate to Volume Snapshot Classes Page

        """
        logger.info("Navigate to Volume Snapshot Classes Page")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Storage"])
        self.do_click(locator=self.page_nav["volumesnapshotclasses_page"])

    def navigate_volumesnapshotcontents_page(self):
        """
        Navigate to Volume Snapshot Contents Page

        """
        logger.info("Navigate to Volume Snapshot Contents Page")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Storage"])
        self.do_click(locator=self.page_nav["volumesnapshotcontents_page"])

    def navigate_object_buckets_page(self):
        """
        Navigate to Object Buckets Page

        """
        logger.info("Navigate to Object Buckets Page")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Storage"])
        self.do_click(locator=self.page_nav["object_buckets_page"])

    def navigate_object_bucket_claims_page(self):
        """
        Navigate to Object Bucket Claims Page

        """
        logger.info("Navigate to Object Bucket Claims Page")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Storage"])
        self.do_click(locator=self.page_nav["object_bucket_claims_page"])

    def navigate_alerting_page(self):
        """
        Navigate to Alerting Page

        """
        logger.info("Navigate to Alerting Page")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Monitoring"])
        self.do_click(locator=self.page_nav["alerting_page"])

    def navigate_metrics_page(self):
        """
        Navigate to Metrics Page

        """
        logger.info("Navigate to Metrics Page")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Monitoring"])
        self.do_click(locator=self.page_nav["metrics_page"])

    def navigate_dashboards_page(self):
        """
        Navigate to Dashboards Page

        """
        logger.info("Navigate to Dashboards Page")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Monitoring"])
        self.do_click(locator=self.page_nav["dashboards_page"])

    def navigate_pods_page(self):
        """
        Navigate to Pods Page

        """
        logger.info("Navigate to Pods Page")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Workloads"])
        self.do_click(locator=self.page_nav["Pods"])


def take_screenshot(driver):
    """
    Take screenshot using python code

    Args:
        driver (Selenium WebDriver)

    """
    screenshots_folder = os.path.join(
        os.path.expanduser(ocsci_config.RUN["log_dir"]),
        f"screenshots_ui_{ocsci_config.RUN['run_id']}",
    )
    if not os.path.isdir(screenshots_folder):
        os.mkdir(screenshots_folder)
    time.sleep(1)
    filename = os.path.join(
        screenshots_folder,
        f"{datetime.datetime.now().strftime('%Y-%m-%dT%H-%M-%S.%f')}.png",
    )
    logger.info(f"Creating snapshot: {filename}")
    driver.save_screenshot(filename)
    time.sleep(0.5)


@retry(TimeoutException, tries=3, delay=3, backoff=2)
@retry(WebDriverException, tries=3, delay=3, backoff=2)
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
            chrome_options.add_argument("--allow-insecure-localhost")
            capabilities = chrome_options.to_capabilities()
            capabilities["acceptInsecureCerts"] = True

        # headless browsers are web browsers without a GUI
        headless = ocsci_config.UI_SELENIUM.get("headless")
        if headless:
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("window-size=1920,1400")

        chrome_browser_type = ocsci_config.UI_SELENIUM.get("chrome_type")
        driver = webdriver.Chrome(
            ChromeDriverManager(chrome_type=chrome_browser_type).install(),
            chrome_options=chrome_options,
        )
    else:
        raise ValueError(f"Not Support on {browser}")

    wait = WebDriverWait(driver, 60)
    driver.maximize_window()
    driver.get(console_url)
    if config.ENV_DATA["flexy_deployment"]:
        try:
            element = wait.until(
                ec.element_to_be_clickable(
                    (login_loc["flexy_kubeadmin"][1], login_loc["flexy_kubeadmin"][0])
                )
            )
            element.click()
        except TimeoutException as e:
            take_screenshot(driver)
            logger.error(e)
    element = wait.until(
        ec.element_to_be_clickable((login_loc["username"][1], login_loc["username"][0]))
    )
    take_screenshot(driver)
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
    WebDriverWait(driver, 60).until(ec.title_is(login_loc["ocp_page"]))
    return driver


def close_browser(driver):
    """
    Close Selenium WebDriver

    Args:
        driver (Selenium WebDriver)

    """
    take_screenshot(driver)
    driver.close()
