from pathlib import Path
import datetime
import logging
import os
import gc
import time
import zipfile
from functools import reduce
from selenium import webdriver
from selenium.common.exceptions import (
    TimeoutException,
    WebDriverException,
    NoSuchElementException,
    StaleElementReferenceException,
    ElementClickInterceptedException,
)
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait
from urllib.parse import urlparse
from webdriver_manager.chrome import ChromeDriverManager
from ocs_ci.framework import config
from ocs_ci.framework import config as ocsci_config
from ocs_ci.helpers.helpers import get_current_test_name
from ocs_ci.ocs import constants
from ocs_ci.ocs.constants import HCI_PROVIDER_CLIENT_PLATFORMS
from ocs_ci.ocs.exceptions import (
    NotSupportedProxyConfiguration,
)
from ocs_ci.ocs.ocp import get_ocp_url
from ocs_ci.ocs.ui.views import locators
from ocs_ci.utility.templating import Templating
from ocs_ci.utility.retry import retry
from ocs_ci.utility import version
from ocs_ci.utility.utils import (
    TimeoutSampler,
    get_kubeadmin_password,
    get_ocp_version,
)

logger = logging.getLogger(__name__)


def wait_for_element_to_be_clickable(locator, timeout=30):
    """
    Wait for an element to be clickable.

    Args:
        locator (tuple): A tuple containing the locator strategy (e.g., By.ID, By.XPATH) and the locator value.
        timeout (int): Maximum time (in seconds) to wait for the element to be clickable. Defaults to 30 seconds.

    Returns:
        selenium.webdriver.remote.webelement.WebElement: The clickable web element.

    """
    wait = WebDriverWait(SeleniumDriver(), timeout)
    try:
        web_element = wait.until(ec.element_to_be_clickable((locator[1], locator[0])))
    except TimeoutException:
        take_screenshot()
        copy_dom()
        raise
    return web_element


def wait_for_element_to_be_visible(locator, timeout=30):
    """
    Wait for element to be visible. Use when Web element is not have to be clickable (icons, disabled btns, etc.)
    Method does not fail when Web element not found

    Args:
         locator (tuple): (GUI element needs to operate on (str), type (By)).
         timeout (int): Looks for a web element until timeout (sec) occurs

    Returns:
        selenium.webdriver.remote.webelement.WebElement: Visible web element.
    """
    wait = WebDriverWait(SeleniumDriver(), timeout)
    try:
        web_element = wait.until(
            ec.visibility_of_element_located((locator[1], locator[0]))
        )
    except TimeoutException:
        take_screenshot()
        copy_dom()
        raise
    return web_element


class BaseUI:
    """
    Base Class for UI Tests

    """

    def __init__(self):
        self.driver = SeleniumDriver()
        if self.__class__.__name__ != BaseUI.__name__:
            logger.info(f"You are on * {repr(self)} *")
        base_ui_logs_dir = os.path.join(
            os.path.expanduser(ocsci_config.RUN["log_dir"]),
            f"ui_logs_dir_{ocsci_config.RUN['run_id']}",
        )
        logger.info(f"UI logs directory class {base_ui_logs_dir}")
        self.screenshots_folder = os.path.join(
            base_ui_logs_dir,
            "screenshots_ui",
            get_current_test_name(),
        )
        self.dom_folder = os.path.join(
            base_ui_logs_dir,
            "dom",
            get_current_test_name(),
        )
        if not os.path.isdir(self.screenshots_folder):
            Path(self.screenshots_folder).mkdir(parents=True, exist_ok=True)
        logger.debug(f"screenshots folder:{self.screenshots_folder}")

        if not os.path.isdir(self.dom_folder):
            Path(self.dom_folder).mkdir(parents=True, exist_ok=True)
        logger.debug(f"dom files folder:{self.dom_folder}")

        self.ocp_version = get_ocp_version()
        self.running_ocp_semantic_version = version.get_semantic_ocp_running_version()
        self.ocp_version_full = version.get_semantic_ocp_version_from_config()
        self.ocs_version_semantic = version.get_semantic_ocs_version_from_config()
        self.ocp_version_semantic = version.get_semantic_ocp_version_from_config()

        self.page_nav = self.deep_get(locators, self.ocp_version, "page")
        self.generic_locators = self.deep_get(locators, self.ocp_version, "generic")
        self.validation_loc = self.deep_get(locators, self.ocp_version, "validation")
        self.dep_loc = self.deep_get(locators, self.ocp_version, "deployment")
        self.pvc_loc = self.deep_get(locators, self.ocp_version, "pvc")
        self.bp_loc = self.deep_get(locators, self.ocp_version, "block_pool")
        self.sc_loc = self.deep_get(locators, self.ocp_version, "storageclass")
        self.ocs_loc = self.deep_get(locators, self.ocp_version, "ocs_operator")
        self.bucketclass = self.deep_get(locators, self.ocp_version, "bucketclass")
        self.mcg_stores = self.deep_get(locators, self.ocp_version, "mcg_stores")
        self.acm_page_nav = self.deep_get(locators, self.ocp_version, "acm_page")
        self.obc_loc = self.deep_get(locators, self.ocp_version, "obc")
        self.add_capacity_ui_loc = self.deep_get(
            locators, self.ocp_version, "add_capacity"
        )
        self.topology_loc = self.deep_get(locators, self.ocp_version, "topology")
        self.storage_clients_loc = self.deep_get(locators, self.ocp_version, "storage")
        self.alerting_loc = self.deep_get(locators, self.ocp_version, "alerting")

    def __repr__(self):
        return f"{self.__class__.__name__} Web Page"

    @classmethod
    def deep_get(cls, dictionary, *keys):
        return reduce(lambda d, key: d.get(key) if d else None, keys, dictionary)

    def do_click(
        self,
        locator,
        timeout=30,
        enable_screenshot=False,
        copy_dom=False,
        avoid_stale=False,
    ):
        """
        Click on Button/link on OpenShift Console

        locator (tuple): (GUI element needs to operate on (str), type (By))
        timeout (int): Looks for a web element repeatedly until timeout (sec) happens.
        enable_screenshot (bool): take screenshot
        copy_dom (bool): copy page source of the webpage
        avoid_stale (bool): if got StaleElementReferenceException, caused by reference to stale, cached element,
        refresh the page once and try click again
        * don't use when refreshed page expected to be different from initial page, or loose input values
        """

        def _do_click(_locator, _timeout=30, _enable_screenshot=False, _copy_dom=False):
            # wait for page fully loaded only if an element was not located
            # prevents needless waiting and frequent crushes on ODF Overview page,
            # when metrics and alerts frequently updated
            if not self.get_elements(_locator):
                self.page_has_loaded()
            screenshot = (
                ocsci_config.UI_SELENIUM.get("screenshot") and enable_screenshot
            )
            if screenshot:
                self.take_screenshot()
            if _copy_dom:
                self.copy_dom()

            wait = WebDriverWait(self.driver, timeout)
            try:
                if (
                    version.get_semantic_version(get_ocp_version(), True)
                    <= version.VERSION_4_11
                ):
                    element = wait.until(
                        ec.element_to_be_clickable((locator[1], locator[0]))
                    )
                else:
                    element = wait.until(
                        ec.visibility_of_element_located((locator[1], locator[0]))
                    )
                element.click()
            except TimeoutException as e:
                self.take_screenshot()
                self.copy_dom()
                logger.error(e)
                raise TimeoutException(
                    f"Failed to find the element ({locator[1]},{locator[0]})"
                )

        try:
            _do_click(locator, timeout, enable_screenshot, copy_dom)
        except StaleElementReferenceException:
            if avoid_stale:
                logger.info("Refresh page to avoid reference to a stale element")
                self.driver.refresh()
                _do_click(locator, timeout, enable_screenshot, copy_dom)
            else:
                raise
        except ElementClickInterceptedException:
            # appears due to JS graphics on the page: one element overlapping another, or dynamic graphics in progress
            logger.info("ElementClickInterceptedException, try click again")
            take_screenshot("ElementClickInterceptedException")
            self.copy_dom()
            time.sleep(5)
            _do_click(locator, timeout, enable_screenshot, copy_dom)

    def do_click_by_id(self, id, timeout=30):
        return self.do_click((id, By.ID), timeout)

    def do_click_by_xpath(self, xpath, timeout=30):
        """
        Function to click on a web element using XPATH
        Args:
            xpath (str): xpath to interact with web element
            timeout (int): timeout until which an exception won't be raised

        Returns:
                Clicks on the web element found

        """
        return self.do_click((xpath, By.XPATH), timeout)

    def find_an_element_by_xpath(self, locator):
        """
        Function to find an element using xpath

        Args:
            locator (str): locator of the element to be found

        Returns:
            an object of the type WebElement

        """
        element = self.driver.find_element_by_xpath(locator)
        return element

    def do_send_keys(self, locator, text, timeout=30):
        """
        Send text to element on OpenShift Console

        locator (tuple): (GUI element needs to operate on (str), type (By))
        text (str): Send text to element
        timeout (int): Looks for a web element repeatedly until timeout (sec) happens.

        """
        # wait for page fully loaded only if an element was not located
        # prevents needless waiting and frequent crushes on ODF Overview page,
        # when metrics and alerts frequently updated
        if not self.get_elements(locator):
            self.page_has_loaded()
        wait = WebDriverWait(self.driver, timeout)
        try:
            if (
                version.get_semantic_version(get_ocp_version(), True)
                <= version.VERSION_4_11
            ):
                element = wait.until(
                    ec.presence_of_element_located((locator[1], locator[0]))
                )
            else:
                element = wait.until(
                    ec.visibility_of_element_located((locator[1], locator[0]))
                )
            element.send_keys(text)
        except TimeoutException as e:
            self.take_screenshot()
            self.copy_dom()
            logger.error(e)
            raise TimeoutException(
                f"Failed to find the element ({locator[1]},{locator[0]})"
            )
        return element

    def is_expanded(self, locator, timeout=30):
        """
        Check whether an element is in an expanded or collapsed state

        Args:
            locator (tuple): (GUI element needs to operate on (str), type (By))
            timeout (int): Looks for a web element repeatedly until timeout (sec) happens.

        return:
            bool: True if element expended, False otherwise

        """
        wait = WebDriverWait(self.driver, timeout)
        try:
            element = wait.until(ec.element_to_be_clickable((locator[1], locator[0])))
        except TimeoutException:
            # element_to_be_clickable() doesn't work as expected so just to harden
            # we are using presence_of_element_located
            element = wait.until(
                ec.presence_of_element_located((locator[1], locator[0]))
            )
        return True if element.get_attribute("aria-expanded") == "true" else False

    def choose_expanded_mode(self, mode, locator):
        """
        Select the element mode (expanded or collapsed)

        mode (bool): True if element expended, False otherwise
        locator (tuple): (GUI element needs to operate on (str), type (By))

        """
        current_mode = self.is_expanded(locator=locator, timeout=180)
        if mode != current_mode:
            self.do_click(locator=locator, enable_screenshot=False)

    def get_checkbox_status(self, locator, timeout=30):
        """
        Checkbox Status

        Args:
            locator (tuple): (GUI element needs to operate on (str), type (By))
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
        locator (tuple): (GUI element needs to operate on (str), type (By))

        """
        current_status = self.get_checkbox_status(locator=locator)
        if status != current_status:
            self.do_click(locator=locator)

    def check_element_text(self, expected_text, element="*", take_screenshot=False):
        """
        Check if the text matches the expected text.

        Args:
            expected_text (string): The expected text.
            element (str): element
            take_screenshot (bool): if screenshot should be taken

        return:
            bool: True if the text matches the expected text, False otherwise

        """
        if take_screenshot:
            self.take_screenshot()
        element_list = self.driver.find_elements_by_xpath(
            f"//{element}[contains(text(), '{expected_text}')]"
        )
        return len(element_list) > 0

    def check_number_occurrences_text(self, expected_text, number, element="*"):
        """
        The number of times the string appears on the web page

        Args:
            expected_text (string): The expected text.
            number (int): The number of times the string appears on the web page

        return:
            bool: True if the text matches the expected text, False otherwise

        """
        element_list = self.driver.find_elements_by_xpath(
            f"//{element}[contains(text(), '{expected_text}')]"
        )
        return len(element_list) == number

    def get_element_text(self, locator):
        """
        Get the inner text of an element in locator.

        Args:
            locator (tuple): (GUI element needs to operate on (str), type (By)).

        Return:
            str: The text captured.
        """
        return self.driver.find_element(by=locator[1], value=locator[0]).text

    def get_elements(self, locator):
        """
        Get an elements list. Useful to count number of elements presented on page, etc.

        Args:
            locator (tuple): (GUI element needs to operate on (str), type (By)).

        Return:
            list: The list of WebElements
        """
        return self.driver.find_elements(by=locator[1], value=locator[0])

    def wait_for_element_to_be_visible(self, locator, timeout=30):
        """
        Wait for element to be visible. Use when Web element is not have to be clickable (icons, disabled btns, etc.)
        Method does not fail when Web element not found

        Args:
             locator (tuple): (GUI element needs to operate on (str), type (By)).
             timeout (int): Looks for a web element until timeout (sec) occurs
        """
        wait = WebDriverWait(self.driver, timeout)
        return wait.until(ec.visibility_of_element_located((locator[1], locator[0])))

    def wait_for_element_to_be_present(self, locator, timeout=30):
        """
        Wait for element to be present. Use when Web element should be present, but may be placed above another element
        on the z-layer
        Method does not fail when Web element not found

        Args:
             locator (tuple): (GUI element needs to operate on (str), type (By)).
             timeout (int): Looks for a web element until timeout (sec) occurs
        """
        wait = WebDriverWait(self.driver, timeout)
        return wait.until(ec.presence_of_element_located((locator[1], locator[0])))

    def get_element_attribute(self, locator, attribute, safe: bool = False):
        """
        Get attribute from WebElement

        Args:
            locator (tuple): (GUI element needs to operate on (str), type (By)).
            attribute (str): the value of this attribute will be extracted from WebElement
            safe(bool): if True exception will not raise when element not found. Default option - not safe

        Returns:
            str: value of the attribute of requested and found WebElement
        """
        web_elements = self.get_elements(locator)
        if safe:
            if not len(web_elements):
                return
        return web_elements[0].get_attribute(attribute)

    def wait_for_element_attribute(
        self, locator, attribute, attribute_value, timeout, sleep
    ):
        """
        Method to wait attribute have specific value. Fails the test if attribure value not equal to expected
        Args:
            locator (tuple): (GUI element needs to operate on (str), type (By)).
            attribute (str): the value of this attribute will be extracted from WebElement
            attribute_value (str): the value attribute (can be None as well)
            timeout (int): timeout in seconds
            sleep (int): sleep interval in seconds
        """
        for sample in TimeoutSampler(
            timeout=timeout,
            sleep=sleep,
            func=self.get_element_attribute,
            locator=locator,
            attribute=attribute,
            safe=True,
        ):
            if sample == attribute_value:
                break

    def page_has_loaded(
        self, retries=5, sleep_time=2, module_loc=("html", By.TAG_NAME)
    ):
        """
        Waits for page to completely load by comparing current page hash values.
        Not suitable for pages that use frequent dynamically content (less than sleep_time)

        Args:
            retries (int): How much time in sleep_time to wait for page to load
            sleep_time (int): Time to wait between every pool of dom hash
            module_loc (tuple): locator of the module of the page awaited to be loaded
        """

        @retry(TimeoutException)
        def get_page_hash():
            """
            Get dom html hash
            """
            self.check_element_presence(module_loc[::-1])
            dom = self.get_element_attribute(module_loc, "innerHTML")
            dom_hash = hash(dom.encode("utf-8"))
            return dom_hash

        page_hash = "empty"
        page_hash_new = ""

        # comparing old and new page DOM hash together to verify the page is fully loaded
        retry_counter = 0
        while page_hash != page_hash_new:
            if retry_counter > 0:
                logger.info(f"page not loaded yet: {self.driver.current_url}")
            retry_counter += 1
            page_hash = get_page_hash()
            time.sleep(sleep_time)
            page_hash_new = get_page_hash()
            if retry_counter == retries:
                logger.error(
                    f"Current URL did not finish loading in {retries * sleep_time}"
                )
                self.take_screenshot()
                return
        logger.info(f"page loaded: {self.driver.current_url}")

    def refresh_page(self):
        """
        Refresh Web Page

        """
        self.driver.refresh()

    def navigate_backward(self):
        """
        Navigate to a previous Web Page

        """
        self.driver.back()

    def scroll_into_view(self, locator):
        """
        Scroll element into view

        """
        actions = ActionChains(self.driver)
        element = self.driver.find_element(locator[1], locator[0])
        actions.move_to_element(element).perform()

    def take_screenshot(self, name_suffix: str = ""):
        """
        Take screenshot using python code

        """
        take_screenshot(
            screenshots_folder=self.screenshots_folder, name_suffix=name_suffix
        )

    def copy_dom(self, name_suffix: str = ""):
        """
        Get page source of the webpage

        """
        copy_dom(dom_folder=self.dom_folder, name_suffix=name_suffix)

    def do_clear(self, locator, timeout=30):
        """
        Clear the existing text from UI

        Args:
            locator (tuple): (GUI element needs to operate on (str), type (By))
            timeout (int): Looks for a web element until timeout (sec) occurs

        """
        wait = WebDriverWait(self.driver, timeout)
        element = wait.until(ec.element_to_be_clickable((locator[1], locator[0])))
        element.clear()

    def clear_with_ctrl_a_del(self, locator, timeout=30):
        """
        Clear the existing text using CTRL + a and then Del keys,
        as on some elements .clear() function doesn't always work correctly.

        """
        wait = WebDriverWait(self.driver, timeout)
        element = wait.until(ec.element_to_be_clickable((locator[1], locator[0])))
        element.send_keys(Keys.CONTROL, "a")
        element.send_keys(Keys.DELETE)

    def wait_until_expected_text_is_found(self, locator, expected_text, timeout=60):
        """
        Method to wait for a expected text to appear on the UI (use of explicit wait type),
        this method is helpful in working with elements which appear on completion of certain action and
        ignores all the listed exceptions for the given timeout.

        Args:
            locator (tuple): (GUI element needs to operate on (str), type (By))
            expected_text (str): Text which needs to be searched on UI
            timeout (int): Looks for a web element repeatedly until timeout (sec) occurs

        Returns:
            bool: Returns True if the expected element text is found, False otherwise

        """
        wait = WebDriverWait(
            self.driver,
            timeout=timeout,
            poll_frequency=1,
        )
        try:
            wait.until(
                ec.text_to_be_present_in_element(
                    (locator[1], locator[0]), expected_text
                )
            )
            return True
        except TimeoutException:
            self.take_screenshot()
            logger.warning(
                f"Locator {locator[1]} {locator[0]} did not find text {expected_text}"
            )
            return False

    def check_element_presence(self, locator, timeout=5):
        """
        Check if an web element is present on the web console or not.


        Args:
             locator (tuple): (GUI element needs to operate on (str), type (By))
             timeout (int): Looks for a web element repeatedly until timeout (sec) occurs
        Returns:
            bool: True if the element is found, returns False otherwise and raises NoSuchElementException

        """
        try:
            ignored_exceptions = (
                NoSuchElementException,
                StaleElementReferenceException,
            )
            wait = WebDriverWait(
                self.driver,
                timeout=timeout,
                ignored_exceptions=ignored_exceptions,
                poll_frequency=1,
            )
            wait.until(ec.presence_of_element_located(locator))
            return True
        except (NoSuchElementException, StaleElementReferenceException):
            logger.error("Expected element not found on UI")
            self.take_screenshot()
            return False
        except TimeoutException:
            logger.error(f"Timedout while waiting for element with {locator}")
            self.take_screenshot()
            return False

    def wait_for_endswith_url(self, endswith, timeout=60):
        """
        Wait for endswith url to load

        Args:
            endswith (string): url endswith string for which we need to wait
            timeout (int): Timeout in seconds

        """
        wait = WebDriverWait(self.driver, timeout=timeout)
        wait.until(ec.url_matches(endswith))

    def clear_input_gradually(self, locator):
        """
        Clean input field by gradually deleting characters one by one.
        This way we avoid common automation issue when input field is not cleared.

        Returns:
            bool: True if the input element is successfully cleared, False otherwise.
        """
        wait_for_element_to_be_visible(locator, 30)
        elements = self.get_elements(locator)
        input_el = elements[0]
        input_len = len(str(input_el.get_attribute("value")))

        # timeout in seconds will be equal to a number of symbols to be removed, but not less than 30s
        timeout = input_len if input_len > 30 else 30
        timeout = time.time() + timeout
        if len(elements):
            while len(str(input_el.get_attribute("value"))) != 0:
                if time.time() < timeout:
                    # to remove text from the input independently where the caret is use both delete and backspace
                    input_el.send_keys(Keys.BACKSPACE, Keys.DELETE)
                    time.sleep(0.05)
                else:
                    raise TimeoutException("time to clear input os out")
        else:
            logger.error("test input locator not found")
            return False
        return True


def screenshot_dom_location(type_loc="screenshot"):
    """
    Get the location for copy DOM/screenshot

    Args:
        type_loc (str): if type_loc is "screenshot" the location for copy screeenshot else DOM

    """
    base_ui_logs_dir = os.path.join(
        os.path.expanduser(ocsci_config.RUN["log_dir"]),
        f"ui_logs_dir_{ocsci_config.RUN['run_id']}",
    )
    logger.info(f"UI logs directory function {base_ui_logs_dir}")
    if type_loc == "screenshot":
        return os.path.join(
            base_ui_logs_dir,
            "screenshots_ui",
            get_current_test_name(),
        )
    else:
        return os.path.join(
            base_ui_logs_dir,
            "dom",
            get_current_test_name(),
        )


def copy_dom(name_suffix: str = "", dom_folder=None):
    """
    Copy DOM using python code

    Args:
        name_suffix (str): name suffix, will be added before extension. Optional argument
        dom_folder (str): path to folder where dom text file will be saved
    """
    if dom_folder is None:
        dom_folder = screenshot_dom_location(type_loc="dom")
    if not os.path.isdir(dom_folder):
        Path(dom_folder).mkdir(parents=True, exist_ok=True)
    time.sleep(1)
    if name_suffix:
        name_suffix = f"_{name_suffix}"
    filename = os.path.join(
        dom_folder,
        f"{datetime.datetime.now().strftime('%Y-%m-%dT%H-%M-%S.%f')}{name_suffix}_DOM.txt",
    )
    logger.info(f"Copy DOM file: {filename}")
    html = SeleniumDriver().page_source
    with open(filename, "w") as f:
        f.write(html)
    time.sleep(0.5)


def take_screenshot(name_suffix: str = "", screenshots_folder=None):
    """
    Take screenshot using python code

    Args:
        name_suffix (str): name suffix, will be added before extension. Optional argument
        screenshots_folder (str): path to folder where screenshot will be saved
    """
    if screenshots_folder is None:
        screenshots_folder = screenshot_dom_location(type_loc="screenshot")
    if not os.path.isdir(screenshots_folder):
        Path(screenshots_folder).mkdir(parents=True, exist_ok=True)
    time.sleep(1)
    if name_suffix:
        name_suffix = f"_{name_suffix}"
    filename = os.path.join(
        screenshots_folder,
        f"{datetime.datetime.now().strftime('%Y-%m-%dT%H-%M-%S.%f')}{name_suffix}.png",
    )
    logger.debug(f"Creating screenshot: {filename}")
    SeleniumDriver().save_screenshot(filename)
    time.sleep(0.5)


def garbage_collector_webdriver():
    """
    Garbage Collector for webdriver objs

    """
    collected_objs = gc.get_objects()
    for obj in collected_objs:
        if str(type(obj)) == constants.WEB_DRIVER_CHROME_OBJ_TYPE:
            try:
                logger.debug(
                    f"garbage collector to quit webdriver session id {obj.session_id}"
                )
                obj.quit()
                SeleniumDriver.remove_instance()
            except WebDriverException as e:
                logger.error(e)


class SeleniumDriver(WebDriver):

    # noinspection PyUnresolvedReferences
    def __new__(cls):
        if not hasattr(cls, "instance") or not hasattr(cls.instance, "driver"):
            logger.debug("Creating instance of Selenium Driver")
            cls.instance = super(SeleniumDriver, cls).__new__(cls)
            cls.instance.driver = cls._set_driver()
        else:
            logger.debug(
                "SeleniumDriver instance already exists, driver created earlier"
            )
        return cls.instance.driver

    @classmethod
    def _set_driver(cls) -> WebDriver:
        browser = ocsci_config.UI_SELENIUM.get("browser_type")
        if browser == "chrome":
            logger.info("chrome browser")
            chrome_options = Options()

            ignore_ssl = ocsci_config.UI_SELENIUM.get("ignore_ssl")
            if ignore_ssl:
                chrome_options.add_argument("--ignore-ssl-errors=yes")
                chrome_options.add_argument("--ignore-certificate-errors")
                chrome_options.add_argument("--allow-insecure-localhost")
                if config.ENV_DATA.get("import_clusters_to_acm"):
                    # Dev shm should be disabled when sending big amonut characters,
                    # like the cert sections of a kubeconfig
                    chrome_options.add_argument("--disable-dev-shm-usage")
                capabilities = chrome_options.to_capabilities()
                capabilities["acceptInsecureCerts"] = True

            # headless browsers are web browsers without a GUI
            headless = ocsci_config.UI_SELENIUM.get("headless")
            if headless:
                chrome_options.add_argument("--headless=new")
                chrome_options.add_argument("window-size=1920,1400")

            # use proxy server, if required
            if (
                config.DEPLOYMENT.get("proxy")
                or config.DEPLOYMENT.get("disconnected")
                or config.ENV_DATA.get("private_link")
            ) and config.ENV_DATA.get("client_http_proxy"):
                client_proxy = urlparse(config.ENV_DATA.get("client_http_proxy"))
                # there is a big difference between configuring not authenticated
                # and authenticated proxy server for Chrome:
                # * not authenticated proxy can be configured via --proxy-server
                #   command line parameter
                # * authenticated proxy have to be provided through customly
                #   created Extension and it doesn't work in headless mode!
                if not client_proxy.username:
                    # not authenticated proxy
                    logger.info(
                        f"Configuring not authenticated proxy ('{client_proxy.geturl()}') for browser"
                    )
                    chrome_options.add_argument(
                        f"--proxy-server={client_proxy.geturl()}"
                    )
                elif not headless:
                    # authenticated proxy, not headless mode
                    # create Chrome extension with proxy settings
                    logger.info(
                        f"Configuring authenticated proxy ('{client_proxy.geturl()}') for browser"
                    )
                    _templating = Templating()
                    manifest_json = _templating.render_template(
                        constants.CHROME_PROXY_EXTENSION_MANIFEST_TEMPLATE, {}
                    )
                    background_js = _templating.render_template(
                        constants.CHROME_PROXY_EXTENSION_BACKGROUND_TEMPLATE,
                        {"proxy": client_proxy},
                    )
                    pluginfile = "/tmp/proxy_auth_plugin.zip"
                    with zipfile.ZipFile(pluginfile, "w") as zp:
                        zp.writestr("manifest.json", manifest_json)
                        zp.writestr("background.js", background_js)
                    chrome_options.add_extension(pluginfile)
                else:
                    # authenticated proxy, headless mode
                    logger.error(
                        "It is not possible to configure authenticated proxy "
                        f"('{client_proxy.geturl()}') for browser in headless mode"
                    )
                    raise NotSupportedProxyConfiguration(
                        "Unable to configure authenticated proxy in headless browser mode!"
                    )

            chrome_browser_type = ocsci_config.UI_SELENIUM.get("chrome_type")
            driver = webdriver.Chrome(
                ChromeDriverManager(chrome_type=chrome_browser_type).install(),
                options=chrome_options,
            )
        else:
            raise ValueError(f"No Support on {browser}")
        return driver

    @classmethod
    def remove_instance(cls):
        if hasattr(cls, "instance"):
            delattr(cls, "instance")
        else:
            logger.info("SeleniumDriver instance attr not found")


@retry(
    exception_to_check=(TimeoutException, WebDriverException, AttributeError),
    tries=3,
    delay=3,
    backoff=2,
    func=garbage_collector_webdriver,
)
def login_ui(console_url=None, username=None, password=None):
    """
    Login to OpenShift Console

    Args:
        console_url (str): ocp console url
        username(str): User which is other than admin user,
        password(str): Password of user other than admin user

    return:
        driver (Selenium WebDriver)

    """
    default_console = False
    if not console_url:
        console_url = get_ocp_url()
        default_console = True
    logger.info("Get password of OCP console")
    if password is None:
        password = get_kubeadmin_password()
        password = password.rstrip()
    ocp_version = get_ocp_version()
    login_loc = locators[ocp_version]["login"]
    page_nav_loc = locators[ocp_version]["page"]
    driver = SeleniumDriver()
    driver.maximize_window()
    driver.implicitly_wait(10)
    driver.get(console_url)
    # Validate proceeding to the login console before taking any action:
    proceed_to_login_console()

    try:
        wait = WebDriverWait(driver, 15)
        if username is not None:
            element = wait.until(
                ec.element_to_be_clickable(
                    (
                        login_loc["username_my_htpasswd"][1],
                        login_loc["username_my_htpasswd"][0],
                    )
                ),
                message="'Log in with my_htpasswd_provider' text is not present",
            )
        else:
            element = wait.until(
                ec.element_to_be_clickable(
                    (
                        login_loc["kubeadmin_login_approval"][1],
                        login_loc["kubeadmin_login_approval"][0],
                    )
                ),
                message="'Log in with kube:admin' text is not present",
            )
        element.click()
    except TimeoutException:
        take_screenshot("login")
        copy_dom("login")
        logger.warning(
            "Login with my_htpasswd_provider or kube:admin text not found, trying to login"
        )

    username_el = wait_for_element_to_be_clickable(login_loc["username"], 60)
    if username is None:
        username = constants.KUBEADMIN
    username_el.send_keys(username)

    password_el = wait_for_element_to_be_clickable(login_loc["password"], 60)
    password_el.send_keys(password)

    confirm_login_el = wait_for_element_to_be_clickable(login_loc["click_login"], 60)
    confirm_login_el.click()

    hci_platform_conf_confirmed = (
        config.ENV_DATA["platform"].lower() in HCI_PROVIDER_CLIENT_PLATFORMS
    )

    if hci_platform_conf_confirmed:
        dashboard_url = console_url + "/dashboards"
        # proceed to local-cluster page if not already there. The rule is always to start from the local-cluster page
        # when the hci platform is confirmed and proceed to the client if needed from within the test
        current_url = driver.current_url
        logger.info(f"Current url: {current_url}")
        if current_url != dashboard_url:
            # timeout is unusually high for different scenarios when default page is not loaded immediately
            logger.info("Navigate to 'Local Cluster' page")
            navigate_to_local_cluster(
                acm_page=locators[ocp_version]["acm_page"], timeout=180
            )
            logger.info(
                f"'Local Cluster' page is loaded, current url: {driver.current_url}"
            )
        else:
            NotImplementedError(
                f"Platform {config.ENV_DATA['platform']} is not supported"
            )

    if default_console is True and username is constants.KUBEADMIN:
        wait_for_element_to_be_visible(page_nav_loc["page_navigator_sidebar"], 180)

    if username is not constants.KUBEADMIN and not hci_platform_conf_confirmed:
        # OCP 4.14 and OCP 4.15 observed default user role is an admin
        skip_tour_el = wait_for_element_to_be_clickable(login_loc["skip_tour"], 180)
        skip_tour_el.click()
    return driver


def close_browser():
    """
    Close Selenium WebDriver

    """
    logger.info("Close browser")
    take_screenshot("close_browser")
    copy_dom("close_browser")
    SeleniumDriver().quit()
    SeleniumDriver.remove_instance()
    time.sleep(10)
    garbage_collector_webdriver()


def proceed_to_login_console():
    """
    Proceed to the login console, if needed to confirm this action in a page that appears before.
    This is required to be as a solo function, because the driver initializes in the login_ui function.
    Function needs to be called just before login

    Returns:
        None

    """
    driver = SeleniumDriver()
    login_loc = locators[get_ocp_version()]["login"]
    if driver.title == login_loc["pre_login_page_title"]:
        proceed_btn = driver.find_element(
            by=login_loc["proceed_to_login_btn"][1],
            value=login_loc["proceed_to_login_btn"][0],
        )
        proceed_btn.click()
        try:
            WebDriverWait(driver, 60).until(ec.title_is(login_loc["login_page_title"]))
        except TimeoutException:
            copy_dom("proceed_to_login_console")
            take_screenshot("proceed_to_login_console")
            raise


def navigate_to_local_cluster(**kwargs):
    """
    Navigate to Local Cluster page, if not already there
    :param kwargs: acm_page locators dict, timeout

    :raises TimeoutException: if timeout occurs, and local clusters page is not loaded
    """
    if "acm_page" in kwargs:
        acm_page_loc = kwargs["acm_page"]
    else:
        acm_page_loc = locators[get_ocp_version()]["acm_page"]
    if "timeout" in kwargs:
        timeout = kwargs["timeout"]
    else:
        timeout = 30

    all_clusters_dropdown = acm_page_loc["all-clusters_dropdown"]
    try:
        logger.info("Navigate to Local Cluster page. Click all clusters dropdown")
        acm_dropdown = wait_for_element_to_be_visible(all_clusters_dropdown, timeout)
        acm_dropdown.click()
        local_cluster_item = wait_for_element_to_be_visible(
            acm_page_loc["local-cluster_dropdown_item"]
        )
        logger.info("Navigate to Local Cluster page. Click local cluster item")
        local_cluster_item.click()
    except TimeoutException:
        wait_for_element_to_be_visible(acm_page_loc["local-cluster_dropdown"])


def navigate_to_all_clusters(**kwargs):
    """
    Navigate to All Clusters page, if not already there
    :param kwargs: acm_page locators dict, timeout

    :raises TimeoutException: if timeout occurs, and All clusters acm page is not loaded
    """
    if "acm_page" in kwargs:
        acm_page = kwargs["acm_page"]
    else:
        acm_page = locators[get_ocp_version()]["acm_page"]
    if "timeout" in kwargs:
        timeout = kwargs["timeout"]
    else:
        timeout = 30

    local_clusters_dropdown = acm_page["local-cluster_dropdown"]
    try:
        acm_dropdown = wait_for_element_to_be_visible(local_clusters_dropdown, timeout)
        acm_dropdown.click()
        all_clusters_item = wait_for_element_to_be_visible(
            acm_page["all-clusters_dropdown_item"]
        )
        all_clusters_item.click()
    except TimeoutException:
        wait_for_element_to_be_visible(acm_page["all-clusters_dropdown"])
