import time
import logging
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    TimeoutException,
    StaleElementReferenceException,
)

from ocs_ci.ocs import constants
from ocs_ci.ocs.ui.base_ui import BaseUI


log = logging.getLogger(__name__)


class OLSUI(BaseUI):
    """
    OLS UI wrapper.

    """

    def __init__(self):
        super().__init__()
        self.driver.implicitly_wait(constants.OLS_UI_IMPLICIT_WAIT_SEC)

    OLS_BUTTON_XPATH = "//button[@aria-label='Red Hat OpenShift Lightspeed']"
    TEXTAREA_INPUT_XPATH = "//textarea"
    TEXT_INPUT_XPATH = "//input[@type='text']"
    SEND_BUTTON_TYPE_XPATH = "//button[@type='submit']"
    SEND_BUTTON_ARIA_XPATH = "//button[@aria-label='Send']"
    ANSWERS_XPATH = "//div[contains(@class,'pf-chatbot__message-response')]"

    def _wait_until_elements_exist(self, xpath, timeout=None):
        """

        Polls until at least one element exists for the xpath.
        Safe for dynamic / multi-element locators.

        Args:
            xpath (str): XPath locator.
            timeout (float, optional): Max seconds to poll. Defaults to
                ``constants.OLS_UI_ELEMENT_POLL_TIMEOUT_SEC``.

        Returns:
            list: Non-empty list of matching WebElements.

        Raises:
            TimeoutException: If no elements appear within ``timeout``.

        """
        if timeout is None:
            timeout = constants.OLS_UI_ELEMENT_POLL_TIMEOUT_SEC
        start = time.time()
        while time.time() - start < timeout:
            elements = self.driver.find_elements(By.XPATH, xpath)
            if elements:
                return elements
            time.sleep(0.5)

        raise TimeoutException(f"Elements not found for xpath: {xpath}")

    def open_ols(self):
        """

        Navigate to OLS Chat Box

        """
        log.info("Opening OLS chat")
        self._wait_until_elements_exist(self.OLS_BUTTON_XPATH)
        self.do_click_by_xpath(self.OLS_BUTTON_XPATH)

    def _get_question_input(self):
        """

        Resolve the chat input (textarea or fallback text input).

        Raises:
            TimeoutException: If neither input type appears within the locator timeout.

        """
        for xpath in (self.TEXTAREA_INPUT_XPATH, self.TEXT_INPUT_XPATH):
            try:
                elems = self._wait_until_elements_exist(
                    xpath, timeout=constants.OLS_UI_INPUT_LOCATOR_TIMEOUT_SEC
                )
                return elems[0]
            except TimeoutException:
                continue
        raise TimeoutException(
            "Neither textarea nor text input found for OLS question entry"
        )

    def _click_send(self):
        for xpath in (self.SEND_BUTTON_TYPE_XPATH, self.SEND_BUTTON_ARIA_XPATH):
            try:
                self._wait_until_elements_exist(
                    xpath, timeout=constants.OLS_UI_SEND_BUTTON_TIMEOUT_SEC
                )
                self.do_click_by_xpath(xpath)
                return
            except TimeoutException:
                continue
        raise TimeoutException("No send button found for OLS chat")

    def _wait_for_answer(self, timeout=None):
        if timeout is None:
            timeout = constants.OLS_UI_ANSWER_APPEAR_TIMEOUT_SEC
        return self._wait_until_elements_exist(self.ANSWERS_XPATH, timeout=timeout)

    def _get_stable_text(self, timeout=None, interval=None):
        """

        Wait until the last answer element's text stops changing (streaming done).

        Args:
            timeout (float, optional): Max seconds to wait for the answer to stabilize.
            interval (float, optional): Seconds between polls.

        Returns:
            str: Final answer text.

        Raises:
            TimeoutError: If the answer does not stabilize within ``timeout``.

        """
        if timeout is None:
            timeout = constants.OLS_UI_ANSWER_STABLE_TIMEOUT_SEC
        if interval is None:
            interval = constants.OLS_UI_ANSWER_STABLE_INTERVAL_SEC
        start_time = time.time()
        last_text = ""

        while time.time() - start_time < timeout:
            try:
                elements = self.driver.find_elements(By.XPATH, self.ANSWERS_XPATH)
                if not elements:
                    time.sleep(interval)
                    continue

                current_text = elements[-1].text.strip()

                if current_text and current_text == last_text:
                    log.info("Answer streaming completed.")
                    return current_text

                last_text = current_text
                log.debug("Waiting for OLS to finish typing...")
            except StaleElementReferenceException:
                log.debug("Stale answer element; re-fetching on next poll.")
            time.sleep(interval)

        raise TimeoutError("OLS answer took too long to complete.")

    def ask_question(self, question):
        log.info(f"Asking question: {question}")

        input_elem = self._get_question_input()
        input_elem.clear()
        input_elem.send_keys(question)

        self._click_send()

        self._wait_for_answer()
        return self._get_stable_text()
