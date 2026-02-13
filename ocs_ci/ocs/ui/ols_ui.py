import time
import logging
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException

from ocs_ci.ocs.ui.base_ui import BaseUI


log = logging.getLogger(__name__)


class OLSUI(BaseUI):
    """
    OLS UI wrapper.

    """

    def __init__(self):
        super().__init__()
        self.driver.implicitly_wait(5)

    OLS_BUTTON_XPATH = "//button[@aria-label='Red Hat OpenShift Lightspeed']"
    TEXTAREA_INPUT_XPATH = "//textarea"
    TEXT_INPUT_XPATH = "//input[@type='text']"
    SEND_BUTTON_TYPE_XPATH = "//button[@type='submit']"
    SEND_BUTTON_ARIA_XPATH = "//button[@aria-label='Send']"
    ANSWERS_XPATH = "//div[contains(@class,'pf-chatbot__message-response')]"

    def _wait_until_elements_exist(self, xpath, timeout=60):
        """

        Polls until at least one element exists for the xpath.
        Safe for dynamic / multi-element locators.

        """
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
        try:
            elems = self._wait_until_elements_exist(
                self.TEXTAREA_INPUT_XPATH, timeout=5
            )
            return elems[0]
        except TimeoutException:
            elems = self._wait_until_elements_exist(self.TEXT_INPUT_XPATH, timeout=5)
            return elems[0]

    def _click_send(self):
        try:
            self._wait_until_elements_exist(self.SEND_BUTTON_TYPE_XPATH, timeout=5)
            self.do_click_by_xpath(self.SEND_BUTTON_TYPE_XPATH)
        except TimeoutException:
            self._wait_until_elements_exist(self.SEND_BUTTON_ARIA_XPATH, timeout=5)
            self.do_click_by_xpath(self.SEND_BUTTON_ARIA_XPATH)

    def _wait_for_answer(self, timeout=180):
        return self._wait_until_elements_exist(self.ANSWERS_XPATH, timeout=timeout)

    def _get_stable_text(self, timeout=300, interval=5):
        start_time = time.time()
        last_text = ""

        while time.time() - start_time < timeout:
            # Re-fetch the last element to avoid StaleElementReferenceException
            elements = self.driver.find_elements(By.XPATH, self.ANSWERS_XPATH)
            if not elements:
                continue

            current_text = elements[-1].text.strip()

            # Check if text has content and has stopped growing
            if current_text and current_text == last_text:
                log.info("Answer streaming completed.")
                return current_text

            last_text = current_text
            log.debug("Waiting for OLS to finish typing...")
            time.sleep(interval)

        raise TimeoutError("OLS answer took too long to complete.")

    def ask_question(self, question):
        log.info(f"Asking question: {question}")

        input_elem = self._get_question_input()
        input_elem.clear()
        input_elem.send_keys(question)

        self._click_send()

        self._wait_for_answer()
        return self._get_stable_text(timeout=300)
