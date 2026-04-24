"""
Base page helpers for CNV / Virtualization UI flows.

"""

import logging
import time
from typing import Callable, Iterable, Tuple

from ocs_ci.ocs.exceptions import TimeoutExpiredError
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait

from ocs_ci.ocs.ui.base_ui import BaseUI

logger = logging.getLogger(__name__)

Locator = Tuple[str, By]


class BasePage(BaseUI):
    """
    Thin POM base: common navigation / wait / reload helpers for console tests.
    """

    def wait(self, timeout: float) -> WebDriverWait:
        return WebDriverWait(self.driver, timeout)

    def reload_page(self, wait_for_dom: bool = True) -> None:
        self.driver.refresh()
        if wait_for_dom:
            self.page_has_loaded(retries=20, sleep_time=1)

    def reload_until(
        self,
        success: Callable[[WebDriver], bool],
        *,
        total_timeout: float = 120,
        reload_interval: float = 30,
    ) -> None:
        """
        Poll ``success(driver)`` using short WebDriverWait slices; if
        ``reload_interval`` elapses without success, reload and keep polling
        until ``total_timeout`` expires.
        """
        deadline = time.monotonic() + total_timeout
        last_reload = time.monotonic()

        while time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            slice_timeout = min(5.0, remaining)
            try:
                WebDriverWait(self.driver, slice_timeout, poll_frequency=0.5).until(
                    lambda d: bool(success(d))
                )
                return
            except TimeoutException:
                pass
            now = time.monotonic()
            if now - last_reload >= reload_interval:
                logger.info("reload_until: reloading after customize/template delay")
                self.reload_page()
                last_reload = now

        raise TimeoutExpiredError(
            total_timeout,
            f"Condition not met within {total_timeout}s (with reload fallback)",
        )

    def wait_any_visible(self, locators: Iterable[Locator], timeout: float = 60):
        loc_list = list(locators)
        if not loc_list:
            raise ValueError("locators must not be empty")
        conds = [ec.visibility_of_element_located((by, val)) for val, by in loc_list]
        return self.wait(timeout).until(ec.any_of(*conds))

    def wait_any_clickable(self, locators: Iterable[Locator], timeout: float = 60):
        loc_list = list(locators)
        if not loc_list:
            raise ValueError("locators must not be empty")
        conds = [ec.element_to_be_clickable((by, val)) for val, by in loc_list]
        return self.wait(timeout).until(ec.any_of(*conds))

    def click_first_matching(
        self, locators: Iterable[Locator], timeout: float = 60
    ) -> None:
        el = self.wait_any_clickable(locators, timeout=timeout)
        el.click()

    def send_keys_when_visible(
        self, locator: Locator, text: str, timeout: float = 60, clear: bool = True
    ) -> None:
        value, by = locator
        el = self.wait(timeout).until(ec.visibility_of_element_located((by, value)))
        if clear:
            el.clear()
        el.send_keys(text)

    def element_visible(self, locator: Locator, timeout: float = 5) -> bool:
        value, by = locator
        try:
            self.wait(timeout).until(ec.visibility_of_element_located((by, value)))
            return True
        except TimeoutException:
            return False
