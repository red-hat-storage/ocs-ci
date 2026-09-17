"""
Page Object Model for Volume Health Card UI component
"""

import logging
import re
from dataclasses import dataclass

from selenium.common.exceptions import TimeoutException
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.ui.helpers_ui import format_locator
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


@dataclass
class RowData:
    """Data structure for Volume Health table row"""

    pvc_name: str
    node_name: str
    events_href: str
    pvc_href: str
    node_href: str


class VolumeHealthCard(PageNavigator):
    """
    Page Object Model for Volume Health Card on Block and File tab.
    Provides methods to interact with healthy and unhealthy states.
    """

    def __init__(self):
        super().__init__()

    def is_card_present(self):
        """
        Check if Volume Health Card is present on the page.

        Returns:
            bool: True if card is present, False otherwise
        """
        logger.info("Checking Volume Health Card presence")
        result = len(self.get_elements(self.validation_loc["volume_health_card"])) > 0
        logger.info(f"Card present: {result}")
        if not result:
            self.take_screenshot("volume_health_card_not_found")
        return result

    def is_healthy(self):
        """
        Check if card shows healthy state (success icon visible).

        Returns:
            bool: True if healthy state shown, False otherwise
        """
        logger.info("Checking if card shows healthy state")
        result = (
            len(self.get_elements(self.validation_loc["volume_health_success_icon"]))
            > 0
        )
        logger.info(f"Card healthy: {result}")
        if not result:
            self.take_screenshot("card_not_healthy")
        return result

    def get_no_issues_text(self):
        """
        Get the 'no issues' text from healthy state.

        Returns:
            str: Text content (typically "No issues found.")
        """
        logger.info("Extracting 'no issues' text from card")
        self.wait_for_element_to_be_visible(
            self.validation_loc["volume_health_no_issues_text"], timeout=10
        )
        text = self.get_element_text(
            self.validation_loc["volume_health_no_issues_text"]
        )
        logger.info(f"Text: '{text}'")
        return text

    def click_view_all_pvcs(self):
        """
        Click the 'View all PVCs' link in healthy state.
        Navigates away to the PVC list page.
        """
        logger.info("Clicking 'View all PVCs' link")
        self.take_screenshot("before_click_view_all_pvcs")
        self.do_click(self.validation_loc["volume_health_view_all_pvcs_link"])
        logger.info("Navigated to PVC list page")
        self.take_screenshot("after_click_view_all_pvcs")

    def get_attention_text(self):
        """
        Get the attention banner text (e.g., "1 volume needs attention").

        Returns:
            str: Attention text
        """
        logger.info("Extracting attention banner text")
        self.wait_for_element_to_be_visible(
            self.validation_loc["volume_health_attention_text"], timeout=10
        )
        text = self.get_element_text(
            self.validation_loc["volume_health_attention_text"]
        )
        logger.info(f"Attention text: '{text}'")
        return text

    def get_attention_count(self):
        """
        Parse the number of unhealthy PVCs from attention text.

        Returns:
            int: Number of PVCs needing attention
        """
        logger.info("Parsing PVC count from attention text")
        text = self.get_attention_text()

        match = re.search(
            r"(\d+)\s+(?:volume|PersistentVolumeClaim)", text, re.IGNORECASE
        )
        if match:
            count = int(match.group(1))
        else:
            count = 0

        logger.info(f"Found {count} PVC(s) needing attention")
        return count

    def get_table_rows(self):
        """
        Get all table rows from unhealthy state table.

        Returns:
            list: List of WebElement objects (table rows)
        """
        logger.info("Fetching table rows from health card")
        rows = self.get_elements(self.validation_loc["volume_health_table_rows"])
        logger.info(f"Found {len(rows)} row(s)")
        return rows

    def get_row_data(self, row):
        """
        Parse data from a single table row.

        Args:
            row: WebElement representing a table row

        Returns:
            RowData: Parsed row data
        """
        pvc_locator = self.validation_loc["volume_health_table_pvc_link"]
        pvc_link = row.find_element(pvc_locator[1], pvc_locator[0])
        pvc_href = pvc_link.get_attribute("href") or ""
        pvc_name = (
            pvc_href.rstrip("/").split("/")[-1] if pvc_href else pvc_link.text.strip()
        )

        node_locator = self.validation_loc["volume_health_table_node_link"]
        node_link = row.find_element(node_locator[1], node_locator[0])
        node_href = node_link.get_attribute("href") or ""
        node_name = (
            node_href.rstrip("/").split("/")[-1]
            if node_href
            else node_link.text.strip()
        )

        events_locator = self.validation_loc["volume_health_table_events_link"]
        events_link = row.find_element(events_locator[1], events_locator[0])
        events_href = events_link.get_attribute("href") or ""

        logger.info(f"Parsing row: PVC={pvc_name}, Node={node_name}")

        return RowData(
            pvc_name=pvc_name,
            node_name=node_name,
            events_href=events_href,
            pvc_href=pvc_href,
            node_href=node_href,
        )

    def get_all_row_data(self):
        """
        Extract data from all table rows.

        Returns:
            list[RowData]: List of parsed row data
        """
        logger.info("Extracting all row data from table")
        rows = self.get_table_rows()
        all_data = [self.get_row_data(row) for row in rows]
        logger.info(f"Parsed {len(all_data)} row(s)")
        self.take_screenshot(f"table_parsed_{len(all_data)}_rows")
        return all_data

    def _is_pvc_in_table(self, pvc_name):
        """
        Helper method to check if PVC is in table (for wait polling).

        Uses an absolute locator to fetch all PVC link hrefs directly from
        the card root — avoids stale element errors that occur when iterating
        row WebElements and calling find_element on them after a re-render.

        Args:
            pvc_name (str): PVC name to check

        Returns:
            bool: True if PVC found in table
        """
        pvc_links = self.get_elements(
            self.validation_loc["volume_health_table_pvc_hrefs"]
        )
        for link in pvc_links:
            href = link.get_attribute("href") or ""
            name = href.rstrip("/").split("/")[-1]
            if name == pvc_name:
                return True
        return False

    def wait_for_pvc_in_table(self, pvc_name, timeout=60):
        """
        Wait for PVC to appear in Volume Health table.

        Args:
            pvc_name (str): PVC name to wait for
            timeout (int): Timeout in seconds (default: 60)

        Returns:
            bool: True if found, False if timeout
        """
        logger.info(f"Waiting for PVC '{pvc_name}' in table (timeout={timeout}s)")

        try:
            for is_present in TimeoutSampler(
                timeout=timeout,
                sleep=5,
                func=self._is_pvc_in_table,
                pvc_name=pvc_name,
            ):
                if is_present:
                    logger.info(f"PVC '{pvc_name}' found in health table")
                    return True
        except (TimeoutException, TimeoutExpiredError):
            logger.error(f"PVC '{pvc_name}' not found after {timeout}s")
            self.take_screenshot(f"pvc_{pvc_name}_not_in_table")
            self.copy_dom(f"pvc_{pvc_name}_not_in_table")
            return False

        return False

    def _is_pvc_not_in_table(self, pvc_name):
        """
        Helper method to check if PVC is NOT in table (for wait polling).

        Args:
            pvc_name (str): PVC name to check

        Returns:
            bool: True if PVC not found in table
        """
        rows = self.get_table_rows()
        if len(rows) == 0:
            return True  # No rows = PVC not in table

        for row in rows:
            row_data = self.get_row_data(row)
            if row_data.pvc_name == pvc_name:
                return False
        return True

    def wait_for_pvc_not_in_table(self, pvc_name, timeout=60):
        """
        Wait for PVC to disappear from Volume Health table (recovery).

        Args:
            pvc_name (str): PVC name to wait for removal
            timeout (int): Timeout in seconds (default: 60)

        Returns:
            bool: True if PVC cleared, False if timeout
        """
        logger.info(
            f"Waiting for PVC '{pvc_name}' to clear from table (timeout={timeout}s)"
        )

        try:
            for is_absent in TimeoutSampler(
                timeout=timeout,
                sleep=5,
                func=self._is_pvc_not_in_table,
                pvc_name=pvc_name,
            ):
                if is_absent:
                    logger.info(f"PVC '{pvc_name}' no longer in table")
                    return True
        except (TimeoutException, TimeoutExpiredError):
            logger.error(f"PVC '{pvc_name}' still present after {timeout}s")
            self.take_screenshot(f"pvc_{pvc_name}_still_in_table")
            self.copy_dom(f"pvc_{pvc_name}_still_in_table")
            return False

        return False

    def wait_for_healthy(self, timeout=300):
        """
        Wait for card to return to healthy state (success icon visible).

        Args:
            timeout (int): Timeout in seconds (default: 300)

        Returns:
            bool: True if healthy, False if timeout
        """
        logger.info(f"Waiting for card to return to healthy state (timeout={timeout}s)")

        try:
            for is_healthy in TimeoutSampler(
                timeout=timeout,
                sleep=10,
                func=self.is_healthy,
            ):
                if is_healthy:
                    logger.info("Card returned to healthy state")
                    return True
        except (TimeoutException, TimeoutExpiredError):
            logger.error(f"Card not healthy after {timeout}s")
            self.take_screenshot("card_not_healthy_timeout")
            self.copy_dom("card_not_healthy_timeout")
            return False

        return False

    def click_view_events(self, pvc_name):
        """
        Click 'View events' link for a specific PVC.
        Navigates away to the PVC events page.

        Args:
            pvc_name (str): PVC name to view events for
        """
        logger.info(f"Clicking 'View events' link for PVC '{pvc_name}'")
        self.take_screenshot(f"before_click_events_{pvc_name}")

        locator = format_locator(
            self.validation_loc["volume_health_view_events_link"], pvc_name
        )
        self.do_click(locator)

        logger.info(f"Navigated to events page for PVC '{pvc_name}'")
        self.take_screenshot(f"after_click_events_{pvc_name}")
