from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By

from ocs_ci.ocs.ui.helpers_ui import logger
from ocs_ci.ocs.constants import (
    ENCRYPTION_DASHBOARD_CONTEXT_MAP,
    UI_SUCCESS_ICON_COLORS,
)
from ocs_ci.ocs.ui.page_objects.data_foundation_tabs_common import (
    DataFoundationTabBar,
)


class EncryptionModule(DataFoundationTabBar):
    def _get_encryption_summary(self, context_key):
        """
        Generic method to collect encryption summary based on the context.

        Args:
            context_key (str): Key to determine the validation location.

        Returns:
            dict: Encryption summary for the given context.
        """
        encryption_summary = {
            "object_storage": {"status": False, "kms": False},
            "cluster_wide_encryption": {"status": False, "kms": False},
            "storageclass_encryption": {"status": False, "kms": False},
            "intransit_encryption": {"status": False},
        }

        logger.info(f"Getting Encryption Summary for context: {context_key}")

        self.wait_for_encryption_summary_ready(context_key)

        enabled_locator = self.validation_loc["encryption_summary"][context_key][
            "enabled"
        ]

        # Open the encryption summary popup
        self.do_click(enabled_locator, enable_screenshot=True)

        self.page_has_loaded(
            module_loc=self.validation_loc["encryption_summary"][context_key][
                "encryption_content_data"
            ]
        )

        # Get the root element for encryption details
        encryption_content_location = self.validation_loc["encryption_summary"][
            context_key
        ]["encryption_content_data"]
        root_elements = self.get_elements(encryption_content_location)

        if not root_elements:
            raise ValueError("Error getting root web element")
        root_element = root_elements[0]

        # Extract headers and statuses (PF6 uses data-test on icons, not color attr)
        for header in root_element.find_elements(By.TAG_NAME, "h6"):
            header_text = EncryptionModule._encryption_header_text(header)
            if header_text not in ENCRYPTION_DASHBOARD_CONTEXT_MAP:
                continue
            context = ENCRYPTION_DASHBOARD_CONTEXT_MAP[header_text]
            encryption_summary[context]["status"] = self._encryption_row_status_enabled(
                header
            )

        # Process encryption summary text
        current_context = None
        encryption_summary_text = self.get_element_text(encryption_content_location)

        for line in map(str.strip, encryption_summary_text.split("\n")):
            if line in ENCRYPTION_DASHBOARD_CONTEXT_MAP:
                current_context = ENCRYPTION_DASHBOARD_CONTEXT_MAP[line]
            elif current_context and current_context in encryption_summary:
                encryption_summary[current_context]["kms"] = (
                    line.split(":")[-1].strip()
                    if "External Key Management Service" in line
                    else False
                )

        logger.info(f"Encryption Summary for {context_key}: {encryption_summary}")

        # Close the popup
        logger.info("Closing the popup")
        self.do_click(
            self.validation_loc["encryption_summary"][context_key]["close"],
            enable_screenshot=True,
        )

        return encryption_summary

    @staticmethod
    def _encryption_header_text(header_element) -> str:
        """Return visible header label (PF6 h6 elements may have empty .text)."""
        text = (header_element.text or "").strip()
        if text:
            return text
        return (header_element.get_attribute("textContent") or "").strip()

    def _encryption_row_status_enabled(self, header_element) -> bool:
        """
        Return whether the encryption row shows an enabled/success status icon.

        PF5 icons expose color="#3e8635"; PF6 icons use data-test="success-icon".
        """
        row = None
        for row_xpath in self.validation_loc["encryption_summary"]["status_row_xpaths"]:
            try:
                row = header_element.find_element(By.XPATH, row_xpath)
                break
            except NoSuchElementException:
                continue
            except Exception as e:
                logger.warning(
                    "Unexpected error locating encryption status row via %s: %s",
                    row_xpath,
                    e,
                )
                raise

        if row is None:
            return False

        if row.find_elements(By.CSS_SELECTOR, '[data-test="success-icon"]'):
            return True

        for svg in row.find_elements(By.TAG_NAME, "svg"):
            color = (
                svg.get_attribute("color") or svg.get_attribute("fill") or ""
            ).lower()
            if color in UI_SUCCESS_ICON_COLORS:
                return True
        return False

    def wait_for_encryption_summary_ready(self, context_key):
        """
        Wait until the encryption summary control is present on the dashboard.

        Args:
            context_key (str): Key under encryption_summary locators
                (e.g. "file_and_block", "object_storage").
        """
        enabled_locator = self.validation_loc["encryption_summary"][context_key][
            "enabled"
        ]
        self.wait_for_element_to_be_present(enabled_locator, timeout=60)
        self.page_has_loaded(module_loc=enabled_locator, retries=15, sleep_time=2)

    def get_object_encryption_summary(self):
        """
        Retrieve the encryption summary for the object details page.

        Returns:
            dict: Encryption summary on object details page.
        """
        return self._get_encryption_summary("object_storage")

    def get_block_file_encryption_summary(self):
        """
        Retrieve the encryption summary for the block and file page.

        Returns:
            dict: Encryption summary on block and file page.
        """
        return self._get_encryption_summary("file_and_block")
