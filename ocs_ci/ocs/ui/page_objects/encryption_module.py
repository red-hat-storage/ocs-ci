from selenium.webdriver.common.by import By

from ocs_ci.ocs.ui.helpers_ui import logger
from ocs_ci.ocs.constants import ENCRYPTION_DASHBOARD_CONTEXT_MAP
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

        # Open the encryption summary popup
        self.do_click(
            self.validation_loc["encryption_summary"][context_key]["enabled"],
            enable_screenshot=True,
        )

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

        # Extract headers and statuses
        enc_headers = [
            head
            for head in root_element.find_elements(By.TAG_NAME, "h6")
            if head.text in ENCRYPTION_DASHBOARD_CONTEXT_MAP
        ]
        enc_status = [
            svg
            for svg in root_element.find_elements(By.TAG_NAME, "svg")
            if svg.get_attribute("color")
        ]

        for header, svg in zip(enc_headers, enc_status):
            context = ENCRYPTION_DASHBOARD_CONTEXT_MAP[header.text]
            encryption_summary[context]["status"] = (
                svg.get_attribute("color") == "#3e8635"
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
