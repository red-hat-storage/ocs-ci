import time

from ocs_ci.ocs.ui.helpers_ui import logger
from ocs_ci.ocs.ui.page_objects.storage_system_details import StorageSystemDetails
from selenium.webdriver.common.by import By


class ObjectDetails(StorageSystemDetails):
    def __init__(self):
        StorageSystemDetails.__init__(self)

    def get_encryption_summary(self):
        """
        Collecting Encryption summary shown in the Object details page.

        Returns:
            encryption_summary (dict): encryption summary on object details page.
        """
        encryption_summary = {
            "object_storage": {"status": None, "kms": ""},
            "intransit_encryption": {"status": None},
        }

        logger.info("Getting Block and File Encryption Summary Details")

        # Open the encryption summary popup
        self.do_click(
            self.validation_loc["encryption_summary"]["object"]["enabled"],
            enable_screenshot=True,
        )

        time.sleep(3)

        # Context and status mappings
        context_map = {
            "Object storage": "object_storage",
            "In-transit encryption": "intransit_encryption",
        }

        # Get elements for text and root
        encryption_content_location = self.validation_loc["encryption_summary"][
            "object"
        ]["encryption_content_data"]
        encryption_summary_text = self.get_element_text(encryption_content_location)
        root_elements = self.get_elements(encryption_content_location)

        if not root_elements:
            raise ValueError("Error getting root web element")
        root_element = root_elements[0]

        # Function to extract status from an SVG element
        def extract_status(svg_path):
            try:
                svg_element = root_element.find_element(By.CSS_SELECTOR, svg_path)
                if svg_element and svg_element.tag_name == "svg":
                    if svg_element.get_attribute("data-test") == "success-icon":
                        return True
                    else:
                        return False
            except Exception as e:
                logger.error(f"Error extracting status: {e}")
                return None

        # Process encryption summary text
        current_context = None
        for line in encryption_summary_text.split("\n"):
            line = line.strip()
            if line in context_map:
                current_context = context_map[line]
                continue

            if (
                current_context == "object_storage"
                and "External Key Management Service" in line
            ):
                encryption_summary[current_context]["kms"] = line.split(":")[-1].strip()
                encryption_summary[current_context]["status"] = extract_status(
                    "div.pf-v5-l-flex:nth-child(1) > div:nth-child(2) > svg"
                )
            elif current_context == "intransit_encryption":
                encryption_summary[current_context]["status"] = extract_status(
                    "div.pf-v5-l-flex:nth-child(4) > div:nth-child(2) > svg"
                )

        logger.info(f"Encryption Summary: {encryption_summary}")

        # Close the popup
        logger.info("Closing the popup")
        self.do_click(
            self.validation_loc["encryption_summary"]["object"]["close"],
            enable_screenshot=True,
        )

        return encryption_summary
