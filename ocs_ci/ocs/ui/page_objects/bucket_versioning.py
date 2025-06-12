import logging
import time
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException

from ocs_ci.ocs.ui.page_objects.object_storage import ObjectStorage
from ocs_ci.ocs.ui.page_objects.confirm_dialog import ConfirmDialog

logger = logging.getLogger(__name__)


class BucketVersioning(ObjectStorage, ConfirmDialog):
    """
    A class representation for bucket versioning UI operations.
    Inherits from ObjectStorage and ConfirmDialog for base UI functionality.
    """

    def _navigate_to_bucket_properties(self, bucket_name: str = None) -> None:
        """
        Navigate to bucket properties tab.

        Args:
            bucket_name (str, optional): Name of the bucket. If None, uses first bucket.

        Raises:
            NoSuchElementException: If UI elements are not found.
        """
        logger.info("Navigating to object storage buckets page")
        self.nav_object_storage_page()

        if bucket_name:
            self.do_click(f"//tr//a[contains(text(), '{bucket_name}')]", By.XPATH)
        else:
            self.do_click(self.bucket_tab["first_bucket"])

        self.do_click(self.bucket_tab["properties_tab"])
        logger.info("Navigated to Properties tab")

    def set_versioning_state(self, enabled: bool, bucket_name: str = None) -> bool:
        """
        Set versioning state to enabled or disabled (following base_ui pattern).

        Args:
            enabled (bool): True to enable versioning, False to disable.
            bucket_name (str, optional): Name of the bucket. If None, uses first bucket.

        Returns:
            bool: True if state was changed, False if already in desired state.

        Raises:
            NoSuchElementException: If UI elements are not found.
        """
        current_state = self.is_versioning_enabled(bucket_name)
        if enabled != current_state:
            self.do_click(self.bucket_tab["versioning_toggle"], enable_screenshot=False)
            action = "enable" if enabled else "disable"
            logger.info(f"Clicked versioning toggle to {action} versioning")

            # Handle confirmation dialog if enabling versioning
            if enabled:
                self._handle_versioning_confirmation()

            time.sleep(2)  # Wait for UI to update
            return True
        else:
            state_text = "enabled" if enabled else "disabled"
            logger.info(f"Versioning already {state_text}")
            return False

    def _handle_versioning_confirmation(self, confirm: bool = True) -> None:
        """
        Handle the versioning confirmation dialog that appears when enabling versioning.

        Args:
            confirm (bool): True to confirm (click Enable), False to cancel.

        Raises:
            NoSuchElementException: If confirmation dialog elements are not found.
        """
        try:
            logger.info("Handling versioning confirmation dialog")
            if confirm:
                self.do_click(
                    self.bucket_tab["versioning_enable_confirm_button"], timeout=10
                )
                logger.info("Clicked Enable in versioning confirmation dialog")
            else:
                self.do_click(
                    self.bucket_tab["versioning_cancel_confirm_button"], timeout=10
                )
                logger.info("Clicked Cancel in versioning confirmation dialog")
        except NoSuchElementException:
            logger.error("Could not find versioning confirmation dialog")
            raise

    def is_versioning_enabled(self, bucket_name: str = None) -> bool:
        """
        Check if versioning is already enabled for the specified bucket.

        Args:
            bucket_name (str, optional): Name of the bucket. If None, uses first bucket.

        Returns:
            bool: True if versioning is enabled, False otherwise.

        Raises:
            NoSuchElementException: If UI elements are not found.
        """
        self._navigate_to_bucket_properties(bucket_name)

        # Check versioning status using the checkbox input (most reliable method)
        try:
            # Method 1: Check if the checkbox input is checked (most reliable)
            checkbox_elements = self.get_elements(
                self.bucket_tab["versioning_checkbox_input"]
            )
            if checkbox_elements:
                is_checked = checkbox_elements[0].is_selected()
                logger.info(f"Versioning status (checkbox checked): {is_checked}")
                return is_checked

            # Method 2: Fallback - check checkbox 'checked' attribute
            checkbox_checked_attr = self.get_element_attribute(
                self.bucket_tab["versioning_checkbox_input"], "checked", safe=True
            )
            if checkbox_checked_attr is not None:
                is_enabled = checkbox_checked_attr == "true"
                logger.info(f"Versioning status (checked attribute): {is_enabled}")
                return is_enabled

            # Method 3: Final fallback - check status text
            versioning_status = self.get_element_text(
                self.bucket_tab["versioning_status"]
            )
            is_enabled = versioning_status.lower() != "disabled"
            logger.info(f"Versioning status (text fallback): {versioning_status}")
            return is_enabled
        except NoSuchElementException:
            logger.error("Could not find versioning status element")
            return False

    def enable_versioning(self, bucket_name: str = None) -> bool:
        """
        Enable versioning for the specified bucket.
        """
        try:
            changed = self.set_versioning_state(enabled=True, bucket_name=bucket_name)
            if changed:
                if self.is_versioning_enabled():
                    logger.info("Versioning enabled successfully")
                    return True
                else:
                    logger.error("Failed to enable versioning")
                    return False
            else:
                return False
        except NoSuchElementException:
            logger.error("Could not find versioning toggle element")
            return False
