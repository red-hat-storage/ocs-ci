import logging
import time
from typing import Optional
from selenium.common.exceptions import NoSuchElementException

from ocs_ci.ocs.ui.page_objects.object_storage import ObjectStorage
from ocs_ci.ocs.ui.page_objects.confirm_dialog import ConfirmDialog

logger = logging.getLogger(__name__)


class BucketVersioning(ObjectStorage, ConfirmDialog):
    """
    A class representation for bucket versioning UI operations.
    Inherits from ObjectStorage and ConfirmDialog for base UI functionality.
    """

    # Class constants for timeouts and wait times
    UI_UPDATE_WAIT_TIME = 2
    DIALOG_TIMEOUT = 10

    def _navigate_to_bucket_properties(self, bucket_name: Optional[str] = None) -> None:
        """
        Navigate to bucket properties tab.

        Args:
            bucket_name (str, optional): Name of the bucket. If None, uses first bucket.

        Raises:
            NoSuchElementException: If UI elements are not found.
        """
        self.nav_object_storage_page()

        if bucket_name:
            bucket_locator = (
                self.bucket_tab["bucket_link_by_name"][0].format(bucket_name),
                self.bucket_tab["bucket_link_by_name"][1],
            )
            self.do_click(bucket_locator)
        else:
            self.do_click(self.bucket_tab["first_bucket"])

        self.do_click(self.bucket_tab["properties_tab"])

    def set_versioning_state(
        self, enabled: bool, bucket_name: Optional[str] = None
    ) -> bool:
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

            # Handle confirmation dialog if enabling versioning
            if enabled:
                self._handle_versioning_confirmation()

            time.sleep(self.UI_UPDATE_WAIT_TIME)  # Wait for UI to update
            return True
        else:
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
            if confirm:
                self.do_click(
                    self.bucket_tab["versioning_enable_confirm_button"],
                    timeout=self.DIALOG_TIMEOUT,
                )
            else:
                self.do_click(
                    self.bucket_tab["versioning_cancel_confirm_button"],
                    timeout=self.DIALOG_TIMEOUT,
                )
        except NoSuchElementException:
            logger.exception("Could not find versioning confirmation dialog")
            raise

    def is_versioning_enabled(self, bucket_name: Optional[str] = None) -> bool:
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

        return self._check_versioning_status()

    def _check_versioning_status(self) -> bool:
        """
        Check versioning status using multiple fallback methods.

        Returns:
            bool: True if versioning is enabled, False otherwise.
        """
        try:
            # Method 1: Check if the checkbox input is checked (most reliable)
            checkbox_elements = self.get_elements(
                self.bucket_tab["versioning_checkbox_input"]
            )
            if checkbox_elements:
                return checkbox_elements[0].is_selected()

            # Method 2: Fallback - check checkbox 'checked' attribute
            checkbox_checked_attr = self.get_element_attribute(
                self.bucket_tab["versioning_checkbox_input"], "checked", safe=True
            )
            if checkbox_checked_attr is not None:
                return checkbox_checked_attr == "true"

            # Method 3: Final fallback - check status text
            versioning_status = self.get_element_text(
                self.bucket_tab["versioning_status"]
            )
            return versioning_status.lower() != "disabled"
        except NoSuchElementException:
            logger.error("Could not find versioning status element")
            return False

    def enable_versioning(self, bucket_name: Optional[str] = None) -> bool:
        """
        Enable versioning for the specified bucket.

        Args:
            bucket_name (str, optional): Name of the bucket. If None, uses first bucket.

        Returns:
            bool: True if versioning was enabled, False if already enabled or failed.

        Raises:
            NoSuchElementException: If UI elements are not found.
        """
        changed = self.set_versioning_state(enabled=True, bucket_name=bucket_name)
        if not changed:
            return False

        if self.is_versioning_enabled(bucket_name):
            return True
        else:
            logger.error("Failed to enable versioning - UI state did not change")
            return False
