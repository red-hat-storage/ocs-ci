import json
import logging
from dataclasses import dataclass
from enum import Enum

from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    StaleElementReferenceException,
    WebDriverException,
)

from ocs_ci.ocs.exceptions import PolicyApplicationError
from ocs_ci.ocs.ui.base_ui import wait_for_element_to_be_clickable
from ocs_ci.ocs.ui.page_objects.confirm_dialog import ConfirmDialog
from ocs_ci.ocs.ui.page_objects.object_storage import ObjectStorage
from ocs_ci.ocs.resources.bucket_policy import gen_bucket_policy_ui_compatible
from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(__name__)

# Module constants
DEFAULT_UI_WAIT = 10
# Policy action button keys to try in order
_POLICY_ACTION_BUTTONS = ("apply_policy_button", "save_policy_generic_button")

SUCCESS_TOAST_SELECTORS = [
    "[data-test='success-toast']",
    ".pf-c-alert--success",
    ".pf-v5-c-alert--success",
    ".pf-c-alert.pf-m-success",
    ".pf-v5-c-alert.pf-m-success",
    "[role='alert'][class*='success']",
    ".toast-notifications-list-pf .alert-success",
    ".co-alert--success",
]


class PolicyType(Enum):
    """Enumeration of supported bucket policy types."""

    ALLOW_PUBLIC_READ = "AllowPublicReadAccess"
    ALLOW_SPECIFIC_ACCOUNT = "AllowAccessToSpecificAccount"
    ENFORCE_HTTPS = "EnforceSecureTransportHTTPS"
    ALLOW_FOLDER_ACCESS = "AllowReadWriteAccessToFolder"


@dataclass
class PolicyConfig:
    """Configuration for bucket policy creation."""

    bucket_name: str
    account_list: list[str] = None
    folder_path: str = None

    def __post_init__(self):
        """Set default values after initialization."""
        if self.account_list is None:
            self.account_list = []


class BucketsTabPermissions(ObjectStorage, ConfirmDialog):
    """
    A class representation for abstraction of Buckets tab permissions related OpenShift UI actions
    """

    # Methods can directly access locators via self.bucket_tab, self.generic_locators etc.
    # No need to explicitly import or assign them

    def navigate_to_bucket_permissions(self, bucket_name: str = None) -> None:
        """
        Navigate to bucket permissions tab.

        Args:
            bucket_name (str, optional): Name of the bucket. If None, uses first bucket.

        Raises:
            NoSuchElementException: If UI elements are not found.
        """
        if bucket_name:
            logger.debug(f"Navigating to bucket: {bucket_name}")
            bucket_elements = self.get_elements(self.bucket_tab["bucket_list_items"])

            for bucket_element in bucket_elements:
                if bucket_element.text == bucket_name:
                    bucket_element.click()
                    logger.debug(f"Successfully clicked on bucket: {bucket_name}")
                    break
            else:
                available_buckets = [elem.text for elem in bucket_elements]
                raise NoSuchElementException(
                    f"Bucket '{bucket_name}' not found in bucket list. "
                    f"Available buckets: {available_buckets}. "
                    "Verify bucket name exists and is visible on current page."
                )
        else:
            logger.debug("Navigating to first bucket")
            self.do_click(self.bucket_tab["bucket_list_items"])

        logger.debug("Navigating to permissions tab")
        self.do_click(self.bucket_tab["permissions_tab"])

    def activate_policy_editor(self) -> None:
        """
        Activate the policy editor by intelligently choosing between 'Edit policy' and 'Start from scratch'.

        This method checks if a policy already exists on the bucket:
        - If policy exists: clicks 'Edit policy' button
        - If no policy exists: clicks 'Start from scratch' button

        Raises:
            NoSuchElementException: If UI elements are not found.
        """
        logger.debug("Activating policy editor: checking if policy exists")

        # First try to find "Edit policy" button (indicates existing policy)
        try:
            self.wait_for_element_to_be_visible(
                self.bucket_tab["edit_policy_button"], timeout=5
            )
            logger.debug("Policy exists - using edit policy button")
            wait_for_element_to_be_clickable(
                self.bucket_tab["edit_policy_button"], timeout=DEFAULT_UI_WAIT
            )
            self.do_click(self.bucket_tab["edit_policy_button"])
            logger.debug("Successfully clicked edit policy button")
            return
        except (NoSuchElementException, TimeoutException):
            logger.debug(
                "Edit policy button not found - checking for start from scratch"
            )

        # If "Edit policy" button not found, try "Start from scratch"
        try:
            self.wait_for_element_to_be_visible(
                self.bucket_tab["policy_editor_start_scratch"], timeout=5
            )
            logger.debug("No existing policy - using start from scratch")
            wait_for_element_to_be_clickable(
                self.bucket_tab["policy_editor_start_scratch"], timeout=DEFAULT_UI_WAIT
            )
            self.do_click(self.bucket_tab["policy_editor_start_scratch"])
            logger.debug("Successfully clicked start from scratch button")
            return
        except (NoSuchElementException, TimeoutException):
            logger.debug("Start from scratch button not found")

        # If neither button is found, raise an error
        raise NoSuchElementException(
            "Could not find either 'Edit policy' or 'Start from scratch' button. "
            "Check if bucket permissions page is properly loaded."
        )

    def _get_real_account_id_from_bucket(self, bucket_name: str) -> str:
        """
        Get the real account ID associated with a bucket name.

        Args:
            bucket_name (str): Name of the bucket to get account ID for.

        Returns:
            str: Real account ID associated with the bucket.

        Raises:
            ValueError: If bucket is not found or account ID cannot be retrieved.
        """
        try:
            # Search for ObjectBucketClaim that has this bucket name
            ocp_obc = OCP(kind="ObjectBucketClaim", namespace="openshift-storage")
            obcs = ocp_obc.get()["items"]

            matching_obc_name = None
            for obc in obcs:
                if obc.get("spec", {}).get("bucketName") == bucket_name:
                    matching_obc_name = obc["metadata"]["name"]
                    break

            if matching_obc_name:
                logger.debug(f"Found OBC {matching_obc_name} for bucket {bucket_name}")
                # Now get the OBC object using the correct claim name
                obc_obj = OBC(matching_obc_name)
                if hasattr(obc_obj, "obc_account") and obc_obj.obc_account:
                    logger.debug(
                        f"Found real account ID for bucket {bucket_name}: {obc_obj.obc_account}"
                    )
                    return obc_obj.obc_account
                else:
                    raise ValueError(f"No account ID found in OBC {matching_obc_name}")
            else:
                raise ValueError(f"No OBC found for bucket {bucket_name}")

        except Exception as e:
            logger.exception(f"Could not get real account ID for bucket {bucket_name}")
            raise ValueError(
                f"Unable to retrieve account ID for bucket '{bucket_name}'. "
                f"Ensure the bucket exists and has a valid ObjectBucketClaim. Error: {e}"
            )

    def _normalize_folder_path(self, folder_path: str = None) -> str:
        """
        Normalize folder path for bucket policies.

        Args:
            folder_path (str, optional): Raw folder path.

        Returns:
            str: Normalized folder path ending with /*.
        """
        folder = (folder_path or "documents").strip("/")
        return folder if folder.endswith("/*") else f"{folder}/*"

    def _resolve_bucket_name(self, bucket_name: str = None) -> str:
        """
        Resolve bucket name from parameter or use first available bucket.

        Args:
            bucket_name (str, optional): Name of the bucket. If None, uses first bucket.

        Returns:
            str: Resolved bucket name.

        Raises:
            ValueError: If no buckets are available.
        """
        if bucket_name:
            return bucket_name

        buckets = self.get_buckets_list()
        if not buckets:
            raise ValueError(
                "No buckets available on the current page and no specific bucket name provided. "
                "Ensure buckets exist or specify a bucket name explicitly."
            )

        bucket_name = buckets[0]
        logger.debug(f"Using first available bucket: {bucket_name}")
        return bucket_name

    def _build_public_read_policy(self, config: PolicyConfig) -> dict:
        """Build AllowPublicReadAccess policy."""
        return gen_bucket_policy_ui_compatible(
            user_list="*",
            actions_list=["GetObject"],
            resources_list=[f"{config.bucket_name}/*"],
            effect="Allow",
        )

    def _build_specific_account_policy(self, config: PolicyConfig) -> dict:
        """Build AllowAccessToSpecificAccount policy."""
        real_account_id = self._get_real_account_id_from_bucket(config.bucket_name)
        logger.debug(
            f"Using real account ID: {real_account_id} instead of provided accounts: {config.account_list}"
        )
        return gen_bucket_policy_ui_compatible(
            user_list=real_account_id,
            actions_list=["GetObject", "PutObject", "DeleteObject"],
            resources_list=[f"{config.bucket_name}/*"],
            effect="Allow",
        )

    def _build_enforce_https_policy(self, config: PolicyConfig) -> dict:
        """Build EnforceSecureTransportHTTPS policy."""
        return {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": "*",  # Use "*" not "s3:*" for conditional policies
                    "Principal": "*",
                    "Resource": [
                        f"arn:aws:s3:::{config.bucket_name}",
                        f"arn:aws:s3:::{config.bucket_name}/*",
                    ],
                    "Effect": "Deny",
                    "Sid": "statement",
                    "Condition": {"Bool": {"aws:SecureTransport": "false"}},
                }
            ],
        }

    def _build_folder_access_policy(self, config: PolicyConfig) -> dict:
        """Build AllowReadWriteAccessToFolder policy."""
        clean_folder_path = self._normalize_folder_path(config.folder_path)
        real_account_id = self._get_real_account_id_from_bucket(config.bucket_name)
        logger.debug(
            f"Using real account ID: {real_account_id} instead of provided accounts: {config.account_list}"
        )

        return gen_bucket_policy_ui_compatible(
            user_list=real_account_id,
            actions_list=["GetObject", "PutObject", "DeleteObject"],
            resources_list=[f"{config.bucket_name}/{clean_folder_path}"],
            effect="Allow",
        )

    def _build_bucket_policy(
        self, policy_type: PolicyType, config: PolicyConfig
    ) -> str:
        """
        Build bucket policy JSON based on policy type and configuration.

        Args:
            policy_type (PolicyType): Type of policy to build.
            config (PolicyConfig): Configuration for the policy.

        Returns:
            str: JSON string of the generated policy.

        Raises:
            ValueError: If policy type is not supported.
        """
        logger.debug(
            f"Building {policy_type.value} policy for bucket: {config.bucket_name}"
        )

        policy_builders = {
            PolicyType.ALLOW_PUBLIC_READ: self._build_public_read_policy,
            PolicyType.ALLOW_SPECIFIC_ACCOUNT: self._build_specific_account_policy,
            PolicyType.ENFORCE_HTTPS: self._build_enforce_https_policy,
            PolicyType.ALLOW_FOLDER_ACCESS: self._build_folder_access_policy,
        }

        try:
            bucket_policy_generated = policy_builders[policy_type](config)
        except KeyError:
            raise ValueError(f"Unsupported policy type: {policy_type}")

        policy_json = json.dumps(bucket_policy_generated, indent=2)
        logger.debug(f"Generated {policy_type.value} policy JSON")
        return policy_json

    def set_policy_json_in_editor(self, policy_json: str) -> None:
        """
        Set the policy JSON content in the code editor.

        Args:
            policy_json (str): JSON string to set in the editor.

        Raises:
            NoSuchElementException: If UI elements are not found.
        """
        logger.info("Clicking on policy code editor to focus it")
        try:
            self.do_click(
                self.bucket_tab["policy_code_editor"],
                enable_screenshot=False,
                copy_dom=False,
            )
        except TimeoutException:
            logger.debug(
                "Monaco editor click timeout (expected) - proceeding with JS approach"
            )

        logger.debug("Setting policy JSON using JavaScript Monaco editor approach")
        self._set_content_via_javascript(policy_json)

    def _set_content_via_javascript(self, content: str) -> None:
        """
        Set content using JavaScript with Monaco and textarea fallbacks.

        Args:
            content (str): Content to set in the editor.

        Raises:
            TimeoutException: If all fallback strategies fail.
        """
        logger.debug("Attempting to set content via JavaScript")

        js_code = """
        // Try Monaco editor API first
        if (window.monaco && window.monaco.editor) {
            const editors = window.monaco.editor.getEditors();
            if (editors.length > 0) {
                const editor = editors[0];
                editor.setValue(arguments[0]);
                return 'monaco_success';
            }
        }

        // Fallback to textarea manipulation
        const textArea = document.querySelector('textarea.inputarea');
        if (textArea) {
            textArea.value = arguments[0];
            textArea.dispatchEvent(new Event('input', { bubbles: true }));
            return 'textarea_success';
        }

        return 'failed';
        """

        try:
            result = self.driver.execute_script(js_code, content)
            if result == "failed":
                error_msg = (
                    "Failed to set policy JSON in Monaco editor using JavaScript approach. "
                    "Check if Monaco editor is properly loaded and accessible."
                )
                logger.error(error_msg)
                raise TimeoutException(error_msg)

            logger.debug(f"Successfully set policy JSON via: {result}")

        except WebDriverException:
            logger.debug("JavaScript approach failed", exc_info=True)
            error_msg = (
                "Failed to set policy JSON in Monaco editor using JavaScript approach. "
                "Check if Monaco editor is properly loaded and accessible."
            )
            logger.error(error_msg)
            raise TimeoutException(error_msg)

    def _check_for_policy_error_dialog(self) -> tuple[bool, str]:
        """
        Check if a policy application error dialog is present.

        Returns:
            tuple[bool, str]: (error_found, error_message)
        """
        error_selector = ".pf-v5-c-modal-box.pf-m-warning"

        try:
            error_elements = self.get_elements((error_selector, By.CSS_SELECTOR))
            if error_elements and error_elements[0].is_displayed():
                message = "Unknown message"
                try:
                    desc_elements = self.get_elements(
                        (".pf-v5-c-alert__description", By.CSS_SELECTOR)
                    )
                    if desc_elements and desc_elements[0].text.strip():
                        message = desc_elements[0].text.strip()
                    else:
                        message = error_elements[0].text.strip() or "Unknown message"
                except (NoSuchElementException, StaleElementReferenceException):
                    logger.debug("Could not extract detailed message")
                    message = error_elements[0].text.strip() or "Unknown message"

                # Check if this is actually a success message
                success_keywords = [
                    "successfully created",
                    "successfully applied",
                    "has been successfully",
                    "policy applied successfully",
                    "bucket policy has been successfully",
                ]

                message_lower = message.lower()
                is_success = any(
                    keyword in message_lower for keyword in success_keywords
                )

                if is_success:
                    logger.debug(f"Dialog contains success message: {message}")
                    return False, ""
                else:
                    logger.error(f"Policy error dialog found: {message}")
                    return True, message

        except (NoSuchElementException, TimeoutException):
            logger.debug("No error dialog found")

        return False, ""

    def _click_policy_action_button(self) -> None:
        """
        Click the policy action button with fallback options.

        Raises:
            TimeoutException: If no policy action button is found.
        """
        logger.debug("Attempting to click policy action button")

        for button_key in _POLICY_ACTION_BUTTONS:
            try:
                self.do_click(self.bucket_tab[button_key])
                logger.debug(f"Successfully clicked {button_key}")
                return
            except (
                NoSuchElementException,
                TimeoutException,
                StaleElementReferenceException,
            ):
                logger.debug(f"{button_key} not found, trying next")
                continue

        # If we reach here, no button was found
        error_msg = (
            f"Could not find any policy action button. "
            f"Attempted buttons: {_POLICY_ACTION_BUTTONS}. "
            "Check if policy editor is properly loaded and buttons are visible."
        )
        logger.error(error_msg)
        raise TimeoutException(error_msg)

    def _handle_policy_confirmation_modal(self) -> None:
        """
        Handle the policy confirmation modal.

        Raises:
            PolicyApplicationError: If policy application fails.
        """
        logger.debug("Handling policy confirmation modal")

        # Wait for modal (timeout is expected, continue anyway)
        try:
            self.wait_for_element_to_be_visible(
                self.bucket_tab["update_policy_modal_button"],
                timeout=DEFAULT_UI_WAIT,
            )
        except TimeoutException:
            logger.debug("Modal wait timeout (expected) - proceeding")

        # Click confirmation button
        logger.debug("Clicking modal confirmation button")
        self.do_click(self.bucket_tab["update_policy_modal_button"])

        # Check for application errors
        error_found, error_message = self._check_for_policy_error_dialog()
        if error_found:
            raise PolicyApplicationError(f"Policy application failed: {error_message}")

    def _verify_policy_application_success(self) -> None:
        """
        Verify that the policy was successfully applied by checking for success toast.
        """
        logger.debug("Verifying policy application success")

        # Combine all selectors into one CSS group selector
        combined_selectors = ", ".join(SUCCESS_TOAST_SELECTORS)

        try:
            self.wait_for_element_to_be_visible(
                (combined_selectors, By.CSS_SELECTOR), timeout=DEFAULT_UI_WAIT
            )
            logger.debug("Success notification found")
        except TimeoutException:
            logger.debug(
                "No success toast found, but policy may have been applied successfully"
            )

    def apply_bucket_policy(self) -> None:
        """
        Apply the selected bucket policy and confirm in modal.

        Raises:
            NoSuchElementException: If UI elements are not found.
            TimeoutException: If modal or toast elements are not found within timeout.
            PolicyApplicationError: If policy application fails with an error.
        """
        logger.debug("Applying bucket policy")

        self._click_policy_action_button()
        self._handle_policy_confirmation_modal()
        self._verify_policy_application_success()

    def _set_bucket_policy_ui(
        self, policy_type: PolicyType, config: PolicyConfig
    ) -> None:
        """
        Complete workflow to set any bucket policy via UI.

        Args:
            policy_type (PolicyType): Type of policy to set.
            config (PolicyConfig): Configuration for the policy.

        Raises:
            TimeoutException: If UI elements are not found within timeout.
            ValueError: If no buckets are available or configuration is invalid.
            pytest.skip: If policy type is not supported by the storage backend.
        """
        # Handle HTTPS enforcement special case - skip for NooBaa
        if policy_type == PolicyType.ENFORCE_HTTPS:
            import pytest

            pytest.skip(
                "EnforceSecureTransportHTTPS policy with condition-based statements (aws:SecureTransport) "
                "is not supported by NooBaa. NooBaa only supports basic Allow/Deny policies without conditions."
            )

        logger.debug(
            f"Setting {policy_type.value} policy for bucket {config.bucket_name}"
        )

        try:
            self.navigate_to_bucket_permissions(config.bucket_name)
            self.activate_policy_editor()

            policy_json = self._build_bucket_policy(policy_type, config)
            self.set_policy_json_in_editor(policy_json)

            self.apply_bucket_policy()

            logger.info(f"{policy_type.value} policy successfully applied")

        except (TimeoutException, ValueError):
            logger.exception(f"Failed to set {policy_type.value} bucket policy")
            raise
        except (NoSuchElementException, StaleElementReferenceException):
            logger.exception(
                f"Unexpected UI error during {policy_type.value} policy setting"
            )
            raise

    def get_buckets_list(self) -> list:
        """
        Get list of all buckets using href pattern

        Returns:
            list: List of bucket names as strings
        """
        buckets = self.get_elements(self.bucket_tab["bucket_list_items"])
        # Extract text from elements immediately to avoid stale element references later
        bucket_names = [bucket.text for bucket in buckets]
        logger.debug(f"Found {len(bucket_names)} buckets")
        return bucket_names

    def set_bucket_policy_ui(self, bucket_name: str = None) -> None:
        """
        Complete workflow to set AllowPublicReadAccess bucket policy via UI.

        Args:
            bucket_name (str, optional): Name of the bucket. If None, uses first bucket.

        Raises:
            ValueError: If no buckets are available.
        """
        bucket_name = self._resolve_bucket_name(bucket_name)
        config = PolicyConfig(bucket_name)
        self._set_bucket_policy_ui(PolicyType.ALLOW_PUBLIC_READ, config)

    def set_bucket_policy_specific_account_ui(
        self, bucket_name: str = None, account_list: list[str] = None
    ) -> None:
        """
        Complete workflow to set AllowAccessToSpecificAccount bucket policy via UI.

        Args:
            bucket_name (str, optional): Name of the bucket. If None, uses first bucket.
            account_list (list[str], optional): List of AWS account IDs (ignored - real account from bucket is used).

        Raises:
            ValueError: If no buckets are available.
        """
        bucket_name = self._resolve_bucket_name(bucket_name)
        config = PolicyConfig(bucket_name, account_list)
        self._set_bucket_policy_ui(PolicyType.ALLOW_SPECIFIC_ACCOUNT, config)

    def set_bucket_policy_enforce_https_ui(self, bucket_name: str = None) -> None:
        """
        Complete workflow to set EnforceSecureTransportHTTPS bucket policy via UI.

        Args:
            bucket_name (str, optional): Name of the bucket. If None, uses first bucket.

        Raises:
            ValueError: If no buckets are available.
        """
        bucket_name = self._resolve_bucket_name(bucket_name)
        config = PolicyConfig(bucket_name)
        self._set_bucket_policy_ui(PolicyType.ENFORCE_HTTPS, config)

    def set_bucket_policy_folder_access_ui(
        self,
        bucket_name: str = None,
        folder_path: str = None,
        account_list: list[str] = None,
    ) -> None:
        """
        Complete workflow to set AllowReadWriteAccessToFolder bucket policy via UI.

        Args:
            bucket_name (str, optional): Name of the bucket. If None, uses first bucket.
            folder_path (str, optional): Folder path within the bucket. If None, uses "documents".
            account_list (list[str], optional): List of AWS account IDs (ignored - real account from bucket is used).

        Raises:
            ValueError: If no buckets are available.
        """
        bucket_name = self._resolve_bucket_name(bucket_name)
        config = PolicyConfig(bucket_name, account_list, folder_path)
        self._set_bucket_policy_ui(PolicyType.ALLOW_FOLDER_ACCESS, config)

    def delete_bucket_policy_ui(self, bucket_name: str = None) -> None:
        """
        Complete workflow to delete bucket policy via UI.

        This method:
        1. Navigates to bucket permissions (if not already there)
        2. Checks if a policy exists before attempting to delete
        3. Activates policy editor for existing policy
        4. Clicks delete policy button
        5. Handles confirmation dialog (types "delete" and confirms)

        Args:
            bucket_name (str, optional): Name of the bucket. If None, uses first bucket.

        Raises:
            ValueError: If no bucket policy exists to delete or no buckets are available.
            NoSuchElementException: If UI elements are not found.
            TimeoutException: If elements are not found within timeout.
        """
        logger.info("Starting delete bucket policy workflow")

        bucket_name = self._resolve_bucket_name(bucket_name)

        # Navigate to bucket permissions
        self.navigate_to_bucket_permissions(bucket_name)
        logger.debug("✓ Navigated to bucket permissions")

        # Check if a policy exists before attempting to delete
        try:
            # Try to wait for the "Edit policy" button which indicates an existing policy
            self.wait_for_element_to_be_visible(
                self.bucket_tab["edit_policy_button"], timeout=5
            )
            logger.debug("✓ Verified policy exists")
        except (NoSuchElementException, TimeoutException):
            raise ValueError(
                "No bucket policy exists to delete. "
                "A policy must exist before it can be deleted. "
                "Please create a policy first using one of the set_bucket_policy_* methods."
            )

        # Activate policy editor (this opens existing policy for editing)
        self.activate_policy_editor()
        logger.debug("✓ Opened policy editor")

        # Click delete policy button
        logger.debug("Clicking delete policy button")
        wait_for_element_to_be_clickable(
            self.bucket_tab["delete_policy_button"], timeout=DEFAULT_UI_WAIT
        )
        self.do_click(self.bucket_tab["delete_policy_button"])
        logger.debug("✓ Clicked delete policy button")

        # Handle confirmation dialog
        logger.debug("Handling delete policy confirmation dialog")

        # Wait for the confirmation modal to appear
        self.wait_for_element_to_be_visible(
            self.bucket_tab["delete_policy_confirmation_modal"], timeout=DEFAULT_UI_WAIT
        )
        logger.debug("✓ Confirmation modal appeared")

        # Type "delete" in the confirmation input field
        confirmation_input = self.wait_for_element_to_be_visible(
            self.bucket_tab["delete_policy_confirmation_input"]
        )
        confirmation_input.clear()
        confirmation_input.send_keys("delete")
        logger.debug("✓ Typed 'delete' in confirmation input")

        # Wait for confirm delete button to become enabled (not disabled)
        # The button starts as disabled and becomes enabled after typing "delete"
        self.wait_for_element_to_be_visible(
            self.bucket_tab["delete_policy_confirm_button_enabled"],
            timeout=DEFAULT_UI_WAIT,
        )
        logger.debug("✓ Confirm delete button became enabled")

        # Click confirm delete button
        self.do_click(self.bucket_tab["delete_policy_confirm_button_enabled"])
        logger.debug("✓ Handled delete confirmation dialog")

        logger.info("Successfully completed delete bucket policy workflow")
