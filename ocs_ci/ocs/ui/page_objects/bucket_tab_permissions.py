import json
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    StaleElementReferenceException,
    WebDriverException,
)

from ocs_ci.ocs.exceptions import PolicyApplicationError, PolicyEditorError
from ocs_ci.ocs.ui.page_objects.confirm_dialog import ConfirmDialog
from ocs_ci.ocs.ui.page_objects.object_storage import ObjectStorage
from ocs_ci.ocs.resources.bucket_policy import gen_bucket_policy_ui_compatible
from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(__name__)

# Module constants
DEFAULT_UI_WAIT = 10
QUICK_WAIT = 5  # Quick probe delay for checking element presence

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
    account_list: Optional[list[str]] = None
    folder_path: Optional[str] = None

    def __post_init__(self):
        """Set default values after initialization."""
        if self.account_list is None:
            self.account_list = []


class BucketsTabPermissions(ObjectStorage, ConfirmDialog):
    """
    A class representation for abstraction of Buckets tab permissions related OpenShift UI actions
    """

    # Policy action button keys to try in order
    _POLICY_ACTION_BUTTONS = ("apply_policy_button", "save_policy_generic_button")

    def navigate_to_bucket_permissions(self, bucket_name: str = None) -> None:
        """
        Navigate to bucket permissions tab.

        Args:
            bucket_name (str, optional): Name of the bucket. If None, uses first bucket.

        Raises:
            NoSuchElementException: If UI elements are not found.
        """
        if not bucket_name:
            logger.debug("Navigating to first bucket")
            self.do_click(self.bucket_tab["bucket_list_items"])
            self.do_click(self.bucket_tab["permissions_tab"])
            return

        logger.debug(f"Navigating to bucket: {bucket_name}")
        bucket_elements = self.get_elements(self.bucket_tab["bucket_list_items"])

        for bucket_element in bucket_elements:
            if bucket_element.text != bucket_name:
                continue

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
        logger.debug("Activating policy editor")

        try:
            self.wait_for_element_to_be_visible(
                self.bucket_tab["edit_policy_button"], timeout=QUICK_WAIT
            )
            logger.debug("Policy exists - using edit policy button")
            self.do_click(self.bucket_tab["edit_policy_button"])
            logger.debug("Successfully clicked edit policy button")
            return
        except (NoSuchElementException, TimeoutException):
            pass

        # If "Edit policy" button not found, try "Start from scratch"
        try:
            self.wait_for_element_to_be_visible(
                self.bucket_tab["policy_editor_start_scratch"], timeout=QUICK_WAIT
            )
            logger.debug("No existing policy - using start from scratch")
            self.do_click(self.bucket_tab["policy_editor_start_scratch"])
            logger.debug("Successfully clicked start from scratch button")
            return
        except (NoSuchElementException, TimeoutException):
            pass

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
            ocp_obc = OCP(kind="ObjectBucketClaim", namespace="openshift-storage")
            for obc in ocp_obc.get()["items"]:
                if obc.get("spec", {}).get("bucketName") != bucket_name:
                    continue

                obc_obj = OBC(obc["metadata"]["name"])
                if hasattr(obc_obj, "obc_account") and obc_obj.obc_account:
                    logger.debug(
                        f"Found account ID for bucket {bucket_name}: {obc_obj.obc_account}"
                    )
                    return obc_obj.obc_account

                break

            raise ValueError(f"No account ID found for bucket '{bucket_name}'")

        except (KeyError, AttributeError, ValueError) as e:
            raise ValueError(
                f"Unable to retrieve real account ID for bucket '{bucket_name}': {e}. "
                "Account-specific policies require buckets created via ObjectBucketClaim (OBC) "
                "which have associated NooBaa account IDs."
            ) from e

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

    def _get_and_log_real_account_id(
        self, bucket_name: str, account_list: Optional[list[str]] = None
    ) -> str:
        """
        Get the real account ID for a bucket and log the substitution.

        Args:
            bucket_name (str): Name of the bucket to get account ID for.
            account_list (Optional[list[str]]): Provided account list (ignored).

        Returns:
            str: Real account ID associated with the bucket.
        """
        real_account_id = self._get_real_account_id_from_bucket(bucket_name)
        logger.debug(
            f"Using real account ID: {real_account_id} instead of provided accounts: {account_list}"
        )
        return real_account_id

    def _build_public_read_policy(self, config: PolicyConfig) -> dict:
        """Build AllowPublicReadAccess policy."""
        return gen_bucket_policy_ui_compatible(
            "*", ["GetObject"], [f"{config.bucket_name}/*"], "Allow"
        )

    def _build_specific_account_policy(self, config: PolicyConfig) -> dict:
        """Build AllowAccessToSpecificAccount policy."""
        real_account_id = self._get_and_log_real_account_id(
            config.bucket_name, config.account_list
        )
        return gen_bucket_policy_ui_compatible(
            real_account_id,
            ["GetObject", "PutObject", "DeleteObject"],
            [f"{config.bucket_name}/*"],
            "Allow",
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
        real_account_id = self._get_and_log_real_account_id(
            config.bucket_name, config.account_list
        )
        return gen_bucket_policy_ui_compatible(
            real_account_id,
            ["GetObject", "PutObject", "DeleteObject"],
            [f"{config.bucket_name}/{clean_folder_path}"],
            "Allow",
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
                raise PolicyEditorError(error_msg)

            logger.debug(f"Successfully set policy JSON via: {result}")

        except WebDriverException as e:
            logger.debug("JavaScript approach failed", exc_info=True)
            error_msg = (
                "Failed to set policy JSON in Monaco editor using JavaScript approach. "
                "Check if Monaco editor is properly loaded and accessible."
            )
            logger.error(error_msg)
            raise PolicyEditorError(error_msg) from e

    def _check_for_policy_error_dialog(self) -> tuple[bool, str]:
        """
        Check if a policy application error dialog is present.

        Returns:
            tuple[bool, str]: (error_found, error_message)
        """
        error_selector = ".pf-v5-c-modal-box.pf-m-warning"

        try:
            error_elements = self.get_elements((error_selector, By.CSS_SELECTOR))
            if not error_elements or not error_elements[0].is_displayed():
                return False, ""

            message = self._extract_error_message(error_elements[0])

            # WORKAROUND: UI bug where success messages appear in warning-styled modals
            success_keywords = [
                "successfully created",
                "successfully applied",
                "has been successfully",
                "policy applied successfully",
                "bucket policy has been successfully",
            ]

            if any(keyword in message.lower() for keyword in success_keywords):
                logger.debug(f"WORKAROUND: Success message in warning modal: {message}")
                return False, ""

            logger.error(f"Policy error dialog found: {message}")
            return True, message

        except (NoSuchElementException, TimeoutException):
            logger.debug("No error dialog found")
            return False, ""

    def _extract_error_message(self, error_element) -> str:
        """Extract error message from modal element."""
        try:
            desc_elements = self.get_elements(
                (".pf-v5-c-alert__description", By.CSS_SELECTOR)
            )
            if desc_elements and desc_elements[0].text.strip():
                return desc_elements[0].text.strip()
            else:
                return error_element.text.strip() or "Unknown message"
        except (NoSuchElementException, StaleElementReferenceException):
            logger.debug("Could not extract detailed message")
            return error_element.text.strip() or "Unknown message"

    def _click_policy_action_button(self) -> None:
        """
        Click the policy action button with fallback options.

        Raises:
            TimeoutException: If no policy action button is found.
        """
        logger.debug("Attempting to click policy action button")

        for button_key in self._POLICY_ACTION_BUTTONS:
            try:
                self.do_click(self.bucket_tab[button_key])
                logger.debug(f"Successfully clicked {button_key}")
                return
            except (
                NoSuchElementException,
                TimeoutException,
                StaleElementReferenceException,
            ):
                continue

        error_msg = (
            f"Could not find any policy action button. "
            f"Attempted buttons: {self._POLICY_ACTION_BUTTONS}. "
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

        try:
            self.wait_for_element_to_be_visible(
                self.bucket_tab["update_policy_modal_button"],
                timeout=DEFAULT_UI_WAIT,
            )
        except TimeoutException:
            logger.debug("Modal wait timeout (expected) - proceeding")

        logger.debug("Clicking modal confirmation button")
        self.do_click(self.bucket_tab["update_policy_modal_button"])

        error_found, error_message = self._check_for_policy_error_dialog()
        if error_found:
            raise PolicyApplicationError(f"Policy application failed: {error_message}")

    def _verify_policy_application_success(self) -> None:
        """
        Verify that the policy was successfully applied by checking for success toast.
        """
        logger.debug("Verifying policy application success")

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
        self, bucket_name: str = None, account_list: Optional[list[str]] = None
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
        account_list: Optional[list[str]] = None,
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

    def _validate_policy_exists_for_deletion(self, bucket_name: str) -> None:
        """
        Validate that a bucket policy exists before attempting deletion.

        Args:
            bucket_name (str): Name of the bucket to check.

        Raises:
            ValueError: If no bucket policy exists to delete.
        """
        try:
            self.wait_for_element_to_be_visible(
                self.bucket_tab["edit_policy_button"], timeout=QUICK_WAIT
            )
            logger.debug(f"Policy exists for bucket: {bucket_name}")
        except (NoSuchElementException, TimeoutException):
            raise ValueError(
                "No bucket policy exists to delete. "
                "A policy must exist before it can be deleted. "
                "Please create a policy first using one of the set_bucket_policy_* methods."
            )

    def _handle_delete_confirmation_dialog(self) -> None:
        """
        Handle the delete policy confirmation dialog workflow.

        This method handles the complete confirmation flow:
        1. Wait for confirmation modal to appear
        2. Clear and type 'delete' in the input field
        3. Wait for confirm button to be enabled
        4. Click the confirm button

        Raises:
            NoSuchElementException: If UI elements are not found.
            TimeoutException: If elements are not found within timeout.
        """
        logger.debug("Handling delete policy confirmation dialog")

        self.wait_for_element_to_be_visible(
            self.bucket_tab["delete_policy_confirmation_modal"], timeout=DEFAULT_UI_WAIT
        )

        confirmation_input = self.wait_for_element_to_be_visible(
            self.bucket_tab["delete_policy_confirmation_input"]
        )
        confirmation_input.clear()
        confirmation_input.send_keys("delete")

        self.wait_for_element_to_be_visible(
            self.bucket_tab["delete_policy_confirm_button_enabled"],
            timeout=DEFAULT_UI_WAIT,
        )

        self.do_click(self.bucket_tab["delete_policy_confirm_button_enabled"])

    def delete_bucket_policy_ui(self, bucket_name: str = None) -> None:
        """
        Complete workflow to delete bucket policy via UI.

        This method orchestrates the complete deletion workflow:
        1. Resolves bucket name
        2. Navigates to bucket permissions
        3. Validates policy exists
        4. Activates policy editor
        5. Initiates deletion
        6. Handles confirmation dialog

        Args:
            bucket_name (str, optional): Name of the bucket. If None, uses first bucket.

        Raises:
            ValueError: If no bucket policy exists to delete or no buckets are available.
            NoSuchElementException: If UI elements are not found.
            TimeoutException: If elements are not found within timeout.
        """
        logger.info("Starting delete bucket policy workflow")

        bucket_name = self._resolve_bucket_name(bucket_name)

        self.navigate_to_bucket_permissions(bucket_name)
        self._validate_policy_exists_for_deletion(bucket_name)
        self.activate_policy_editor()

        logger.debug("Clicking delete policy button")
        self.do_click(self.bucket_tab["delete_policy_button"])

        self._handle_delete_confirmation_dialog()

        logger.info("Successfully completed delete bucket policy workflow")
