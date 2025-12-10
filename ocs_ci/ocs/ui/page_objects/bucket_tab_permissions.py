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

from ocs_ci.ocs.exceptions import PolicyApplicationError
from ocs_ci.ocs.ui.page_objects.confirm_dialog import ConfirmDialog
from ocs_ci.ocs.ui.page_objects.object_storage import ObjectStorage
from ocs_ci.ocs.resources.bucket_policy import gen_bucket_policy_ui_compatible
from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(__name__)

# Module constants
DEFAULT_UI_WAIT = 10
QUICK_WAIT = 5


class PolicyType(Enum):
    """Enumeration of supported bucket policy types."""

    ALLOW_PUBLIC_READ = "AllowPublicReadAccess"
    ALLOW_SPECIFIC_ACCOUNT = "AllowAccessToSpecificAccount"
    ENFORCE_HTTPS = "EnforceSecureTransportHTTPS"
    ALLOW_FOLDER_ACCESS = "AllowReadWriteAccessToFolder"


class BlockPublicAccessType(Enum):
    BLOCK_ALL = "BlockAllPublicAccess"
    BLOCK_NEW_POLICIES = "BlockAccessByNewPolicies"
    BLOCK_CROSS_ACCOUNT = "BlockCrossAccountAccess"


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

    def navigate_back_to_buckets_list(self):
        """
        Navigate back to buckets list page.

        Returns:
            BucketsTab: Instance of BucketsTab page object.
        """
        return self.navigate_buckets_page()

    def activate_policy_editor(self) -> None:
        """
        Activate the policy editor by intelligently choosing between 'Edit policy' and 'Start from scratch'.

        This method checks if a policy already exists on the bucket:
        - If policy exists: clicks 'Edit policy' button
        - If no policy exists: clicks 'Start from scratch' button

        Raises:
            NoSuchElementException: If UI elements are not found.
        """
        try:
            self.do_click(self.bucket_tab["edit_policy_button"], timeout=QUICK_WAIT)
            return
        except (NoSuchElementException, TimeoutException):
            pass

        try:
            self.do_click(
                self.bucket_tab["policy_editor_start_scratch"], timeout=QUICK_WAIT
            )
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

        return buckets[0]

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
        return self._get_real_account_id_from_bucket(bucket_name)

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

        return json.dumps(bucket_policy_generated, indent=2)

    def set_policy_json_in_editor(self, policy_json: str) -> None:
        """
        Set the policy JSON content in the code editor.

        Args:
            policy_json (str): JSON string to set in the editor.

        Raises:
            NoSuchElementException: If UI elements are not found.
        """
        try:
            self.do_click(
                self.bucket_tab["policy_code_editor"],
                enable_screenshot=False,
                copy_dom=False,
            )
        except TimeoutException:
            pass

        self._set_content_via_javascript(policy_json)

    def _set_content_via_javascript(self, content: str) -> None:
        """
        Set content using JavaScript with Monaco and textarea fallbacks.

        Args:
            content (str): Content to set in the editor.

        Raises:
            TimeoutException: If all fallback strategies fail.
        """
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
                raise TimeoutException(error_msg)

        except WebDriverException as e:
            error_msg = (
                "Failed to set policy JSON in Monaco editor using JavaScript approach. "
                "Check if Monaco editor is properly loaded and accessible."
            )
            logger.exception(error_msg)
            raise TimeoutException(error_msg) from e

    def _check_for_policy_error_dialog(self) -> tuple[bool, str]:
        """
        Check if a policy application error dialog is present.

        Returns:
            tuple[bool, str]: (error_found, error_message)
        """
        error_selector = "[role='dialog'][class*='pf-m-warning']"

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
                return False, ""

            logger.error(f"Policy error dialog found: {message}")
            return True, message

        except (
            NoSuchElementException,
            TimeoutException,
            StaleElementReferenceException,
        ):
            return False, ""

    def _extract_error_message(self, error_element) -> str:
        """Extract error message from modal element."""
        try:
            desc_elements = self.get_elements(
                ("[class*='-c-alert__description']", By.CSS_SELECTOR)
            )
            if desc_elements and desc_elements[0].text.strip():
                return desc_elements[0].text.strip()
            else:
                return error_element.text.strip() or "Unknown message"
        except (NoSuchElementException, StaleElementReferenceException):
            return error_element.text.strip() or "Unknown message"

    def _click_policy_action_button(self) -> None:
        """
        Click the policy action button with fallback options.

        Raises:
            TimeoutException: If no policy action button is found.
        """
        for button_key in self._POLICY_ACTION_BUTTONS:
            try:
                self.do_click(self.bucket_tab[button_key])
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
        self.wait_for_element_to_be_visible(
            self.bucket_tab["update_policy_modal_button"],
            timeout=DEFAULT_UI_WAIT,
        )
        self.do_click(self.bucket_tab["update_policy_modal_button"])

        # Wait for modal to close and DOM to stabilize before checking for errors
        self.page_has_loaded(sleep_time=3)

        error_found, error_message = self._check_for_policy_error_dialog()
        if error_found:
            raise PolicyApplicationError(f"Policy application failed: {error_message}")

    def _verify_policy_application_success(self) -> None:
        """
        Verify that the policy was successfully applied by checking for success toast.
        """
        combined_selectors = ", ".join(self.bucket_tab["success_toast_selectors"])

        try:
            self.wait_for_element_to_be_visible(
                (combined_selectors, By.CSS_SELECTOR), timeout=DEFAULT_UI_WAIT
            )
        except TimeoutException:
            pass

    def apply_bucket_policy(self) -> None:
        """
        Apply the selected bucket policy and confirm in modal.

        Raises:
            NoSuchElementException: If UI elements are not found.
            TimeoutException: If modal or toast elements are not found within timeout.
            PolicyApplicationError: If policy application fails with an error.
        """
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
        if policy_type == PolicyType.ENFORCE_HTTPS:
            import pytest

            pytest.skip(
                "EnforceSecureTransportHTTPS policy with condition-based statements (aws:SecureTransport) "
                "is not supported by NooBaa. NooBaa only supports basic Allow/Deny policies without conditions."
            )

        try:
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
        return [bucket.text for bucket in buckets]

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
        1. Wait for confirmation input field (modal is implied)
        2. Click input to focus it
        3. Clear and type 'delete' in the input field
        4. Wait for confirm button to be enabled
        5. Click the confirm button

        Raises:
            NoSuchElementException: If UI elements are not found.
            TimeoutException: If elements are not found within timeout.
        """
        confirmation_input = self.wait_for_element_to_be_visible(
            self.bucket_tab["delete_policy_confirmation_input"]
        )
        confirmation_input.click()
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
        2. Validates policy exists
        3. Activates policy editor
        4. Initiates deletion
        5. Handles confirmation dialog

        Args:
            bucket_name (str, optional): Name of the bucket. If None, uses first bucket.

        Raises:
            ValueError: If no bucket policy exists to delete or no buckets are available.
            NoSuchElementException: If UI elements are not found.
            TimeoutException: If elements are not found within timeout.
        """
        logger.info("Starting delete bucket policy workflow")

        bucket_name = self._resolve_bucket_name(bucket_name)

        self._validate_policy_exists_for_deletion(bucket_name)
        self.activate_policy_editor()

        self.do_click(self.bucket_tab["delete_policy_button"])

        self._handle_delete_confirmation_dialog()

        logger.info("Successfully completed delete bucket policy workflow")

    def navigate_to_block_public_access_tab(self) -> None:
        """
        Navigate to the 'Block public access' tab from within the bucket 'Permissions' tab.

        Raises:
            NoSuchElementException: If 'Block public access' tab is not found.
        """
        try:
            self.do_click(self.bucket_tab["block_public_access_tab"])
        except (NoSuchElementException, TimeoutException):
            raise NoSuchElementException(
                "Could not find 'Block public access' tab. "
                "Check if bucket permissions tab is properly loaded."
            )

    def verify_block_public_access(
        self, block_public_access: BlockPublicAccessType
    ) -> None:
        """
        Verifies that block public access button is working as expected.

        The workflow is as follows:
        1. Check the corresponding checkbox and save the page
        2. Verify that the checkbox is indeed checked and the text appearing near it is the expected one
        3. Uncheck the corresponding checkbox and save the page
        4. Verify that the checkbox is indeed unchecked and the text appearing near it is the expected one

        Args:
            block_public_access (BlockPublicAccessType): Block public access type to be verified.

        Raises:
            NoSuchElementException: If UI elements are not found.
        """
        try:

            block_public_access_dict = {
                BlockPublicAccessType.BLOCK_ALL: {
                    "checkbox": "block_all_public_access_checkbox",
                    "msg": "block_all_public_access_msg",
                    "text_checked": "Blocked all",
                    "text_unchecked": "Unblocked all",
                },
                BlockPublicAccessType.BLOCK_NEW_POLICIES: {
                    "checkbox": "block_new_public_policies_checkbox",
                    "msg": "block_new_public_policies_msg",
                    "text_checked": "Blocked",
                    "text_unchecked": "Unblocked",
                },
                BlockPublicAccessType.BLOCK_CROSS_ACCOUNT: {
                    "checkbox": "block_cross_account_checkbox",
                    "msg": "block_cross_account_msg",
                    "text_checked": "Blocked",
                    "text_unchecked": "Unblocked",
                },
            }

            checkbox = block_public_access_dict[block_public_access]["checkbox"]
            msg = block_public_access_dict[block_public_access]["msg"]
            text_checked = block_public_access_dict[block_public_access]["text_checked"]
            text_unchecked = block_public_access_dict[block_public_access][
                "text_unchecked"
            ]

            # check the checkbox
            self.do_click(
                self.bucket_tab["manage_public_access_settings_button"],
                timeout=QUICK_WAIT,
            )
            self.do_click(self.bucket_tab[checkbox], timeout=QUICK_WAIT)
            self.do_click(
                self.bucket_tab["save_public_access_settings_button"],
                timeout=QUICK_WAIT,
            )
            if not self.get_checkbox_status(self.bucket_tab[checkbox]):
                raise ValueError("The checkbox was not checked")

            self.page_has_loaded()
            text = self.get_element_text(self.bucket_tab[msg])
            if text != text_checked:
                raise ValueError(
                    f"The text is not correct, expected {text_checked}, got {text}"
                )

            # uncheck the checkbox
            self.do_click(
                self.bucket_tab["manage_public_access_settings_button"],
                timeout=QUICK_WAIT,
            )
            self.do_click(self.bucket_tab[checkbox], timeout=QUICK_WAIT)

            self.do_click(
                self.bucket_tab["save_public_access_settings_button"],
                timeout=QUICK_WAIT,
            )
            self.wait_for_element_to_be_visible(
                self.bucket_tab["proceed_to_disable_public_access_button"],
                timeout=DEFAULT_UI_WAIT,
            )
            self.do_click(self.bucket_tab["proceed_to_disable_public_access_button"])

            if self.get_checkbox_status(self.bucket_tab[checkbox]):
                raise ValueError("The checkbox was not unchecked")

            text = self.get_element_text(self.bucket_tab[msg])
            if text != text_unchecked:
                raise ValueError(
                    f"The text is not correct, expected {text_unchecked}, got {text}"
                )

            return
        except (NoSuchElementException, TimeoutException) as e:
            raise NoSuchElementException(
                f"Could not find element for {block_public_access.value}. "
                f"Original error: {e}"
            ) from e
