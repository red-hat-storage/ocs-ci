import json
import logging
import uuid
import requests
import os
import time
from dataclasses import dataclass
from typing import Union
from enum import Enum

from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    StaleElementReferenceException,
)

from ocs_ci.ocs.ocp import get_ocp_url
from ocs_ci.ocs import exceptions
from ocs_ci.ocs.exceptions import PolicyApplicationError
from ocs_ci.ocs.ui.page_objects.confirm_dialog import ConfirmDialog
from ocs_ci.ocs.ui.page_objects.object_storage import ObjectStorage
from ocs_ci.utility import version
from ocs_ci.utility.utils import generate_folder_with_files
from ocs_ci.ocs.resources.bucket_policy import gen_bucket_policy_ui_compatible
from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(__name__)


# Module constants
DEFAULT_UI_WAIT = 10
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
            self.account_list = ["123456789012"]  # Placeholder


@dataclass
class LocatorConfig:
    """Configuration for UI locator-based buttons."""

    locator_key: str
    description: str


@dataclass
class SelectorConfig:
    """Configuration for direct CSS selector-based buttons."""

    selector: str
    by_type: By
    description: str


ButtonConfig = Union[LocatorConfig, SelectorConfig]


class BucketsTab(ObjectStorage, ConfirmDialog):
    """
    A class representation for abstraction of Buckets tab related OpenShift UI actions
    """

    # Methods can directly access locators via self.bucket_tab, self.generic_locators etc.
    # No need to explicitly import or assign them

    def create_bucket_ui(self, method: str) -> ObjectStorage:
        """
        Creates a bucket via UI using specified method.

        Args:
            method (str): Creation method, either 'obc' or 's3'.

        Returns:
            ObjectStorage: Instance of ObjectStorage class.

        Raises:
            ValueError: If method is not 'obc' or 's3'.
        """
        self.do_click(self.bucket_tab["create_bucket_button"])
        if method == "obc":
            return self.create_bucket_via_obc()
        elif method == "s3":
            return self.create_bucket_via_s3()
        else:
            raise ValueError(f"Invalid method: {method}")

    def create_bucket_via_obc(self) -> ObjectStorage:
        """
        Creates bucket via OBC with improved dropdown handling.

        Returns:
            ObjectStorage: Instance of ObjectStorage class.

        Raises:
            NoSuchElementException: If UI elements are not found.
        """
        name_generator = f"test-bucket-obc-{uuid.uuid4()}"

        logger.info("Clicking create bucket via OBC button")
        self.do_click(self.bucket_tab["create_bucket_button_obc"])
        self.do_send_keys(self.bucket_tab["obc_bucket_name_input"], name_generator)
        logger.info("Selecting storage class")
        try:
            # Try primary locator first
            try:
                logger.info("Trying to find dropdown by data-test attribute")
                self.do_click(self.bucket_tab["storage_class_dropdown"])
                logger.info("Dropdown found")
            except NoSuchElementException:
                # Fallback to aria label if data-test not found
                logger.info("Trying to find dropdown by aria label")

            logger.info("Selecting noobaa storage class option")
            self.do_click(self.bucket_tab["storage_class_noobaa_option"])

        except NoSuchElementException:
            logger.exception("Failed to select storage class")
            raise

        logger.info("Clicking submit button to create OBC")
        time.sleep(2)  # This sleep is needed to make sure the OBC is created
        self.do_click(self.bucket_tab["submit_button_obc"])

        logger.info("Waiting for OBC to be created")
        return ObjectStorage()

    def create_bucket_via_s3(self) -> ObjectStorage:
        """
        Creates bucket via S3 method.

        Returns:
            ObjectStorage: Instance of ObjectStorage class.

        Raises:
            NoSuchElementException: If UI elements are not found.
        """
        name_generator = f"test-bucket-s3-{uuid.uuid4()}"

        self.do_click(self.bucket_tab["create_bucket_button_s3"])
        self.do_send_keys(self.bucket_tab["s3_bucket_name_input"], name_generator)
        self.do_click(self.bucket_tab["submit_button_obc"])
        return ObjectStorage()

    def create_folder_in_bucket(
        self, bucket_name: str = None, folder_name: str = None
    ) -> str:
        """
        Creates folder in specified bucket and uploads a file to it.

        Args:
            bucket_name (str, optional): Name of the bucket. If None, uses first bucket.
            folder_name (str, optional): Name of the folder. If None, generates random name.

        Returns:
            str: Name of the created folder.

        Raises:
            NoSuchElementException: If UI elements are not found.
        """
        # Note That object must be uploaded to the folder before navigating out of the bucket,
        # else the folder will be vanished

        if bucket_name:
            self.do_click(f"//tr//a[contains(text(), '{bucket_name}')]", By.XPATH)
        else:
            self.do_click(self.bucket_tab["first_bucket"])

        if not folder_name:
            folder_name = f"test-folder-{uuid.uuid4()}"

        self.do_click(self.bucket_tab["create_folder_button"])
        self.do_send_keys(self.bucket_tab["folder_name_input"], folder_name)
        self.do_click(self.bucket_tab["submit_button_folder"])

        # Get folder path and file generator
        folder_path, file_generator = generate_folder_with_files(num_files=400)

        # Create all files by consuming the generator
        files = list(file_generator)  # This creates all files
        logger.info(f"Created {len(files)} files in folder")

        logger.info("=== DEBUG: STARTING FILE UPLOAD ===")

        try:
            # Find the hidden file input
            file_input = self.driver.find_element(
                By.XPATH, "//input[@type='file'][@webkitdirectory]"
            )
            logger.info("Found directory input")

            logger.info(f"Files in folder: {os.listdir(folder_path)}")
            logger.info(
                "File input attributes before: "
                f"{self.driver.execute_script('return arguments[0].attributes;', file_input)}"
            )

            # Make the input visible but keep directory requirement
            self.driver.execute_script(
                """
                arguments[0].style.display = 'block';
                arguments[0].style.visibility = 'visible';
                arguments[0].style.height = '1px';
                arguments[0].style.width = '1px';
                arguments[0].style.opacity = '1';
            """,
                file_input,
            )
            logger.info("Modified file input for direct interaction")
            attrs = self.driver.execute_script(
                "return arguments[0].attributes;", file_input
            )
            logger.info(f"File input attributes after: {attrs}")

            logger.info(f"Sending folder path: {folder_path}")
            file_input.send_keys(folder_path)

            # Allow plenty of time for all files to be uploaded
            logger.info("Waiting 45 seconds for files to be fully uploaded...")
            time.sleep(45)
            logger.info("File upload wait completed")

            return folder_name

        except NoSuchElementException:
            logger.exception("Error during file upload")
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

    def create_multiple_buckets_ui(
        self, s3_buckets: int = 0, obc_buckets: int = 0
    ) -> list:
        """
        Creates multiple S3 and OBC buckets.

        Args:
            s3_buckets (int): Number of S3 buckets to create.
            obc_buckets (int): Number of OBC buckets to create.

        Returns:
            list: List of created bucket names.

        Raises:
            ValueError: If both s3_buckets and obc_buckets are 0.
        """
        if s3_buckets <= 0 and obc_buckets <= 0:
            raise ValueError("At least one bucket type must have a positive count")

        created_buckets = []

        self.navigate_buckets_page()
        self.refresh_page()
        self.page_has_loaded(sleep_time=2)

        # Create S3 buckets
        for i in range(s3_buckets):
            logger.debug(f"Creating S3 bucket #{i + 1}")
            bucket = self.create_bucket_ui(method="s3")
            created_buckets.append(bucket)

            if i < s3_buckets - 1 or obc_buckets > 0:
                self.navigate_buckets_page()
                self.page_has_loaded(sleep_time=2)
                logger.info("Navigated back to buckets page for next creation")

        # Create OBC buckets
        for i in range(obc_buckets):
            logger.debug(f"Creating OBC bucket #{i + 1}")
            bucket = self.create_bucket_ui(method="obc")
            created_buckets.append(bucket)

            if i < obc_buckets - 1:
                self.navigate_buckets_page()
                self.page_has_loaded(sleep_time=2)
                logger.info("Navigated back to buckets page for next creation")

        logger.info(f"Successfully created {len(created_buckets)} buckets")
        return created_buckets

    def has_pagination_controls(self) -> bool:
        """
        Check if pagination controls are visible by attempting to find the next button.

        Returns:
            bool: True if pagination controls are found, False otherwise
        """
        try:
            element_found = (
                len(self.get_elements(self.bucket_tab["pagination_next_button"])) > 0
            )
            logger.info(
                f"Pagination controls {'found' if element_found else 'not found'}"
            )
            return element_found
        except (NoSuchElementException, TimeoutException):
            logger.exception("Error checking pagination controls")
            return False

    def navigate_to_next_page(self) -> bool:
        """
        Navigate to the next page of buckets if available.

        Returns:
            bool: True if successfully navigated to next page, False otherwise
        """
        try:
            current_page_buckets = self.get_buckets_list()
            logger.info(
                f"Current page has {len(current_page_buckets)} buckets before navigation"
            )

            logger.info("Clicking next page button to move to next page")
            self.do_click(self.bucket_tab["pagination_next_button"])
            logger.info("Waiting for page to load after pagination")
            self.page_has_loaded(sleep_time=2)

            new_page_buckets = self.get_buckets_list()
            logger.info(
                f"New page has {len(new_page_buckets)} buckets after navigation"
            )

            current_page_set = set(current_page_buckets)
            new_page_set = set(new_page_buckets)
            if current_page_set == new_page_set:
                logger.warning(
                    "Navigation appears to have occurred, but bucket lists are identical"
                )
            else:
                logger.info(
                    f"Successfully navigated to next page: found {len(new_page_set - current_page_set)} new buckets"
                )

            return True
        except (
            NoSuchElementException,
            TimeoutException,
            StaleElementReferenceException,
        ):
            logger.exception("Error navigating to next page")
            return False

    def navigate_to_previous_page(self) -> bool:
        """
        Navigate to the previous page of buckets if available.

        Returns:
            bool: True if successfully navigated to previous page, False otherwise
        """
        try:
            current_page_buckets = self.get_buckets_list()
            logger.info(
                f"Current page has {len(current_page_buckets)} buckets before navigation"
            )

            logger.info("Clicking previous page button to move to previous page")
            self.do_click(self.bucket_tab["pagination_prev_button"])
            logger.info("Waiting for page to load after pagination")
            self.page_has_loaded(sleep_time=2)

            new_page_buckets = self.get_buckets_list()
            logger.info(
                f"New page has {len(new_page_buckets)} buckets after navigation"
            )

            current_page_set = set(current_page_buckets)
            new_page_set = set(new_page_buckets)
            if current_page_set == new_page_set:
                logger.warning(
                    "Navigation appears to have occurred, but bucket lists are identical"
                )
            else:
                diff_count = len(new_page_set - current_page_set)
                logger.info(
                    f"Successfully navigated to previous page: found {diff_count} different buckets"
                )

            return True
        except (
            NoSuchElementException,
            TimeoutException,
            StaleElementReferenceException,
        ):
            logger.exception("Error navigating to previous page")
            return False

    def delete_bucket_ui(self, delete_via, expect_fail, resource_name):
        """
        Delete an Object Bucket via the UI

        Args:
            delete_via (str): delete via 'OB/Actions' or via 'three dots'
            expect_fail (bool): verify if OB removal fails with proper PopUp message
            resource_name (str): Object Bucket Claim's name. The resource with its suffix will be deleted
        """
        logger.info(f"Attempting to delete bucket: {resource_name}")
        self.navigate_buckets_page()

        logger.info(f"Searching for bucket: {resource_name}")
        self.do_send_keys(self.generic_locators["search_resource_field"], resource_name)
        time.sleep(2)

        if delete_via == "three_dots":
            try:
                logger.info("Clicking action button")
                self.do_click(
                    self.bucket_tab["bucket_action_button"], enable_screenshot=True
                )
                time.sleep(1)

                logger.info("Selecting delete option from dropdown")
                self.do_click(
                    self.bucket_tab["bucket_delete_option"], enable_screenshot=True
                )
                time.sleep(1)

                logger.info(
                    f"Attempting to enter bucket name for confirmation: {resource_name}"
                )

                time.sleep(1.5)

                try:
                    dialog = self.driver.find_element(
                        By.CSS_SELECTOR, ".pf-v5-c-modal-box"
                    )
                    dialog.click()
                    time.sleep(0.5)

                    input_field = dialog.find_element(By.CSS_SELECTOR, "input")
                    input_field.clear()
                    input_field.send_keys(resource_name)
                    logger.info(
                        "Successfully entered bucket name in confirmation dialog"
                    )
                except (NoSuchElementException, StaleElementReferenceException):
                    logger.exception("Failed to enter bucket name")
                    self.take_screenshot()
                    self.copy_dom()

                logger.info("Clicking confirm button")
                self.do_click(
                    self.bucket_tab["bucket_confirm_button"], enable_screenshot=True
                )
                logger.info(
                    f"Successfully initiated deletion of bucket: {resource_name}"
                )
                time.sleep(5)

            except (
                NoSuchElementException,
                TimeoutException,
                StaleElementReferenceException,
            ):
                logger.exception("Error during bucket deletion")
                self.take_screenshot()
                self.copy_dom()

                if not expect_fail:
                    logger.info("Falling back to standard deletion method")
                    try:
                        self.delete_resource(delete_via, resource_name)
                    except Exception:
                        logger.exception("Fallback deletion also failed")
                        if not expect_fail:
                            raise
        else:
            logger.info("Using 'Actions' approach for deletion")
            self.delete_resource(delete_via, resource_name)

        if expect_fail:

            def _check_three_dots_disabled(text):
                logger.info(text)
                # locator of three_dots btn aligned with the specific resource name
                locator = (
                    f"//tr[contains(., '{resource_name}')]//button[@data-test='kebab-button'] | "
                    f"//td[@data-label='Name' and normalize-space()='{resource_name}']"
                    "/following-sibling::td//button[@aria-label='Kebab toggle']",
                    By.XPATH,
                )
                # when three_dots element is active attribute 'disabled' does not exist
                # it could be disabled="true" or with no value
                try:
                    self.wait_for_element_attribute(
                        locator,
                        attribute="disabled",
                        attribute_value="true",
                        timeout=5,
                        sleep=1,
                    )
                except exceptions.TimeoutExpiredError:
                    self.wait_for_element_attribute(
                        locator,
                        attribute="disabled",
                        attribute_value=None,
                        timeout=5,
                        sleep=1,
                    )

                # this popup is not available on ODF 4.18 and above
                if self.ocp_version_semantic < version.VERSION_4_18:
                    # PopUp is not reachable via Selenium driver. It does not appear in DOM
                    URL = f"{get_ocp_url()}/locales/resource.json?lng=en&ns=plugin__odf-console"

                    cookies = self.driver.get_cookies()
                    session = requests.Session()
                    for cookie in cookies:
                        session.cookies.set(cookie["name"], cookie["value"])

                    popup_str = (
                        "The corresponding ObjectBucketClaim must be deleted first."
                    )
                    logger.info(f"Send req to {URL}. Get PopUp with {popup_str}")

                    resp = session.get(url=URL, verify=False)
                    json_resp = resp.json()

                    assert (
                        popup_str == json_resp[popup_str]
                    ), f"No expected Popup. See full response: \n {json.dumps(json_resp)}"

            _check_three_dots_disabled("check three dots inactive automatically")
            self.refresh_page()
            self.page_has_loaded(sleep_time=2)
            _check_three_dots_disabled("check three dots inactive after refresh")

    def navigate_to_bucket_permissions(self, bucket_name: str = None) -> None:
        """
        Navigate to bucket permissions tab.

        Args:
            bucket_name (str, optional): Name of the bucket. If None, uses first bucket.

        Raises:
            NoSuchElementException: If UI elements are not found.
        """
        if bucket_name:
            logger.info(f"Navigating to bucket: {bucket_name}")
            bucket_elements = self.get_elements(self.bucket_tab["bucket_list_items"])

            for bucket_element in bucket_elements:
                if bucket_element.text == bucket_name:
                    bucket_locator = (
                        f"//a[contains(text(), '{bucket_name}')]",
                        By.XPATH,
                    )
                    self.do_click(bucket_locator)
                    logger.info(f"Successfully clicked on bucket: {bucket_name}")
                    break
            else:
                available_buckets = [elem.text for elem in bucket_elements]
                raise NoSuchElementException(
                    f"Bucket '{bucket_name}' not found in bucket list. "
                    f"Available buckets: {available_buckets}. "
                    "Verify bucket name exists and is visible on current page."
                )
        else:
            logger.info("Navigating to first bucket")
            self.do_click(self.bucket_tab["bucket_list_items"])
            bucket_elements = self.get_elements(self.bucket_tab["bucket_list_items"])
            if bucket_elements:
                logger.info(
                    f"Successfully clicked on first bucket: {bucket_elements[0].text}"
                )
            else:
                raise NoSuchElementException(
                    "No buckets found on the current page. "
                    "Ensure buckets exist or check if pagination is needed."
                )

        logger.info("Navigating to permissions tab")
        self.do_click(self.bucket_tab["permissions_tab"])

    def activate_policy_editor(self) -> None:
        """
        Activate the policy editor by clicking 'Start from scratch'.

        Raises:
            NoSuchElementException: If UI elements are not found.
        """
        logger.info("Clicking start from scratch to activate policy editor")

        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.do_click(self.bucket_tab["policy_editor_start_scratch"])
                logger.debug("Successfully clicked start from scratch button")
                break
            except StaleElementReferenceException:
                logger.debug(
                    f"Stale element on attempt {attempt + 1}/{max_retries}, retrying..."
                )
                if attempt == max_retries - 1:
                    logger.error("Max retries reached for start from scratch button")
                    raise
                time.sleep(1)

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
            logger.warning(
                f"Could not get real account ID for bucket {bucket_name}: {e}"
            )
            # Fallback to a default test account ID if real account cannot be retrieved
            logger.warning("Using fallback account ID for testing")
            return "123456789012"

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
            ValueError: If policy type is not supported or configuration is invalid.
        """
        logger.debug(
            f"Building {policy_type.value} policy for bucket: {config.bucket_name}"
        )

        if policy_type == PolicyType.ALLOW_PUBLIC_READ:
            bucket_policy_generated = gen_bucket_policy_ui_compatible(
                user_list="*",
                actions_list=["GetObject"],
                resources_list=[f"{config.bucket_name}/*"],
                effect="Allow",
            )

        elif policy_type == PolicyType.ALLOW_SPECIFIC_ACCOUNT:
            # Get real account ID from the bucket instead of using provided fake account IDs
            real_account_id = self._get_real_account_id_from_bucket(config.bucket_name)
            logger.info(
                f"Using real account ID: {real_account_id} instead of provided accounts: {config.account_list}"
            )

            bucket_policy_generated = gen_bucket_policy_ui_compatible(
                user_list=real_account_id,
                actions_list=["GetObject", "PutObject", "DeleteObject"],
                resources_list=[f"{config.bucket_name}/*"],
                effect="Allow",
            )

        elif policy_type == PolicyType.ENFORCE_HTTPS:
            # For HTTPS enforcement policies with conditions, we need to use "*" action
            # without the s3: prefix, so we build the policy manually instead of using
            # gen_bucket_policy_ui_compatible which automatically adds s3: prefix
            bucket_policy_generated = {
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

        elif policy_type == PolicyType.ALLOW_FOLDER_ACCESS:
            if not config.folder_path:
                config.folder_path = "documents"
                logger.debug(f"Using default folder path: {config.folder_path}")

            clean_folder_path = config.folder_path.strip("/")
            if not clean_folder_path.endswith("/*"):
                clean_folder_path = f"{clean_folder_path}/*"

            # Get real account ID from the bucket instead of using provided fake account IDs
            real_account_id = self._get_real_account_id_from_bucket(config.bucket_name)
            logger.info(
                f"Using real account ID: {real_account_id} instead of provided accounts: {config.account_list}"
            )

            bucket_policy_generated = gen_bucket_policy_ui_compatible(
                user_list=real_account_id,
                actions_list=["GetObject", "PutObject", "DeleteObject"],
                resources_list=[f"{config.bucket_name}/{clean_folder_path}"],
                effect="Allow",
            )

        else:
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

        # Use JavaScript to set Monaco editor value directly
        try:
            logger.debug("Attempting to set Monaco editor value via JavaScript")
            js_code = """
            // Find Monaco editor instance
            if (window.monaco && window.monaco.editor) {
                const editors = window.monaco.editor.getEditors();
                if (editors.length > 0) {
                    const editor = editors[0];
                    editor.setValue(arguments[0]);
                    return 'success';
                }
            }
            // Alternative: Try direct DOM manipulation
            const textArea = document.querySelector('textarea.inputarea');
            if (textArea) {
                textArea.value = arguments[0];
                textArea.dispatchEvent(new Event('input', { bubbles: true }));
                return 'textarea_success';
            }
            return 'not_found';
            """
            result = self.driver.execute_script(js_code, policy_json)
            if result in ["success", "textarea_success"]:
                logger.debug(f"Successfully set policy JSON via JavaScript: {result}")
                return
            else:
                logger.debug(f"JavaScript approach failed, returned: {result}")
        except (NoSuchElementException, TimeoutException):
            logger.debug("JavaScript approach failed", exc_info=True)

        error_msg = (
            "Failed to set policy JSON in Monaco editor using JavaScript approach. "
            "Check if Monaco editor is properly loaded and accessible."
        )
        logger.error(error_msg)
        raise TimeoutException(error_msg)

    def _click_first_existing_button(self, button_configs: list[ButtonConfig]) -> bool:
        """
        Helper method to click the first existing button from a list of configurations.

        Args:
            button_configs (list[ButtonConfig]): List of LocatorConfig or SelectorConfig instances

        Returns:
            bool: True if a button was clicked, False otherwise
        """
        for config in button_configs:
            try:
                if isinstance(config, LocatorConfig):
                    logger.debug(f"Trying to {config.description.lower()}")
                    self.do_click(self.bucket_tab[config.locator_key])
                    logger.debug(f"Successfully clicked {config.description} button")
                    return True
                elif isinstance(config, SelectorConfig):
                    logger.debug(f"Trying selector: {config.selector}")
                    elements = self.get_elements((config.selector, config.by_type))
                    if elements:
                        element = elements[0]
                        logger.debug(
                            f"Found button with text: '{element.text}' using {config.description}"
                        )
                        self.do_click((config.selector, config.by_type))
                        logger.debug(
                            f"Successfully clicked {config.description} button via selector"
                        )
                        return True
            except (
                NoSuchElementException,
                TimeoutException,
                StaleElementReferenceException,
            ) as e:
                logger.debug(
                    f"Button attempt failed for {config.description}: {type(e).__name__}"
                )
                continue
        return False

    def _check_for_policy_error_dialog(self) -> tuple[bool, str]:
        """
        Check if a policy application error dialog is present.
        Now intelligently distinguishes between actual errors and success messages.

        Returns:
            tuple[bool, str]: (error_found, error_message)
                - error_found: True if actual error dialog is found, False otherwise
                - error_message: The error message text, empty string if no error
        """
        error_selector = ".pf-v5-c-modal-box.pf-m-warning"

        try:
            logger.debug(
                f"Checking for policy error dialog with selector: {error_selector}"
            )
            error_elements = self.get_elements((error_selector, By.CSS_SELECTOR))
            if error_elements:
                error_element = error_elements[0]
                if error_element.is_displayed():
                    logger.debug("Found dialog with warning style")

                    message = "Unknown message"
                    try:
                        desc_elements = self.get_elements(
                            (".pf-v5-c-alert__description", By.CSS_SELECTOR)
                        )
                        if desc_elements and desc_elements[0].text.strip():
                            message = desc_elements[0].text.strip()
                        else:
                            message = error_element.text.strip() or "Unknown message"
                    except (
                        NoSuchElementException,
                        StaleElementReferenceException,
                    ) as e:
                        logger.debug(f"Could not extract message: {e}")
                        message = error_element.text.strip() or "Unknown message"

                    success_keywords = [
                        "successfully created",
                        "successfully applied",
                        "has been successfully",
                        "policy applied successfully",
                        "bucket policy has been successfully",
                        "success",
                    ]

                    error_keywords = [
                        "error",
                        "failed",
                        "invalid",
                        "unauthorized",
                        "forbidden",
                        "denied",
                        "cannot",
                        "unable to",
                        "malformed",
                        "syntax error",
                        "policy validation failed",
                    ]

                    message_lower = message.lower()
                    is_success = any(
                        keyword in message_lower for keyword in success_keywords
                    )
                    is_error = any(
                        keyword in message_lower for keyword in error_keywords
                    )

                    if is_success and not is_error:
                        logger.debug(f"Dialog contains success message: {message}")
                        return (
                            False,
                            "",
                        )
                    elif is_error:
                        logger.error(
                            f"Policy error dialog found with message: {message}"
                        )
                        return True, message  # Actual error
                    else:
                        logger.warning(
                            f"Ambiguous dialog message, treating as error: {message}"
                        )
                        return True, message

        except (NoSuchElementException, TimeoutException):
            logger.debug("No error dialog found")
            pass

        return False, ""

    def apply_bucket_policy(self) -> None:
        """
        Apply the selected bucket policy and confirm in modal.

        Raises:
            NoSuchElementException: If UI elements are not found.
            TimeoutException: If modal or toast elements are not found within timeout.
            PolicyApplicationError: If policy application fails with an error.
        """
        logger.debug("Applying bucket policy")

        button_configs = [
            LocatorConfig("apply_policy_button", "Apply policy"),
            LocatorConfig("save_policy_generic_button", "Save (generic)"),
        ]

        if not self._click_first_existing_button(button_configs):
            attempted_buttons = [config.description for config in button_configs]
            error_msg = (
                f"Could not find any policy action button. "
                f"Attempted buttons: {attempted_buttons}. "
                "Check if policy editor is properly loaded and buttons are visible."
            )
            logger.error(error_msg)
            raise TimeoutException(error_msg)

        try:
            logger.debug("Waiting for policy update modal")
            try:
                self.wait_for_element_to_be_visible(
                    self.bucket_tab["update_policy_modal_button"],
                    timeout=DEFAULT_UI_WAIT,
                )
            except TimeoutException:
                logger.debug(
                    "Modal wait timeout (expected) - proceeding with button click"
                )

            logger.debug("Confirming policy update in modal")
            self.do_click(self.bucket_tab["update_policy_modal_button"])

            logger.debug("Checking for policy application errors")
            error_found, error_message = self._check_for_policy_error_dialog()
            if error_found:
                raise PolicyApplicationError(
                    f"Policy application failed: {error_message}"
                )

            logger.debug("Waiting for success indication")
            for selector in SUCCESS_TOAST_SELECTORS:
                try:
                    logger.debug(f"Trying success selector: {selector}")
                    try:
                        self.wait_for_element_to_be_visible(
                            (selector, By.CSS_SELECTOR), timeout=DEFAULT_UI_WAIT
                        )
                        logger.debug(
                            f"Success notification found with selector: {selector}"
                        )
                        return
                    except TimeoutException:
                        logger.debug(f"Toast selector {selector} timeout (expected)")
                        continue
                except TimeoutException:
                    continue

            logger.warning(
                "No success toast found, but policy may have been applied successfully"
            )

        except (TimeoutException, PolicyApplicationError):
            logger.exception("Error during policy application")
            raise

    def _resolve_bucket_name(self, bucket_name: str = None) -> str:
        """
        Resolve bucket name, using first available bucket if None provided.

        Args:
            bucket_name (str, optional): Name of the bucket. If None, uses first bucket.

        Returns:
            str: Resolved bucket name.

        Raises:
            ValueError: If no buckets are available.
        """
        if bucket_name is None:
            buckets = self.get_buckets_list()
            if not buckets:
                raise ValueError(
                    "No buckets available on the current page and no specific bucket name provided. "
                    "Ensure buckets exist or specify a bucket name explicitly."
                )
            bucket_name = buckets[0]
            logger.debug(f"Using first available bucket: {bucket_name}")
        return bucket_name

    def _set_bucket_policy_ui(
        self, policy_type: PolicyType, config: PolicyConfig
    ) -> None:
        """
        Complete workflow to set any bucket policy via UI.

        Args:
            policy_type (PolicyType): Type of policy to set.
            config (PolicyConfig): Configuration for the policy.

        Returns:
            None

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

            logger.debug(f"{policy_type.value} policy successfully applied")

        except (TimeoutException, ValueError):
            logger.exception(f"Failed to set {policy_type.value} bucket policy")
            raise
        except (NoSuchElementException, StaleElementReferenceException):
            logger.exception(
                f"Unexpected UI error during {policy_type.value} policy setting"
            )
            raise

    def set_bucket_policy_ui(self, bucket_name: str = None) -> None:
        """
        Complete workflow to set AllowPublicReadAccess bucket policy via UI.

        Args:
            bucket_name (str, optional): Name of the bucket. If None, uses first bucket.
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
        """
        bucket_name = self._resolve_bucket_name(bucket_name)
        config = PolicyConfig(bucket_name, account_list or ["123456789012"])
        self._set_bucket_policy_ui(PolicyType.ALLOW_SPECIFIC_ACCOUNT, config)

    def set_bucket_policy_enforce_https_ui(self, bucket_name: str = None) -> None:
        """
        Complete workflow to set EnforceSecureTransportHTTPS bucket policy via UI.

        Args:
            bucket_name (str, optional): Name of the bucket. If None, uses first bucket.
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
        """
        bucket_name = self._resolve_bucket_name(bucket_name)
        config = PolicyConfig(
            bucket_name, account_list or ["123456789012"], folder_path
        )
        self._set_bucket_policy_ui(PolicyType.ALLOW_FOLDER_ACCESS, config)
