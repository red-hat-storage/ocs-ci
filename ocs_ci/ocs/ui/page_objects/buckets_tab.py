import json
import logging
import uuid
import requests
import os
import time

from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    StaleElementReferenceException,
)

from ocs_ci.ocs.ocp import get_ocp_url
from ocs_ci.ocs import exceptions
from ocs_ci.ocs.ui.page_objects.confirm_dialog import ConfirmDialog
from ocs_ci.ocs.ui.page_objects.object_storage import ObjectStorage
from ocs_ci.utility import version
from ocs_ci.utility.utils import generate_folder_with_files

logger = logging.getLogger(__name__)


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
