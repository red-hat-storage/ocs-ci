import json
import logging
import uuid
import requests
import random
import string
import os
import time

from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException

from ocs_ci.ocs.ocp import get_ocp_url
from ocs_ci.ocs import exceptions
from ocs_ci.ocs.ui.page_objects.confirm_dialog import ConfirmDialog
from ocs_ci.ocs.ui.page_objects.object_storage import ObjectStorage
from ocs_ci.utility import version

logger = logging.getLogger(__name__)


class BucketsTab(ObjectStorage, ConfirmDialog):
    """
    A class representation for abstraction of Buckets tab related OpenShift UI actions
    """

    # Methods can directly access locators via self.bucket_tab, self.generic_locators etc.
    # No need to explicitly import or assign them

    def generate_folder_with_file(self) -> str:
        """
        Generates a random folder with random text file inside it in /tmp folder.

        Returns:
            str: Full path to the generated folder.

        Raises:
            OSError: If folder creation or file write fails.
        """
        folder_name = "".join(
            random.choices(string.ascii_lowercase + string.digits, k=8)
        )
        folder_path = os.path.join("/tmp", folder_name)
        os.makedirs(folder_path, exist_ok=True)

        filename = (
            "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
            + ".txt"
        )
        filepath = os.path.join(folder_path, filename)

        content = "".join(
            random.choices(string.ascii_letters + string.digits + " \n", k=100)
        )

        with open(filepath, "w") as f:
            f.write(content)

        return folder_path

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

            # Select the noobaa option
            logger.info("Selecting noobaa storage class option")
            self.do_click(self.bucket_tab["storage_class_noobaa_option"])

        except NoSuchElementException as e:
            logger.error(f"Failed to select storage class: {str(e)}")
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

        folder_path = self.generate_folder_with_file()

        logger.info("=== DEBUG: STARTING FILE UPLOAD ===")

        try:
            # Find the hidden file input
            file_input = self.driver.find_element(
                By.XPATH, "//input[@type='file'][@webkitdirectory]"
            )
            logger.info("Found directory input")

            # Make the input visible and remove directory requirement
            self.driver.execute_script(
                """
                arguments[0].removeAttribute('webkitdirectory');
                arguments[0].removeAttribute('directory');
                arguments[0].style.display = 'block';
                arguments[0].style.visibility = 'visible';
                arguments[0].style.height = '1px';
                arguments[0].style.width = '1px';
                arguments[0].style.opacity = '1';
            """,
                file_input,
            )
            logger.info("Modified file input for direct interaction")

            file_path = os.path.join(folder_path, os.listdir(folder_path)[0])
            logger.info(f"Sending file path: {file_path}")

            file_input.send_keys(file_path)
            logger.info("Successfully sent file path")
            return folder_name

        except NoSuchElementException as e:
            logger.error(f"Error during file upload: {str(e)}")
            raise

    def delete_bucket_ui(self, delete_via, expect_fail, resource_name):
        """
        Delete an Object Bucket via the UI

        delete_via (str): delete via 'OB/Actions' or via 'three dots'
        expect_fail (str): verify if OB removal fails with proper PopUp message
        resource_name (str): Object Bucket Claim's name. The resource with its suffix will be deleted
        """
        self.navigate_buckets_page()
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
            self.driver.refresh()
            self.page_has_loaded(sleep_time=2)
            _check_three_dots_disabled("check three dots inactive after refresh")
