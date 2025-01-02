import json
import logging
import uuid
import requests
import random
import string
import os

from selenium.webdriver.common.by import By

from ocs_ci.ocs.ocp import get_ocp_url
from ocs_ci.ocs import exceptions
from ocs_ci.ocs.ui.page_objects.confirm_dialog import ConfirmDialog
from ocs_ci.ocs.ui.page_objects.object_storage import ObjectStorage
from ocs_ci.utility import version

logger = logging.getLogger(__name__)


class BucketsTab(ObjectStorage, ConfirmDialog):
    def __init__(self):
        super().__init__()

    def generate_folder_with_file(self):
        # TODO: move to utils
        """
        Generates a random folder with a random text file inside it in /tmp folder

        Returns:
            str: Full path to the generated folder
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

    def create_bucket_ui(self, method):
        locators = {
            "create_bucket_button": ("//*[@id='yaml-create']", By.XPATH),
        }
        self.do_click(locators["create_bucket_button"])
        if method == "obc":
            self.create_bucket_via_obc()
        elif method == "s3":
            self.create_bucket_via_s3()
        else:
            raise ValueError(f"Invalid method: {method}")

    def create_bucket_via_obc(self):
        locators = {
            "create_bucket_button_obc": (
                "//*[@id='content-scrollable']/section/div[2]/div[1]/div/div[2]/div[1]/div[2]",
                By.XPATH,
            ),
            "storage_class_dropdown": (
                "[data-test='sc-dropdown']",
                By.CSS_SELECTOR,
            ),
            "storage_class_noobaa_option": (
                "div.pf-v5-c-dropdown div:nth-of-type(2)",
                By.CSS_SELECTOR,
            ),
            "submit_button": (
                "[data-test='obc-create']",
                By.CSS_SELECTOR,
            ),
        }

        self.do_click(locators["create_bucket_button_obc"])
        self.do_click(locators["storage_class_dropdown"])
        self.do_click(locators["storage_class_noobaa_option"])
        self.do_click(locators["submit_button"])

    def create_bucket_via_s3(self):
        name_generator = f"test-bucket-s3-{uuid.uuid4()}"

        locators = {
            "create_bucket_button_s3": (
                "div:nth-of-type(2) > div.pf-v5-c-tile__body",
                By.CSS_SELECTOR,
            ),
            "bucket_name_input": (
                "[data-test='bucket-name']",
                By.CSS_SELECTOR,
            ),
            "submit_button": (
                "[data-test='obc-create']",
                By.CSS_SELECTOR,
            ),
        }

        self.do_click(locators["create_bucket_button_s3"])
        self.do_send_keys(locators["bucket_name_input"], name_generator)
        self.do_click(locators["submit_button"])

    def create_folder_in_bucket(self, bucket_name=None, folder_name=None):
        # Note That object must be uploaded to the folder before navigating out of the bucket,
        # else the folder will be vanished

        locators = {
            "first_bucket": (
                "tr:nth-of-type(1) a",
                By.CSS_SELECTOR,
            ),
            "create_folder_button": (
                "div.pf-v5-u-w-50 > div > button.pf-v5-c-button",
                By.CSS_SELECTOR,
            ),
            "folder_name_input": (
                "#folder-name",
                By.CSS_SELECTOR,
            ),
            "submit_button": (
                "button.pf-m-primary",
                By.CSS_SELECTOR,
            ),
            "upload_button": (
                "//button[contains(text(), 'Upload')]",
                By.XPATH,
            ),
            "file_input": (
                "input[type='file']",
                By.CSS_SELECTOR,
            ),
            "upload_submit": (
                "//button[contains(@class, 'pf-m-primary') and contains(text(), 'Upload')]",
                By.XPATH,
            ),
        }

        if bucket_name:
            self.do_click(f"//tr//a[contains(text(), '{bucket_name}')]", By.XPATH)
        else:
            self.do_click(locators["first_bucket"])

        if not folder_name:
            folder_name = f"test-folder-{uuid.uuid4()}"

        self.do_click(locators["create_folder_button"])
        self.do_send_keys(locators["folder_name_input"], folder_name)
        self.do_click(locators["submit_button"])

        folder_path = self.generate_folder_with_file()

        self.do_click(locators["upload_button"])
        file_path = os.path.join(
            folder_path, os.listdir(folder_path)[0]
        )  # Get the generated file
        self.do_send_keys(locators["file_input"], file_path)
        self.do_click(locators["upload_submit"])

        return folder_name

    def check_folder_details(self, folder_name):
        pass

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
