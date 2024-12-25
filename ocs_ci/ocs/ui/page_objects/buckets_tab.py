import json
import logging
import requests

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
            "create_bucket_button": (
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

        self.do_click(locators["create_bucket_button"])

        # Click the dropdown using CSS selector
        self.do_click(locators["storage_class_dropdown"])

        # Select the noobaa storage class option using CSS selector
        self.do_click(locators["storage_class_noobaa_option"])

        # Click submit using CSS selector
        self.do_click(locators["submit_button"])

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
