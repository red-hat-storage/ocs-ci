import json

import requests
from selenium.webdriver.common.by import By

from ocs_ci.ocs.ocp import get_ocp_url
from ocs_ci.ocs.ui.mcg_ui import logger
from ocs_ci.ocs.ui.page_objects.object_bucket_claims_tab import BucketsUI
from ocs_ci.ocs.ui.page_objects.object_service import ObjectService


class ObjectBucketsTab(ObjectService, BucketsUI):
    def __init__(self):
        super().__init__()

    def delete_object_bucket_ui(self, delete_via, expect_fail, resource_name):
        """
        Delete an Object Bucket via the UI

        delete_via (str): delete via 'OB/Actions' or via 'three dots'
        expect_fail (str): verify if OB removal fails with proper PopUp message
        resource_name (str): Object Bucket Claim's name. The resource with its suffix will be deleted
        """
        self.navigate_object_buckets_page()
        self.delete_resource(delete_via, resource_name)

        if expect_fail:

            def _check_three_dots_disabled(text):
                logger.info(text)
                # locator of three_dots btn aligned with the specific resource name
                locator = (
                    f"//td[@id='name']//a[contains(text(), '{resource_name}')]"
                    "/../../..//button[@aria-label='Actions']",
                    By.XPATH,
                )
                # when three_dots element is active attribute 'disabled' does not exist
                self.wait_for_element_attribute(
                    locator,
                    attribute="disabled",
                    attribute_value="true",
                    timeout=5,
                    sleep=1,
                )

                # PopUp is not reachable via Selenium driver. It does not appear in DOM
                URL = f"{get_ocp_url()}/locales/resource.json?lng=en&ns=plugin__odf-console"

                cookies = self.driver.get_cookies()
                session = requests.Session()
                for cookie in cookies:
                    session.cookies.set(cookie["name"], cookie["value"])

                popup_str = "The corresponding ObjectBucketClaim must be deleted first."
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
