import re
import logging

from selenium.webdriver.common.by import By
from ocs_ci.ocs.ui.base_ui import BaseUI

logger = logging.getLogger(__name__)


# aws docs region table locators

locate_aws_regions = {"region_table": ('//*[@id="w101aac10b9c15"]', By.XPATH)}
locate_noobaa_regions = {
    "regions_list": ('//*[@id="read-only-cursor-text-area"]', By.XPATH)
}


class NavigateWebURL(BaseUI):
    def __init__(self):
        super().__init__()

    def open_url(self, web_url):
        """
        This will opens the url and returns the driver instance

        Returns:
            driver (Selenium WebDriver)
        """
        logger.info(f"Opening URL {web_url}")
        self.driver.maximize_window()
        self.driver.implicitly_wait(10)
        self.driver.get(web_url)
        self.take_screenshot()

    def fetch_aws_regions(self):
        aws_regions_dict = {}
        i = 1
        while True:
            xpath = f'{locate_aws_regions["region_table"][0]}/tbody/tr[{i}]'
            locator = (By.XPATH, xpath)
            if self.check_element_presence(locator):
                region_name = self.find_an_element_by_xpath(f"{xpath}/td[1]").text
                region_code = self.find_an_element_by_xpath(f"{xpath}/td[2]/code").text
                logger.info(f"Region: {region_name} Code: {region_code}")
                aws_regions_dict[region_name] = region_code
            else:
                break
            i += 1
        return aws_regions_dict

    def fetch_noobaa_regions(self):
        return self.find_an_element_by_xpath(
            locate_noobaa_regions["regions_list"][0]
        ).text


def test_verify_aws_regions_list():
    # Fetch all the regions list from the official aws documentation
    aws_docs_url = "https://docs.aws.amazon.com/general/latest/gr/rande.html"
    noobaaa_code_url = (
        "https://github.com/noobaa/noobaa-operator/blob/master/pkg/util/util.go#L1108"
    )
    navigate_aws_doc = NavigateWebURL()
    navigate_aws_doc.open_url(aws_docs_url)
    aws_regions = navigate_aws_doc.fetch_aws_regions()
    navigate_noobaa_code = NavigateWebURL()
    navigate_noobaa_code.open_url(noobaaa_code_url)
    for region in aws_regions.keys():
        noobaa_regions_str = navigate_noobaa_code.fetch_noobaa_regions()
        pattern = rf'"{region}":\s+"{aws_regions[region]}"'
        assert re.search(pattern, noobaa_regions_str) is not None, (
            f"Region {region}: {aws_regions[region]} is not found in the Noobaa operator code "
            f"https://github.com/noobaa/noobaa-operator/blob/master/pkg/util/util.go#L1108"
        )
