import re
import logging

from ocs_ci.framework.pytest_customization.marks import tier3, polarion_id, bugzilla
from ocs_ci.ocs.constants import NOOBAA_REGIONS_CODE_URL, AWS_REGIONS_DOC_URL
from ocs_ci.ocs.ui.views import locate_aws_regions, locate_noobaa_regions
from ocs_ci.ocs.ui.base_ui import BaseUI

logger = logging.getLogger(__name__)


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
            locator = (locate_aws_regions["region_table"][1], xpath)
            if self.check_element_presence(locator):
                region_name = self.find_an_element_by_xpath(f"{xpath}/td[1]").text
                region_code = self.find_an_element_by_xpath(f"{xpath}/td[2]/code").text
                aws_regions_dict[region_name] = region_code
            else:
                break
            i += 1
        return aws_regions_dict

    def fetch_noobaa_regions(self):
        return self.find_an_element_by_xpath(
            locate_noobaa_regions["regions_list"][0]
        ).text


@tier3
@bugzilla("2183480")
@polarion_id("OCS-5153")
def test_verify_aws_regions_list():
    # Fetch all the regions list from the official aws documentation
    navigate_aws_doc = NavigateWebURL()
    navigate_aws_doc.open_url(AWS_REGIONS_DOC_URL)
    aws_regions = navigate_aws_doc.fetch_aws_regions()
    navigate_noobaa_code = NavigateWebURL()
    navigate_noobaa_code.open_url(NOOBAA_REGIONS_CODE_URL)
    for region in aws_regions.values():
        noobaa_regions_str = navigate_noobaa_code.fetch_noobaa_regions()
        pattern = rf'"{region}":\s+"{region}"'
        assert re.search(pattern, noobaa_regions_str) is not None, (
            f"Region {region}: {aws_regions[region]} is not found in the Noobaa operator code "
            f"https://github.com/noobaa/noobaa-operator/blob/master/pkg/util/util.go#L1108"
        )
