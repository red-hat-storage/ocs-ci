import re
import logging
import time
import pytest

from ocs_ci.framework.pytest_customization.marks import tier3, polarion_id, bugzilla
from ocs_ci.ocs.constants import NOOBAA_REGIONS_CODE_URL, AWS_REGIONS_DOC_URL
from ocs_ci.ocs.ui.views import locate_aws_regions, locate_noobaa_regions
from ocs_ci.ocs.ui.base_ui import BaseUI, garbage_collector_webdriver

logger = logging.getLogger(__name__)


class NavigateAWSDocsWebURL(BaseUI):
    def __init__(self):
        super().__init__()

    def open_url(self, web_url):
        """
        This will open the url and returns the driver instance

        Returns:
            driver (Selenium WebDriver)
        """
        logger.info(f"Opening URL {web_url}")
        self.driver.maximize_window()
        self.driver.implicitly_wait(10)
        self.driver.get(web_url)
        self.take_screenshot()

    def close_browser(self):
        """
        Close the driver instances

        """
        self.driver.quit()
        time.sleep(10)
        garbage_collector_webdriver()

    def fetch_aws_regions(self):
        """
        Fetch AWS regions from the AWS docs URL

        Returns:
            aws_regions_dict: Dictionary representing
            the region name and region code

        """
        aws_regions_dict = {}
        index = 1
        while True:
            xpath = f'{locate_aws_regions["region_table"][0]}/tbody/tr[{index}]'
            locator = (locate_aws_regions["region_table"][1], xpath)
            if self.check_element_presence(locator):
                region_name = self.find_an_element_by_xpath(f"{xpath}/td[1]").text
                region_code = self.find_an_element_by_xpath(f"{xpath}/td[2]/code").text
                aws_regions_dict[region_name] = region_code
            else:
                break
            index += 1
        return aws_regions_dict

    def fetch_noobaa_regions(self):
        """
        Fetch Noobaa regions list from the source code

        """
        return self.find_an_element_by_xpath(locate_noobaa_regions["regions_list"]).text


@pytest.fixture()
def setup_browser(request):
    """
    Fixture to setup driver instance and teardown

    """
    nav_obj = None

    def factory(navigate_obj, url):
        nonlocal nav_obj
        nav_obj = navigate_obj
        nav_obj.open_url(url)

    yield factory

    if nav_obj:
        nav_obj.close_browser()


@tier3
@bugzilla("2183480")
@polarion_id("OCS-5153")
def test_verify_aws_regions_list(setup_browser):
    """
    This test performs validation of noobaa operator code
    for the regions with the supported regions list in the
    official AWS docs. Intention is to make sure Noobaa has
    support for all the regions supported by AWS

    """
    # Fetch all the regions list from the official aws documentation
    navigate_aws_doc = NavigateAWSDocsWebURL()
    setup_browser(navigate_aws_doc, AWS_REGIONS_DOC_URL)
    aws_regions = navigate_aws_doc.fetch_aws_regions()
    logger.info(
        "These are the regions listed in the official AWS docs:" f"{aws_regions}"
    )

    navigate_noobaa_code = NavigateAWSDocsWebURL()
    setup_browser(navigate_noobaa_code, NOOBAA_REGIONS_CODE_URL)
    for region in aws_regions.values():

        # fetch the noobaa regions parameters from the
        # source code
        noobaa_regions_str = navigate_noobaa_code.fetch_noobaa_regions()
        pattern = rf'"{region}":\s+"{region}"'
        assert re.search(pattern, noobaa_regions_str) is not None, (
            f"Region {region}: {aws_regions[region]} is not found in the Noobaa operator code "
            f"https://github.com/noobaa/noobaa-operator/blob/master/pkg/util/util.go#L1108"
        )
