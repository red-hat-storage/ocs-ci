from urllib.error import HTTPError
import logging
import ssl
import urllib.request

from ocs_ci.helpers.helpers import get_noobaa_url
from ocs_ci.framework.testlib import (
    ManageTest,
    tier2,
    skipif_ocs_version,
    bugzilla,
    skipif_external_mode,
)


log = logging.getLogger(__name__)


@tier2
@bugzilla("1943388")
@skipif_ocs_version("<4.8")
@skipif_external_mode
class TestNoobaaXssVulnerability(ManageTest):
    """
    Test Process:
    1.Get Noobaa URL
    2.Get HTML of https://{url}/robots.txt
    3.Search the string "We dug ... URL" and
    4.Verify there is no Cross-site scripting vulnerability

    """

    def test_noobaa_xss_vulnerability(self):
        """
        Verify there is no Cross-site scripting vulnerability with noobaa management URL.

        """
        url = get_noobaa_url()
        url_path = f"https://{url}/robots.txt"
        html_content = self.get_html_content(url_path)
        logging.info(f"HTML content:{html_content}")
        expected_string = ">We dug the earth, but couldn't find your requested URL</"
        if expected_string not in html_content:
            ValueError(
                f"expected_string:{expected_string}, not found on html file:{html_content}"
            )

    def get_html_content(self, url):
        """
        Get HTML content

        Args:
            url (str): the url of noobaa console
        Returns:
            str: html string
        """
        log.info("Get HTML content")
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        req = urllib.request.Request(url=url)
        try:
            urllib.request.urlopen(req, context=ctx)
        except HTTPError as e:
            content = e.read()
            log.info(f"html content: {content}")
        return content
