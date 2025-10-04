import logging
import re
import hashlib
import requests
from ocs_ci.utility.retry import retry
from requests import TooManyRedirects, Timeout, HTTPError

from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.ocs.ui.page_objects.searchbar import SearchBar


logger = logging.getLogger(__name__)


class Runbook:
    """
    Runbook object used for checking the runbook content.
    May be instantiated with the runbook hash value as expected result for the test;
    If the runbook hash value is not provided, text should be provided and the hash value will be calculated
    """

    def __init__(self, text=None, runbook_hash=None):

        if not text and not runbook_hash:
            raise ValueError("Runbook text or hash value should be provided")

        self.text = text
        if not runbook_hash:
            self.runbook_hash = hashlib.md5(self.text.strip().encode()).hexdigest()
        else:
            self.runbook_hash = runbook_hash

    def __repr__(self):
        """
        get base64 hash of the Raw runbook page content
        """

        return self.runbook_hash

    def __eq__(self, other):
        """
        Compare two instances based on their hash values
        """
        if isinstance(other, Runbook):
            return self.runbook_hash == other.runbook_hash
        return False

    def __ne__(self, other):
        """
        Compare two instances based on their hash values
        """
        return not self.__eq__(other)

    def check_text_content(self, headers, *args):
        """
        Check if the text is valid

        Returns:
            bool: True if the text is valid, False otherwise

        """
        if hasattr(self, "text") and not self.text:
            raise ValueError(
                "Runbook text is empty, this object instantiated with hash value as Expected result for the test "
                "and should not be checked"
            )
        # Check if the text contains all the headers
        for chapter in headers:
            if not re.search(f"## {headers}", self.text):
                logger.error(f"Chapter '{chapter}' is missing.")
                return False

        # Check additional text arguments if provided
        for arg in args:
            if not re.search(arg, self.text):
                logger.error(f"arg '{arg}' is missing in the text.")
                return False

        return True


class Alerting(PageNavigator):
    """
    Alerting page that contains 3 tabs - Alerts, Silences and Alerting Rules
    The default tab is Alerts
    """

    def __init__(self):
        PageNavigator.__init__(self)

    def nav_alerts(self):
        """
        Navigate to Alerts tab. Default tab when opening the Alerting page

        Returns:
            Alerts: Alerts page

        """
        logger.info("Navigate to Alerts tab")
        self.do_click(self.validation_loc["alerts-tab-link"], enable_screenshot=True)
        return Alerts()

    def nav_silences(self):
        """
        Navigate to Silences tab

        Returns:
            Silences: Silences page

        """
        logger.info("Navigate to Silences tab")
        self.do_click(self.alerting_loc["silences-tab-link"], enable_screenshot=True)
        return Silences()

    def nav_alerting_rules(self):
        """
        Navigate to Alerting Rules tab

        Returns:
            AlertingRules: Alerting Rules page

        """
        logger.info("Navigate to Alerting Rules tab")
        self.do_click(
            self.alerting_loc["alerting-rules-tab-link"], enable_screenshot=True
        )
        return AlertingRules()


class Alerts(Alerting, SearchBar):
    """
    Alerts page object
    """

    def __init__(self):
        Alerting.__init__(self)
        SearchBar.__init__(self)

    def search_alert(self, search_text):
        """
        Search for Alerts

        Args:
            search_text (str): Text to search

        """
        self.search(search_text)


class Silences(Alerting):
    """Silences page object where all the silences are listed and new silences may be created."""

    pass


class AlertingRules(Alerting, SearchBar):
    """
    Alerting Rules page object. Contains all the alerts existing in the cluster that are being monitored.
    Use 'oc get prometheusrules -n openshift-storage ocs-prometheus-rules -o yaml' to get the list of alerts
    """

    def __init__(self):
        Alerting.__init__(self)
        SearchBar.__init__(self)

    def navigate_alerting_rule_details(self, alert_name):
        """
        Navigate to Alerting Rule Details

        Args:
            alert_name (str): Alert name

        Returns:
            AlertDetails: Alert Rule Details page

        """
        logger.info(f"Navigate to Alerting Rule Details for {alert_name}")
        # clearing search no longer needed in 4.19
        # self.clear_search()
        self.search(alert_name)

        from ocs_ci.ocs.ui.helpers_ui import format_locator

        self.do_click(
            format_locator(self.alerting_loc["alerting_rule_details_link"], alert_name),
            avoid_stale=True,
        )
        return AlertDetails()


def convert_github_link_to_raw(link):
    """
    Convert GitHub link to raw link

    Args:
        link (str): GitHub link

    Returns:
        Raw GitHub link or None if the link is not valid

    """
    # Define pattern to match the GitHub link
    pattern = r"https://github\.com/(?P<owner>[^/]+)/(?P<repo>[^/]+)/blob/(?P<branch>[^/]+)/(?P<path>.+)"

    # Match the pattern in the raw link
    match = re.match(pattern, link)
    if match:
        owner = match.group("owner")
        repo = match.group("repo")
        branch = match.group("branch")
        path = match.group("path")

        # Construct the raw GitHub link
        raw_github_link = (
            f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{path}"
        )
        return raw_github_link
    else:
        return None


class AlertDetails(PageNavigator):
    """
    Alert Details page object
    """

    def __init__(self):
        super().__init__()

    def get_runbook_link(self):
        """
        Get Runbook Link

        Returns:
            str: Runbook link

        """
        return self.get_element_text(self.alerting_loc["runbook_link"])

    def get_raw_runbook(self):
        """
        Get Runbook

        Returns:
            Runbook: Runbook page

        """
        runbook_link = self.get_runbook_link()
        raw_github_link = convert_github_link_to_raw(runbook_link)
        if raw_github_link:
            logger.debug(f"Get Runbook from {raw_github_link}")
            resp = retry(
                (HTTPError, ConnectionError, Timeout, TooManyRedirects),
                tries=3,
                delay=10,
            )(requests.get)(raw_github_link)
            return Runbook(resp.text)
        else:
            logger.error("Invalid GitHub link")
            return None
