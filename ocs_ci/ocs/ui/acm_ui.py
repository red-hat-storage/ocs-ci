import logging

from ocs_ci.ocs.ui.base_ui import BaseUI, PageNavigator
from ocs_ci.ocs.ui.views import locators
from ocs_ci.utility.utils import get_ocp_version

log = logging.getLogger(__name__)


class AcmPageNavigator(BaseUI):
    """
    ACM Page Navigator Class

    """

    def __init__(self, driver):
        super().__init__(driver)
        self.ocp_version = get_ocp_version()
        self.acm_page_nav = locators[self.ocp_version]["acm_page"]

    def navigate_welcome_page(self):
        """
        Navigate to ACM Welcome Page

        """
        log.info("Navigate into Home Page")
        self.choose_expanded_mode(mode=True, locator=self.acm_page_nav["Home"])
        self.do_click(locator=self.acm_page_nav["Welcome_page"])

    def navigate_overview_page(self):
        """
        Navigate to ACM Overview Page

        """
        log.info("Navigate into Overview Page")
        self.choose_expanded_mode(mode=True, locator=self.acm_page_nav["Home"])
        self.do_click(locator=self.acm_page_nav["Overview_page"])

    def navigate_clusters_page(self):
        """
        Navigate to ACM Clusters Page

        """
        log.info("Navigate into Clusters Page")
        self.choose_expanded_mode(mode=True, locator=self.acm_page_nav["Infrastructure"])
        self.do_click(locator=self.acm_page_nav["Clusters_page"])

    def navigate_bare_metal_assets_page(self):
        """
        Navigate to ACM Bare Metal Assets Page

        """
        log.info("Navigate into Bare Metal Assets Page")
        self.choose_expanded_mode(
            mode=True, locator=self.acm_page_nav["Infrastructure"]
        )
        self.do_click(locator=self.acm_page_nav["Bare_metal_assets_page"])

    def navigate_automation_page(self):
        """
        Navigate to ACM Automation Page

        """
        log.info("Navigate into Automation Page")
        self.choose_expanded_mode(
            mode=True, locator=self.acm_page_nav["Infrastructure"]
        )
        self.do_click(locator=self.acm_page_nav["Automation_page"])

    def navigate_infrastructure_env_page(self):
        """
        Navigate to ACM Infrastructure Environments Page

        """
        log.info("Navigate into Infrastructure Environments Page")
        self.choose_expanded_mode(
            mode=True, locator=self.acm_page_nav["Infrastructure"]
        )
        self.do_click(locator=self.acm_page_nav["Infrastructure_environments_page"])

    def navigate_applications_page(self):
        """
        Navigate to ACM Applications Page

        """
        log.info("Navigate into Applications Page")
        self.do_click(locator=self.acm_page_nav["Applications"])

    def navigate_governance_page(self):
        """
        Navigate to ACM Governance Page

        """
        log.info("Navigate into Governance Page")
        self.do_click(locator=self.acm_page_nav["Governance"])

    def navigate_credentials_page(self):
        """
        Navigate to ACM Credentials Page

        """
        log.info("Navigate into Governance Page")
        self.do_click(locator=self.acm_page_nav["Credentials"])
