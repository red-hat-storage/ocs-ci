import logging

from selenium.common.exceptions import TimeoutException

from ocs_ci.ocs.ui.acm_ui import AcmPageNavigator
from ocs_ci.ocs.ui.views import locators_for_current_ocp_version

logger = logging.getLogger(__name__)


class MCOOperatorPage(AcmPageNavigator):
    """
    Page Object for MCO (Multicluster Orchestrator) Operator details
    on the ACM hub cluster UI.
    """

    def __init__(self):
        super().__init__()
        self.ocp_loc = locators_for_current_ocp_version()
        self.dep_loc = self.ocp_loc["deployment"]

    def navigate_to_installed_operators(self):
        """
        Navigate directly to the Installed Operators page in the
        openshift-operators namespace.

        The ACM console opens in "Fleet Management" perspective,
        so we bypass the perspective switcher by loading the
        Installed Operators URL directly.
        """
        logger.info("Navigating directly to Installed Operators page")
        current_url = self.driver.current_url
        base_url = current_url.split("/multicloud")[0]
        installed_operators_url = (
            f"{base_url}/k8s/ns/openshift-operators"
            "/operators.coreos.com~v1alpha1~ClusterServiceVersion"
        )
        self.driver.get(installed_operators_url)
        self.page_has_loaded()
        self.take_screenshot()

    def search_for_operator(self, operator_name):
        """
        Search for an operator by name in the Installed Operators
        page.

        Args:
            operator_name (str): Name of the operator to search for
        """
        logger.info(f"Searching for operator: {operator_name}")
        search_box = self.wait_for_element_to_be_visible(
            self.dep_loc["mco_search_operators"], timeout=30
        )
        search_box.clear()
        search_box.send_keys(operator_name)
        self.take_screenshot()

    def click_mco_operator(self):
        """
        Click on the MCO operator row to view details.

        Raises:
            AssertionError: If the operator is not found, or if it
                still displays the old (pre-rebranding) name.
        """
        logger.info("Clicking on MCO operator to view details")
        try:
            self.do_click(self.dep_loc["mco_operator_row"], timeout=30)
        except TimeoutException:
            logger.error(
                "MCO operator with expected name "
                "'DF Multicluster Orchestrator' "
                "not found in Installed Operators"
            )
            self.take_screenshot()
            loc = self.dep_loc["mco_operator_row_old_name"]
            if self.check_element_presence((loc[1], loc[0]), timeout=10):
                raise AssertionError(
                    "Operator still displays old name "
                    "'ODF Multicluster Orchestrator' "
                    "instead of rebranded name "
                    "'DF Multicluster Orchestrator'"
                )
            raise AssertionError(
                "MCO operator not found in Installed Operators "
                "with either old or new name"
            )

    def get_operator_display_name(self):
        """
        Get the operator display name from the details page.

        Returns:
            str: The operator display name text
        """
        element = self.wait_for_element_to_be_visible(
            self.dep_loc["operator_display_name"], timeout=30
        )
        return element.text

    def get_operator_provider(self):
        """
        Get the operator provider from the details page.

        Returns:
            str: The provider text, or None if not found
        """
        try:
            element = self.wait_for_element_to_be_visible(
                self.dep_loc["operator_provider"], timeout=30
            )
            return element.text
        except TimeoutException:
            logger.warning("Provider information not found on the page")
            self.take_screenshot()
            return None

    def verify_operator_installed_status(self):
        """
        Verify the operator shows 'Installed' status.

        Raises:
            AssertionError: If the Installed status indicator is
                not found or not displayed.
        """
        logger.info("Verifying operator installation status")
        try:
            element = self.wait_for_element_to_be_visible(
                self.dep_loc["operator_installed_status"], timeout=30
            )
            status_text = element.text.strip()
            assert status_text in (
                "Installed",
                "Succeeded",
            ), f"Unexpected operator status: '{status_text}'"
            logger.info("Operator status: %s", status_text)
            self.take_screenshot()
        except TimeoutException:
            logger.error("Operator 'Installed' status indicator not found")
            self.take_screenshot()
            raise AssertionError("Operator does not show 'Installed' status")
