"""Page object for Configure Performance page (RHSTOR-8629)."""

import logging
from typing import Optional

from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.ui.base_ui import (
    SeleniumDriver,
    wait_for_element_to_be_clickable,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import wait_for_noobaa_db_ready
from ocs_ci.ocs.resources.storage_cluster import get_noobaa_phase
from ocs_ci.ocs.ui.page_objects.storage_cluster import StorageClusterPage
from ocs_ci.ocs.ui.helpers_ui import format_locator
from ocs_ci.utility.utils import TimeoutSampler


logger = logging.getLogger(__name__)

# Text shown on the MCG dropdown button until a profile is selected for the first time
MCG_PROFILE_PLACEHOLDER = "select a profile"


class ConfigurePerformancePage(StorageClusterPage):
    """Configure Performance page with Core Storage and MCG sections."""

    def __init__(self):
        super().__init__()

    def is_core_storage_section_present(self) -> bool:
        """Check if Core Storage section is present.

        Returns:
            bool: True if the Core Storage section is rendered on the page.
        """
        logger.info("Checking if Core Storage section is present")
        return bool(
            self.get_elements(self.configure_performance_loc["core_storage_section"])
        )

    def is_mcg_section_present(self) -> bool:
        """Check if MCG section is present.

        Returns:
            bool: True if the Multicloud Object Gateway section is rendered.
        """
        logger.info("Checking if MCG section is present")
        return bool(self.get_elements(self.configure_performance_loc["mcg_section"]))

    def verify_sections_separated(self) -> bool:
        """Verify the Core Storage and MCG sections are rendered separately.

        Returns:
            bool: True when both sections resolve to distinct elements placed
                at different positions on the page.
        """
        logger.info("Verifying sections are visually separated")
        core_elements = self.get_elements(
            self.configure_performance_loc["core_storage_section"]
        )
        mcg_elements = self.get_elements(self.configure_performance_loc["mcg_section"])
        if not core_elements or not mcg_elements:
            logger.warning(
                "Core Storage present: %s, MCG present: %s",
                bool(core_elements),
                bool(mcg_elements),
            )
            return False

        core = core_elements[0]
        mcg = mcg_elements[0]
        separated = core.location != mcg.location
        logger.info(
            "Sections separated: %s (Core: %s, MCG: %s)",
            separated,
            core.location,
            mcg.location,
        )
        return separated

    def is_core_storage_inline(self) -> bool:
        """Verify Core Storage displays inline (not modal).

        Returns:
            bool: True when the section is not rendered inside a modal/dialog.
        """
        logger.info("Checking if Core Storage section is inline")
        core_elements = self.get_elements(
            self.configure_performance_loc["core_storage_section"]
        )
        if not core_elements:
            raise ValueError("Core Storage section not found on page")
        core = core_elements[0]
        classes = (core.get_attribute("class") or "").lower()
        return "modal" not in classes and "dialog" not in classes

    def is_core_storage_select_button_present(self) -> bool:
        """Check if Core Storage 'Select options' button is present.

        Returns:
            bool: True if the Core Storage profile dropdown button is rendered.
        """
        logger.info("Checking for Core Storage 'Select options' button")
        return bool(
            self.get_elements(
                self.configure_performance_loc["core_storage_select_profile_button"]
            )
        )

    def is_mcg_select_button_present(self) -> bool:
        """Check if the MCG profile dropdown button is present.

        Returns:
            bool: True if the MCG profile dropdown button is rendered.
        """
        logger.info("Checking for MCG profile dropdown button")
        return bool(
            self.get_elements(
                self.configure_performance_loc["mcg_select_profile_button"]
            )
        )

    def get_core_storage_nodes(self) -> list[dict]:
        """Get nodes from Core Storage section.

        Returns:
            list: Dicts with the node name, labels and WebElement.
        """
        logger.info("Fetching Core Storage nodes")
        core_elements = self.get_elements(
            self.configure_performance_loc["core_storage_section"]
        )
        if not core_elements:
            raise ValueError("Core Storage section not found on page")
        return self._collect_nodes(core_elements[0])

    def get_mcg_nodes(self) -> list[dict]:
        """Get nodes from MCG section.

        Returns:
            list: Dicts with the node name, labels and WebElement.
        """
        logger.info("Fetching MCG nodes")
        mcg_elements = self.get_elements(self.configure_performance_loc["mcg_section"])
        if not mcg_elements:
            raise ValueError("MCG section not found on page")
        return self._collect_nodes(mcg_elements[0])

    def _collect_nodes(self, section) -> list[dict]:
        """Collect the node entries rendered inside a page section.

        Args:
            section (WebElement): Section element to search within.

        Returns:
            list: Dicts with the node name, labels and WebElement.
        """
        node_item = self.configure_performance_loc["node_item"]
        node_name = self.configure_performance_loc["node_name"]
        nodes = section.find_elements(node_item[1], node_item[0])
        return [
            {
                "name": node.find_element(node_name[1], node_name[0]).text,
                "labels": node.get_attribute("data-labels") or "",
                "element": node,
            }
            for node in nodes
        ]

    def verify_all_nodes_have_ocs_label(self, nodes: list[dict]) -> bool:
        """Verify all nodes have OCS label.

        Args:
            nodes (list): Node dicts as returned by get_core_storage_nodes().

        Returns:
            bool: True when every node carries an OCS/openshift-storage label.
        """
        for node in nodes:
            labels = (node.get("labels") or "").lower()
            if "ocs" not in labels and "openshift-storage" not in labels:
                return False
        return True

    def has_node_selection_option(self) -> bool:
        """Check if Core Storage has node selection option.

        Returns:
            bool: True when the Core Storage section exposes node checkboxes.
        """
        logger.info("Checking for node selection options in Core Storage")
        core_elements = self.get_elements(
            self.configure_performance_loc["core_storage_section"]
        )
        if not core_elements:
            raise ValueError("Core Storage section not found on page")
        core = core_elements[0]
        node_selection = self.configure_performance_loc["node_selection_option"]
        return bool(core.find_elements(node_selection[1], node_selection[0]))

    def _profile_option_locator(self, profile_name: str) -> tuple:
        """Build the dropdown option locator for a profile name.

        The UI renders profile names with spaces ("Mixed workload") while the
        CR and the tests use hyphenated names ("mixed-workload").

        Args:
            profile_name (str): Hyphenated or spaced profile name.

        Returns:
            tuple: Formatted locator for the profile menu option.
        """
        return format_locator(
            self.configure_performance_loc["profile_menu_option"],
            profile_name.replace("-", " ").lower(),
        )

    def _get_selected_profile_text(self, button_loc: tuple) -> str:
        """Read a profile dropdown button label as a hyphenated profile name.

        Args:
            button_loc (tuple): Locator of the profile dropdown button.

        Returns:
            str: Lower-case, hyphenated button label.
        """
        return self.get_element_text(button_loc).strip().lower().replace(" ", "-")

    def _wait_for_selected_profile(
        self, button_loc: tuple, expected_profile: str, timeout: int = 30
    ) -> bool:
        """Wait until a profile dropdown button reflects the selected profile.

        Args:
            button_loc (tuple): Locator of the profile dropdown button.
            expected_profile (str): Hyphenated profile name to wait for.
            timeout (int): Seconds to wait for the button label to update.

        Returns:
            bool: True when the button label matches, False when it times out.
        """
        try:
            WebDriverWait(SeleniumDriver(), timeout).until(
                lambda _: self._get_selected_profile_text(button_loc)
                == expected_profile
            )
            return True
        except TimeoutException:
            return False

    def select_mcg_profile(self, profile_name: str):
        """Select MCG profile from the dropdown menu.

        Args:
            profile_name (str): Profile name (e.g. "default", "mixed-workload").

        Returns:
            ConfigurePerformancePage: self

        Raises:
            TimeoutException: If the selection does not register in the UI.
        """
        logger.info("Selecting MCG profile: %s", profile_name)
        button_loc = self.configure_performance_loc["mcg_select_profile_button"]
        option_loc = self._profile_option_locator(profile_name)
        expected_profile = profile_name.strip().lower().replace(" ", "-")

        logger.info("Opening the MCG profile dropdown")
        wait_for_element_to_be_clickable(button_loc).click()

        # A native click comes first; the JS click is the second attempt, for
        # the case where a transient overlay intercepts the pointer event.
        for js_click in (False, True):
            option = wait_for_element_to_be_clickable(option_loc)
            logger.info(
                "Clicking MCG profile option '%s' (js_click=%s)", profile_name, js_click
            )
            if js_click:
                self.click_with_script(option_loc)
            else:
                option.click()

            if self._wait_for_selected_profile(button_loc, expected_profile):
                logger.info("MCG profile '%s' is selected in the UI", profile_name)
                return self
            logger.warning(
                "MCG profile '%s' did not register in the UI, button still shows '%s'",
                profile_name,
                self._get_selected_profile_text(button_loc),
            )

        self.take_screenshot("mcg_profile_selection_failed")
        self.copy_dom("mcg_profile_selection_failed")
        raise TimeoutException(
            f"MCG profile '{profile_name}' did not register in the UI"
        )

    def select_core_storage_profile(self, profile_name: str):
        """Select Core Storage profile from the dropdown menu.

        Args:
            profile_name (str): Profile name (e.g. "balanced", "lean").

        Returns:
            ConfigurePerformancePage: self

        Raises:
            TimeoutException: If the dropdown or the option does not appear.
        """
        logger.info("Selecting Core Storage profile: %s", profile_name)
        button_loc = self.configure_performance_loc[
            "core_storage_select_profile_button"
        ]
        option_loc = self._profile_option_locator(profile_name)

        logger.info("Opening the Core Storage profile dropdown")
        wait_for_element_to_be_clickable(button_loc).click()

        logger.info("Clicking Core Storage profile option: %s", profile_name)
        wait_for_element_to_be_clickable(option_loc).click()
        return self

    def save_configuration(self):
        """Save configuration and wait for the page to navigate away.

        Returns:
            ConfigurePerformancePage: self
        """
        logger.info("Saving configuration")
        save_button = self.configure_performance_loc["save_button"]

        try:
            save_btn = wait_for_element_to_be_clickable(save_button, timeout=60)
        except TimeoutException:
            if not self.get_elements(save_button):
                logger.error("Save button not found on page")
            else:
                logger.info("Save button stayed disabled, no changes to save")
            return self

        original_url = SeleniumDriver().current_url
        logger.info("Clicking Save button, waiting for page navigation")
        save_btn.click()

        try:
            WebDriverWait(SeleniumDriver(), 30).until(ec.url_changes(original_url))
            logger.info("Page navigated away from Configure Performance page")
        except TimeoutException:
            logger.warning("Page did not navigate after save")

        logger.info("Configuration saved successfully")
        return self

    def cancel_changes(self):
        """Cancel changes.

        Returns:
            ConfigurePerformancePage: self
        """
        logger.info("Cancelling configuration changes")
        self.do_click(self.configure_performance_loc["cancel_button"])
        logger.info("Configuration changes cancelled")
        return self

    def _get_storage_cluster_data(self) -> dict:
        """Get StorageCluster resource YAML.

        Returns:
            dict: StorageCluster CR content.
        """
        sc = OCP(
            kind=constants.STORAGECLUSTER,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            resource_name=constants.DEFAULT_CLUSTERNAME,
        )
        logger.info("Fetching StorageCluster CR")
        return sc.get()

    def get_mcg_profile_from_cr(self) -> str:
        """Get MCG profile from StorageCluster CR.

        Returns:
            str: Lower-case profile name.

        Raises:
            ValueError: If performanceProfile is not set in CR.
        """
        data = self._get_storage_cluster_data()
        profile = self.deep_get(data, "spec", "multiCloudGateway", "performanceProfile")
        if profile is None:
            msg = (
                "MCG performanceProfile not found in StorageCluster CR. "
                "Ensure the cluster has MCG configured and profile is set."
            )
            logger.error(msg)
            raise ValueError(msg)
        profile = profile.lower()
        logger.info("Read MCG profile from CR: %s", profile)
        return profile

    def get_core_storage_profile_from_cr(self) -> str:
        """Get Core Storage profile from StorageCluster CR.

        Returns:
            str: Lower-case profile name.

        Raises:
            ValueError: If resourceProfile is not set in CR.
        """
        data = self._get_storage_cluster_data()
        profile = self.deep_get(data, "spec", "resourceProfile")
        if profile is None:
            msg = (
                "Core Storage resourceProfile not found in StorageCluster CR. "
                "Ensure the cluster has ODF configured and profile is set."
            )
            logger.error(msg)
            raise ValueError(msg)
        profile = profile.lower()
        logger.info("Read Core Storage profile from CR: %s", profile)
        return profile

    def wait_for_mcg_profile_in_cr(
        self, expected_profile: str, timeout: int = 120
    ) -> Optional[str]:
        """Wait until the StorageCluster CR reports the expected MCG profile.

        Args:
            expected_profile (str): Profile name expected in the CR.
            timeout (int): Seconds to wait for the CR to be reconciled.

        Returns:
            Optional[str]: Last profile read from the CR. It differs from the
                expected one when the wait times out.
        """
        return self._wait_for_profile_in_cr(
            self.get_mcg_profile_from_cr, expected_profile, timeout
        )

    def wait_for_core_storage_profile_in_cr(
        self, expected_profile: str, timeout: int = 120
    ) -> Optional[str]:
        """Wait until the StorageCluster CR reports the expected Core profile.

        Args:
            expected_profile (str): Profile name expected in the CR.
            timeout (int): Seconds to wait for the CR to be reconciled.

        Returns:
            Optional[str]: Last profile read from the CR. It differs from the
                expected one when the wait times out.
        """
        return self._wait_for_profile_in_cr(
            self.get_core_storage_profile_from_cr, expected_profile, timeout
        )

    @staticmethod
    def _wait_for_profile_in_cr(
        getter, expected_profile: str, timeout: int
    ) -> Optional[str]:
        """Poll a StorageCluster CR getter until it returns the expected value.

        The page object only reports what the CR holds; the assertion on the
        returned value belongs to the test.

        Args:
            getter (callable): Function reading a profile name from the CR.
            expected_profile (str): Profile name to wait for.
            timeout (int): Seconds to wait for the CR to be reconciled.

        Returns:
            Optional[str]: Last profile read from the CR, or None if the CR
                never exposed the profile within the timeout.
        """
        logger.info(
            "Waiting up to %ss for StorageCluster CR to report profile '%s'",
            timeout,
            expected_profile,
        )
        sampler = TimeoutSampler(timeout, 5, getter)
        try:
            for profile in sampler:
                if profile == expected_profile:
                    return profile
        except TimeoutExpiredError:
            logger.warning(
                "StorageCluster CR still reports '%s' after %ss, expected '%s'",
                sampler.last_result,
                timeout,
                expected_profile,
            )
        return sampler.last_result

    def wait_for_noobaa_ready(self, timeout: int = 900) -> Optional[str]:
        """Wait for NooBaa to settle after a performance profile change.

        Changing a profile resizes the NooBaa DB, which makes CNPG roll the
        DB pods and drives the NooBaa CR out of Ready. Tests that run later
        in the session are gated on that phase, so a test that changes a
        profile must leave NooBaa settled behind it.

        Args:
            timeout (int): Seconds to wait for the NooBaa CR to report Ready.

        Returns:
            Optional[str]: Last NooBaa phase observed. It differs from
                "Ready" when the wait times out.
        """
        logger.info("Waiting for the NooBaa DB pods to stabilize")
        try:
            wait_for_noobaa_db_ready()
        except TimeoutExpiredError as e:
            logger.warning("NooBaa DB pods did not stabilize: %s", e)

        namespace = config.ENV_DATA["cluster_namespace"]
        logger.info("Waiting up to %ss for NooBaa to report Ready", timeout)
        sampler = TimeoutSampler(timeout, 10, get_noobaa_phase, namespace)
        try:
            for phase in sampler:
                if phase == constants.STATUS_READY:
                    logger.info("NooBaa is Ready")
                    return phase
        except TimeoutExpiredError:
            logger.warning(
                "NooBaa is still in phase '%s' after %ss",
                sampler.last_result,
                timeout,
            )
        return sampler.last_result

    def verify_page_integrity(self) -> bool:
        """Verify page has both sections.

        Returns:
            bool: True when both the Core Storage and MCG sections are present.
        """
        return self.is_core_storage_section_present() and self.is_mcg_section_present()

    def get_mcg_profile_from_ui(self) -> Optional[str]:
        """Get selected MCG profile from UI.

        Returns:
            Optional[str]: Profile name (e.g. "mixed-workload"), or None if unselected.
        """
        logger.info("Reading selected MCG profile from UI")
        button_loc = self.configure_performance_loc["mcg_select_profile_button"]
        try:
            profile = self._get_selected_profile_text(button_loc)
        except Exception as e:
            raise ValueError(f"Failed to read MCG profile from UI: {e}")
        if MCG_PROFILE_PLACEHOLDER.replace(" ", "-") in profile:
            logger.info("MCG profile from UI: not selected yet (placeholder shown)")
            return None
        logger.info("MCG profile from UI: %s", profile)
        return profile

    def get_core_storage_profile_from_ui(self) -> str:
        """Get selected Core Storage profile from UI.

        Returns:
            str: Lower-case profile name.
        """
        logger.info("Reading selected Core Storage profile from UI")
        button_loc = self.configure_performance_loc[
            "core_storage_select_profile_button"
        ]
        try:
            profile_text = self.get_element_text(button_loc).strip().lower()
        except Exception as e:
            raise ValueError(f"Failed to read Core Storage profile from UI: {e}")
        profile = profile_text.split()[0] if profile_text else profile_text
        logger.info("Core Storage profile from UI: %s", profile)
        return profile
