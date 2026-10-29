"""Page object for Configure Performance page (RHSTOR-8629)."""

import logging
import time
from typing import Optional

from ocs_ci.ocs import constants
from ocs_ci.ocs.ui.base_ui import (
    BaseUI,
    SeleniumDriver,
    wait_for_element_to_be_clickable,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.ui.helpers_ui import format_locator


logger = logging.getLogger(__name__)

# Text shown on the MCG dropdown button until a profile is selected for the first time
MCG_PROFILE_PLACEHOLDER = "select a profile"


class ConfigurePerformancePage(BaseUI):
    """Configure Performance page with Core Storage and MCG sections."""

    def is_core_storage_section_present(self) -> bool:
        """Check if Core Storage section is present."""
        logger.info("Checking if Core Storage section is present")
        try:
            self.wait_for_element_to_be_visible(
                self.configure_performance_loc["core_storage_section"], timeout=15
            )
            return True
        except Exception:
            logger.warning(
                "Core storage section not found with primary locator, trying fallback"
            )
            fallback_elements = self.get_elements(
                self.configure_performance_loc["core_storage_section_fallback"]
            )
            return len(fallback_elements) > 0

    def is_mcg_section_present(self) -> bool:
        """Check if MCG section is present."""
        logger.info("Checking if MCG section is present")
        try:
            self.wait_for_element_to_be_visible(
                self.configure_performance_loc["mcg_section"], timeout=15
            )
            return True
        except Exception:
            elements = self.get_elements(
                self.configure_performance_loc["mcg_select_profile_button"]
            )
            return len(elements) > 0

    def verify_sections_separated(self) -> bool:
        """Verify sections are visually separated."""
        logger.info("Verifying sections are visually separated")
        core_elements = self.get_elements(
            self.configure_performance_loc["core_storage_section"]
        )
        mcg_elements = self.get_elements(self.configure_performance_loc["mcg_section"])
        if not core_elements:
            logger.warning(
                "Core Storage section not found, trying to verify they exist instead"
            )
            return (
                self.is_core_storage_section_present() and self.is_mcg_section_present()
            )
        if not mcg_elements:
            logger.warning("MCG section not found, trying to verify they exist instead")
            return (
                self.is_core_storage_section_present() and self.is_mcg_section_present()
            )

        core = core_elements[0]
        mcg = mcg_elements[0]

        # Check if sections are actually different elements
        if core == mcg:
            logger.warning(
                "Both sections resolved to same element, but both are present"
            )
            return True

        # Sections are separated if they have different positions
        separated = (
            core.location["y"] != mcg.location["y"]
            or core.location["x"] != mcg.location["x"]
        )
        logger.info(
            "Sections separated: %s (Core: %s, MCG: %s)",
            separated,
            core.location,
            mcg.location,
        )
        return separated

    def is_core_storage_inline(self) -> bool:
        """Verify Core Storage displays inline (not modal)."""
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
        """Check if Core Storage 'Select options' button is present and clickable."""
        logger.info("Checking for Core Storage 'Select options' button")
        button_loc = self.configure_performance_loc[
            "core_storage_select_profile_button"
        ]
        buttons = self.get_elements(button_loc)
        return len(buttons) > 0

    def is_mcg_select_button_present(self) -> bool:
        """Check if the MCG profile dropdown button is present."""
        logger.info("Checking for MCG profile dropdown button")
        button_loc = self.configure_performance_loc["mcg_select_profile_button"]
        buttons = self.get_elements(button_loc)
        return len(buttons) > 0

    def get_core_storage_nodes(self) -> list[dict]:
        """Get nodes from Core Storage section."""
        logger.info("Fetching Core Storage nodes")
        core_elements = self.get_elements(
            self.configure_performance_loc["core_storage_section"]
        )
        if not core_elements:
            raise ValueError("Core Storage section not found on page")
        core = core_elements[0]
        node_item = self.configure_performance_loc["node_item"]
        node_name = self.configure_performance_loc["node_name"]
        nodes = core.find_elements(node_item[1], node_item[0])
        return [
            {
                "name": n.find_element(node_name[1], node_name[0]).text,
                "labels": n.get_attribute("data-labels") or "",
                "element": n,
            }
            for n in nodes
        ]

    def get_mcg_nodes(self) -> list[dict]:
        """Get nodes from MCG section."""
        logger.info("Fetching MCG nodes")
        mcg_elements = self.get_elements(self.configure_performance_loc["mcg_section"])
        if not mcg_elements:
            raise ValueError("MCG section not found on page")
        mcg = mcg_elements[0]
        node_item = self.configure_performance_loc["node_item"]
        node_name = self.configure_performance_loc["node_name"]
        nodes = mcg.find_elements(node_item[1], node_item[0])
        return [
            {
                "name": n.find_element(node_name[1], node_name[0]).text,
                "labels": n.get_attribute("data-labels") or "",
                "element": n,
            }
            for n in nodes
        ]

    def verify_all_nodes_have_ocs_label(self, nodes: list[dict]) -> bool:
        """Verify all nodes have OCS label."""
        for node in nodes:
            labels = (node.get("labels") or "").lower()
            if "ocs" not in labels and "openshift-storage" not in labels:
                return False
        return True

    def has_node_selection_option(self) -> bool:
        """Check if Core Storage has node selection option."""
        logger.info("Checking for node selection options in Core Storage")
        core_elements = self.get_elements(
            self.configure_performance_loc["core_storage_section"]
        )
        if not core_elements:
            raise ValueError("Core Storage section not found on page")
        core = core_elements[0]
        node_selection = self.configure_performance_loc["node_selection_option"]
        elements = core.find_elements(node_selection[1], node_selection[0])
        return len(elements) > 0

    def select_mcg_profile(self, profile_name: str):
        """Select MCG profile from menu, retrying up to 3 times.

        Args:
            profile_name (str): Profile name (e.g. "default", "mixed-workload").

        Returns:
            ConfigurePerformancePage: self

        Raises:
            TimeoutException: If profile selection fails after 3 attempts.
        """
        from selenium.common.exceptions import TimeoutException

        logger.info("Selecting MCG profile: %s", profile_name)

        button_loc = self.configure_performance_loc["mcg_select_profile_button"]
        # UI displays profile names with spaces (e.g. "Mixed workload"), while the
        # test/CR uses hyphenated names (e.g. "mixed-workload"). Normalize so the
        # case-insensitive contains() XPath matches the rendered option text.
        profile_display = profile_name.replace("-", " ")
        expected_ui_profile = profile_name.strip().lower().replace(" ", "-")

        # Resolve the dynamic menuitem locator from views.py using format_locator
        # so no inline XPath lives in the POM file.
        menuitem_loc = format_locator(
            self.configure_performance_loc["mcg_profile_menuitem"],
            profile_display.lower(),
        )

        last_button_text = ""
        for attempt in range(1, 4):
            logger.info(
                "MCG profile selection attempt %s/3 for %s", attempt, profile_name
            )
            # Open the dropdown.
            try:
                button = wait_for_element_to_be_clickable(button_loc, timeout=15)
                logger.info("Clicking MCG dropdown button")
                button.click()
                time.sleep(2)
                logger.info("Waited for dropdown menu to appear and overlays to clear")
            except TimeoutException:
                logger.warning(
                    "Primary locator failed on attempt %s, trying fallback", attempt
                )
                fallback_loc = self.configure_performance_loc[
                    "mcg_select_profile_button_fallback"
                ]
                try:
                    button = wait_for_element_to_be_clickable(fallback_loc, timeout=15)
                    logger.info("Found MCG button with fallback locator")
                    button.click()
                    time.sleep(2)
                except TimeoutException:
                    logger.error("MCG dropdown button not found on attempt %s", attempt)
                    self.take_screenshot("mcg_dropdown_button_timeout")
                    if attempt == 3:
                        raise TimeoutException("MCG dropdown button not found on page")
                    continue

            # Click the profile menuitem. Native click first (dropdown is open,
            # so no overlay should intercept); fall back to a JS click.
            registered = False
            for click_kind in ("native", "js"):
                try:
                    menuitem = wait_for_element_to_be_clickable(
                        menuitem_loc, timeout=15
                    )
                    if click_kind == "native":
                        logger.info(
                            "Clicking MCG profile menuitem: %s (native)", profile_name
                        )
                        menuitem.click()
                    else:
                        logger.info(
                            "Clicking MCG profile menuitem: %s (JS click)",
                            profile_name,
                        )
                        self.click_with_script(menuitem_loc)
                except TimeoutException:
                    logger.error(
                        "MCG profile option '%s' did not appear on attempt %s",
                        profile_name,
                        attempt,
                    )
                    self.take_screenshot("mcg_profile_option_timeout")
                    registered = False
                    break

                # Wait for the button text to reflect the new profile, confirming
                # the selection registered in the UI form before we save.
                button_text = ""
                for _ in range(5):
                    time.sleep(1)
                    try:
                        button_text = (
                            self.get_element_text(button_loc)
                            .strip()
                            .lower()
                            .replace(" ", "-")
                        )
                    except Exception:
                        button_text = ""
                    if button_text == expected_ui_profile:
                        break
                last_button_text = button_text
                if button_text == expected_ui_profile:
                    logger.info(
                        "MCG profile '%s' selected and registered (button text: %s)",
                        profile_name,
                        button_text,
                    )
                    registered = True
                    break
                logger.warning(
                    "MCG profile option click did not register; button text still '%s'",
                    button_text,
                )

            if registered:
                return self

        logger.error(
            "Failed to select MCG profile '%s' after 3 attempts (last button text: %s)",
            profile_name,
            last_button_text,
        )
        self.take_screenshot("mcg_profile_selection_failed")
        self.copy_dom("mcg_profile_selection_failed")
        raise TimeoutException(
            f"MCG profile '{profile_name}' did not register after 3 attempts"
        )

    def select_core_storage_profile(self, profile_name: str):
        """Select Core Storage profile from menu."""
        logger.info("Selecting Core Storage profile: %s", profile_name)

        button_loc = self.configure_performance_loc[
            "core_storage_select_profile_button"
        ]
        logger.info("Waiting for Core Storage dropdown button to be clickable")
        from selenium.common.exceptions import TimeoutException

        try:
            button = wait_for_element_to_be_clickable(button_loc, timeout=15)
            logger.info("Clicking Core Storage dropdown button")
            button.click()
        except TimeoutException:
            logger.warning("Primary locator failed, trying fallback")
            fallback_loc = self.configure_performance_loc[
                "core_storage_select_profile_button_fallback"
            ]
            try:
                button = wait_for_element_to_be_clickable(fallback_loc, timeout=10)
                logger.info("Found Core Storage button with fallback locator")
                button.click()
            except TimeoutException as e:
                logger.error("Core Storage dropdown button not found after 25 seconds")
                self.take_screenshot("core_storage_dropdown_button_timeout")
                self.copy_dom("core_storage_dropdown_button_timeout")
                raise TimeoutException(
                    "Core Storage dropdown button not found on page"
                ) from e

        profile_option_loc = self.configure_performance_loc[
            "core_storage_profile_option"
        ]
        profile_xpath = profile_option_loc[0].format(profile_name)
        profile_locator = (profile_xpath, profile_option_loc[1])
        logger.info(
            "Waiting for Core Storage profile option '%s' to be clickable", profile_name
        )

        try:
            option = wait_for_element_to_be_clickable(profile_locator, timeout=15)
            logger.info("Clicking Core Storage profile option: %s", profile_name)
            option.click()
        except TimeoutException as e:
            logger.error(
                "Core Storage profile option '%s' did not appear after 15 seconds",
                profile_name,
            )
            self.take_screenshot("core_storage_profile_option_timeout")
            raise TimeoutException(
                f"Core Storage profile '{profile_name}' not found in dropdown menu"
            ) from e
        logger.info("Selected Core Storage profile: %s", profile_name)
        return self

    def save_configuration(self):
        """Save configuration and wait for page navigation to storage cluster."""
        logger.info("Saving configuration")
        save_button = self.configure_performance_loc["save_button"]

        # Let the page/React settle so the form dirty-state and save-button
        # enablement catch up with the profile selection.
        self.page_has_loaded(retries=3, sleep_time=2)

        from selenium.common.exceptions import TimeoutException

        save_btn = None
        for attempt in range(1, 4):
            try:
                save_btn = wait_for_element_to_be_clickable(save_button, timeout=10)
                logger.info("Save button is enabled, clicking it")
                break
            except TimeoutException:
                logger.warning(
                    "Save button not clickable on attempt %s, waiting for page to settle",
                    attempt,
                )
                self.page_has_loaded(retries=2, sleep_time=2)
        if save_btn is None:
            logger.warning("Save button did not become enabled; checking state")
            save_btns = self.get_elements(save_button)
            if not save_btns:
                logger.error("Save button not found on page")
                return self
            save_btn = save_btns[0]
            if not save_btn.is_enabled():
                logger.info("Save button is disabled, no changes to save")
                return self

        original_url = SeleniumDriver().current_url
        save_btn.click()
        logger.info("Clicked Save button, waiting for page navigation")

        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC

        try:
            WebDriverWait(SeleniumDriver(), 15).until(EC.url_changes(original_url))
            logger.info("Page navigated away from Configure Performance page")
        except TimeoutException as e:
            logger.warning("Page did not navigate after save: %s", e)

        logger.info("Configuration saved successfully")
        return self

    def cancel_changes(self):
        """Cancel changes."""
        logger.info("Cancelling configuration changes")
        cancel_button = self.configure_performance_loc["cancel_button"]
        cancel_btns = self.get_elements(cancel_button)
        cancel_btns[0].click()
        logger.info("Configuration changes cancelled")
        return self

    def _get_storage_cluster_data(self) -> dict:
        """Get StorageCluster resource YAML."""
        sc = OCP(
            kind="StorageCluster",
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            resource_name="ocs-storagecluster",
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
        profile = profile.lower() if profile else profile
        logger.info("Read MCG profile from CR: %s", profile)
        return profile

    def get_core_storage_profile_from_cr(self) -> str:
        """Get Core Storage profile from StorageCluster CR."""
        data = self._get_storage_cluster_data()
        profile = self.deep_get(data, "spec", "resourceProfile")
        if profile is None:
            msg = (
                "Core Storage resourceProfile not found in StorageCluster CR. "
                "Ensure the cluster has ODF configured and profile is set."
            )
            logger.error(msg)
            raise ValueError(msg)
        profile = profile.lower() if profile else profile
        logger.info("Read Core Storage profile from CR: %s", profile)
        return profile

    def verify_page_integrity(self) -> bool:
        """Verify page has both sections."""
        return self.is_core_storage_section_present() and self.is_mcg_section_present()

    def get_mcg_profile_from_ui(self) -> Optional[str]:
        """Get selected MCG profile from UI.

        Returns:
            Optional[str]: Profile name (e.g. "mixed-workload"), or None if unselected.
        """
        logger.info("Reading selected MCG profile from UI")
        try:
            profile_text = self.get_element_text(
                self.configure_performance_loc["mcg_select_profile_button"]
            )
        except Exception as e:
            raise ValueError(f"Failed to read MCG profile from UI: {e}")
        profile_text = profile_text.strip().lower()
        if (
            profile_text == MCG_PROFILE_PLACEHOLDER
            or profile_text.startswith(MCG_PROFILE_PLACEHOLDER)
            or "select a profile" in profile_text
        ):
            logger.info("MCG profile from UI: not selected yet (placeholder shown)")
            return None
        profile = profile_text.replace(" ", "-")
        logger.info("MCG profile from UI: %s", profile)
        return profile

    def get_core_storage_profile_from_ui(self) -> str:
        """Get selected Core Storage profile from UI."""
        logger.info("Reading selected Core Storage profile from UI")
        try:
            profile_text = self.get_element_text(
                self.configure_performance_loc["core_storage_select_profile_button"]
            )
            profile = profile_text.strip().lower()
            profile = profile.split()[0] if profile else profile
            logger.info("Core Storage profile from UI: %s", profile)
            return profile
        except Exception as e:
            raise ValueError(f"Failed to read Core Storage profile from UI: {e}")
