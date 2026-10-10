"""Page object for Configure Performance page (RHSTOR-8629)."""

import logging
import time

from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.ui import WebDriverWait

from ocs_ci.ocs import constants
from ocs_ci.ocs.ui.base_ui import BaseUI
from ocs_ci.ocs.ocp import OCP


logger = logging.getLogger(__name__)


class ConfigurePerformancePage(BaseUI):
    """Configure Performance page with Core Storage and MCG sections."""

    def is_core_storage_section_present(self) -> bool:
        """Check if Core Storage section is present."""
        return (
            len(
                self.get_elements(
                    self.configure_performance_loc["core_storage_section"]
                )
            )
            > 0
        )

    def is_mcg_section_present(self) -> bool:
        """Check if MCG section is present."""
        return len(self.get_elements(self.configure_performance_loc["mcg_section"])) > 0

    def verify_sections_separated(self) -> bool:
        """Verify sections are visually separated."""
        logger.info("Verifying sections are visually separated")
        core_elements = self.get_elements(
            self.configure_performance_loc["core_storage_section"]
        )
        mcg_elements = self.get_elements(self.configure_performance_loc["mcg_section"])
        if not core_elements:
            raise ValueError("Core Storage section not found on page")
        if not mcg_elements:
            raise ValueError("MCG section not found on page")
        core = core_elements[0]
        mcg = mcg_elements[0]
        return (
            core.location["y"] != mcg.location["y"]
            or core.location["x"] != mcg.location["x"]
        )

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
        """Select MCG profile."""
        from ocs_ci.ocs.ui.helpers_ui import SeleniumDriver

        logger.info("Selecting MCG profile: %s", profile_name)
        driver = SeleniumDriver()
        mcg_selector = self.configure_performance_loc["mcg_profile_selector"]
        select_element = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((mcg_selector[1], mcg_selector[0]))
        )
        select = Select(select_element)
        target_profile = profile_name.lower()
        for option in select.options:
            if option.text.strip().lower() == target_profile:
                option.click()
                logger.info("Selected MCG profile: %s", profile_name)
                return self
        raise ValueError("MCG profile '%s' not found in UI options" % profile_name)

    def select_core_storage_profile(self, profile_name: str):
        """Select Core Storage profile."""
        from ocs_ci.ocs.ui.helpers_ui import SeleniumDriver

        logger.info("Selecting Core Storage profile: %s", profile_name)
        driver = SeleniumDriver()
        core_selector = self.configure_performance_loc["core_storage_selector"]
        select_element = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((core_selector[1], core_selector[0]))
        )
        select = Select(select_element)
        target_profile = profile_name.lower()
        for option in select.options:
            if option.text.strip().lower() == target_profile:
                option.click()
                logger.info("Selected Core Storage profile: %s", profile_name)
                return self
        raise ValueError(
            "Core Storage profile '%s' not found in UI options" % profile_name
        )

    def save_configuration(self):
        """Save configuration and wait for modal to close."""
        from ocs_ci.ocs.ui.helpers_ui import SeleniumDriver

        logger.info("Saving configuration")
        driver = SeleniumDriver()
        save_button = self.configure_performance_loc["save_button"]
        save_btn = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((save_button[1], save_button[0]))
        )
        save_btn.click()
        logger.info("Waiting for save operation to complete")
        try:
            WebDriverWait(driver, 30).until(EC.staleness_of(save_btn))
        except Exception as e:
            logger.warning("Save button did not become stale: %s", e)
            time.sleep(3)
        time.sleep(2)
        logger.info("Configuration saved successfully")
        return self

    def cancel_changes(self):
        """Cancel changes."""
        from ocs_ci.ocs.ui.helpers_ui import SeleniumDriver

        logger.info("Cancelling configuration changes")
        driver = SeleniumDriver()
        cancel_button = self.configure_performance_loc["cancel_button"]
        WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((cancel_button[1], cancel_button[0]))
        ).click()
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
        """Get MCG profile from StorageCluster CR."""
        data = self._get_storage_cluster_data()
        profile = self.deep_get(data, "spec", "multiCloudGateway", "performanceProfile")
        if profile is None:
            msg = (
                "MCG performanceProfile not found in StorageCluster CR. "
                "Ensure the cluster has MCG configured and profile is set."
            )
            logger.error(msg)
            raise ValueError(msg)
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
        logger.info("Read Core Storage profile from CR: %s", profile)
        return profile

    def verify_page_integrity(self) -> bool:
        """Verify page has both sections."""
        return self.is_core_storage_section_present() and self.is_mcg_section_present()

    def get_mcg_profile_from_ui(self) -> str:
        """Get selected MCG profile from UI dropdown."""
        logger.info("Reading selected MCG profile from UI")
        mcg_selector = self.configure_performance_loc["mcg_profile_selector"]
        elements = self.get_elements(mcg_selector)
        if not elements:
            raise ValueError("MCG profile selector not found on page")
        select_element = elements[0]
        profile = Select(select_element).first_selected_option.text.strip().lower()
        logger.info("MCG profile from UI: %s", profile)
        return profile

    def get_core_storage_profile_from_ui(self) -> str:
        """Get selected Core Storage profile from UI dropdown."""
        logger.info("Reading selected Core Storage profile from UI")
        core_selector = self.configure_performance_loc["core_storage_selector"]
        elements = self.get_elements(core_selector)
        if not elements:
            raise ValueError("Core Storage profile selector not found on page")
        select_element = elements[0]
        profile = Select(select_element).first_selected_option.text.strip().lower()
        logger.info("Core Storage profile from UI: %s", profile)
        return profile
