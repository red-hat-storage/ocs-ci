"""Page object for Configure Performance page (RHSTOR-8629)."""

import logging
from typing import List, Optional

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from ocs_ci.ocs import constants
from ocs_ci.ocs.ui.base_ui import BaseUI
from ocs_ci.ocs.ocp import OCP


logger = logging.getLogger(__name__)


class ConfigurePerformancePage(BaseUI):
    """Configure Performance page with Core Storage and MCG sections."""

    def is_core_storage_section_present(self) -> bool:
        """Check if Core Storage section is present."""
        from ocs_ci.ocs.ui.views import locators

        core_storage_section = locators["configure_performance"]["core_storage_section"]
        elements = self.get_elements(core_storage_section)
        return len(elements) > 0

    def is_mcg_section_present(self) -> bool:
        """Check if MCG section is present."""
        from ocs_ci.ocs.ui.views import locators

        mcg_section = locators["configure_performance"]["mcg_section"]
        elements = self.get_elements(mcg_section)
        return len(elements) > 0

    def verify_sections_separated(self) -> bool:
        """Verify sections are visually separated."""
        from ocs_ci.ocs.ui.views import locators
        from ocs_ci.ocs.ui.helpers_ui import SeleniumDriver

        driver = SeleniumDriver()
        core_storage_section = locators["configure_performance"]["core_storage_section"]
        mcg_section = locators["configure_performance"]["mcg_section"]
        core = driver.find_element(core_storage_section[1], core_storage_section[0])
        mcg = driver.find_element(mcg_section[1], mcg_section[0])
        return (
            core.location["y"] != mcg.location["y"]
            or core.location["x"] != mcg.location["x"]
        )

    def is_core_storage_inline(self) -> bool:
        """Verify Core Storage displays inline (not modal)."""
        from ocs_ci.ocs.ui.views import locators
        from ocs_ci.ocs.ui.helpers_ui import SeleniumDriver

        driver = SeleniumDriver()
        core_storage_section = locators["configure_performance"]["core_storage_section"]
        core = driver.find_element(core_storage_section[1], core_storage_section[0])
        classes = (core.get_attribute("class") or "").lower()
        return "modal" not in classes and "dialog" not in classes

    def get_core_storage_nodes(self) -> List[dict]:
        """Get nodes from Core Storage section."""
        from ocs_ci.ocs.ui.views import locators
        from ocs_ci.ocs.ui.helpers_ui import SeleniumDriver

        driver = SeleniumDriver()
        core_storage_section = locators["configure_performance"]["core_storage_section"]
        core = driver.find_element(core_storage_section[1], core_storage_section[0])
        nodes = core.find_elements(By.XPATH, ".//div[contains(@class, 'node')]")
        return [
            {
                "name": n.find_element(By.XPATH, ".//*[@class='node-name']").text,
                "labels": n.get_attribute("data-labels") or "",
                "element": n,
            }
            for n in nodes
        ]

    def get_mcg_nodes(self) -> List[dict]:
        """Get nodes from MCG section."""
        from ocs_ci.ocs.ui.views import locators
        from ocs_ci.ocs.ui.helpers_ui import SeleniumDriver

        driver = SeleniumDriver()
        mcg_section = locators["configure_performance"]["mcg_section"]
        mcg = driver.find_element(mcg_section[1], mcg_section[0])
        nodes = mcg.find_elements(By.XPATH, ".//div[contains(@class, 'node')]")
        return [
            {
                "name": n.find_element(By.XPATH, ".//*[@class='node-name']").text,
                "labels": n.get_attribute("data-labels") or "",
                "element": n,
            }
            for n in nodes
        ]

    def verify_all_nodes_have_ocs_label(self, nodes: List[dict]) -> bool:
        """Verify all nodes have OCS label."""
        for node in nodes:
            labels = (node.get("labels") or "").lower()
            if "ocs" not in labels and "openshift-storage" not in labels:
                return False
        return True

    def has_node_selection_option(self) -> bool:
        """Check if Core Storage has node selection option."""
        from ocs_ci.ocs.ui.views import locators
        from ocs_ci.ocs.ui.helpers_ui import SeleniumDriver

        driver = SeleniumDriver()
        core_storage_section = locators["configure_performance"]["core_storage_section"]
        core = driver.find_element(core_storage_section[1], core_storage_section[0])
        elements = core.find_elements(
            By.XPATH,
            ".//input[@type='checkbox' or @type='radio'] | .//button[contains(text(), 'Select')]",
        )
        return len(elements) > 0

    def select_mcg_profile(self, profile_name: str):
        """Select MCG profile."""
        from ocs_ci.ocs.ui.views import locators
        from ocs_ci.ocs.ui.helpers_ui import SeleniumDriver
        from selenium.webdriver.support.select import Select

        logger.info(f"Selecting MCG profile: {profile_name}")
        driver = SeleniumDriver()
        mcg_selector = locators["configure_performance"]["mcg_profile_selector"]
        select_element = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((mcg_selector[1], mcg_selector[0]))
        )
        Select(select_element).select_by_visible_text(profile_name)
        logger.info(f"Selected MCG profile: {profile_name}")
        return self

    def select_core_storage_profile(self, profile_name: str):
        """Select Core Storage profile."""
        from ocs_ci.ocs.ui.views import locators
        from ocs_ci.ocs.ui.helpers_ui import SeleniumDriver
        from selenium.webdriver.support.select import Select

        logger.info(f"Selecting Core Storage profile: {profile_name}")
        driver = SeleniumDriver()
        core_selector = locators["configure_performance"]["core_storage_selector"]
        select_element = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((core_selector[1], core_selector[0]))
        )
        Select(select_element).select_by_visible_text(profile_name)
        logger.info(f"Selected Core Storage profile: {profile_name}")
        return self

    def save_configuration(self):
        """Save configuration."""
        from ocs_ci.ocs.ui.views import locators
        from ocs_ci.ocs.ui.helpers_ui import SeleniumDriver

        logger.info("Saving configuration")
        driver = SeleniumDriver()
        save_button = locators["configure_performance"]["save_button"]
        save_btn = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((save_button[1], save_button[0]))
        )
        save_btn.click()
        WebDriverWait(driver, 30).until(EC.staleness_of(save_btn))
        logger.info("Configuration saved")
        return self

    def cancel_changes(self):
        """Cancel changes."""
        from ocs_ci.ocs.ui.views import locators
        from ocs_ci.ocs.ui.helpers_ui import SeleniumDriver

        driver = SeleniumDriver()
        cancel_button = locators["configure_performance"]["cancel_button"]
        WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((cancel_button[1], cancel_button[0]))
        ).click()
        return self

    def _get_storage_cluster_data(self) -> dict:
        """Get StorageCluster resource YAML."""
        sc = OCP(
            kind="StorageCluster",
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            resource_name="ocs-storagecluster",
        )
        return sc.get()

    def get_mcg_profile_from_cr(self) -> Optional[str]:
        """Get MCG profile from StorageCluster CR."""
        data = self._get_storage_cluster_data()
        return self.deep_get(data, "spec", "multiCloudGateway", "performanceProfile")

    def get_core_storage_profile_from_cr(self) -> Optional[str]:
        """Get Core Storage profile from StorageCluster CR."""
        data = self._get_storage_cluster_data()
        return self.deep_get(data, "spec", "resourceProfile")

    def verify_page_integrity(self) -> bool:
        """Verify page has both sections."""
        return self.is_core_storage_section_present() and self.is_mcg_section_present()

    def get_mcg_profile_from_ui(self) -> Optional[str]:
        """Get selected MCG profile from UI dropdown."""
        from ocs_ci.ocs.ui.views import locators
        from ocs_ci.ocs.ui.helpers_ui import SeleniumDriver
        from selenium.webdriver.support.select import Select

        mcg_selector = locators["configure_performance"]["mcg_profile_selector"]
        driver = SeleniumDriver()
        select_element = driver.find_element(mcg_selector[1], mcg_selector[0])
        return Select(select_element).first_selected_option.text.strip().lower()

    def get_core_storage_profile_from_ui(self) -> Optional[str]:
        """Get selected Core Storage profile from UI dropdown."""
        from ocs_ci.ocs.ui.views import locators
        from ocs_ci.ocs.ui.helpers_ui import SeleniumDriver
        from selenium.webdriver.support.select import Select

        core_selector = locators["configure_performance"]["core_storage_selector"]
        driver = SeleniumDriver()
        select_element = driver.find_element(core_selector[1], core_selector[0])
        return Select(select_element).first_selected_option.text.strip().lower()
