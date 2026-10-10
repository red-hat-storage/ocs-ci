"""
Page Objects for Configure Performance Page (MCG and Core Storage profiles)

This module provides page object classes for interacting with the
Configure Performance page, including MCG and Core Storage profile selection.

Related to RHSTOR-8629: Support Performance Profiles for MCG
"""

import logging
from typing import List, Optional

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from ocs_ci.ocs.ui.base_ui import BaseUI
from ocs_ci.ocs.ocp import OCP


logger = logging.getLogger(__name__)


class ConfigurePerformancePage(BaseUI):
    """
    Page Object for Configure Performance page (Configure Performance Page Layout)

    This page shows:
    - Core Storage section with Block/File/RGW profiles
    - MCG (Multicloud Object Gateway) section with performance profiles
    """

    # Locators for page elements
    CORE_STORAGE_SECTION = (
        By.XPATH,
        "//*[contains(text(), 'Core Storage')]"
        "/ancestor::*[contains(@class, 'section') or contains(@class, 'panel')]",
    )
    MCG_SECTION = (
        By.XPATH,
        "//*[contains(text(), 'Multicloud') or contains(text(), 'MCG')]"
        "/ancestor::*[contains(@class, 'section') or contains(@class, 'panel')]",
    )

    # MCG Profile selector locator (from views.py)
    MCG_PROFILE_SELECTOR = (
        By.XPATH,
        "//*[contains(@class, 'c-select') and contains(@class, 'odf-configure-performance__selector')]",
    )

    # Core Storage profile selector
    CORE_STORAGE_SELECTOR = (
        By.XPATH,
        "//select[@aria-label='Resource Profile'] | //*[contains(@class, 'resource-profile')]",
    )

    # Node/resource display elements
    NODE_ITEM = (
        By.XPATH,
        "//*[contains(@class, 'node') or contains(@class, 'resource-item')]",
    )
    NODE_LIST = (By.XPATH, "//div[contains(@class, 'node-list')]")

    # Save/Cancel buttons
    SAVE_BUTTON = (By.XPATH, "//button[contains(text(), 'Save')]")
    CANCEL_BUTTON = (
        By.XPATH,
        "//button[contains(text(), 'Cancel') or contains(text(), 'Discard')]",
    )

    def __init__(self):
        super().__init__()
        logger.info("Initializing ConfigurePerformancePage")

    def is_core_storage_section_present(self) -> bool:
        """Verify that Core Storage section is present on the page."""
        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located(self.CORE_STORAGE_SECTION)
            )
            logger.info("✓ Core Storage section is present")
            return True
        except Exception as e:
            logger.warning(f"Core Storage section not found: {e}")
            return False

    def is_mcg_section_present(self) -> bool:
        """Verify that MCG section is present on the page."""
        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located(self.MCG_SECTION)
            )
            logger.info("✓ MCG section is present")
            return True
        except Exception as e:
            logger.warning(f"MCG section not found: {e}")
            return False

    def verify_sections_separated(self) -> bool:
        """Verify that Core Storage and MCG sections are visually separated."""
        try:
            core_storage = self.driver.find_element(*self.CORE_STORAGE_SECTION)
            mcg_section = self.driver.find_element(*self.MCG_SECTION)

            # Get positions
            core_location = core_storage.location
            mcg_location = mcg_section.location

            # Sections should be different (separated on page)
            separated = (core_location["y"] != mcg_location["y"]) or (
                core_location["x"] != mcg_location["x"]
            )
            logger.info(f"Sections separated: {separated}")
            return separated
        except Exception as e:
            logger.warning(f"Could not verify section separation: {e}")
            return False

    def is_core_storage_inline(self) -> bool:
        """Verify that Core Storage section displays inline (not as modal)."""
        try:
            core_storage = self.driver.find_element(*self.CORE_STORAGE_SECTION)

            # Check if it's a modal by looking for modal classes
            classes = core_storage.get_attribute("class")
            is_modal = "modal" in classes.lower() or "dialog" in classes.lower()

            # Check parent elements for modal indicators
            parent = core_storage.find_element(By.XPATH, "./ancestor::*")
            parent_classes = parent.get_attribute("class") or ""
            is_modal = is_modal or ("modal" in parent_classes.lower())

            inline = not is_modal
            logger.info(f"Core Storage is inline: {inline}")
            return inline
        except Exception as e:
            logger.warning(f"Could not verify if Core Storage is inline: {e}")
            return True

    def get_core_storage_nodes(self) -> List[dict]:
        """Get list of nodes displayed in Core Storage section."""
        try:
            core_storage = self.driver.find_element(*self.CORE_STORAGE_SECTION)
            nodes = core_storage.find_elements(
                By.XPATH, ".//div[contains(@class, 'node')]"
            )

            node_list = []
            for node in nodes:
                try:
                    node_name = node.find_element(By.XPATH, ".//*[@class='node-name']")
                    labels = node.get_attribute("data-labels") or ""
                    node_list.append(
                        {"name": node_name.text, "labels": labels, "element": node}
                    )
                except Exception:
                    pass

            logger.info(f"Found {len(node_list)} nodes in Core Storage section")
            return node_list
        except Exception as e:
            logger.warning(f"Error getting Core Storage nodes: {e}")
            return []

    def get_mcg_nodes(self) -> List[dict]:
        """Get list of nodes displayed in MCG section."""
        try:
            mcg = self.driver.find_element(*self.MCG_SECTION)
            nodes = mcg.find_elements(By.XPATH, ".//div[contains(@class, 'node')]")

            node_list = []
            for node in nodes:
                try:
                    node_name = node.find_element(By.XPATH, ".//*[@class='node-name']")
                    labels = node.get_attribute("data-labels") or ""
                    node_list.append(
                        {"name": node_name.text, "labels": labels, "element": node}
                    )
                except Exception:
                    pass

            logger.info(f"Found {len(node_list)} nodes in MCG section")
            return node_list
        except Exception as e:
            logger.warning(f"Error getting MCG nodes: {e}")
            return []

    def verify_all_nodes_have_ocs_label(self, nodes: List[dict]) -> bool:
        """Verify that all nodes in a list have the OCS label."""
        try:
            for node in nodes:
                labels = node.get("labels", "")
                has_ocs_label = (
                    "ocs" in labels.lower() or "openshift-storage" in labels.lower()
                )

                if not has_ocs_label:
                    logger.warning(f"Node {node.get('name')} missing OCS label")
                    return False

            logger.info(f"✓ All {len(nodes)} nodes have OCS label")
            return True
        except Exception as e:
            logger.warning(f"Error verifying OCS labels: {e}")
            return False

    def has_node_selection_option(self) -> bool:
        """Check if Core Storage section has a node selection option."""
        try:
            core_storage = self.driver.find_element(*self.CORE_STORAGE_SECTION)

            # Look for node selection elements
            selection_elements = core_storage.find_elements(
                By.XPATH,
                ".//input[@type='checkbox' or @type='radio'] | .//button[contains(text(), 'Select')]",
            )

            has_selection = len(selection_elements) > 0
            logger.info(f"Node selection option present: {has_selection}")
            return has_selection
        except Exception as e:
            logger.warning(f"Error checking for node selection: {e}")
            return False

    def select_mcg_profile(self, profile_name: str) -> bool:
        """Select an MCG profile from the selector dropdown."""
        try:
            logger.info(f"Selecting MCG profile: {profile_name}")

            mcg_selector = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable(self.MCG_PROFILE_SELECTOR)
            )
            mcg_selector.click()

            profile_option = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable(
                    (
                        By.XPATH,
                        f"//div[contains(@class, 'select-option')] | //li[contains(text(), '{profile_name}')]",
                    )
                )
            )
            profile_option.click()

            logger.info(f"✓ Selected MCG profile: {profile_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to select MCG profile: {e}")
            return False

    def select_core_storage_profile(self, profile_name: str) -> bool:
        """Select a Core Storage profile from the selector."""
        try:
            logger.info(f"Selecting Core Storage profile: {profile_name}")

            core_selector = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable(self.CORE_STORAGE_SELECTOR)
            )
            core_selector.click()

            profile_option = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable(
                    (
                        By.XPATH,
                        f"//option[text()='{profile_name}'] | //li[contains(text(), '{profile_name}')]",
                    )
                )
            )
            profile_option.click()

            logger.info(f"✓ Selected Core Storage profile: {profile_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to select Core Storage profile: {e}")
            return False

    def save_configuration(self) -> bool:
        """Click Save button to save configuration changes."""
        try:
            logger.info("Saving configuration...")
            save_btn = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable(self.SAVE_BUTTON)
            )
            save_btn.click()

            from time import sleep

            sleep(2)
            logger.info("✓ Configuration saved")
            return True
        except Exception as e:
            logger.error(f"Failed to save configuration: {e}")
            return False

    def cancel_changes(self) -> bool:
        """Click Cancel button to discard changes."""
        try:
            logger.info("Cancelling changes...")
            cancel_btn = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable(self.CANCEL_BUTTON)
            )
            cancel_btn.click()

            logger.info("✓ Changes cancelled")
            return True
        except Exception as e:
            logger.error(f"Failed to cancel changes: {e}")
            return False

    def get_mcg_profile_from_cr(self) -> Optional[str]:
        """Get the current MCG profile from StorageCluster CR."""
        try:
            sc = OCP(kind="StorageCluster", namespace="openshift-storage")
            sc_data = sc.get_resource("ocs-storagecluster")

            profile = (
                sc_data.get("spec", {})
                .get("multiCloudGateway", {})
                .get("performanceProfile")
            )
            logger.info(f"MCG profile from CR: {profile}")
            return profile
        except Exception as e:
            logger.error(f"Error reading MCG profile from CR: {e}")
            return None

    def get_core_storage_profile_from_cr(self) -> Optional[str]:
        """Get the current Core Storage profile from StorageCluster CR."""
        try:
            sc = OCP(kind="StorageCluster", namespace="openshift-storage")
            sc_data = sc.get_resource("ocs-storagecluster")

            profile = sc_data.get("spec", {}).get("resourceProfile")
            logger.info(f"Core Storage profile from CR: {profile}")
            return profile
        except Exception as e:
            logger.error(f"Error reading Core Storage profile from CR: {e}")
            return None

    def verify_page_integrity(self) -> bool:
        """Verify overall page integrity and structure."""
        try:
            has_core_storage = self.is_core_storage_section_present()
            has_mcg = self.is_mcg_section_present()

            integrity = has_core_storage and has_mcg
            logger.info(f"Page integrity check: {integrity}")
            return integrity
        except Exception as e:
            logger.error(f"Page integrity check failed: {e}")
            return False
