"""
Page object for the Fusion Access HA — Stretch Cluster node-selection wizard.

Provides helpers for the Connect External Storage → Storage for SAN UI flow
that expose the "Include control plane nodes" and "Stretch cluster" toggles
introduced in RHSTOR-8877.
"""

import logging

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.ui.base_ui import BaseUI, wait_for_element_to_be_clickable
from ocs_ci.ocs.ui.helpers_ui import format_locator
from ocs_ci.ocs.ui.page_objects.fusion_access_ui import FusionAccessUI
from ocs_ci.ocs.ui.views import STRETCH_CLUSTER_LOCATORS, FDF_SAN_LOCATORS

logger = logging.getLogger(__name__)

# Canonical role strings that the UI uses inside the node table
ROLE_DISK_NODE = "disk-node"
ROLE_CLUSTER_NODE = "cluster-node"
ROLE_ARBITER_NODE = "arbiter-node"


class StretchClusterSANUI(FusionAccessUI):
    """
    Extend :class:`~ocs_ci.ocs.ui.page_objects.fusion_access_ui.FusionAccessUI`
    with stretch-cluster specific interactions on the Connect External Storage →
    Storage for SAN node-selection page.

    Usage example::

        ui = StretchClusterSANUI()
        ui.navigate_to_connect_external_storage_san()
        # Assertions against default state (HP-01)
        assert not ui.is_include_control_plane_enabled()
        assert not ui.is_stretch_cluster_enabled()
    """


    def navigate_to_connect_external_storage_san(self):
        """
        Navigate through the wizard to the node-selection step for SAN storage.

        Clicks: Connect external systems → SAN radio → Next.
        """
        logger.info("Navigating to Connect External Storage > Storage for SAN")
        self.click_connect_external_systems()
        self.select_storage_area_network()
        self.click_next_button()
        logger.info("Reached the SAN node-selection wizard page")


    def is_include_control_plane_enabled(self):
        """
        Return whether the "Include control plane nodes" toggle is checked/active.

        Returns:
            bool: True when the toggle is ON, False when OFF.
        """
        el = self.get_elements(STRETCH_CLUSTER_LOCATORS["include_control_plane_toggle"])
        if not el:
            logger.debug("include_control_plane_toggle element not found — returning False")
            return False
        state = el[0].get_attribute("aria-checked") or el[0].get_attribute("checked")
        enabled = state in ("true", "checked", True)
        logger.debug(f"Include control plane nodes toggle state: {enabled}")
        return enabled

    def is_stretch_cluster_enabled(self):
        """
        Return whether the "Stretch cluster" toggle is checked/active.

        Returns:
            bool: True when the toggle is ON, False when OFF.
        """
        el = self.get_elements(STRETCH_CLUSTER_LOCATORS["stretch_cluster_toggle"])
        if not el:
            logger.debug("stretch_cluster_toggle element not found — returning False")
            return False
        state = el[0].get_attribute("aria-checked") or el[0].get_attribute("checked")
        enabled = state in ("true", "checked", True)
        logger.debug(f"Stretch cluster toggle state: {enabled}")
        return enabled

    def is_include_control_plane_disabled(self):
        """
        Return whether the "Include control plane nodes" toggle is currently
        disabled (i.e. read-only because Stretch cluster is active).

        Returns:
            bool: True when the element is disabled.
        """
        el = self.get_elements(STRETCH_CLUSTER_LOCATORS["include_control_plane_toggle"])
        if not el:
            return False
        disabled = el[0].get_attribute("disabled") or el[0].get_attribute("aria-disabled")
        result = disabled in ("true", "", True)
        logger.debug(f"Include control plane nodes disabled: {result}")
        return result


    def enable_include_control_plane(self):
        """
        Click the "Include control plane nodes" toggle to enable it.

        Raises:
            TimeoutExpiredError: If the toggle is not found or not clickable.
        """
        logger.info("Enabling 'Include control plane nodes' toggle")
        wait_for_element_to_be_clickable(
            STRETCH_CLUSTER_LOCATORS["include_control_plane_toggle"], timeout=30
        )
        self.do_click(
            STRETCH_CLUSTER_LOCATORS["include_control_plane_toggle"],
            enable_screenshot=True,
        )
        logger.info("'Include control plane nodes' toggle enabled")

    def enable_stretch_cluster(self):
        """
        Click the "Stretch cluster" toggle to enable it.

        Raises:
            TimeoutExpiredError: If the toggle is not found or not clickable.
        """
        logger.info("Enabling 'Stretch cluster' toggle")
        wait_for_element_to_be_clickable(
            STRETCH_CLUSTER_LOCATORS["stretch_cluster_toggle"], timeout=30
        )
        self.do_click(
            STRETCH_CLUSTER_LOCATORS["stretch_cluster_toggle"],
            enable_screenshot=True,
        )
        logger.info("'Stretch cluster' toggle enabled")


    def get_visible_node_rows(self):
        """
        Return all node rows currently visible in the node table.

        Returns:
            list[selenium.webdriver.remote.webelement.WebElement]:
                One entry per visible node row.
        """
        rows = self.get_elements(STRETCH_CLUSTER_LOCATORS["node_table_rows"])
        logger.debug(f"Visible node table rows: {len(rows)}")
        return rows

    def get_node_role_in_row(self, row_index):
        """
        Return the role text for the node at ``row_index`` (1-based).

        Args:
            row_index (int): 1-based row number.

        Returns:
            str: Role text (e.g. ``'disk-node'``, ``'cluster-node'``, ``'arbiter-node'``).

        Raises:
            TimeoutExpiredError: If the cell is not found within the timeout.
        """
        locator = format_locator(
            STRETCH_CLUSTER_LOCATORS["node_table_role_cell"], i=row_index
        )
        role_text = self.get_element_text(locator)
        logger.debug(f"Node row {row_index} role: {role_text!r}")
        return role_text.strip()

    def get_all_visible_roles(self):
        """
        Return the role text for every visible node row.

        Returns:
            list[str]: Role text for rows 1 through N.
        """
        rows = self.get_visible_node_rows()
        roles = [self.get_node_role_in_row(i + 1) for i in range(len(rows))]
        logger.debug(f"All visible node roles: {roles}")
        return roles

    def get_node_name_in_row(self, row_index):
        """
        Return the node name text for the node at ``row_index`` (1-based).

        Args:
            row_index (int): 1-based row number.

        Returns:
            str: Node name.
        """
        locator = format_locator(
            STRETCH_CLUSTER_LOCATORS["node_table_name_cell"], i=row_index
        )
        name = self.get_element_text(locator).strip()
        logger.debug(f"Node row {row_index} name: {name!r}")
        return name

    def is_role_dropdown_disabled_for_node(self, node_name):
        """
        Return whether the role dropdown is disabled for a given node.

        Args:
            node_name (str): The node hostname as shown in the UI table.

        Returns:
            bool: True when the dropdown button is disabled.
        """
        locator = format_locator(
            STRETCH_CLUSTER_LOCATORS["arbiter_node_role_dropdown"],
            arbiter_node=node_name,
        )
        elements = self.get_elements(locator)
        if not elements:
            logger.debug(
                f"Role dropdown not found for node '{node_name}' — treating as absent"
            )
            return False
        disabled = elements[0].get_attribute("disabled") or elements[0].get_attribute(
            "aria-disabled"
        )
        result = disabled in ("true", "", True)
        logger.debug(f"Role dropdown disabled for '{node_name}': {result}")
        return result

    def get_role_dropdown_options_for_node(self, node_name):
        """
        Open the role dropdown for ``node_name`` and return the available
        option texts.

        Args:
            node_name (str): Node hostname as shown in the table.

        Returns:
            list[str]: Text of each clickable dropdown option.

        Raises:
            TimeoutExpiredError: If the dropdown is not found or not clickable.
        """
        dropdown_locator = format_locator(
            STRETCH_CLUSTER_LOCATORS["node_role_dropdown_by_name"],
            node_name=node_name,
        )
        logger.info(f"Opening role dropdown for node '{node_name}'")
        wait_for_element_to_be_clickable(dropdown_locator, timeout=15)
        self.do_click(dropdown_locator)

        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located(
                    (
                        STRETCH_CLUSTER_LOCATORS["node_role_dropdown_options"][1],
                        STRETCH_CLUSTER_LOCATORS["node_role_dropdown_options"][0],
                    )
                )
            )
        except Exception as err:
            self.take_screenshot("role_dropdown_no_options")
            raise TimeoutExpiredError(
                f"Role dropdown options did not appear for node '{node_name}'"
            ) from err

        options = self.get_elements(STRETCH_CLUSTER_LOCATORS["node_role_dropdown_options"])
        option_texts = [o.text.strip() for o in options if o.text.strip()]
        logger.info(
            f"Role dropdown options for '{node_name}': {option_texts}"
        )
        # Close dropdown by pressing Escape or clicking elsewhere
        self.driver.execute_script("arguments[0].blur();", options[0])
        return option_texts

    def change_node_role(self, node_name, target_role):
        """
        Open the role dropdown for ``node_name`` and select ``target_role``.

        Args:
            node_name (str): Node hostname as shown in the table.
            target_role (str): Target role text, e.g. ``'cluster-node'``.

        Raises:
            TimeoutExpiredError: If the dropdown or the target option is not found.
        """
        dropdown_locator = format_locator(
            STRETCH_CLUSTER_LOCATORS["node_role_dropdown_by_name"],
            node_name=node_name,
        )
        logger.info(f"Changing role of node '{node_name}' to '{target_role}'")
        wait_for_element_to_be_clickable(dropdown_locator, timeout=15)
        self.do_click(dropdown_locator)

        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located(
                    (
                        STRETCH_CLUSTER_LOCATORS["node_role_dropdown_options"][1],
                        STRETCH_CLUSTER_LOCATORS["node_role_dropdown_options"][0],
                    )
                )
            )
        except Exception as err:
            self.take_screenshot("change_role_no_options")
            raise TimeoutExpiredError(
                f"Role dropdown options did not appear for node '{node_name}'"
            ) from err

        options = self.get_elements(STRETCH_CLUSTER_LOCATORS["node_role_dropdown_options"])
        for option in options:
            if target_role.lower() in option.text.lower():
                option.click()
                logger.info(
                    f"Role of node '{node_name}' changed to '{target_role}'"
                )
                return

        self.take_screenshot(f"change_role_not_found_{target_role}")
        raise TimeoutExpiredError(
            f"Role option '{target_role}' not found in dropdown for node '{node_name}'"
        )



    def is_no_arbiter_alert_visible(self):
        """
        Return whether the "no arbiter node" alert banner is currently visible.

        Returns:
            bool: True if the alert is present on the page.
        """
        el = self.get_elements(STRETCH_CLUSTER_LOCATORS["no_arbiter_alert"])
        visible = bool(el)
        logger.debug(f"No-arbiter alert visible: {visible}")
        return visible



    def get_lun_discovery_rows(self):
        """
        Return all LUN rows visible in the LUN discovery table.

        Returns:
            list[selenium.webdriver.remote.webelement.WebElement]:
                One entry per visible LUN row.
        """
        rows = self.get_elements(STRETCH_CLUSTER_LOCATORS["lun_discovery_rows"])
        logger.debug(f"LUN discovery rows visible: {len(rows)}")
        return rows

    def get_lun_wwids(self):
        """
        Return WWID strings for every LUN row currently displayed.

        Uses the second column (WWID) of the LUN table, consistent with how
        :class:`~ocs_ci.ocs.ui.page_objects.fusion_access_ui.FusionAccessUI`
        reads row IDs.

        Returns:
            list[str]: WWID value for each row.
        """
        rows = self.get_lun_discovery_rows()
        wwids = []
        for i, _ in enumerate(rows, start=1):
            locator = (
                FDF_SAN_LOCATORS["lun_table_row_id"][0].format(i=i),
                FDF_SAN_LOCATORS["lun_table_row_id"][1],
            )
            wwid = self.get_element_text(locator).strip()
            wwids.append(wwid)
        logger.debug(f"Discovered LUN WWIDs: {wwids}")
        return wwids
