"""
Page object for the Fusion Access HA — Stretch Cluster node-selection wizard.

Provides helpers for the Connect External Storage → Storage for SAN UI flow
that expose the "Include control plane nodes" and "Stretch cluster" toggles
introduced in RHSTOR-8877.
"""

import logging

from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.ui.base_ui import SeleniumDriver, wait_for_element_to_be_clickable
from ocs_ci.ocs.ui.helpers_ui import format_locator
from ocs_ci.ocs.ui.page_objects.fusion_access_ui import FusionAccessUI
from ocs_ci.ocs.ui.views import STRETCH_CLUSTER_LOCATORS
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)

# Seconds to wait for a wizard page / table to reach the expected state
_UI_WAIT_TIMEOUT = 60

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

        Clicks: Storage > External systems → Connect external systems →
        SAN radio → Next.

        Returns:
            StretchClusterSANUI: self, so the call can be chained.
        """
        logger.info("Navigating to Connect External Storage > Storage for SAN")
        self.nav_external_systems_page()
        self.click_connect_external_systems()
        self.select_storage_area_network()
        self.click_next_button()
        logger.info("Waiting for the SAN node-selection page to render")
        self._wait_for_node_table()
        logger.info("Reached the SAN node-selection wizard page")
        return self

    def _wait_for_node_table(self):
        """
        Wait until the node-selection table has at least one node row.

        Raises:
            TimeoutExpiredError: If no node row appears in time.
        """
        rows = self.wait_for_elements(
            STRETCH_CLUSTER_LOCATORS["node_table_rows"], timeout=_UI_WAIT_TIMEOUT
        )
        if not rows:
            self.take_screenshot("node_table_not_rendered")
            raise TimeoutExpiredError(
                "Node-selection table did not render any node rows within "
                f"{_UI_WAIT_TIMEOUT}s"
            )

    def _get_toggle(self, locator_key, label):
        """
        Return the toggle element for *locator_key*.

        Args:
            locator_key (str): Key into ``STRETCH_CLUSTER_LOCATORS``.
            label (str): Human readable toggle name, used in the error message.

        Returns:
            selenium.webdriver.remote.webelement.WebElement: The toggle.

        Raises:
            TimeoutExpiredError: If the toggle is not present on the page. A
                missing toggle is a product/locator problem and must not be
                reported as "off" or "enabled" — that would silently pass the
                default-state assertions.
        """
        elements = self.wait_for_elements(
            STRETCH_CLUSTER_LOCATORS[locator_key], timeout=_UI_WAIT_TIMEOUT
        )
        if not elements:
            self.take_screenshot(f"{locator_key}_not_found")
            raise TimeoutExpiredError(
                f"'{label}' toggle was not found on the node-selection page"
            )
        return elements[0]

    @staticmethod
    def _is_checked(element):
        """
        Return whether a toggle element is in the checked/on state.

        Args:
            element (selenium.webdriver.remote.webelement.WebElement): Toggle.

        Returns:
            bool: True when the toggle is ON.
        """
        state = element.get_attribute("aria-checked") or element.get_attribute(
            "checked"
        )
        return str(state).lower() in ("true", "checked")

    @staticmethod
    def _is_disabled(element):
        """
        Return whether an element is disabled.

        A boolean ``disabled`` attribute is reported by Selenium either as
        ``"true"`` or as an empty string, so both count as disabled; a missing
        attribute comes back as ``None`` and does not.

        Args:
            element (selenium.webdriver.remote.webelement.WebElement): Element.

        Returns:
            bool: True when the element is disabled.
        """
        for attribute in ("disabled", "aria-disabled"):
            value = element.get_attribute(attribute)
            if value is not None and str(value).lower() in ("true", ""):
                return True
        return False

    def is_include_control_plane_enabled(self):
        """
        Return whether the "Include control plane nodes" toggle is checked/active.

        Returns:
            bool: True when the toggle is ON, False when OFF.

        Raises:
            TimeoutExpiredError: If the toggle is not present on the page.
        """
        enabled = self._is_checked(
            self._get_toggle(
                "include_control_plane_toggle", "Include control plane nodes"
            )
        )
        logger.debug(f"Include control plane nodes toggle state: {enabled}")
        return enabled

    def is_stretch_cluster_enabled(self):
        """
        Return whether the "Stretch cluster" toggle is checked/active.

        Returns:
            bool: True when the toggle is ON, False when OFF.

        Raises:
            TimeoutExpiredError: If the toggle is not present on the page.
        """
        enabled = self._is_checked(
            self._get_toggle("stretch_cluster_toggle", "Stretch cluster")
        )
        logger.debug(f"Stretch cluster toggle state: {enabled}")
        return enabled

    def is_include_control_plane_disabled(self):
        """
        Return whether the "Include control plane nodes" toggle is currently
        disabled (i.e. read-only because Stretch cluster is active).

        Returns:
            bool: True when the element is disabled.

        Raises:
            TimeoutExpiredError: If the toggle is not present on the page.
        """
        element = self._get_toggle(
            "include_control_plane_toggle", "Include control plane nodes"
        )
        result = self._is_disabled(element)
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
        self._wait_for_toggle_state(
            "include_control_plane_toggle", "Include control plane nodes", True
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
        self._wait_for_toggle_state("stretch_cluster_toggle", "Stretch cluster", True)
        logger.info("'Stretch cluster' toggle enabled")

    def _wait_for_toggle_state(self, locator_key, label, expected):
        """
        Wait until a toggle reports the expected checked state.

        Args:
            locator_key (str): Key into ``STRETCH_CLUSTER_LOCATORS``.
            label (str): Human readable toggle name, used in the error message.
            expected (bool): Checked state to wait for.

        Raises:
            TimeoutExpiredError: If the toggle does not reach the expected state.
        """
        try:
            for element in TimeoutSampler(
                timeout=_UI_WAIT_TIMEOUT,
                sleep=2,
                func=self._get_toggle,
                locator_key=locator_key,
                label=label,
            ):
                if self._is_checked(element) is expected:
                    return
        except TimeoutExpiredError:
            self.take_screenshot(f"{locator_key}_state_not_reached")
            raise TimeoutExpiredError(
                f"'{label}' toggle did not become "
                f"{'ON' if expected else 'OFF'} within {_UI_WAIT_TIMEOUT}s"
            )

    def get_visible_node_rows(self):
        """
        Return all node rows currently visible in the node table.

        Returns:
            list[selenium.webdriver.remote.webelement.WebElement]:
                One entry per visible node row.
        """
        rows = self.wait_for_elements(
            STRETCH_CLUSTER_LOCATORS["node_table_rows"], timeout=_UI_WAIT_TIMEOUT
        )
        logger.debug(f"Visible node table rows: {len(rows)}")
        return rows

    @staticmethod
    def _read_row_cell(row, locator_key):
        """
        Return the stripped text of a cell inside an already-found table row.

        Reading the cell from the row element, rather than by row position,
        keeps the value tied to the row even if the table re-renders or
        reorders between calls.

        Args:
            row (selenium.webdriver.remote.webelement.WebElement): Table row.
            locator_key (str): Key into ``STRETCH_CLUSTER_LOCATORS`` holding a
                row-relative locator.

        Returns:
            str: Cell text.
        """
        by_value, by = STRETCH_CLUSTER_LOCATORS[locator_key]
        return row.find_element(by, by_value).text.strip()

    def get_node_roles_by_name(self):
        """
        Return the role of every visible node, keyed by node name.

        Each row is read as a unit (name and role from the same row element).

        Returns:
            dict: Mapping of node name to the role text shown in the UI.
        """
        roles = {
            self._read_row_cell(row, "node_row_name_cell"): self._read_row_cell(
                row, "node_row_role_cell"
            )
            for row in self.get_visible_node_rows()
        }
        logger.debug(f"Node roles by name: {roles}")
        return roles

    def get_all_visible_roles(self):
        """
        Return the role text for every visible node row.

        Returns:
            list[str]: Role text of each visible row.
        """
        roles = list(self.get_node_roles_by_name().values())
        logger.debug(f"All visible node roles: {roles}")
        return roles

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
            self.take_screenshot(f"role_dropdown_missing_{node_name}")
            raise TimeoutExpiredError(
                f"Role dropdown was not found for node '{node_name}'"
            )
        result = self._is_disabled(elements[0])
        logger.debug(f"Role dropdown disabled for '{node_name}': {result}")
        return result

    def _open_role_dropdown(self, node_name):
        """
        Open the role dropdown for ``node_name`` and return its options.

        Args:
            node_name (str): Node hostname as shown in the table.

        Returns:
            list[selenium.webdriver.remote.webelement.WebElement]: Option
                elements of the open dropdown.

        Raises:
            TimeoutExpiredError: If the dropdown is not clickable or no option
                appears.
        """
        dropdown_locator = format_locator(
            STRETCH_CLUSTER_LOCATORS["node_role_dropdown_by_name"],
            node_name=node_name,
        )
        logger.info(f"Opening role dropdown for node '{node_name}'")
        wait_for_element_to_be_clickable(dropdown_locator, timeout=15)
        self.do_click(dropdown_locator)

        options_locator = STRETCH_CLUSTER_LOCATORS["node_role_dropdown_options"]
        try:
            WebDriverWait(SeleniumDriver(), 10).until(
                EC.presence_of_element_located((options_locator[1], options_locator[0]))
            )
        except TimeoutException as err:
            self.take_screenshot(f"role_dropdown_no_options_{node_name}")
            raise TimeoutExpiredError(
                f"Role dropdown options did not appear for node '{node_name}'"
            ) from err

        return self.get_elements(options_locator)

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
        options = self._open_role_dropdown(node_name)
        option_texts = [o.text.strip() for o in options if o.text.strip()]
        logger.info(f"Role dropdown options for '{node_name}': {option_texts}")
        # Close the dropdown again so the next interaction starts from a known
        # state.
        SeleniumDriver().execute_script("arguments[0].blur();", options[0])
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
        logger.info(f"Changing role of node '{node_name}' to '{target_role}'")
        options = self._open_role_dropdown(node_name)
        wanted = self._normalize_role(target_role)
        for option in options:
            if wanted in self._normalize_role(option.text):
                option.click()
                self._wait_for_node_role(node_name, target_role)
                logger.info(f"Role of node '{node_name}' changed to '{target_role}'")
                return

        self.take_screenshot(f"change_role_not_found_{target_role}")
        raise TimeoutExpiredError(
            f"Role option '{target_role}' not found in dropdown for node '{node_name}'"
        )

    @staticmethod
    def _normalize_role(role_text):
        """
        Normalize a role label so ``'cluster-node'`` matches ``'Cluster node'``.

        Hyphens become spaces, whitespace is collapsed and case is folded.

        Args:
            role_text (str): Role label as passed in or as shown by the UI.

        Returns:
            str: Normalized label.
        """
        return " ".join(role_text.replace("-", " ").split()).casefold()

    def _wait_for_node_role(self, node_name, expected_role):
        """
        Wait until the role cell of ``node_name`` shows ``expected_role``.

        Args:
            node_name (str): Node hostname as shown in the table.
            expected_role (str): Role expected after the change; compared after
                normalization.

        Raises:
            TimeoutExpiredError: If the cell does not show the role in time.
        """
        locator = format_locator(
            STRETCH_CLUSTER_LOCATORS["node_role_cell_by_name"], node_name=node_name
        )
        wanted = self._normalize_role(expected_role)
        try:
            for cells in TimeoutSampler(
                timeout=_UI_WAIT_TIMEOUT,
                sleep=2,
                func=self.get_elements,
                locator=locator,
            ):
                if cells and self._normalize_role(cells[0].text) == wanted:
                    return
        except TimeoutExpiredError:
            self.take_screenshot(f"role_not_applied_{node_name}")
            raise TimeoutExpiredError(
                f"Role of node '{node_name}' did not change to "
                f"'{expected_role}' within {_UI_WAIT_TIMEOUT}s"
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
        rows = self.wait_for_elements(
            STRETCH_CLUSTER_LOCATORS["lun_discovery_rows"], timeout=_UI_WAIT_TIMEOUT
        )
        logger.debug(f"LUN discovery rows visible: {len(rows)}")
        return rows

    def get_lun_wwids(self):
        """
        Return WWID strings for every LUN row currently displayed.

        Waits for LUN discovery to populate the table first. Each WWID is read
        from its own row element, found by the column label; when the cell
        carries no label the second cell of the same row is used.

        Returns:
            list[str]: WWID value for each row. Empty when discovery produced no
                rows within the wait.
        """
        wwids = []
        for row in self.get_lun_discovery_rows():
            by_value, by = STRETCH_CLUSTER_LOCATORS["lun_row_wwid_cell"]
            cells = row.find_elements(by, by_value)
            if not cells:
                by_value, by = STRETCH_CLUSTER_LOCATORS["lun_row_cells"]
                all_cells = row.find_elements(by, by_value)
                logger.warning(
                    "LUN row has no WWID-labelled cell; using its second cell"
                )
                cells = all_cells[1:2]
            wwids.append(cells[0].text.strip() if cells else "")
        logger.debug(f"Discovered LUN WWIDs: {wwids}")
        return wwids
