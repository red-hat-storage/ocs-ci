"""
Page object for the CephFS Subvolume Metrics section on the ODF Block and
File tab (RHSTOR-7679, new in ODF 4.22).

Navigation path:
  Storage > Storage Cluster > Block and File tab
  → "Current top 10 subvolumes" card / section

The page object provides:
  - Tab switching between IOPS / Latency / Throughput rankings
  - Reading ranking table rows (name, namespace, metric value)
  - Opening and closing the per-PVC detail drawer
  - Reading pod and node information from the detail drawer
  - Empty-state detection
  - Search / filter

TODO: All XPath locators in views.py (validation_4_22) must be verified
against the final 4.22 ODF console implementation before enabling the
test suite.  See OCSQE-4576.
"""

from ocs_ci.ocs.ui.base_ui import logger
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator


class CephFSSubvolumeMetricsPage(PageNavigator):
    """
    Interactions with the CephFS Subvolume Metrics section of the ODF
    Block and File tab.
    """

    def __init__(self):
        super().__init__()

    # ------------------------------------------------------------------
    # Tab switching
    # ------------------------------------------------------------------

    def click_iops_tab(self):
        """Switch the ranking view to IOPS."""
        logger.info("Switching CephFS subvolume ranking to IOPS")
        self.do_click(
            self.validation_loc["cephfs_subvolume_iops_tab"],
            enable_screenshot=True,
        )
        self.page_has_loaded()

    def click_latency_tab(self):
        """Switch the ranking view to Latency."""
        logger.info("Switching CephFS subvolume ranking to Latency")
        self.do_click(
            self.validation_loc["cephfs_subvolume_latency_tab"],
            enable_screenshot=True,
        )
        self.page_has_loaded()

    def click_throughput_tab(self):
        """Switch the ranking view to Throughput."""
        logger.info("Switching CephFS subvolume ranking to Throughput")
        self.do_click(
            self.validation_loc["cephfs_subvolume_throughput_tab"],
            enable_screenshot=True,
        )
        self.page_has_loaded()

    # ------------------------------------------------------------------
    # Card / section presence
    # ------------------------------------------------------------------

    def is_metrics_card_present(self) -> bool:
        """
        Check whether the CephFS subvolume metrics card is visible on the
        Block and File tab.

        Returns:
            bool: True if the card is present, False otherwise.
        """
        elements = self.get_elements(
            self.validation_loc["cephfs_subvolume_metrics_card"]
        )
        return len(elements) > 0

    def is_empty_state(self) -> bool:
        """
        Check whether the empty-state message is shown (no CephFS PVCs).

        Returns:
            bool: True if empty state is displayed.
        """
        elements = self.get_elements(
            self.validation_loc["cephfs_subvolume_empty_state"]
        )
        return len(elements) > 0

    # ------------------------------------------------------------------
    # Ranking table
    # ------------------------------------------------------------------

    def get_ranking_rows(self) -> list:
        """
        Return all visible ranking table row elements.

        Returns:
            list: WebElement list, one per row (max 10 in production).
        """
        return self.get_elements(self.validation_loc["cephfs_subvolume_table_rows"])

    def get_ranking_row_data(self) -> list[dict]:
        """
        Parse each visible ranking row into a dict with keys:
        name, namespace, value.

        Returns:
            list[dict]: Parsed row data sorted as displayed.
        """
        rows = self.get_ranking_rows()
        result = []
        for row in rows:
            name = row.find_elements(*self.validation_loc["cephfs_subvolume_row_name"])
            namespace = row.find_elements(
                *self.validation_loc["cephfs_subvolume_row_namespace"]
            )
            value = row.find_elements(
                *self.validation_loc["cephfs_subvolume_row_value"]
            )
            result.append(
                {
                    "name": name[0].text.strip() if name else "",
                    "namespace": namespace[0].text.strip() if namespace else "",
                    "value": value[0].text.strip() if value else "",
                }
            )
        return result

    # ------------------------------------------------------------------
    # Detail drawer
    # ------------------------------------------------------------------

    def click_ranking_row(self, row_index: int = 0):
        """
        Click a ranking row to open its detail drawer.

        Args:
            row_index (int): Zero-based index of the row to click.
        """
        rows = self.get_ranking_rows()
        assert rows, "No ranking rows found; cannot open detail drawer"
        assert row_index < len(
            rows
        ), f"row_index={row_index} out of range (found {len(rows)} rows)"
        logger.info(f"Clicking ranking row index {row_index} to open detail drawer")
        rows[row_index].click()
        self.wait_for_element_to_be_visible(
            self.validation_loc["cephfs_subvolume_detail_drawer"]
        )

    def is_detail_drawer_open(self) -> bool:
        """
        Check whether the detail drawer is currently open.

        Returns:
            bool: True if the drawer is visible.
        """
        return (
            len(
                self.get_elements(self.validation_loc["cephfs_subvolume_detail_drawer"])
            )
            > 0
        )

    def close_detail_drawer(self):
        """Close the detail drawer via the X button."""
        logger.info("Closing CephFS subvolume detail drawer")
        self.do_click(
            self.validation_loc["cephfs_subvolume_detail_drawer_close"],
            enable_screenshot=True,
        )

    def get_detail_drawer_pods(self) -> list[dict]:
        """
        Return pod rows from the detail drawer.

        Returns:
            list[dict]: Each dict has keys: pod_name, namespace, node, status.
        """
        assert self.is_detail_drawer_open(), "Detail drawer is not open"
        rows = self.get_elements(self.validation_loc["cephfs_subvolume_detail_pods"])
        result = []
        for row in rows:
            cells = row.find_elements("tag name", "td")
            result.append(
                {
                    "pod_name": cells[0].text.strip() if len(cells) > 0 else "",
                    "namespace": cells[1].text.strip() if len(cells) > 1 else "",
                    "node": cells[2].text.strip() if len(cells) > 2 else "",
                    "status": cells[3].text.strip() if len(cells) > 3 else "",
                }
            )
        return result

    # ------------------------------------------------------------------
    # Search / filter
    # ------------------------------------------------------------------

    def search_pvc(self, pvc_name: str):
        """
        Type into the search bar to filter the ranking table by PVC name.

        Args:
            pvc_name (str): Substring to search for.
        """
        logger.info(f"Searching CephFS subvolume metrics for PVC: {pvc_name}")
        self.do_send_keys(
            self.validation_loc["cephfs_subvolume_search"],
            pvc_name,
        )
        self.page_has_loaded()

    def clear_search(self):
        """Clear the search bar and restore the full ranking table."""
        logger.info("Clearing CephFS subvolume metrics search bar")
        search_el = self.get_elements(self.validation_loc["cephfs_subvolume_search"])
        if search_el:
            search_el[0].clear()
            self.page_has_loaded()
