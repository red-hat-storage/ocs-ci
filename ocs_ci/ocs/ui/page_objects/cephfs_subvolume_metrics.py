import logging

from ocs_ci.ocs import constants
from ocs_ci.ocs.ui.helpers_ui import format_locator
from ocs_ci.ocs.ui.page_objects.block_and_file import BlockAndFile

logger = logging.getLogger(__name__)


class CephFSSubvolumeMetricsCard(BlockAndFile):
    """
    Page object for the CephFS subvolume metrics card on the Block and File tab.

    Provides interactions with the 'Current top 10 subvolumes on all clusters'
    card: scroll-to, title check, metric dropdown, help popover, and table.
    """

    def __init__(self):
        super().__init__()
        self.card_title_loc = self.validation_loc["cephfs_subvolume_card_title"]
        self.metric_toggle_loc = self.validation_loc["cephfs_subvolume_metric_toggle"]
        self.metric_option_loc = self.validation_loc["cephfs_subvolume_metric_option"]
        self.table_rows_loc = self.validation_loc["cephfs_subvolume_table_rows"]
        self.help_button_loc = self.validation_loc["cephfs_subvolume_help_button"]
        self.popover_body_loc = self.validation_loc["cephfs_subvolume_popover_body"]
        self.col_headers_loc = self.validation_loc["cephfs_subvolume_col_headers"]

    def navigate_to_cephfs_subvolume_section(self):
        """Scroll the Block and File tab to bring the CephFS subvolume card into view."""
        logger.info("Scrolling to CephFS subvolume metrics card")
        self.scroll_into_view(self.card_title_loc)

    def verify_cephfs_subvolume_section_visible(self):
        """
        Verify the card title is visible on the Block and File tab.

        Returns:
            bool: True if the card title element is visible, False otherwise.
        """
        logger.info("Verifying CephFS subvolume metrics card is visible")
        self.navigate_to_cephfs_subvolume_section()
        return self.check_element_text(constants.CEPHFS_SUBVOLUME_METRICS_CARD_TITLE)

    def get_cephfs_subvolume_metric_toggle_text(self):
        """
        Return the label currently shown on the metric dropdown toggle.

        Returns:
            str: One of 'Total IOPS', 'Total Latency', 'Total Throughput'.
        """
        logger.info("Reading active metric from CephFS subvolume dropdown")
        return self.get_element_text(self.metric_toggle_loc).strip()

    def switch_cephfs_subvolume_metric(self, metric_label):
        """
        Select a metric from the CephFS subvolume dropdown.

        Args:
            metric_label (str): One of 'Total IOPS', 'Total Latency',
                'Total Throughput'.
        """
        logger.info("Switching CephFS subvolume metric to: %s", metric_label)
        self.do_click(self.metric_toggle_loc)
        self.do_click(format_locator(self.metric_option_loc, metric_label))

    def click_cephfs_subvolume_help_button(self):
        """Click the help (?) button next to the CephFS subvolume card title."""
        logger.info("Clicking CephFS subvolume help button")
        self.do_click(self.help_button_loc)

    def verify_cephfs_subvolume_popover_text(self, expected_text, timeout=15):
        """
        Wait for the help popover to contain expected_text and return the result.

        Uses an explicit poll so the assertion is resilient to React rendering
        lag after the help button is clicked.

        Args:
            expected_text (str): Substring that must appear in the popover body.
            timeout (int): Maximum seconds to wait for the text.

        Returns:
            bool: True if expected_text appears within timeout, False otherwise.
        """
        logger.info(
            "Waiting for CephFS subvolume help popover to contain: %s", expected_text
        )
        return self.wait_until_expected_text_is_found(
            self.popover_body_loc,
            expected_text,
            timeout=timeout,
        )

    def get_cephfs_subvolume_column_headers(self):
        """
        Return the column header labels from the subvolume table.

        Returns:
            list[str]: Column header texts, e.g. ['Name', 'Namespace', 'Total IOPS'].
        """
        logger.info("Reading CephFS subvolume table column headers")
        headers = self.get_elements(self.col_headers_loc)
        return [h.text.strip() for h in headers]

    def get_cephfs_subvolume_row_count(self):
        """
        Return the number of rows currently displayed in the subvolume table.

        Returns:
            int: Row count (0 if the table is empty or not yet loaded).
        """
        rows = self.get_elements(self.table_rows_loc)
        logger.info("CephFS subvolume table row count: %d", len(rows))
        return len(rows)
