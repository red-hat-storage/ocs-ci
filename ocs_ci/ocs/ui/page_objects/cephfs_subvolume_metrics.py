import logging

from ocs_ci.ocs.ui.helpers_ui import format_locator
from ocs_ci.ocs.ui.page_objects.block_and_file import BlockAndFile
from ocs_ci.utility.utils import TimeoutSampler

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
        self.first_row_value_loc = self.validation_loc[
            "cephfs_subvolume_first_row_value"
        ]
        self.first_row_name_button_loc = self.validation_loc[
            "cephfs_subvolume_first_row_name_button"
        ]
        self.name_popover_loc = self.validation_loc["cephfs_subvolume_name_popover"]
        self.related_pods_header_loc = self.validation_loc[
            "cephfs_subvolume_related_pods_header"
        ]
        self.related_pods_links_loc = self.validation_loc[
            "cephfs_subvolume_related_pods_links"
        ]
        self.view_all_link_loc = self.validation_loc["cephfs_subvolume_view_all_link"]
        self.row_by_namespace_loc = self.validation_loc[
            "cephfs_subvolume_row_by_namespace"
        ]
        self.value_by_namespace_loc = self.validation_loc[
            "cephfs_subvolume_value_by_namespace"
        ]

    def navigate_to_cephfs_subvolume_section(self):
        """Scroll the Block and File tab to bring the CephFS subvolume card into view."""
        logger.info("Scrolling to CephFS subvolume metrics card")
        self.scroll_into_view(self.card_title_loc)

    def verify_cephfs_subvolume_section_visible(self):
        """
        Verify the card title element is present on the Block and File tab.

        Uses the scoped `card_title_loc` locator rather than a global text
        search, so the check is specific to the CephFS subvolume card element.

        Returns:
            bool: True if the card title element is found, False otherwise.
        """
        logger.info("Verifying CephFS subvolume metrics card is visible")
        self.navigate_to_cephfs_subvolume_section()
        return len(self.get_elements(self.card_title_loc)) > 0

    def get_cephfs_subvolume_metric_toggle_text(self):
        """
        Return the label currently shown on the metric dropdown toggle.

        Returns:
            str: One of 'Total IOPS', 'Total Latency', 'Total Throughput'.
        """
        logger.info("Reading active metric from CephFS subvolume dropdown")
        return self.get_element_text(self.metric_toggle_loc).strip()

    def switch_cephfs_subvolume_metric(self, metric_label, timeout=15):
        """
        Select a metric from the CephFS subvolume dropdown and wait until
        the toggle reflects the new selection before returning.

        Waiting for the toggle text to update ensures the table has begun
        re-rendering with the new metric data before callers read cell values.

        Args:
            metric_label (str): One of 'Total IOPS', 'Total Latency',
                'Total Throughput'.
            timeout (int): Maximum seconds to wait for the toggle to update.
        """
        logger.info("Switching CephFS subvolume metric to: %s", metric_label)
        self.do_click(self.metric_toggle_loc)
        self.do_click(format_locator(self.metric_option_loc, metric_label))
        self.wait_until_expected_text_is_found(
            self.metric_toggle_loc, metric_label, timeout=timeout
        )

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

    def get_cephfs_subvolume_first_row_value(self, timeout=30):
        """
        Return the metric value cell text from the first table row.

        Waits up to `timeout` seconds for the cell to be present after a
        metric switch (the table re-renders on selection change).

        Args:
            timeout (int): Maximum seconds to wait for the value cell.

        Returns:
            str: e.g. '13 IOPS' (Total IOPS), '5 ms' (Total Latency),
                '100 MBps' (Total Throughput; UI may auto-scale Bps/KBps/MBps/GBps).
        """
        self.wait_for_element_to_be_present(self.first_row_value_loc, timeout=timeout)
        return self.get_element_text(self.first_row_value_loc).strip()

    def get_cephfs_subvolume_row_count(self, timeout=30):
        """
        Return the number of rows currently displayed in the subvolume table.

        Waits up to `timeout` seconds for at least one row to appear before
        reading the count, guarding against async table-load races.

        Args:
            timeout (int): Maximum seconds to wait for the first row.

        Returns:
            int: Row count (0 if no rows appear within timeout).
        """
        self.wait_for_element_to_be_present(self.table_rows_loc, timeout=timeout)
        rows = self.get_elements(self.table_rows_loc)
        logger.info("CephFS subvolume table row count: %d", len(rows))
        return len(rows)

    def click_cephfs_subvolume_first_row_name(self):
        """
        Click the 'Show related pods' button in the Name cell of the first row.

        The Name column renders a <button aria-label='Show related pods'> (not
        an <a> tag). Clicking it opens the 'Related pods' popover.
        """
        logger.info("Clicking first-row name button to open Related pods popover")
        self.do_click(self.first_row_name_button_loc)

    def verify_cephfs_subvolume_related_pods_visible(self, timeout=10):
        """
        Verify the 'Related pods' header is present in the name popover.

        Args:
            timeout (int): Seconds to wait for the header element.

        Returns:
            bool: True if the <header> containing 'Related pods' is found.
        """
        self.wait_for_element_to_be_present(
            self.related_pods_header_loc, timeout=timeout
        )
        return len(self.get_elements(self.related_pods_header_loc)) > 0

    def get_cephfs_subvolume_related_pod_links(self):
        """
        Return the text of all pod links listed in the name popover.

        Excludes the 'View all' link; pod links sit inside
        c-popover__body > ul.c-list > li > span > a.

        Returns:
            list[str]: Pod link labels, e.g.
                ['image-registry-55757b755-cfq71', 'image-registry-55757b755-q7g6c'].
        """
        links = self.get_elements(self.related_pods_links_loc)
        return [link.text.strip() for link in links]

    def verify_namespace_in_subvolume_table(self, namespace, timeout=60):
        """
        Verify a row with the given namespace is visible in the subvolume table.

        Waits up to `timeout` seconds because a newly provisioned subvolume may
        need one or two Prometheus scrape intervals (~30 s each) to appear.

        Args:
            namespace (str): Kubernetes namespace to look for, e.g.
                'cephfs-subvolume-metrics-test'.
            timeout (int): Maximum seconds to wait for the row to appear.

        Returns:
            bool: True if at least one row with that namespace is found.
        """
        logger.info(
            "Waiting for namespace '%s' to appear in subvolume table", namespace
        )
        loc = format_locator(self.row_by_namespace_loc, namespace)
        self.wait_for_element_to_be_present(loc, timeout=timeout)
        return len(self.get_elements(loc)) > 0

    def _all_namespaces_visible(self, namespaces):
        """
        Return True if every namespace in `namespaces` has at least one row
        in the subvolume table, False otherwise.

        Args:
            namespaces (list[str]): Kubernetes namespaces to check.

        Returns:
            bool: True if all namespace rows are present.
        """
        return all(
            bool(self.get_elements(format_locator(self.row_by_namespace_loc, ns)))
            for ns in namespaces
        )

    def wait_for_namespaces_in_subvolume_table(self, namespaces, timeout=360, sleep=20):
        """
        Wait until every namespace in `namespaces` appears as a row in the
        subvolume table.

        Polls every `sleep` seconds for up to `timeout` seconds using
        :class:`~ocs_ci.utility.utils.TimeoutSampler`.

        Args:
            namespaces (list[str]): Kubernetes namespaces to wait for.
            timeout (int): Maximum seconds to wait (default 360).
            sleep (int): Seconds between polls (default 20).

        Raises:
            TimeoutExpiredError: If any namespace is not visible within timeout.
        """
        logger.info(
            "Waiting up to %ds for namespaces %s to appear in subvolume table",
            timeout,
            namespaces,
        )
        for sample in TimeoutSampler(
            timeout=timeout,
            sleep=sleep,
            func=self._all_namespaces_visible,
            namespaces=namespaces,
        ):
            if sample:
                logger.info(
                    "All %d namespaces visible in subvolume table", len(namespaces)
                )
                return

    def get_cephfs_subvolume_value_for_namespace(self, namespace, timeout=60):
        """
        Return the metric value cell text for the row matching the given namespace.

        Args:
            namespace (str): Kubernetes namespace of the target subvolume row.
            timeout (int): Maximum seconds to wait for the value cell.

        Returns:
            str: e.g. '13 IOPS', '5 ms', '100 MBps' (auto-scaled Bps family).
        """
        logger.info(
            "Reading metric value for namespace '%s' from subvolume table", namespace
        )
        loc = format_locator(self.value_by_namespace_loc, namespace)
        self.wait_for_element_to_be_present(loc, timeout=timeout)
        return self.get_element_text(loc).strip()

    def verify_cephfs_subvolume_view_all_link_visible(self, timeout=10):
        """
        Verify the 'View all' link is present at the bottom of the name popover.

        Args:
            timeout (int): Seconds to wait for the link element.

        Returns:
            bool: True if the 'View all' <a> is found within timeout.
        """
        self.wait_for_element_to_be_present(self.view_all_link_loc, timeout=timeout)
        return len(self.get_elements(self.view_all_link_loc)) > 0
