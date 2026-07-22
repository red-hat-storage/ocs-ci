import logging
import math
import re

from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.ui.helpers_ui import format_locator
from ocs_ci.ocs.ui.page_objects.block_and_file import BlockAndFile
from ocs_ci.utility.prometheus import PrometheusAPI
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


_PROMQL_TEMPLATE = (
    "sort_desc("
    "sum by (pv_name,pvc_namespace,subvolume)"
    "(odf_cephfs_subvolume_read_{m}_with_pv"
    " or odf_cephfs_subvolume_write_{m}_with_pv))"
)

METRIC_PROMQL = {
    constants.CEPHFS_SUBVOLUME_DEFAULT_METRIC: (_PROMQL_TEMPLATE.format(m="iops")),
    constants.CEPHFS_SUBVOLUME_METRIC_LATENCY: (
        _PROMQL_TEMPLATE.format(m="latency_msec")
    ),
    constants.CEPHFS_SUBVOLUME_METRIC_THROUGHPUT: (
        _PROMQL_TEMPLATE.format(m="throughput_bps")
    ),
}


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
        self.all_row_values_loc = self.validation_loc["cephfs_subvolume_all_row_values"]

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
        the toggle and column header both reflect the new selection.

        Waiting for the column header ensures the table has fully
        re-rendered with the new metric data before callers read values.

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
        self._wait_for_metric_column_header(metric_label, timeout=timeout)

    def _wait_for_metric_column_header(self, metric_label, timeout=30, sleep=3):
        """
        Poll until the last column header matches ``metric_label``.

        Args:
            metric_label (str): Expected header text.
            timeout (int): Maximum seconds to wait.
            sleep (int): Seconds between polls.
        """
        for headers in TimeoutSampler(
            timeout=timeout,
            sleep=sleep,
            func=self.get_cephfs_subvolume_column_headers,
        ):
            if headers and headers[-1] == metric_label:
                return

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
        metric = self.get_cephfs_subvolume_metric_toggle_text()
        loc = format_locator(self.first_row_value_loc, metric)
        self.wait_for_element_to_be_present(loc, timeout=timeout)
        return self.get_element_text(loc).strip()

    def get_cephfs_subvolume_all_row_values(self, expected_count=1, timeout=30):
        """
        Return the metric value text from every row in the subvolume table.

        Reads the currently active metric from the dropdown toggle and
        collects the corresponding ``td[@data-label]`` cell text for each
        table row.

        Args:
            expected_count (int): Minimum number of value cells to wait
                for before reading (default 1). Guards against reading
                a partially rendered table after a metric switch.
            timeout (int): Maximum seconds to wait for the value cells.

        Returns:
            list[str]: Metric value strings, e.g.
                ``['13 IOPS', '7 IOPS', '2 IOPS']``.
        """
        metric = self.get_cephfs_subvolume_metric_toggle_text()
        loc = format_locator(self.all_row_values_loc, metric)
        for elements in TimeoutSampler(
            timeout=timeout,
            sleep=2,
            func=self.get_elements,
            locator=loc,
        ):
            if len(elements) >= expected_count:
                break
        elements = self.get_elements(loc)
        values = [el.text.strip() for el in elements]
        logger.info("All row values for metric '%s': %s", metric, values)
        return values

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

    def wait_for_row_count(self, expected_count, timeout=360, sleep=20):
        """
        Wait until the subvolume table displays at least ``expected_count`` rows.

        Args:
            expected_count (int): Target row count.
            timeout (int): Maximum seconds to wait (default 360).
            sleep (int): Seconds between polls (default 20).

        Raises:
            TimeoutExpiredError: If the row count does not reach
                ``expected_count`` within ``timeout``.
        """
        logger.info(
            "Waiting up to %ds for subvolume table to show %d rows",
            timeout,
            expected_count,
        )
        for sample in TimeoutSampler(
            timeout=timeout,
            sleep=sleep,
            func=self.get_cephfs_subvolume_row_count,
        ):
            logger.info("Current row count: %d / %d", sample, expected_count)
            if sample >= expected_count:
                return

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
        metric = self.get_cephfs_subvolume_metric_toggle_text()
        loc = format_locator(self.value_by_namespace_loc, namespace, metric)
        self.wait_for_element_to_be_present(loc, timeout=timeout)
        return self.get_element_text(loc).strip()

    def wait_for_valid_metric_value_for_namespace(
        self, namespace, metric, timeout=120, sleep=30
    ):
        """
        Return True if a valid metric value for namespace is found.

        The validation strategy depends on the metric:

        * **Total Throughput**: FIO data writes bypass MDS and go directly to
          OSD, so MDS-tracked throughput may legitimately be 0 Bps.  The value
          is read once and considered valid if it is non-empty and contains a
          Bps unit suffix (Bps / KBps / MBps / GBps).

        * **Other metrics (IOPS, Latency)**: the ODF console refreshes
          Prometheus-backed metrics every ~30 s, so a single read can
          transiently return 0 between scrape windows even when FIO is active.
          This method polls every `sleep` seconds for up to `timeout` seconds
          until the numeric part of the value is > 0.

        Args:
            namespace (str): namespace whose metric value to check.
            metric (str): active metric label (e.g.
                ``constants.CEPHFS_SUBVOLUME_DEFAULT_METRIC``).
            timeout (int): maximum seconds to poll for a non-zero value
                (non-throughput metrics only; default 120).
            sleep (int): seconds between polls
                (non-throughput metrics only; default 30).

        Returns:
            bool: True if a valid value was found within the allowed time.
        """
        if metric == constants.CEPHFS_SUBVOLUME_METRIC_THROUGHPUT:
            value = self.get_cephfs_subvolume_value_for_namespace(namespace)
            return bool(value) and "Bps" in value
        try:
            for value in TimeoutSampler(
                timeout=timeout,
                sleep=sleep,
                func=self.get_cephfs_subvolume_value_for_namespace,
                namespace=namespace,
            ):
                if value:
                    token = value.replace(",", "").split()[0]
                    numeric = re.match(r"^\d+(?:\.\d+)?", token)
                    if numeric and float(numeric.group(0)) > 0:
                        return True
        except TimeoutExpiredError:
            pass
        return False

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

    @staticmethod
    def _parse_ui_metric_value(value_str):
        """
        Parse a UI metric string to a raw float.

        Handles comma-separated thousands and SI-prefixed throughput
        units (KBps, MBps, GBps).

        Args:
            value_str (str): e.g. '13 IOPS', '12,000 ms',
                '4.87 KBps', '0 Bps'.

        Returns:
            float: Raw numeric value in base units.
        """
        multipliers = {
            "ms": 1e-3,
            "KBps": 1e3,
            "MBps": 1e6,
            "GBps": 1e9,
        }
        cleaned = value_str.replace(",", "").strip()
        parts = cleaned.split()
        number = float(parts[0])
        if len(parts) > 1:
            unit = parts[1]
            number *= multipliers.get(unit, 1)
        return number

    @staticmethod
    def _get_prometheus_top_values(metric, threading_lock):
        """
        Query Prometheus and return the top N values sorted descending.

        Args:
            metric (str): Metric label used to look up the PromQL
                expression in ``METRIC_PROMQL``.
            threading_lock: Threading lock for PrometheusAPI.

        Returns:
            list[float]: Top values, capped at
                ``constants.CEPHFS_SUBVOLUME_MAX_TOP_10_ROWS``.
        """
        promql = METRIC_PROMQL[metric]
        api = PrometheusAPI(threading_lock=threading_lock)
        results = api.query(promql)
        values = sorted(
            [float(r["value"][1]) for r in results],
            reverse=True,
        )
        return values[: constants.CEPHFS_SUBVOLUME_MAX_TOP_10_ROWS]

    def _wait_for_nonzero_prometheus_values(
        self,
        metric,
        threading_lock,
        expected_count,
        timeout=360,
        sleep=30,
    ):
        """
        Poll Prometheus until all top values for ``metric`` are > 0.

        Args:
            metric (str): Metric label.
            threading_lock: Threading lock for PrometheusAPI.
            expected_count (int): Number of values expected.
            timeout (int): Maximum seconds to wait.
            sleep (int): Seconds between polls.

        Returns:
            list[float]: Prometheus values once all are non-zero.

        Raises:
            TimeoutExpiredError: If values do not become non-zero
                within ``timeout``.
        """
        result = None
        for values in TimeoutSampler(
            timeout=timeout,
            sleep=sleep,
            func=self._get_prometheus_top_values,
            metric=metric,
            threading_lock=threading_lock,
        ):
            if len(values) >= expected_count and all(
                v > 0 for v in values[:expected_count]
            ):
                result = values[:expected_count]
                break
        return result

    def verify_ui_values_match_prometheus(self, metric, ui_values, threading_lock):
        """
        Compare UI table values against Prometheus for the given metric.

        Polls Prometheus until all values are > 0, then re-reads the
        UI table so both snapshots are fresh before comparing.

        Args:
            metric (str): Active metric label, e.g.
                ``constants.CEPHFS_SUBVOLUME_DEFAULT_METRIC``.
            ui_values (list[str]): Values collected from the UI table,
                e.g. ``['13 IOPS', '7 IOPS']``.
            threading_lock: Session-scoped threading lock for
                PrometheusAPI.

        Raises:
            AssertionError: If a UI value does not match its
                Prometheus counterpart within the allowed tolerance.
        """
        prom_values = self._wait_for_nonzero_prometheus_values(
            metric,
            threading_lock,
            expected_count=len(ui_values),
        )

        logger.info("Prometheus values for '%s': %s", metric, prom_values)

        ui_values = self.get_cephfs_subvolume_all_row_values(
            expected_count=len(prom_values),
        )

        assert len(ui_values) == len(prom_values), (
            f"Row count mismatch for '{metric}': "
            f"UI has {len(ui_values)}, "
            f"Prometheus has {len(prom_values)}"
        )

        for idx, (ui_str, prom_val) in enumerate(zip(ui_values, prom_values)):
            ui_val = self._parse_ui_metric_value(ui_str)
            assert math.isclose(ui_val, prom_val, rel_tol=0.05, abs_tol=1.0), (
                f"Row {idx} '{metric}' mismatch: "
                f"UI={ui_str} ({ui_val}), Prometheus={prom_val}"
            )
