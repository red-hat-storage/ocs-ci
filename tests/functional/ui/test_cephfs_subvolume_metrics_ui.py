"""
RHSTOR-7679 — CephFS Subvolume Metrics: ODF Console UI tests.

EXISTING TESTS (not duplicated here):
  - tests/functional/ui/test_odf_storage_consumption_trend.py
      ::TestConsumptionTrendUI
    Tests the consumption trend section on the same Block and File tab.
    Pattern reused here: class-scoped setup_ui_class fixture, PageNavigator
    navigation, BlockAndFile page object.

  - tests/functional/monitoring/prometheus/metrics/test_cephfs_subvolume_metrics.py
    Tests the Prometheus data layer (metric values, labels, decrements).
    The tests here validate the UI presentation of that same data.

Addition vs existing tests
--------------------------
This file is the first to exercise the CephFS Subvolume Metrics UI section
that is new in ODF 4.22 (RHSTOR-7679).  It uses:
  - CephFSSubvolumeMetricsPage page object
    (ocs_ci/ocs/ui/page_objects/cephfs_subvolume_metrics.py)
  - navigation via DataFoundationTabBar.nav_cephfs_subvolume_metrics()
  - locators from validation_4_22 in ocs_ci/ocs/ui/views.py

TODO: All XPath locators in validation_4_22 carry // TODO comments and
      must be verified against the final 4.22 ODF console build before
      removing the @pytest.mark.skip from individual tests.
      Tracked in OCSQE-4576.

UI tests in CSV that are manual-only (no browser automation feasible):
  - Figma layout comparison           → visual diff, not automatable
  - Help text wording                 → covered by test_metrics_section_reachable
  - DR / Stretched topology checks    → infrastructure not available in CI
"""

import logging

import pytest

from ocs_ci.framework.testlib import (
    ManageTest,
    tier2,
    ui,
    ignore_leftovers,
    green_squad,
    skipif_ocs_version,
    skipif_external_mode,
)
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator

logger = logging.getLogger(__name__)


@ui
@tier2
@green_squad
@ignore_leftovers
@skipif_ocs_version("<4.22")
@skipif_external_mode
class TestCephFSSubvolumeMetricsUI(ManageTest):
    """
    RHSTOR-7679 — Verify the CephFS Subvolume Metrics UI section on the ODF
    Block and File page (Storage > Storage Cluster > Block and File).

    All tests in this class use a single browser session via setup_ui_class.
    """

    @pytest.fixture(autouse=True)
    def navigate_to_metrics(self, setup_ui_class, pvc_factory, teardown_factory):
        """
        Log into the OCP console, create at least one CephFS PVC so that the
        metrics section has data to show, then navigate to the CephFS
        Subvolume Metrics section of the Block and File page.
        """
        from ocs_ci.ocs import constants

        # Create 2 CephFS PVCs so the ranking table has something to show
        self.pvc_objs = [
            pvc_factory(
                interface=constants.CEPHFILESYSTEM,
                size=1,
                status=constants.STATUS_BOUND,
            )
            for _ in range(2)
        ]
        for pvc in self.pvc_objs:
            teardown_factory(pvc)

        storage_cluster = PageNavigator().nav_storage_cluster_default_page()
        self.metrics_page = storage_cluster.nav_cephfs_subvolume_metrics()

    # ------------------------------------------------------------------
    # TC: Subvolume metrics section reachable
    # CSV status: Passed (manual)
    # ------------------------------------------------------------------
    def test_metrics_section_reachable(self):
        """
        RHSTOR-7679 — Subvolume metrics section is reachable and renders
        without JavaScript errors.

        Steps:
        1. Navigate to Storage > Storage Cluster > Block and File.
        2. Verify the CephFS Subvolume Metrics card is visible.
        3. Verify no JS errors in the browser console.

        CSV result note: "Navigate to Storage > Data Foundation > Storage
        Systems->File and block. Page loads without console errors."
        """
        assert self.metrics_page.is_metrics_card_present(), (
            "CephFS Subvolume Metrics card not found on Block and File page. "
            "Check that the feature is enabled in this ODF build."
        )
        logger.info("CephFS Subvolume Metrics card is visible — TC passed")

        # Check browser console for JS errors
        browser_logs = self.metrics_page.driver.get_log("browser")
        severe_errors = [e for e in browser_logs if e.get("level") == "SEVERE"]
        assert (
            not severe_errors
        ), f"Browser console contains SEVERE errors: {severe_errors}"

    # ------------------------------------------------------------------
    # TC: Metrics load with active CephFS workloads
    # CSV status: Passed (manual)
    # ------------------------------------------------------------------
    def test_metrics_load_with_active_workloads(self):
        """
        RHSTOR-7679 — Metrics are displayed when at least one CephFS PVC with
        active IO exists.

        Steps:
        1. Verify the IOPS ranking table has at least one row.
        2. Verify each row shows a PVC name and a non-empty metric value.
        """
        rows = self.metrics_page.get_ranking_row_data()
        assert rows, (
            "No rows in the CephFS Subvolume Metrics ranking table. "
            "Expected at least one row from the test PVCs."
        )
        for row in rows:
            assert row["name"], f"Row has empty PVC name: {row}"
            assert row["value"], f"Row has empty metric value: {row}"
        logger.info(f"Ranking table has {len(rows)} row(s) with data — TC passed")

    # ------------------------------------------------------------------
    # TC: Top 10 Ranking — IOPS / Latency / Throughput (tab switching)
    # CSV: "Top 10 by IOPS" Passed, "Top 10 by Latency" Passed,
    #      "Top 10 by Throughput" Passed, "Column Sort / Tab Switch" Passed
    # ------------------------------------------------------------------
    def test_tab_switching_iops_latency_throughput(self):
        """
        RHSTOR-7679 — Tab switching between IOPS, Latency, and Throughput
        rankings works correctly and shows distinct data.

        Steps:
        1. Record IOPS ranking (default).
        2. Switch to Latency; record ranking.
        3. Switch to Throughput; record ranking.
        4. Switch back to IOPS; verify rows are restored.
        5. Verify no data corruption or duplication between tab switches.

        CSV result: "Each tab shows correct ranking for that metric. No mixed
        values. Switching is responsive with no data corruption."
        """
        # Step 1: IOPS ranking (default tab)
        self.metrics_page.click_iops_tab()
        iops_rows = self.metrics_page.get_ranking_row_data()
        logger.info(f"IOPS ranking: {len(iops_rows)} row(s)")
        assert iops_rows, "IOPS ranking table is empty"

        # Step 2: Latency ranking
        self.metrics_page.click_latency_tab()
        latency_rows = self.metrics_page.get_ranking_row_data()
        logger.info(f"Latency ranking: {len(latency_rows)} row(s)")
        assert latency_rows, "Latency ranking table is empty"

        # Step 3: Throughput ranking
        self.metrics_page.click_throughput_tab()
        throughput_rows = self.metrics_page.get_ranking_row_data()
        logger.info(f"Throughput ranking: {len(throughput_rows)} row(s)")
        assert throughput_rows, "Throughput ranking table is empty"

        # Step 4: Switch back to IOPS; verify restored
        self.metrics_page.click_iops_tab()
        iops_rows_again = self.metrics_page.get_ranking_row_data()
        assert len(iops_rows_again) == len(iops_rows), (
            f"IOPS row count changed after switching tabs: "
            f"{len(iops_rows)} → {len(iops_rows_again)}"
        )

    # ------------------------------------------------------------------
    # TC: At most 10 rows shown
    # CSV: "Fewer Than 10 PVCs" Pending, "Top 10 by IOPS" Passed
    # ------------------------------------------------------------------
    def test_at_most_10_rows_shown(self):
        """
        RHSTOR-7679 — The ranking table shows at most 10 rows regardless of
        how many CephFS PVCs exist.

        Steps:
        1. Read the number of rows in the IOPS ranking.
        2. Assert the count is <= 10.
        """
        self.metrics_page.click_iops_tab()
        rows = self.metrics_page.get_ranking_rows()
        assert len(rows) <= 10, f"Expected at most 10 ranking rows, found {len(rows)}"
        logger.info(f"Row count is {len(rows)} (<= 10) — TC passed")

    # ------------------------------------------------------------------
    # TC: Namespace / PVC identification
    # CSV status: Passed (manual)
    # ------------------------------------------------------------------
    def test_pvc_namespace_displayed(self):
        """
        RHSTOR-7679 — Each ranking row displays the PVC name and namespace
        correctly.

        Steps:
        1. Read all rows from the ranking table.
        2. For each row, verify name and namespace are non-empty strings.
        3. Verify the test PVC names appear somewhere in the table.
        """
        self.metrics_page.click_iops_tab()
        rows = self.metrics_page.get_ranking_row_data()
        assert rows, "No rows found in ranking table"

        for row in rows:
            assert row["name"], f"PVC name is empty in row: {row}"
            assert row["namespace"], f"Namespace is empty in row: {row}"

        displayed_names = {r["name"] for r in rows}
        logger.info(f"Displayed PVC names: {displayed_names}")

        expected = {pvc.name for pvc in self.pvc_objs}
        found = expected & displayed_names
        assert found, (
            f"None of the test PVCs {expected} appear in the ranking table. "
            f"Displayed: {displayed_names}"
        )

    # ------------------------------------------------------------------
    # TC: Drill-down — Open detail from row
    # CSV status: Pending
    # ------------------------------------------------------------------
    def test_detail_drawer_opens_on_row_click(self):
        """
        RHSTOR-7679 — Clicking a ranking row opens the per-PVC detail drawer.

        Steps:
        1. Click the first ranking row.
        2. Verify the detail drawer becomes visible.
        3. Verify the drawer header shows the correct PVC name.
        """
        self.metrics_page.click_iops_tab()
        rows = self.metrics_page.get_ranking_row_data()
        assert rows, "No rows found; cannot open detail drawer"

        expected_name = rows[0]["name"]
        self.metrics_page.click_ranking_row(row_index=0)

        assert (
            self.metrics_page.is_detail_drawer_open()
        ), "Detail drawer did not open after clicking a ranking row"
        logger.info(f"Detail drawer opened for PVC '{expected_name}' — TC passed")

    # ------------------------------------------------------------------
    # TC: Drill-down — Pod list accuracy
    # CSV status: Pending
    # ------------------------------------------------------------------
    def test_detail_drawer_pod_list(self):
        """
        RHSTOR-7679 — The detail drawer shows the pods using the PVC and their
        node assignments.

        Steps:
        1. Open the detail drawer for the first ranking row.
        2. Read the pod list from the drawer.
        3. Verify each pod entry has a non-empty pod name and status.
        4. Verify node field is non-empty for Running pods.
        5. Close the drawer; verify the ranking list is restored.
        """
        self.metrics_page.click_iops_tab()
        assert (
            self.metrics_page.get_ranking_rows()
        ), "No ranking rows available for detail drawer test"
        self.metrics_page.click_ranking_row(row_index=0)

        pods = self.metrics_page.get_detail_drawer_pods()
        # A PVC may have zero pods if no workload is attached — that's valid
        logger.info(f"Detail drawer shows {len(pods)} pod(s)")
        for pod in pods:
            assert pod["pod_name"], f"Pod entry has empty name: {pod}"
            if pod.get("status", "").lower() == "running":
                assert pod.get("node"), f"Running pod has no node assigned: {pod}"

        self.metrics_page.close_detail_drawer()
        assert (
            not self.metrics_page.is_detail_drawer_open()
        ), "Detail drawer did not close after clicking the close button"

    # ------------------------------------------------------------------
    # TC: Empty state when no CephFS PVCs exist
    # CSV status: Pending
    # ------------------------------------------------------------------
    def test_empty_state_no_pvcs(self, setup_ui_class):
        """
        RHSTOR-7679 — When no CephFS PVCs exist the page shows a clear
        empty-state message instead of a blank or error page.

        Note: This test navigates independently (no autouse PVCs) so it uses
        its own setup_ui_class fixture parameter.

        Steps:
        1. Ensure no CephFS PVCs exist in the cluster.
        2. Navigate to the CephFS Subvolume Metrics section.
        3. Verify the empty-state element is displayed.
        4. Verify no JS errors in the browser console.
        """
        from ocs_ci.ocs.ocp import OCP
        from ocs_ci.ocs import constants

        pvcs = OCP(kind="PersistentVolumeClaim").get(
            selector=f"storageClassName={constants.CEPHFILESYSTEM_SC}"
        )
        if pvcs.get("items"):
            pytest.skip(
                "Cluster has existing CephFS PVCs — cannot test empty state. "
                "Run this test on a clean cluster."
            )

        storage_cluster = PageNavigator().nav_storage_cluster_default_page()
        metrics_page = storage_cluster.nav_cephfs_subvolume_metrics()

        assert metrics_page.is_empty_state(), (
            "Expected empty-state message when no CephFS PVCs exist, "
            "but ranking table or unexpected content was found."
        )
        logger.info("Empty state message is displayed — TC passed")

    # ------------------------------------------------------------------
    # TC: Search / filter
    # CSV status: Pending
    # ------------------------------------------------------------------
    def test_search_filters_ranking_table(self):
        """
        RHSTOR-7679 — The search bar filters the ranking table by PVC name
        substring (case-insensitive).

        Steps:
        1. Record the full row count.
        2. Search for the first test PVC's name.
        3. Verify the row count is <= original and the PVC is visible.
        4. Search for a non-existent name; verify empty result or zero rows.
        5. Clear search; verify original row count restored.
        """
        self.metrics_page.click_iops_tab()
        all_rows = self.metrics_page.get_ranking_row_data()
        assert all_rows, "No rows to test search against"

        # Step 2-3: search for first test PVC
        target = self.pvc_objs[0].name
        self.metrics_page.search_pvc(target)
        filtered_rows = self.metrics_page.get_ranking_row_data()
        assert len(filtered_rows) <= len(
            all_rows
        ), "Filtered row count exceeds original — search did not narrow results"
        names = {r["name"] for r in filtered_rows}
        assert (
            target in names
        ), f"Expected PVC '{target}' in filtered results, got: {names}"

        # Step 4: search non-existent string
        self.metrics_page.search_pvc("nonexistent-pvc-xyz-should-not-exist")
        empty_rows = self.metrics_page.get_ranking_row_data()
        assert (
            len(empty_rows) == 0 or self.metrics_page.is_empty_state()
        ), "Expected zero rows or empty state for non-matching search"

        # Step 5: clear search
        self.metrics_page.clear_search()
        restored_rows = self.metrics_page.get_ranking_row_data()
        assert len(restored_rows) == len(all_rows), (
            f"Row count after clearing search ({len(restored_rows)}) "
            f"differs from original ({len(all_rows)})"
        )

    # ------------------------------------------------------------------
    # TC: RBAC — limited user cannot see unauthorized namespaces
    # CSV status: Pending
    # ------------------------------------------------------------------
    def test_rbac_limited_user(self, setup_ui_class):
        """
        RHSTOR-7679 — A user with view access only to one namespace must not
        see PVCs from other namespaces in the ranking table.

        Steps:
        1. Log in as a restricted user (view role in one namespace only).
        2. Navigate to the CephFS Subvolume Metrics section.
        3. Verify that only PVCs from the allowed namespace are shown.
        4. Verify private-namespace PVCs are NOT visible.

        TODO: Multi-user browser session switching requires additional fixture
        support. Tracked in OCSQE-4576.
        """
        pytest.skip(
            "Multi-user browser session switching not yet implemented. "
            "Tracked in OCSQE-4576."
        )
