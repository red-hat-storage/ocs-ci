"""
Tests for RHSTOR-7679: CephFS subvolume metrics on the Block and File dashboard.

Requires ODF 4.22+ (Ceph 9.0) for subvolume-level MDS metrics.
"""

import logging

from ocs_ci.framework.testlib import (
    ManageTest,
    polarion_id,
    skipif_ocs_version,
    tier1,
    tier2,
    ui,
)
from ocs_ci.helpers.cephfs_stress_helpers import create_cephfs_subvolume_workloads
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    skipif_external_mode,
    skipif_mcg_only,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import get_pods_having_label
from ocs_ci.ocs.ui.page_objects.cephfs_subvolume_metrics import (
    CephFSSubvolumeMetricsCard,
)
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator

logger = logging.getLogger(__name__)


@green_squad
@skipif_ocs_version("<4.22")
@skipif_mcg_only
@skipif_external_mode
class TestCephFSSubvolumeMetricsSectionReachable(ManageTest):
    """
    TC1 — Verify the CephFS subvolume metrics card is reachable on the
    Block and File dashboard.

    Testcase: Metrics presence and correctness - Subvolume metrics section reachable
    """

    @tier1
    @ui
    @polarion_id("OCS-8010")
    def test_cephfs_subvolume_metrics_section_reachable(self, setup_ui_class):
        """
        Navigate to Storage Cluster > Block and File, scroll to the CephFS
        subvolume metrics card, and verify it is correctly rendered per the
        design specification.

        Steps:
        1. Verify ODF console plugin pod is in Running state.
        2. Navigate to Storage > Storage Cluster > Block and File tab.
        3. Scroll to the CephFS subvolume metrics card and verify card title
           is visible.
        4. Verify the metric dropdown default is 'Total IOPS'.
        5. Verify the help (?) button is present and its popover contains
           the expected text.
        6. Verify table column headers are Name, Namespace, and Total IOPS.
        7. Verify the table has at least one row.
        """
        logger.test_step("Verify ODF console plugin pod is in Running state")
        odf_console_pods = get_pods_having_label(
            label=constants.ODF_CONSOLE,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        assert odf_console_pods, "ODF console plugin pod not found in openshift-storage"
        for pod in odf_console_pods:
            phase = pod.get("status", {}).get("phase")
            assert phase == constants.STATUS_RUNNING, (
                f"ODF console pod {pod['metadata']['name']} is not Running "
                f"(phase={phase})"
            )

        logger.test_step("Navigate to Storage Cluster > Block and File tab")
        storage_cluster_page = PageNavigator().nav_storage_cluster_default_page()
        storage_cluster_page.validate_block_and_file_tab_active()

        subvolume_metrics_card = CephFSSubvolumeMetricsCard()

        logger.test_step(
            "Scroll to CephFS subvolume metrics card and verify card title is visible"
        )
        assert (
            subvolume_metrics_card.verify_cephfs_subvolume_section_visible()
        ), "CephFS subvolume metrics card title not found on Block and File tab"

        logger.test_step(
            f"Verify metric dropdown default is '{constants.CEPHFS_SUBVOLUME_DEFAULT_METRIC}'"
        )
        toggle_text = subvolume_metrics_card.get_cephfs_subvolume_metric_toggle_text()
        assert toggle_text == constants.CEPHFS_SUBVOLUME_DEFAULT_METRIC, (
            f"Expected default metric '{constants.CEPHFS_SUBVOLUME_DEFAULT_METRIC}', "
            f"got '{toggle_text}'"
        )

        logger.test_step(
            "Verify help (?) button popover contains expected description text"
        )
        subvolume_metrics_card.click_cephfs_subvolume_help_button()
        assert subvolume_metrics_card.verify_cephfs_subvolume_popover_text(
            constants.CEPHFS_SUBVOLUME_POPOVER_TEXT
        ), f"Popover did not contain: '{constants.CEPHFS_SUBVOLUME_POPOVER_TEXT}'"

        expected_col_headers = [
            "Name",
            "Namespace",
            constants.CEPHFS_SUBVOLUME_DEFAULT_METRIC,
        ]
        logger.test_step(f"Verify table column headers are {expected_col_headers}")
        col_headers = subvolume_metrics_card.get_cephfs_subvolume_column_headers()
        assert (
            col_headers == expected_col_headers
        ), f"Unexpected column headers: {col_headers}"

        logger.test_step("Verify table has at least one subvolume row")
        row_count = subvolume_metrics_card.get_cephfs_subvolume_row_count()
        assert (
            row_count > 0
        ), "CephFS subvolume table has no rows; expected at least one subvolume"


@green_squad
@skipif_ocs_version("<4.22")
@skipif_mcg_only
@skipif_external_mode
class TestCephFSSubvolumeMetricsLoadWithActiveWorkload(ManageTest):
    """
    TC2 — Verify CephFS subvolume metrics populate with non-zero values
    when a CephFS workload is active.

    Testcase: Metrics presence and correctness - Metrics Load with Active
    CephFS Workloads
    """

    @tier2
    @ui
    @polarion_id("OCS-8023")
    def test_cephfs_subvolume_metrics_load_with_active_workload(
        self, setup_ui_class, teardown_project_factory
    ):
        """
        Create a CephFS PVC with an IO-generating pod, wait for Prometheus
        to scrape subvolume metrics, then verify the metrics card shows the
        test namespace row with values for all three metric types.

        Note: 'Total Throughput' tracks MDS-level byte flow. FIO data writes
        bypass MDS and go directly to OSD, so throughput may legitimately
        read 0 Bps with an FIO workload. For Throughput the test verifies
        the value is correctly formatted (has a Bps unit suffix) rather than
        asserting > 0.

        Steps:
        1. Create 3 test namespaces, each with a CephFS PVC and FIO running
           at 100 MB/s so all 3 subvolumes appear in the top-10 list.
        2. Navigate to Storage Cluster > Block and File tab.
        3. Poll until all 3 test namespace rows appear (max 6 minutes).
        4. Verify the subvolume card is visible.
        5. Switch to 'Total IOPS': verify column header and that all 3 test
           namespaces appear with non-zero values.
        6. Switch to 'Total Latency': verify column header and non-zero
           values for all 3 test namespace rows.
        7. Switch to 'Total Throughput': verify column header and that
           values carry a Bps unit suffix (Bps/KBps/MBps/GBps).
        """
        logger.test_step(
            "Create 3 CephFS subvolume workloads (namespace + PVC + FIO each)"
        )
        workloads = create_cephfs_subvolume_workloads(
            count=3, teardown_project_factory=teardown_project_factory
        )
        namespaces = [project_obj.namespace for project_obj, _, _ in workloads]

        logger.test_step("Navigate to Storage Cluster > Block and File tab")
        storage_cluster_page = PageNavigator().nav_storage_cluster_default_page()
        storage_cluster_page.validate_block_and_file_tab_active()

        subvolume_metrics_card = CephFSSubvolumeMetricsCard()

        logger.test_step("Verify CephFS subvolume metrics card is visible")
        assert (
            subvolume_metrics_card.verify_cephfs_subvolume_section_visible()
        ), "CephFS subvolume metrics card not visible after IO workload"

        logger.test_step("Wait until subvolume rows are visible (max 6 minutes)")
        subvolume_metrics_card.wait_for_namespaces_in_subvolume_table(namespaces)

        is_throughput_metric = {
            constants.CEPHFS_SUBVOLUME_METRIC_THROUGHPUT,
        }
        for metric in [
            constants.CEPHFS_SUBVOLUME_DEFAULT_METRIC,
            constants.CEPHFS_SUBVOLUME_METRIC_LATENCY,
            constants.CEPHFS_SUBVOLUME_METRIC_THROUGHPUT,
        ]:
            logger.test_step(
                f"Switch to '{metric}' and verify all 3 test namespace rows"
            )
            subvolume_metrics_card.switch_cephfs_subvolume_metric(metric)
            col_headers = subvolume_metrics_card.get_cephfs_subvolume_column_headers()
            assert col_headers[-1] == metric, (
                f"Column header after switching to '{metric}' is "
                f"'{col_headers[-1]}'"
            )
            for namespace in namespaces:
                assert subvolume_metrics_card.verify_namespace_in_subvolume_table(
                    namespace
                ), (
                    f"Namespace '{namespace}' not found in subvolume table "
                    f"for metric '{metric}'"
                )
                value = subvolume_metrics_card.get_cephfs_subvolume_value_for_namespace(
                    namespace
                )
                logger.info("Metric '%s', namespace '%s': %s", metric, namespace, value)
                assert value, (
                    f"Metric value is empty for namespace '{namespace}', "
                    f"metric '{metric}'"
                )
                if metric in is_throughput_metric:
                    # FIO data writes bypass MDS and go directly to OSD, so
                    # MDS-tracked throughput may legitimately be 0 Bps.
                    # Verify format only (value carries a Bps unit suffix).
                    assert "Bps" in value, (
                        f"Throughput value '{value}' for namespace "
                        f"'{namespace}' missing Bps unit suffix"
                    )
                else:
                    numeric = value.replace(",", "").split()[0]
                    assert float(numeric) > 0, (
                        f"Metric '{metric}' is zero for namespace "
                        f"'{namespace}': '{value}'"
                    )


@green_squad
@skipif_ocs_version("<4.22")
@skipif_mcg_only
@skipif_external_mode
class TestCephFSSubvolumeMetricUnitsAndLabels(ManageTest):
    """
    TC3 — Verify the CephFS subvolume metrics card uses human-friendly
    labels and correct unit suffixes for each metric type.

    Testcase: Metrics presence and correctness - Metric Units and Labels
    """

    @tier2
    @ui
    @polarion_id("OCS-8025")
    def test_cephfs_subvolume_metric_units_and_labels(self, setup_ui_class):
        """
        Switch through all three metric options and confirm column headers
        use human-friendly labels and displayed values carry the expected
        unit suffix per the design specification (no raw Prometheus names).

        Steps:
        1. Navigate to Storage Cluster > Block and File tab.
        2. Switch to 'Total IOPS': verify column header and that the value
           carries the 'IOPS' unit suffix.
        3. Switch to 'Total Latency': verify column header and that the
           value contains 'ms'.
        4. Switch to 'Total Throughput': verify column header and that the
           value contains 'Bps' (the console auto-scales: Bps / KBps /
           MBps / GBps depending on the current throughput level).
        """
        logger.test_step("Navigate to Storage Cluster > Block and File tab")
        storage_cluster_page = PageNavigator().nav_storage_cluster_default_page()
        storage_cluster_page.validate_block_and_file_tab_active()

        subvolume_metrics_card = CephFSSubvolumeMetricsCard()
        subvolume_metrics_card.navigate_to_cephfs_subvolume_section()

        metrics_and_units = [
            (constants.CEPHFS_SUBVOLUME_DEFAULT_METRIC, "IOPS"),
            (constants.CEPHFS_SUBVOLUME_METRIC_LATENCY, "ms"),
            # "Bps" appears in all console-scaled variants: Bps, KBps, MBps, GBps.
            (constants.CEPHFS_SUBVOLUME_METRIC_THROUGHPUT, "Bps"),
        ]
        for metric, expected_unit in metrics_and_units:
            logger.test_step(f"Switch to '{metric}' and verify label and unit format")
            subvolume_metrics_card.switch_cephfs_subvolume_metric(metric)
            col_headers = subvolume_metrics_card.get_cephfs_subvolume_column_headers()
            assert (
                col_headers[-1] == metric
            ), f"Expected column header '{metric}', got '{col_headers[-1]}'"

            row_count = subvolume_metrics_card.get_cephfs_subvolume_row_count(
                timeout=30
            )
            assert row_count > 0, (
                f"No rows available for metric '{metric}';"
                " cannot validate unit suffix"
            )

            first_value = subvolume_metrics_card.get_cephfs_subvolume_first_row_value()
            logger.info(
                "Metric '%s': column='%s', sample value='%s'",
                metric,
                col_headers[-1],
                first_value,
            )
            assert expected_unit in first_value, (
                f"Expected unit '{expected_unit}' in value "
                f"'{first_value}' for metric '{metric}'"
            )
