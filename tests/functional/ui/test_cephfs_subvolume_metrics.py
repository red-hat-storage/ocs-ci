"""
Tests for RHSTOR-7679: CephFS subvolume metrics on the Block and File dashboard.

Requires ODF 4.22+ (Ceph 9.0) for subvolume-level MDS metrics.
"""

import logging

from ocs_ci.framework.testlib import (
    ManageTest,
    skipif_ocs_version,
    tier1,
    ui,
)
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import get_pods_having_label
from ocs_ci.ocs.ui.page_objects.cephfs_subvolume_metrics import (
    CephFSSubvolumeMetricsCard,
)
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator

logger = logging.getLogger(__name__)


@green_squad
@skipif_ocs_version("<4.22")
class TestCephFSSubvolumeMetricsSectionReachable(ManageTest):
    """
    TC1 — Verify the CephFS subvolume metrics card is reachable on the
    Block and File dashboard.

    Testcase: Metrics presence and correctness - Subvolume metrics section reachable
    """

    @tier1
    @ui
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
