"""
Tests for RHSTOR-7679: CephFS subvolume metrics on the Block and File dashboard.

Requires ODF 4.22+ (Ceph 9.0) for subvolume-level MDS metrics.
"""

import logging

import pytest

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
    runs_on_provider,
    skipif_external_mode,
    skipif_mcg_only,
)
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.resources.pod import get_pods_having_label
from ocs_ci.helpers.helpers import (
    create_pod,
    create_project,
    create_pvc,
    wait_for_resource_state,
)
from ocs_ci.ocs.ui.page_objects.cephfs_subvolume_metrics import (
    CephFSSubvolumeMetricsCard,
)
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator

logger = logging.getLogger(__name__)


@green_squad
@runs_on_provider
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
@runs_on_provider
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
           values for all 3 test namespace rows (polls up to 2 minutes
           to tolerate transient zero readings between Prometheus scrapes).
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
                assert subvolume_metrics_card.wait_for_valid_metric_value_for_namespace(
                    namespace, metric
                ), (
                    f"Metric '{metric}' did not show a valid value "
                    f"for namespace '{namespace}'"
                )
                value = subvolume_metrics_card.get_cephfs_subvolume_value_for_namespace(
                    namespace
                )
                logger.info("Metric '%s', namespace '%s': %s", metric, namespace, value)


@green_squad
@runs_on_provider
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


@green_squad
@runs_on_provider
@skipif_ocs_version("<4.22")
@skipif_mcg_only
@skipif_external_mode
class TestCephFSSubvolumeTop10Ranking(ManageTest):
    """
    TC — Verify the CephFS subvolume metrics card shows at most 10
    subvolume rows and each row carries a correctly-formatted metric
    value when more than 10 CephFS PVCs exist.

    Testcase: Top 10 Ranking — IOPS, Latency, Throughput
    """

    @pytest.fixture(autouse=True, scope="class")
    def setup_top10_workloads(self, request, setup_ui_class):
        """
        Create 12 CephFS subvolume workloads once for the class, navigate
        to the Block and File tab, and wait until the table is fully
        populated (shows exactly 10 rows, the UI cap).
        """
        logger.test_step(
            "Create %d CephFS subvolume workloads",
            constants.CEPHFS_SUBVOLUME_TOP_10_WORKLOAD_COUNT,
        )
        workloads = create_cephfs_subvolume_workloads(
            count=constants.CEPHFS_SUBVOLUME_TOP_10_WORKLOAD_COUNT,
            project_name_prefix="cephfs-top10-test",
            pvc_size="1Gi",
            fio_size="1GB",
            fio_runtime=900,
        )
        projects = [project_obj for project_obj, _, _ in workloads]

        def finalizer():
            for project_obj in projects:
                try:
                    logger.info("Deleting project %s", project_obj.namespace)
                    project_obj.delete(resource_name=project_obj.namespace)
                    project_obj.wait_for_delete(project_obj.namespace, timeout=180)
                except (CommandFailed, Exception):
                    logger.warning(
                        "Failed to delete project %s",
                        project_obj.namespace,
                        exc_info=True,
                    )

        request.addfinalizer(finalizer)

        logger.test_step("Navigate to Storage Cluster > Block and File tab")
        storage_cluster_page = PageNavigator().nav_storage_cluster_default_page()
        storage_cluster_page.validate_block_and_file_tab_active()

        subvolume_metrics_card = CephFSSubvolumeMetricsCard()

        logger.test_step("Verify CephFS subvolume metrics card is visible")
        assert (
            subvolume_metrics_card.verify_cephfs_subvolume_section_visible()
        ), "CephFS subvolume metrics card not visible"

        logger.test_step(
            "Wait for table to show %d rows (max 6 minutes)",
            constants.CEPHFS_SUBVOLUME_MAX_TOP_10_ROWS,
        )
        subvolume_metrics_card.wait_for_row_count(
            expected_count=constants.CEPHFS_SUBVOLUME_MAX_TOP_10_ROWS,
            timeout=360,
            sleep=20,
        )

    @tier2
    @ui
    @pytest.mark.parametrize(
        argnames=["metric"],
        argvalues=[
            pytest.param(
                constants.CEPHFS_SUBVOLUME_DEFAULT_METRIC,
                marks=pytest.mark.polarion_id("OCS-8053"),
            ),
            pytest.param(
                constants.CEPHFS_SUBVOLUME_METRIC_LATENCY,
                marks=pytest.mark.polarion_id("OCS-8054"),
            ),
            pytest.param(
                constants.CEPHFS_SUBVOLUME_METRIC_THROUGHPUT,
                marks=pytest.mark.polarion_id("OCS-8055"),
            ),
        ],
    )
    def test_cephfs_subvolume_top_10_ranking(self, metric, threading_lock):
        """
        Switch to the given metric and verify the table shows at most
        10 rows with correctly-formatted values that match Prometheus.

        Steps (class-scoped setup, runs once for all parametrized cases):
        1. Create 12 CephFS subvolume workloads (namespace + PVC + FIO
           each) so the cluster has more subvolumes than the UI
           cap of 10 rows.
        2. Navigate to Storage Cluster > Block and File tab.
        3. Verify CephFS subvolume metrics card is visible.
        4. Wait for the table to show exactly 10 rows (max 6 minutes).

        Steps (per parametrized metric):
        5. Switch to the parametrized metric (Total IOPS / Total Latency
           / Total Throughput).
        6. Verify at most 10 rows are displayed.
        7. Verify every displayed row carries a non-empty value with the
           expected unit suffix for the selected metric.
        8. Query Prometheus for the same metric and verify UI values
           match CLI values (positional comparison, sorted descending).
        """
        subvolume_metrics_card = CephFSSubvolumeMetricsCard()

        logger.test_step("Switch to metric '%s'", metric)
        subvolume_metrics_card.switch_cephfs_subvolume_metric(metric)

        row_count = subvolume_metrics_card.get_cephfs_subvolume_row_count(timeout=60)
        assert row_count > 0, f"No subvolume rows visible for metric '{metric}'"

        logger.test_step(
            "Verify at most %d rows are displayed " "(cluster has %d subvolumes)",
            constants.CEPHFS_SUBVOLUME_MAX_TOP_10_ROWS,
            constants.CEPHFS_SUBVOLUME_TOP_10_WORKLOAD_COUNT,
        )
        assert row_count <= constants.CEPHFS_SUBVOLUME_MAX_TOP_10_ROWS, (
            f"Expected at most "
            f"{constants.CEPHFS_SUBVOLUME_MAX_TOP_10_ROWS} rows, "
            f"got {row_count}"
        )
        logger.info(
            "Row count: %d (max %d)",
            row_count,
            constants.CEPHFS_SUBVOLUME_MAX_TOP_10_ROWS,
        )

        expected_unit = constants.CEPHFS_SUBVOLUME_METRIC_EXPECTED_UNITS[metric]
        logger.test_step(
            "Verify all %d rows carry unit '%s'",
            row_count,
            expected_unit,
        )
        all_values = subvolume_metrics_card.get_cephfs_subvolume_all_row_values(
            expected_count=row_count,
        )
        for idx, value in enumerate(all_values):
            assert value, f"Row {idx} has an empty value for metric '{metric}'"
            assert expected_unit in value, (
                f"Row {idx} value '{value}' does not contain "
                f"expected unit '{expected_unit}' "
                f"for metric '{metric}'"
            )

        logger.test_step(
            "Verify UI values match Prometheus for metric '%s'",
            metric,
        )
        subvolume_metrics_card.verify_ui_values_match_prometheus(
            metric=metric,
            ui_values=all_values,
            threading_lock=threading_lock,
        )


@green_squad
@runs_on_provider
@skipif_ocs_version("<4.22")
@skipif_mcg_only
@skipif_external_mode
class TestCephFSSubvolumeDrillDown(ManageTest):
    """
    Drill-down tests for the CephFS subvolume metrics card: clicking a
    row to open the Related pods popover, verifying pod list accuracy,
    node information, multiple-pod scenarios, and detail metrics.
    """

    @pytest.fixture(autouse=True, scope="class")
    def setup(self, request, setup_ui_class):
        """
        Create CephFS workloads for drill-down testing:
        - 1 workload with a single pod
        - 1 RWX workload with 3 pods mounting the same PVC
        Navigate to Block and File tab and wait for rows.
        """
        all_projects = []

        def finalizer():
            for project_obj in all_projects:
                try:
                    logger.info(
                        "Deleting project %s",
                        project_obj.namespace,
                    )
                    project_obj.delete(resource_name=project_obj.namespace)
                    project_obj.wait_for_delete(project_obj.namespace, timeout=180)
                except (CommandFailed, Exception):
                    logger.warning(
                        "Failed to delete project %s",
                        project_obj.namespace,
                        exc_info=True,
                    )

        request.addfinalizer(finalizer)

        logger.test_step("Create single-pod CephFS workload for drill-down tests")
        single_project = create_project()
        all_projects.append(single_project)
        single_pvc = create_pvc(
            sc_name=constants.CEPHFILESYSTEM_SC,
            namespace=single_project.namespace,
            size="1Gi",
            access_mode=constants.ACCESS_MODE_RWX,
        )
        single_pod = create_pod(
            pvc_name=single_pvc.name,
            namespace=single_project.namespace,
            interface_type=constants.CEPHFILESYSTEM,
        )
        wait_for_resource_state(
            single_pod,
            state=constants.STATUS_RUNNING,
            timeout=300,
        )
        single_pod.run_io(
            storage_type=constants.WORKLOAD_STORAGE_TYPE_FS,
            size="1GB",
            rate="100m",
            runtime=900,
        )

        logger.test_step("Create multi-pod CephFS workload (3 pods, 1 RWX PVC)")
        multi_project = create_project()
        all_projects.append(multi_project)
        multi_pvc = create_pvc(
            sc_name=constants.CEPHFILESYSTEM_SC,
            namespace=multi_project.namespace,
            size="1Gi",
            access_mode=constants.ACCESS_MODE_RWX,
        )
        multi_pods = []
        for i in range(3):
            pod_obj = create_pod(
                pvc_name=multi_pvc.name,
                namespace=multi_project.namespace,
                interface_type=constants.CEPHFILESYSTEM,
                pod_name=f"cephfs-multi-pod-{i}",
            )
            wait_for_resource_state(
                pod_obj,
                state=constants.STATUS_RUNNING,
                timeout=300,
            )
            pod_obj.run_io(
                storage_type=constants.WORKLOAD_STORAGE_TYPE_FS,
                size="1GB",
                rate="100m",
                runtime=900,
            )
            multi_pods.append(pod_obj)

        request.cls.single_project = single_project
        request.cls.multi_project = multi_project
        request.cls.multi_pods = multi_pods

        logger.test_step("Navigate to Storage Cluster > Block and File tab")
        storage_cluster_page = PageNavigator().nav_storage_cluster_default_page()
        storage_cluster_page.validate_block_and_file_tab_active()

        subvolume_metrics_card = CephFSSubvolumeMetricsCard()
        assert (
            subvolume_metrics_card.verify_cephfs_subvolume_section_visible()
        ), "CephFS subvolume metrics card not visible"

        logger.test_step("Wait for workload namespaces to appear")
        subvolume_metrics_card.wait_for_namespaces_in_subvolume_table(
            [single_project.namespace, multi_project.namespace]
        )

    @tier2
    @ui
    def test_pod_list_accuracy(self):
        """
        Verify pod names in the Related pods popover match pods
        using the CephFS PVC as reported by ``oc get pods``.

        Steps:
        1. Query pods in the single-pod namespace via CLI.
        2. Click the namespace row name button.
        3. Read pod links from the popover.
        4. Verify the CLI pod names match the popover list exactly
           (no missing and no extra/phantom pods).
        """
        subvolume_metrics_card = CephFSSubvolumeMetricsCard()
        ns = self.single_project.namespace

        logger.test_step("Query pods in namespace '%s' via CLI", ns)
        pod_ocp = ocp.OCP(kind=constants.POD, namespace=ns)
        pod_list = pod_ocp.get()
        cli_pod_names = [
            item["metadata"]["name"]
            for item in pod_list.get("items", [])
            if item.get("status", {}).get("phase") == constants.STATUS_RUNNING
        ]
        assert cli_pod_names, f"No running pods found in namespace '{ns}'"

        logger.test_step("Click namespace '%s' row name button", ns)
        subvolume_metrics_card.navigate_to_cephfs_subvolume_section()
        subvolume_metrics_card.verify_namespace_in_subvolume_table(ns)
        subvolume_metrics_card.click_cephfs_subvolume_row_name_by_namespace(ns)
        assert (
            subvolume_metrics_card.verify_cephfs_subvolume_related_pods_visible()
        ), "Related pods popover not visible"

        popover_pods = subvolume_metrics_card.get_cephfs_subvolume_related_pod_links()

        logger.test_step("Verify CLI pod names appear in popover list")
        for cli_pod in cli_pod_names:
            assert any(cli_pod in link for link in popover_pods), (
                f"Pod '{cli_pod}' from CLI not found in "
                f"popover links: {popover_pods}"
            )

        logger.test_step("Verify no extra pods in popover")
        assert len(popover_pods) == len(cli_pod_names), (
            f"Pod count mismatch: CLI has {len(cli_pod_names)} "
            f"pods {cli_pod_names}, popover has "
            f"{len(popover_pods)} pods {popover_pods}"
        )

    @tier2
    @ui
    def test_multiple_pods_one_pvc(self):
        """
        Verify the Related pods popover lists all 3 pods when an RWX
        CephFS PVC is mounted by multiple pods.

        Steps:
        1. Navigate to the multi-pod namespace row.
        2. Open the Related pods popover (or View all link).
        3. Verify all 3 pod names are present.
        """
        subvolume_metrics_card = CephFSSubvolumeMetricsCard()
        ns = self.multi_project.namespace

        logger.test_step("Navigate to multi-pod namespace '%s' row", ns)
        subvolume_metrics_card.navigate_to_cephfs_subvolume_section()
        assert subvolume_metrics_card.verify_namespace_in_subvolume_table(
            ns
        ), f"Namespace '{ns}' not found in subvolume table"

        logger.test_step("Open Related pods popover for namespace '%s'", ns)
        subvolume_metrics_card.click_cephfs_subvolume_row_name_by_namespace(ns)
        assert (
            subvolume_metrics_card.verify_cephfs_subvolume_related_pods_visible()
        ), "Related pods popover not visible"

        popover_pods = subvolume_metrics_card.get_cephfs_subvolume_related_pod_links()

        logger.test_step("Verify all 3 multi-pods are listed")
        expected_pod_names = [p.name for p in self.multi_pods]
        for pod_name in expected_pod_names:
            assert any(pod_name in link for link in popover_pods), (
                f"Pod '{pod_name}' not found in popover links: " f"{popover_pods}"
            )

    @tier2
    @ui
    def test_metrics_on_detail_view(self, threading_lock):
        """
        Verify the metric values shown for the single-pod namespace
        are valid for all three metrics and consistent with Prometheus.

        Steps:
        1. For each metric (Total IOPS, Total Latency, Total
           Throughput), switch the dropdown and read the value for
           the single-pod namespace from the table.
        2. Verify the value is non-empty and has the expected unit
           suffix.
        3. Query Prometheus for the same metric and verify the UI
           value is consistent within tolerance.
        """
        subvolume_metrics_card = CephFSSubvolumeMetricsCard()
        ns = self.single_project.namespace
        subvolume_metrics_card.navigate_to_cephfs_subvolume_section()

        metrics = [
            constants.CEPHFS_SUBVOLUME_DEFAULT_METRIC,
            constants.CEPHFS_SUBVOLUME_METRIC_LATENCY,
            constants.CEPHFS_SUBVOLUME_METRIC_THROUGHPUT,
        ]

        for metric in metrics:
            logger.test_step(
                "Switch to '%s' and verify value for namespace '%s'",
                metric,
                ns,
            )
            subvolume_metrics_card.verify_metric_value_for_namespace(ns, metric)

            if metric != constants.CEPHFS_SUBVOLUME_METRIC_THROUGHPUT:
                logger.test_step(
                    "Verify UI values match Prometheus for '%s'",
                    metric,
                )
                all_values = (
                    subvolume_metrics_card.get_cephfs_subvolume_all_row_values()
                )
                subvolume_metrics_card.verify_ui_values_match_prometheus(
                    metric=metric,
                    ui_values=all_values,
                    threading_lock=threading_lock,
                )

    @tier2
    @ui
    def test_column_sort_tab_switch(self):
        """
        Verify switching between metric tabs updates the column header
        and table values, and returning to the original tab restores
        the same data.

        Steps:
        1. Navigate to the subvolume metrics card and record values
           on Total IOPS (default).
        2. Switch to Total Latency and verify the column header and
           values update with the correct unit.
        3. Switch to Total Throughput and verify the column header
           and values update with the correct unit.
        4. Switch back to Total IOPS and verify the original column
           header and unit are restored.
        """
        subvolume_metrics_card = CephFSSubvolumeMetricsCard()
        subvolume_metrics_card.navigate_to_cephfs_subvolume_section()

        metrics = [
            constants.CEPHFS_SUBVOLUME_DEFAULT_METRIC,
            constants.CEPHFS_SUBVOLUME_METRIC_LATENCY,
            constants.CEPHFS_SUBVOLUME_METRIC_THROUGHPUT,
        ]

        logger.test_step("Record initial row count on '%s'", metrics[0])
        subvolume_metrics_card.switch_cephfs_subvolume_metric(metrics[0])
        initial_headers = subvolume_metrics_card.get_cephfs_subvolume_column_headers()
        initial_row_count = subvolume_metrics_card.get_cephfs_subvolume_row_count()
        assert initial_headers[-1] == metrics[0], (
            f"Expected column header '{metrics[0]}', " f"got '{initial_headers[-1]}'"
        )

        for metric in metrics[1:]:
            expected_unit = constants.CEPHFS_SUBVOLUME_METRIC_EXPECTED_UNITS[metric]
            logger.test_step(
                "Switch to '%s' and verify column header and values",
                metric,
            )
            subvolume_metrics_card.switch_cephfs_subvolume_metric(metric)
            headers = subvolume_metrics_card.get_cephfs_subvolume_column_headers()
            assert headers[-1] == metric, (
                f"Expected column header '{metric}', " f"got '{headers[-1]}'"
            )
            values = subvolume_metrics_card.get_cephfs_subvolume_all_row_values()
            assert len(values) > 0, f"No values displayed for metric '{metric}'"
            for idx, val in enumerate(values):
                assert expected_unit in val, (
                    f"Row {idx} value '{val}' does not contain "
                    f"unit '{expected_unit}' for metric '{metric}'"
                )

        logger.test_step(
            "Switch back to '%s' and verify header and unit restored",
            metrics[0],
        )
        subvolume_metrics_card.switch_cephfs_subvolume_metric(metrics[0])
        restored_headers = subvolume_metrics_card.get_cephfs_subvolume_column_headers()
        assert restored_headers[-1] == metrics[0], (
            f"Expected restored column header '{metrics[0]}', "
            f"got '{restored_headers[-1]}'"
        )
        restored_unit = constants.CEPHFS_SUBVOLUME_METRIC_EXPECTED_UNITS[metrics[0]]
        restored_values = subvolume_metrics_card.get_cephfs_subvolume_all_row_values()
        assert len(restored_values) == initial_row_count, (
            f"Row count changed after switching back: "
            f"initial={initial_row_count}, "
            f"restored={len(restored_values)}"
        )
        for idx, val in enumerate(restored_values):
            assert restored_unit in val, (
                f"Row {idx} value '{val}' does not contain "
                f"unit '{restored_unit}' after switching back "
                f"to '{metrics[0]}'"
            )
