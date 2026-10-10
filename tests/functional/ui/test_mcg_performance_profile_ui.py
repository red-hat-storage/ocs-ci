"""UI tests for MCG Performance Profiles (RHSTOR-8629)."""

import logging

from ocs_ci.framework.pytest_customization.marks import (
    skipif_disconnected_cluster,
    skipif_hci_client,
    skipif_ibm_cloud_managed,
    skipif_mcg_only,
    black_squad,
    jira,
    polarion_id,
    tier2,
    ui,
)
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.ocs.ui.helpers_ui import test_step


logger = logging.getLogger(__name__)


class TestConfigurePerformancePageLayout:
    """Test Configure Performance page layout."""

    @ui
    @tier2
    @black_squad
    @jira("RHSTOR-8629")
    @polarion_id("OCS-8317")
    @skipif_mcg_only
    @skipif_hci_client
    @skipif_disconnected_cluster
    @skipif_ibm_cloud_managed
    def test_configure_performance_page_layout(self, setup_ui_class):
        """UI-1: Verify Configure Performance page layout."""
        logger.info("Starting Configure Performance page layout test")

        navigator = PageNavigator()
        storage_cluster_page = navigator.nav_storage_cluster_default_page()
        configure_perf_page = storage_cluster_page.click_configure_performance_button()
        assert configure_perf_page is not None
        assert configure_perf_page.is_core_storage_section_present()
        assert configure_perf_page.is_mcg_section_present()
        assert configure_perf_page.verify_sections_separated()
        assert configure_perf_page.is_core_storage_inline()
        core_storage_nodes = configure_perf_page.get_core_storage_nodes()
        assert len(core_storage_nodes) > 0
        assert configure_perf_page.verify_all_nodes_have_ocs_label(core_storage_nodes)
        mcg_nodes = configure_perf_page.get_mcg_nodes()
        assert len(mcg_nodes) >= len(core_storage_nodes)
        assert not configure_perf_page.has_node_selection_option()
        assert configure_perf_page.verify_page_integrity()
        logger.info("✅ UI-1 PASSED")


class TestMCGProfileSelection:
    """Test MCG profile selection."""

    @ui
    @tier2
    @black_squad
    @jira("RHSTOR-8629")
    @polarion_id("OCS-8318")
    @skipif_mcg_only
    @skipif_hci_client
    @skipif_disconnected_cluster
    @skipif_ibm_cloud_managed
    def test_mcg_profile_selection(self, setup_ui_class):
        """UI-3: Verify MCG profile selection updates CR and persists in UI."""
        with test_step("Navigate to Configure Performance page"):
            navigator = PageNavigator()
            storage_cluster_page = navigator.nav_storage_cluster_default_page()
            configure_perf_page = (
                storage_cluster_page.click_configure_performance_button()
            )

        initial_profile = configure_perf_page.get_mcg_profile_from_cr()
        try:
            for profile in ["default", "mixed-workload", "small-objects"]:
                with test_step(f"Select MCG profile: {profile}"):
                    configure_perf_page.select_mcg_profile(profile)
                    configure_perf_page.save_configuration()

                with test_step("Verify CR updated"):
                    cr_profile = configure_perf_page.get_mcg_profile_from_cr()
                    assert (
                        cr_profile == profile
                    ), f"MCG profile in CR should be '{profile}', got '{cr_profile}'"

                with test_step("Reopen page and verify UI shows profile"):
                    storage_cluster_page = navigator.nav_storage_cluster_default_page()
                    configure_perf_page = (
                        storage_cluster_page.click_configure_performance_button()
                    )
                    ui_profile = configure_perf_page.get_mcg_profile_from_ui()
                    assert (
                        ui_profile == profile
                    ), f"MCG profile in UI should be '{profile}', got '{ui_profile}'"
        finally:
            with test_step(f"Restore initial MCG profile: {initial_profile}"):
                configure_perf_page.select_mcg_profile(initial_profile)
                configure_perf_page.save_configuration()
        logger.info("✅ UI-3 PASSED")


class TestCoreStorageRegression:
    """Regression test for Core Storage profile selection."""

    @ui
    @tier2
    @black_squad
    @jira("RHSTOR-8629")
    @polarion_id("OCS-8319")
    @skipif_mcg_only
    @skipif_hci_client
    @skipif_disconnected_cluster
    @skipif_ibm_cloud_managed
    def test_core_storage_profile_selection_regression(self, setup_ui_class):
        """UI-2: Verify Core Storage profile selection updates CR and persists in UI."""
        with test_step("Navigate to Configure Performance page"):
            navigator = PageNavigator()
            storage_cluster_page = navigator.nav_storage_cluster_default_page()
            configure_perf_page = (
                storage_cluster_page.click_configure_performance_button()
            )

        initial_profile = configure_perf_page.get_core_storage_profile_from_cr()
        try:
            for profile in ["lean", "balanced", "performance"]:
                with test_step(f"Select Core Storage profile: {profile}"):
                    configure_perf_page.select_core_storage_profile(profile)
                    configure_perf_page.save_configuration()

                with test_step("Verify CR updated"):
                    cr_profile = configure_perf_page.get_core_storage_profile_from_cr()
                    assert (
                        cr_profile == profile
                    ), f"Core Storage profile in CR should be '{profile}', got '{cr_profile}'"

                with test_step("Reopen page and verify UI shows profile"):
                    storage_cluster_page = navigator.nav_storage_cluster_default_page()
                    configure_perf_page = (
                        storage_cluster_page.click_configure_performance_button()
                    )
                    ui_profile = configure_perf_page.get_core_storage_profile_from_ui()
                    assert (
                        ui_profile == profile
                    ), f"Core Storage profile in UI should be '{profile}', got '{ui_profile}'"
        finally:
            with test_step(f"Restore initial Core Storage profile: {initial_profile}"):
                configure_perf_page.select_core_storage_profile(initial_profile)
                configure_perf_page.save_configuration()
        logger.info("✅ UI-2 PASSED")
