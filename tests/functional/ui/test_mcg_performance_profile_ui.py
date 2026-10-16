"""UI tests for MCG Performance Profiles (RHSTOR-8629)."""

import logging
import time

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

        with test_step("Verify page object loaded"):
            assert configure_perf_page is not None

        with test_step("Verify Core storage section is present"):
            assert configure_perf_page.is_core_storage_section_present()

        with test_step("Verify Multicloud Object Gateway section is present"):
            assert configure_perf_page.is_mcg_section_present()

        with test_step(
            "Verify Core storage 'Select options' button is present and clickable"
        ):
            assert configure_perf_page.is_core_storage_select_button_present()

        with test_step("Verify MCG 'Select a profile' button is present and clickable"):
            assert configure_perf_page.is_mcg_select_button_present()

        with test_step("Verify page integrity with both sections"):
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

        try:
            with test_step("Initialize MCG with default profile"):
                configure_perf_page.select_mcg_profile("default")
                configure_perf_page.save_configuration()
            transitions = [
                ("default", "mixed-workload"),
                ("mixed-workload", "default"),
                ("default", "small-objects"),
                ("small-objects", "default"),
            ]
            for source, target in transitions:
                with test_step(f"Transition: {source} → {target}"):
                    configure_perf_page.select_mcg_profile(target)
                    configure_perf_page.save_configuration()

                with test_step("Verify CR updated"):
                    try:
                        cr_profile = configure_perf_page.get_mcg_profile_from_cr()
                        assert (
                            cr_profile == target
                        ), f"MCG profile in CR should be '{target}', got '{cr_profile}'"
                    except ValueError as e:
                        logger.warning("CR verification failed, retrying: %s", e)
                        time.sleep(2)
                        cr_profile = configure_perf_page.get_mcg_profile_from_cr()
                        assert (
                            cr_profile == target
                        ), f"MCG profile in CR should be '{target}', got '{cr_profile}'"

                with test_step("Reopen page and verify UI shows profile"):
                    storage_cluster_page = navigator.nav_storage_cluster_default_page()
                    configure_perf_page = (
                        storage_cluster_page.click_configure_performance_button()
                    )
                    ui_profile = configure_perf_page.get_mcg_profile_from_ui()
                    assert (
                        ui_profile == target
                    ), f"MCG profile in UI should be '{target}', got '{ui_profile}'"
        finally:
            with test_step("Restore MCG profile to default"):
                configure_perf_page.select_mcg_profile("default")
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

        try:
            initial_profile = configure_perf_page.get_core_storage_profile_from_cr()
        except ValueError:
            with test_step("Initialize Core Storage with balanced profile"):
                configure_perf_page.select_core_storage_profile("balanced")
                configure_perf_page.save_configuration()
            initial_profile = "balanced"
        try:
            transitions = [
                ("balanced", "lean"),
                ("lean", "performance"),
                ("performance", "balanced"),
                ("balanced", "performance"),
                ("performance", "lean"),
                ("lean", "balanced"),
            ]
            for source, target in transitions:
                with test_step(f"Transition: {source} → {target}"):
                    configure_perf_page.select_core_storage_profile(target)
                    configure_perf_page.save_configuration()

                with test_step("Verify CR updated"):
                    try:
                        cr_profile = (
                            configure_perf_page.get_core_storage_profile_from_cr()
                        )
                        assert (
                            cr_profile == target
                        ), f"Core Storage profile in CR should be '{target}', got '{cr_profile}'"
                    except ValueError as e:
                        logger.warning("CR verification failed, retrying: %s", e)
                        time.sleep(2)
                        cr_profile = (
                            configure_perf_page.get_core_storage_profile_from_cr()
                        )
                        assert (
                            cr_profile == target
                        ), f"Core Storage profile in CR should be '{target}', got '{cr_profile}'"

                with test_step("Reopen page and verify UI shows profile"):
                    storage_cluster_page = navigator.nav_storage_cluster_default_page()
                    configure_perf_page = (
                        storage_cluster_page.click_configure_performance_button()
                    )
                    ui_profile = configure_perf_page.get_core_storage_profile_from_ui()
                    assert (
                        ui_profile == target
                    ), f"Core Storage profile in UI should be '{target}', got '{ui_profile}'"
        finally:
            with test_step(f"Restore initial Core Storage profile: {initial_profile}"):
                configure_perf_page.select_core_storage_profile(initial_profile)
                configure_perf_page.save_configuration()
        logger.info("✅ UI-2 PASSED")
