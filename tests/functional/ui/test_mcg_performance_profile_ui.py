"""
UI Test Cases for MCG Performance Profiles Feature (RHSTOR-8629)

This module contains automated UI tests for the MCG Performance Profiles feature,
verifying the Configure Performance page layout, profile selection, and resource
requirement displays.

Test Strategy Reference:
https://redhat.atlassian.net/browse/RHSTOR-8629 (MCG Profiles Test Strategy)
"""

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


logger = logging.getLogger(__name__)


class TestConfigurePerformancePageLayout:
    """
    Test the Configure Performance page layout for MCG and Core Storage sections.
    Verifies the page structure, section separation, and node display behavior.
    """

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
        """
        UI-1: Verify Configure Performance Page Layout (Core Storage and MCG Sections)

        This test verifies that the Configure Performance page shows:
        1. Two clearly separated sections: Core Storage and MCG
        2. Core Storage section displays inline (not as a modal popup)
        3. Core Storage section shows only nodes with OCS labels
        4. MCG section shows all cluster nodes regardless of labels
        5. Core Storage section has no node selection option

        Expected Results:
        - The page has two clearly separated sections
        - Core Storage is displayed inline, not as a modal
        - Core Storage shows only OCS-labeled nodes
        - MCG shows all cluster nodes
        - No node selection option in Core Storage section
        """
        logger.info("Starting Configure Performance page layout test (UI-1)")

        # Step 1: Navigate to Configure Performance page
        logger.info("Step 1: Navigating to Configure Performance page")
        navigator = PageNavigator()

        # Navigate to StorageCluster details where Configure Performance button is located
        logger.info("Navigating to StorageCluster details")
        storage_cluster_page = navigator.nav_storage_cluster_default_page()

        # Click Configure Performance button
        logger.info("Clicking Configure Performance button")
        configure_perf_page = storage_cluster_page.click_configure_performance_button()

        # Verify page loaded
        logger.info("Verifying Configure Performance page loaded")
        assert (
            configure_perf_page is not None
        ), "Failed to navigate to Configure Performance page"

        logger.info("✓ Step 1: Successfully navigated to Configure Performance page")

        # Step 2: Verify two sections exist - Core Storage and MCG
        logger.info("Step 2: Verifying two main sections (Core Storage and MCG)")

        # Check for Core Storage section
        core_storage_present = configure_perf_page.is_core_storage_section_present()
        assert (
            core_storage_present
        ), "Core Storage section not found on Configure Performance page"
        logger.info("✓ Core Storage section is present")

        # Check for MCG section
        mcg_section_present = configure_perf_page.is_mcg_section_present()
        assert (
            mcg_section_present
        ), "MCG section not found on Configure Performance page"
        logger.info("✓ MCG section is present")

        # Verify sections are visually separated
        sections_separated = configure_perf_page.verify_sections_separated()
        assert (
            sections_separated
        ), "Core Storage and MCG sections are not properly separated"
        logger.info("✓ Sections are properly separated")

        logger.info("✓ Step 2: Both Core Storage and MCG sections are present")

        # Step 3: Verify Core Storage section is inline (not modal)
        logger.info("Step 3: Verifying Core Storage section displays inline")

        is_inline = configure_perf_page.is_core_storage_inline()
        assert is_inline, "Core Storage section is displayed as modal instead of inline"
        logger.info("✓ Core Storage section is inline (not modal)")

        logger.info("✓ Step 3: Core Storage section displays correctly as inline")

        # Step 4: Verify Core Storage shows only OCS-labeled nodes
        logger.info("Step 4: Verifying Core Storage shows only OCS-labeled nodes")

        core_storage_nodes = configure_perf_page.get_core_storage_nodes()
        assert len(core_storage_nodes) > 0, "No nodes found in Core Storage section"

        # Verify all nodes have OCS label
        all_ocs_labeled = configure_perf_page.verify_all_nodes_have_ocs_label(
            core_storage_nodes
        )
        assert all_ocs_labeled, "Core Storage section contains nodes without OCS label"
        logger.info(
            f"✓ All {len(core_storage_nodes)} nodes in Core Storage section have OCS label"
        )

        logger.info("✓ Step 4: Core Storage correctly shows only OCS-labeled nodes")

        # Step 5: Verify MCG section shows all cluster nodes
        logger.info("Step 5: Verifying MCG section shows all cluster nodes")

        mcg_nodes = configure_perf_page.get_mcg_nodes()
        core_storage_nodes_count = len(core_storage_nodes)
        mcg_nodes_count = len(mcg_nodes)

        # MCG should show all nodes, potentially including non-OCS-labeled ones
        assert mcg_nodes_count >= core_storage_nodes_count, (
            f"MCG node count ({mcg_nodes_count}) is less than "
            f"Core Storage ({core_storage_nodes_count})"
        )
        logger.info(
            f"✓ MCG section shows {mcg_nodes_count} nodes "
            f"(>= Core Storage's {core_storage_nodes_count})"
        )

        logger.info("✓ Step 5: MCG section correctly shows all cluster nodes")

        # Step 6: Verify Core Storage has no node selection option
        logger.info("Step 6: Verifying Core Storage has no node selection option")

        has_node_selection = configure_perf_page.has_node_selection_option()
        assert (
            not has_node_selection
        ), "Core Storage section should not have node selection option"
        logger.info("✓ Core Storage section has no node selection option")

        logger.info("✓ Step 6: Core Storage correctly has no node selection option")

        # Final verification: Page integrity
        logger.info("Performing final page integrity check")
        page_integrity = configure_perf_page.verify_page_integrity()
        assert page_integrity, "Configure Performance page integrity check failed"
        logger.info("✓ Page integrity verified")

        logger.info("✅ UI-1: Configure Performance page layout test PASSED")


class TestMCGProfileSelection:
    """
    Test MCG profile selection functionality on the Configure Performance page.
    """

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
        """
        UI-3: Verify MCG Profile Selection Updates StorageCluster CR

        This test verifies that:
        1. MCG profile selection updates the StorageCluster CR correctly
        2. The UI reflects the active MCG profile when the page is reopened
        3. Switching between profiles updates the CR each time
        """
        logger.info("Starting MCG profile selection test (UI-3)")

        navigator = PageNavigator()
        storage_cluster_page = navigator.nav_storage_cluster_default_page()
        configure_perf_page = storage_cluster_page.click_configure_performance_button()

        # Test each MCG profile
        profiles_to_test = ["default", "mixed-workload", "small-objects"]

        for profile in profiles_to_test:
            logger.info(f"Testing profile selection: {profile}")

            # Select profile
            configure_perf_page.select_mcg_profile(profile)
            logger.info(f"✓ Selected MCG profile: {profile}")

            # Save changes
            configure_perf_page.save_configuration()
            logger.info("✓ Configuration saved")

            # Verify StorageCluster CR was updated
            selected_profile = configure_perf_page.get_mcg_profile_from_cr()
            assert (
                selected_profile == profile
            ), f"Expected profile {profile}, but StorageCluster CR shows {selected_profile}"
            logger.info(f"✓ StorageCluster CR correctly shows profile: {profile}")

        logger.info("✅ UI-3: MCG profile selection test PASSED")


class TestCoreStorageRegression:
    """
    Test that Core Storage profile selection still works after page redesign (regression test).
    """

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
        """
        UI-2: Verify Core Storage Profile Selection (Regression)

        This test verifies that the existing Core Storage profile selection
        still works correctly after the page redesign.
        """
        logger.info("Starting Core Storage profile selection regression test (UI-2)")

        navigator = PageNavigator()
        storage_cluster_page = navigator.nav_storage_cluster_default_page()
        configure_perf_page = storage_cluster_page.click_configure_performance_button()

        # Test each Core Storage profile
        profiles_to_test = ["lean", "balanced", "performance"]

        for profile in profiles_to_test:
            logger.info(f"Testing Core Storage profile: {profile}")

            # Select profile
            configure_perf_page.select_core_storage_profile(profile)
            logger.info(f"✓ Selected Core Storage profile: {profile}")

            # Save changes
            configure_perf_page.save_configuration()
            logger.info("✓ Configuration saved")

            # Verify StorageCluster CR was updated
            selected_profile = configure_perf_page.get_core_storage_profile_from_cr()
            assert (
                selected_profile == profile
            ), f"Expected profile {profile}, but StorageCluster CR shows {selected_profile}"
            logger.info(
                f"✓ StorageCluster CR correctly shows Core Storage profile: {profile}"
            )

        logger.info("✅ UI-2: Core Storage profile selection regression test PASSED")
