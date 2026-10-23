"""UI tests for MCG Performance Profiles (RHSTOR-8629)."""

import logging
import time

from ocs_ci.framework.pytest_customization.marks import (
    skipif_disconnected_cluster,
    skipif_hci_client,
    skipif_ibm_cloud_managed,
    skipif_mcg_only,
    red_squad,
    mcg,
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
    @red_squad
    @mcg
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
            logger.assertion(f"Page object is not None: actual={configure_perf_page}")
            assert configure_perf_page is not None

        with test_step("Verify Core storage section is present"):
            is_present = configure_perf_page.is_core_storage_section_present()
            logger.assertion(
                f"Core storage section present: expected=True, actual={is_present}"
            )
            assert is_present

        with test_step("Verify Multicloud Object Gateway section is present"):
            is_present = configure_perf_page.is_mcg_section_present()
            logger.assertion(f"MCG section present: expected=True, actual={is_present}")
            assert is_present

        with test_step(
            "Verify Core storage 'Select options' button is present and clickable"
        ):
            is_present = configure_perf_page.is_core_storage_select_button_present()
            logger.assertion(
                f"Core storage select button present: expected=True, actual={is_present}"
            )
            assert is_present

        with test_step("Verify MCG profile dropdown button is present and clickable"):
            is_present = configure_perf_page.is_mcg_select_button_present()
            logger.assertion(
                f"MCG select button present: expected=True, actual={is_present}"
            )
            assert is_present

        with test_step("Verify page integrity with both sections"):
            is_intact = configure_perf_page.verify_page_integrity()
            logger.assertion(
                f"Page integrity verified: expected=True, actual={is_intact}"
            )
            assert is_intact

        logger.info("✅ UI-1 PASSED")


class TestMCGProfileSelection:
    """Test MCG profile selection."""

    @ui
    @tier2
    @red_squad
    @mcg
    @polarion_id("OCS-8318")
    @skipif_mcg_only
    @skipif_hci_client
    @skipif_disconnected_cluster
    @skipif_ibm_cloud_managed
    def test_mcg_profile_selection(self, setup_ui_class):
        """
        UI-3: Verify MCG profile selection updates CR and persists in UI.

        The test works whether or not an MCG profile was ever selected before:
        until the first selection the dropdown shows "Select a profile",
        afterwards it always shows Default / Mixed workload / Small objects.

        Steps:
            1. Open the Configure Performance page and record the initial MCG
               dropdown text ("Select a profile" or an existing profile).
            2. Select the "default" profile and save (no-op if already default).
            3. Verify the CR and the UI (after reopening if a change was saved)
               show "default".
            4. For each transition default -> mixed-workload -> default ->
               small-objects -> default: select, save, verify the StorageCluster
               CR, reopen the page and verify the UI shows the profile.
            5. Finally restore the profile to "default".
        """
        initial_ui_profile = "default"  # fallback
        try:
            with test_step("Navigate to Configure Performance page"):
                navigator = PageNavigator()
                storage_cluster_page = navigator.nav_storage_cluster_default_page()
                configure_perf_page = (
                    storage_cluster_page.click_configure_performance_button()
                )

            with test_step("Record initial MCG profile state in UI"):
                # Until a profile is selected for the first time the button shows
                # "Select a profile" (None here); afterwards it always shows the
                # selected profile name.
                captured_profile = configure_perf_page.get_mcg_profile_from_ui()
                logger.info(
                    "Initial MCG profile in UI: %s",
                    captured_profile or "not selected yet ('Select a profile')",
                )
                logger.assertion(
                    f"Initial profile '{captured_profile}' is a valid profile option"
                )
                assert captured_profile in (
                    None,
                    "default",
                    "mixed-workload",
                    "small-objects",
                ), f"Unexpected initial MCG profile text in UI: '{captured_profile}'"
                if captured_profile:
                    initial_ui_profile = captured_profile

            with test_step("Initialize MCG with default profile"):
                configure_perf_page.select_mcg_profile("default")
                configure_perf_page.save_configuration()

            with test_step("Verify default profile is set in CR and UI"):
                if initial_ui_profile != "default":
                    # A change was saved: reopen the page to read the persisted state
                    storage_cluster_page = navigator.nav_storage_cluster_default_page()
                    configure_perf_page = (
                        storage_cluster_page.click_configure_performance_button()
                    )
                cr_profile = configure_perf_page.get_mcg_profile_from_cr()
                logger.assertion(
                    f"MCG profile from CR: expected='default', actual='{cr_profile}'"
                )
                assert (
                    cr_profile == "default"
                ), f"MCG profile in CR should be 'default', got '{cr_profile}'"
                ui_profile = configure_perf_page.get_mcg_profile_from_ui()
                logger.assertion(
                    f"MCG profile from UI: expected='default', actual='{ui_profile}'"
                )
                assert (
                    ui_profile == "default"
                ), f"MCG profile in UI should be 'default', got '{ui_profile}'"

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
                        logger.assertion(
                            f"MCG profile from CR after transition: expected='{target}', actual='{cr_profile}'"
                        )
                        assert (
                            cr_profile == target
                        ), f"MCG profile in CR should be '{target}', got '{cr_profile}'"
                    except ValueError as e:
                        logger.warning("CR verification failed, retrying: %s", e)
                        time.sleep(2)
                        cr_profile = configure_perf_page.get_mcg_profile_from_cr()
                        logger.assertion(
                            f"MCG profile from CR after transition retry: expected='{target}', actual='{cr_profile}'"
                        )
                        assert (
                            cr_profile == target
                        ), f"MCG profile in CR should be '{target}', got '{cr_profile}'"

                with test_step("Reopen page and verify UI shows profile"):
                    storage_cluster_page = navigator.nav_storage_cluster_default_page()
                    configure_perf_page = (
                        storage_cluster_page.click_configure_performance_button()
                    )
                    ui_profile = configure_perf_page.get_mcg_profile_from_ui()
                    logger.assertion(
                        f"MCG profile from UI after transition: expected='{target}', actual='{ui_profile}'"
                    )
                    assert (
                        ui_profile == target
                    ), f"MCG profile in UI should be '{target}', got '{ui_profile}'"
        finally:
            with test_step(f"Restore MCG profile to initial: {initial_ui_profile}"):
                storage_cluster_page = navigator.nav_storage_cluster_default_page()
                configure_perf_page = (
                    storage_cluster_page.click_configure_performance_button()
                )
                configure_perf_page.select_mcg_profile(initial_ui_profile)
                configure_perf_page.save_configuration()
        logger.info("✅ UI-3 PASSED")


class TestCoreStorageRegression:
    """Regression test for Core Storage profile selection."""

    @ui
    @tier2
    @red_squad
    @mcg
    @polarion_id("OCS-8319")
    @skipif_mcg_only
    @skipif_hci_client
    @skipif_disconnected_cluster
    @skipif_ibm_cloud_managed
    def test_core_storage_profile_selection_regression(self, setup_ui_class):
        """UI-2: Verify Core Storage profile selection updates CR and persists in UI."""
        initial_profile = "balanced"  # fallback
        try:
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
                    # save_configuration() navigates away; reopen the page so the
                    # transitions loop starts from the Configure Performance page.
                    storage_cluster_page = navigator.nav_storage_cluster_default_page()
                    configure_perf_page = (
                        storage_cluster_page.click_configure_performance_button()
                    )
                initial_profile = "balanced"

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
                        logger.assertion(
                            f"Core Storage profile from CR: expected='{target}', actual='{cr_profile}'"
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
                        logger.assertion(
                            f"Core Storage profile from CR retry: expected='{target}', actual='{cr_profile}'"
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
                    logger.assertion(
                        f"Core Storage profile from UI: expected='{target}', actual='{ui_profile}'"
                    )
                    assert (
                        ui_profile == target
                    ), f"Core Storage profile in UI should be '{target}', got '{ui_profile}'"
        finally:
            with test_step(f"Restore initial Core Storage profile: {initial_profile}"):
                storage_cluster_page = navigator.nav_storage_cluster_default_page()
                configure_perf_page = (
                    storage_cluster_page.click_configure_performance_button()
                )
                configure_perf_page.select_core_storage_profile(initial_profile)
                configure_perf_page.save_configuration()
        logger.info("✅ UI-2 PASSED")
