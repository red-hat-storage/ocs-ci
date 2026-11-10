"""UI tests for MCG Performance Profiles (RHSTOR-8629)."""

import logging

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


logger = logging.getLogger(__name__)

# UI-5 moves each section through three profiles, so every step is a real
# change and the Save button is never disabled.
CORE_BASELINE, CORE_ALONE, CORE_COMBINED = "balanced", "lean", "performance"
MCG_BASELINE, MCG_ALONE, MCG_COMBINED = "default", "mixed-workload", "small-objects"


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

        logger.test_step("Verify page object loaded")
        configure_perf_page.take_screenshot("verify_page_object_loaded")
        configure_perf_page.copy_dom("verify_page_object_loaded")
        logger.assertion(f"Page object is not None: actual={configure_perf_page}")
        assert configure_perf_page is not None

        logger.test_step("Verify Core storage section is present")
        configure_perf_page.take_screenshot("verify_core_storage_section")
        configure_perf_page.copy_dom("verify_core_storage_section")
        is_present = configure_perf_page.is_core_storage_section_present()
        logger.assertion(
            f"Core storage section present: expected=True, actual={is_present}"
        )
        assert is_present

        logger.test_step("Verify Multicloud Object Gateway section is present")
        configure_perf_page.take_screenshot("verify_mcg_section")
        configure_perf_page.copy_dom("verify_mcg_section")
        is_present = configure_perf_page.is_mcg_section_present()
        logger.assertion(f"MCG section present: expected=True, actual={is_present}")
        assert is_present

        logger.test_step(
            "Verify Core storage 'Select options' button is present and clickable"
        )
        configure_perf_page.take_screenshot("verify_core_storage_select_button")
        configure_perf_page.copy_dom("verify_core_storage_select_button")
        is_present = configure_perf_page.is_core_storage_select_button_present()
        logger.assertion(
            f"Core storage select button present: expected=True, actual={is_present}"
        )
        assert is_present

        logger.test_step("Verify MCG profile dropdown button is present and clickable")
        configure_perf_page.take_screenshot("verify_mcg_select_button")
        configure_perf_page.copy_dom("verify_mcg_select_button")
        is_present = configure_perf_page.is_mcg_select_button_present()
        logger.assertion(
            f"MCG select button present: expected=True, actual={is_present}"
        )
        assert is_present

        logger.test_step("Verify page integrity with both sections")
        configure_perf_page.take_screenshot("verify_page_integrity")
        configure_perf_page.copy_dom("verify_page_integrity")
        is_intact = configure_perf_page.verify_page_integrity()
        logger.assertion(f"Page integrity verified: expected=True, actual={is_intact}")
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
        """UI-3: Verify MCG profile selection updates CR and persists in UI.

        Steps:
            1. Record initial MCG profile state in UI.
            2. Initialize MCG with 'default' profile and verify CR and UI.
            3. Perform profile transitions, verifying StorageCluster CR and UI.
            4. Restore original MCG profile.
        """
        initial_ui_profile = "default"  # fallback
        try:
            logger.test_step("Navigate to Configure Performance page")
            navigator = PageNavigator()
            storage_cluster_page = navigator.nav_storage_cluster_default_page()
            configure_perf_page = (
                storage_cluster_page.click_configure_performance_button()
            )

            logger.test_step("Record initial MCG profile state in UI")
            # Until a profile is selected for the first time the button shows
            # "Select a profile" (None here); afterwards it always shows the
            # selected profile name.
            configure_perf_page.take_screenshot("record_initial_mcg_profile")
            configure_perf_page.copy_dom("record_initial_mcg_profile")
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

            logger.test_step("Initialize MCG with default profile")
            configure_perf_page.select_mcg_profile("default")
            configure_perf_page.save_configuration()

            logger.test_step("Verify default profile is set in CR and UI")
            if captured_profile != "default":
                # A change was saved: reopen the page to read the persisted state
                storage_cluster_page = navigator.nav_storage_cluster_default_page()
                configure_perf_page = (
                    storage_cluster_page.click_configure_performance_button()
                )
            configure_perf_page.take_screenshot("verify_default_mcg_profile")
            configure_perf_page.copy_dom("verify_default_mcg_profile")
            cr_profile = configure_perf_page.wait_for_mcg_profile_in_cr("default")
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
                logger.test_step(f"Transition: {source} → {target}")
                configure_perf_page.select_mcg_profile(target)
                configure_perf_page.save_configuration()

                logger.test_step("Verify CR updated")
                configure_perf_page.take_screenshot(
                    f"verify_mcg_cr_transition_{target}"
                )
                configure_perf_page.copy_dom(f"verify_mcg_cr_transition_{target}")
                cr_profile = configure_perf_page.wait_for_mcg_profile_in_cr(target)
                logger.assertion(
                    f"MCG profile from CR after transition: expected='{target}', actual='{cr_profile}'"
                )
                assert (
                    cr_profile == target
                ), f"MCG profile in CR should be '{target}', got '{cr_profile}'"

                logger.test_step("Reopen page and verify UI shows profile")
                storage_cluster_page = navigator.nav_storage_cluster_default_page()
                configure_perf_page = (
                    storage_cluster_page.click_configure_performance_button()
                )
                configure_perf_page.take_screenshot(
                    f"verify_mcg_ui_transition_{target}"
                )
                configure_perf_page.copy_dom(f"verify_mcg_ui_transition_{target}")
                ui_profile = configure_perf_page.get_mcg_profile_from_ui()
                logger.assertion(
                    f"MCG profile from UI after transition: expected='{target}', actual='{ui_profile}'"
                )
                assert (
                    ui_profile == target
                ), f"MCG profile in UI should be '{target}', got '{ui_profile}'"
        finally:
            logger.test_step(f"Restore MCG profile to initial: {initial_ui_profile}")
            storage_cluster_page = navigator.nav_storage_cluster_default_page()
            configure_perf_page = (
                storage_cluster_page.click_configure_performance_button()
            )
            configure_perf_page.select_mcg_profile(initial_ui_profile)
            configure_perf_page.save_configuration()
            # The profile change rolls the NooBaa DB pods. Later tests are
            # gated on the NooBaa phase, so settle before handing over.
            logger.test_step("Wait for NooBaa to become Ready after the profile change")
            noobaa_phase = configure_perf_page.wait_for_noobaa_ready()
            logger.info("NooBaa phase after restore: %s", noobaa_phase)
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
            logger.test_step("Navigate to Configure Performance page")
            navigator = PageNavigator()
            storage_cluster_page = navigator.nav_storage_cluster_default_page()
            configure_perf_page = (
                storage_cluster_page.click_configure_performance_button()
            )

            try:
                initial_profile = configure_perf_page.get_core_storage_profile_from_cr()
            except ValueError:
                logger.test_step("Initialize Core Storage with balanced profile")
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
                logger.test_step(f"Transition: {source} → {target}")
                configure_perf_page.select_core_storage_profile(target)
                configure_perf_page.save_configuration()

                logger.test_step("Verify CR updated")
                configure_perf_page.take_screenshot(
                    f"verify_core_storage_cr_transition_{target}"
                )
                configure_perf_page.copy_dom(
                    f"verify_core_storage_cr_transition_{target}"
                )
                cr_profile = configure_perf_page.wait_for_core_storage_profile_in_cr(
                    target
                )
                logger.assertion(
                    f"Core Storage profile from CR: expected='{target}', actual='{cr_profile}'"
                )
                assert (
                    cr_profile == target
                ), f"Core Storage profile in CR should be '{target}', got '{cr_profile}'"

                logger.test_step("Reopen page and verify UI shows profile")
                storage_cluster_page = navigator.nav_storage_cluster_default_page()
                configure_perf_page = (
                    storage_cluster_page.click_configure_performance_button()
                )
                configure_perf_page.take_screenshot(
                    f"verify_core_storage_ui_transition_{target}"
                )
                configure_perf_page.copy_dom(
                    f"verify_core_storage_ui_transition_{target}"
                )
                ui_profile = configure_perf_page.get_core_storage_profile_from_ui()
                logger.assertion(
                    f"Core Storage profile from UI: expected='{target}', actual='{ui_profile}'"
                )
                assert (
                    ui_profile == target
                ), f"Core Storage profile in UI should be '{target}', got '{ui_profile}'"
        finally:
            logger.test_step(f"Restore initial Core Storage profile: {initial_profile}")
            storage_cluster_page = navigator.nav_storage_cluster_default_page()
            configure_perf_page = (
                storage_cluster_page.click_configure_performance_button()
            )
            configure_perf_page.select_core_storage_profile(initial_profile)
            configure_perf_page.save_configuration()
            # resourceProfile resizes NooBaa too, so settle before handing over.
            logger.test_step("Wait for NooBaa to become Ready after the profile change")
            noobaa_phase = configure_perf_page.wait_for_noobaa_ready()
            logger.info("NooBaa phase after restore: %s", noobaa_phase)
        logger.info("✅ UI-2 PASSED")


class TestProfileSectionIndependence:
    """Test that the Core Storage and MCG profile sections are independent."""

    @ui
    @tier2
    @red_squad
    @mcg
    @polarion_id("OCS-8411")
    @skipif_mcg_only
    @skipif_hci_client
    @skipif_disconnected_cluster
    @skipif_ibm_cloud_managed
    def test_profile_section_independence(self, setup_ui_class):
        """UI-5: Verify changing one profile section does not affect the other."""
        navigator = PageNavigator()
        # None records a field that is absent from the CR, so teardown can put
        # it back rather than leaving an explicit profile behind.
        initial_core_profile = None
        initial_mcg_profile = None
        # Until both fields have been read, None means "not recorded" rather
        # than "absent", and teardown must not act on it.
        profiles_recorded = False
        try:
            logger.test_step("Navigate to Configure Performance page")
            storage_cluster_page = navigator.nav_storage_cluster_default_page()
            configure_perf_page = (
                storage_cluster_page.click_configure_performance_button()
            )

            logger.test_step("Record the profiles both sections start from")
            try:
                initial_core_profile = (
                    configure_perf_page.get_core_storage_profile_from_cr()
                )
            except ValueError:
                logger.info("Core Storage resourceProfile is unset at the start")
            try:
                initial_mcg_profile = configure_perf_page.get_mcg_profile_from_cr()
            except ValueError:
                logger.info("MCG performanceProfile is unset at the start")
            profiles_recorded = True
            logger.info(
                "Initial profiles - Core Storage: %s, MCG: %s",
                initial_core_profile,
                initial_mcg_profile,
            )

            logger.test_step(
                f"Put Core Storage on the baseline profile '{CORE_BASELINE}'"
            )
            configure_perf_page.select_core_storage_profile(CORE_BASELINE)
            configure_perf_page.save_configuration()
            configure_perf_page = configure_perf_page.reopen()

            logger.test_step(f"Put MCG on the baseline profile '{MCG_BASELINE}'")
            configure_perf_page.select_mcg_profile(MCG_BASELINE)
            configure_perf_page.save_configuration()
            configure_perf_page = configure_perf_page.reopen()

            logger.test_step("Verify both sections reached the baseline in the CR")
            configure_perf_page.take_screenshot("verify_independence_baseline")
            configure_perf_page.copy_dom("verify_independence_baseline")
            core_profile = configure_perf_page.wait_for_core_storage_profile_in_cr(
                CORE_BASELINE
            )
            logger.assertion(
                f"Core Storage profile from CR: expected='{CORE_BASELINE}', "
                f"actual='{core_profile}'"
            )
            assert (
                core_profile == CORE_BASELINE
            ), f"Core Storage profile in CR should be '{CORE_BASELINE}', got '{core_profile}'"
            mcg_profile = configure_perf_page.wait_for_mcg_profile_in_cr(MCG_BASELINE)
            logger.assertion(
                f"MCG profile from CR: expected='{MCG_BASELINE}', "
                f"actual='{mcg_profile}'"
            )
            assert (
                mcg_profile == MCG_BASELINE
            ), f"MCG profile in CR should be '{MCG_BASELINE}', got '{mcg_profile}'"

            logger.test_step(
                f"Change only the Core Storage profile: {CORE_BASELINE} → {CORE_ALONE}"
            )
            configure_perf_page.select_core_storage_profile(CORE_ALONE)
            configure_perf_page.save_configuration()
            configure_perf_page = configure_perf_page.reopen()
            configure_perf_page.take_screenshot("verify_core_storage_changed_alone")
            configure_perf_page.copy_dom("verify_core_storage_changed_alone")

            core_profile = configure_perf_page.wait_for_core_storage_profile_in_cr(
                CORE_ALONE
            )
            logger.assertion(
                f"Core Storage profile from CR: expected='{CORE_ALONE}', "
                f"actual='{core_profile}'"
            )
            assert (
                core_profile == CORE_ALONE
            ), f"Core Storage profile in CR should be '{CORE_ALONE}', got '{core_profile}'"

            mcg_profile = configure_perf_page.get_mcg_profile_from_cr()
            logger.assertion(
                f"MCG profile from CR after a Core Storage change: "
                f"expected='{MCG_BASELINE}', actual='{mcg_profile}'"
            )
            assert mcg_profile == MCG_BASELINE, (
                f"Changing the Core Storage profile to '{CORE_ALONE}' also changed "
                f"the MCG profile to '{mcg_profile}', expected it to stay "
                f"'{MCG_BASELINE}'"
            )

            logger.test_step(
                f"Change only the MCG profile: {MCG_BASELINE} → {MCG_ALONE}"
            )
            configure_perf_page.select_mcg_profile(MCG_ALONE)
            configure_perf_page.save_configuration()
            configure_perf_page = configure_perf_page.reopen()
            configure_perf_page.take_screenshot("verify_mcg_changed_alone")
            configure_perf_page.copy_dom("verify_mcg_changed_alone")

            mcg_profile = configure_perf_page.wait_for_mcg_profile_in_cr(MCG_ALONE)
            logger.assertion(
                f"MCG profile from CR: expected='{MCG_ALONE}', actual='{mcg_profile}'"
            )
            assert (
                mcg_profile == MCG_ALONE
            ), f"MCG profile in CR should be '{MCG_ALONE}', got '{mcg_profile}'"

            core_profile = configure_perf_page.get_core_storage_profile_from_cr()
            logger.assertion(
                f"Core Storage profile from CR after an MCG change: "
                f"expected='{CORE_ALONE}', actual='{core_profile}'"
            )
            assert core_profile == CORE_ALONE, (
                f"Changing the MCG profile to '{MCG_ALONE}' also changed the Core "
                f"Storage profile to '{core_profile}', expected it to stay "
                f"'{CORE_ALONE}'"
            )

            logger.test_step(
                f"Change both profiles in a single save: Core Storage "
                f"'{CORE_COMBINED}', MCG '{MCG_COMBINED}'"
            )
            configure_perf_page.select_core_storage_profile(CORE_COMBINED)
            configure_perf_page.select_mcg_profile(MCG_COMBINED)
            configure_perf_page.save_configuration()
            configure_perf_page = configure_perf_page.reopen()
            configure_perf_page.take_screenshot("verify_both_profiles_changed")
            configure_perf_page.copy_dom("verify_both_profiles_changed")

            core_profile = configure_perf_page.wait_for_core_storage_profile_in_cr(
                CORE_COMBINED
            )
            logger.assertion(
                f"Core Storage profile from CR after a combined save: "
                f"expected='{CORE_COMBINED}', actual='{core_profile}'"
            )
            assert core_profile == CORE_COMBINED, (
                f"Core Storage profile in CR should be '{CORE_COMBINED}' after the "
                f"combined save, got '{core_profile}'"
            )

            mcg_profile = configure_perf_page.wait_for_mcg_profile_in_cr(MCG_COMBINED)
            logger.assertion(
                f"MCG profile from CR after a combined save: "
                f"expected='{MCG_COMBINED}', actual='{mcg_profile}'"
            )
            assert mcg_profile == MCG_COMBINED, (
                f"MCG profile in CR should be '{MCG_COMBINED}' after the combined "
                f"save, got '{mcg_profile}'"
            )
        finally:
            # Nothing has been changed yet if the starting profiles were never
            # recorded, and acting on them would clear fields the test found
            # set. Each restore below stands on its own so that one failure
            # neither skips the rest nor replaces the result of the test.
            if not profiles_recorded:
                logger.warning(
                    "Starting profiles were never recorded, leaving the "
                    "StorageCluster CR untouched"
                )
            else:
                logger.test_step(
                    f"Restore the initial profiles - Core Storage "
                    f"'{initial_core_profile}', MCG '{initial_mcg_profile}'"
                )
                try:
                    storage_cluster_page = navigator.nav_storage_cluster_default_page()
                    configure_perf_page = (
                        storage_cluster_page.click_configure_performance_button()
                    )
                    configure_perf_page.select_core_storage_profile(
                        initial_core_profile or CORE_BASELINE
                    )
                    configure_perf_page.select_mcg_profile(
                        initial_mcg_profile or MCG_BASELINE
                    )
                    configure_perf_page.save_configuration()
                except Exception:
                    logger.exception("Restoring the profiles through the UI failed")

                # Saving writes whatever the dropdowns show, so a field that
                # was absent at the start is cleared only after that save.
                if initial_core_profile is None:
                    try:
                        configure_perf_page.remove_core_storage_profile()
                    except Exception:
                        logger.exception("Clearing spec.resourceProfile failed")
                if initial_mcg_profile is None:
                    try:
                        configure_perf_page.remove_mcg_profile()
                    except Exception:
                        logger.exception(
                            "Clearing spec.multiCloudGateway.performanceProfile failed"
                        )

                # Both profiles resize NooBaa, so settle before handing over.
                try:
                    logger.test_step(
                        "Wait for NooBaa to become Ready after the restore"
                    )
                    noobaa_phase = configure_perf_page.wait_for_noobaa_ready()
                    logger.info("NooBaa phase after restore: %s", noobaa_phase)
                except Exception:
                    logger.exception(
                        "Waiting for NooBaa to settle after the restore failed"
                    )
        logger.info("✅ UI-5 PASSED")
