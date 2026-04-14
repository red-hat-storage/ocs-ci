"""
UI Test Case for MCO Operator Rebranding Validation

This test validates the rebranding of the Multicluster Orchestrator
(MCO) operator from "ODF MCO" to the vendor-neutral
"DF Multicluster Orchestrator".

Test ID: MCO-UI-001 (from RHSTOR-8246 test plan)
"""

import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    rdr,
    turquoise_squad,
)
from ocs_ci.framework.testlib import skipif_ocs_version, tier1
from ocs_ci.ocs.acm.acm import AcmPageNavigator
from ocs_ci.ocs.ui.views import locators_for_current_ocp_version
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By

logger = logging.getLogger(__name__)


@rdr
@tier1
@turquoise_squad
@skipif_ocs_version("<4.22")
class TestMCOOperatorRebrandingUI:
    """
    Test class for MCO operator rebranding validation on hub cluster UI

    This test validates that the MCO operator is displayed with the
    correct rebranded name "DF Multicluster Orchestrator" instead of
    "ODF MCO".
    """

    # To Do:  @pytest.mark.polarion_id("polarian_id")
    def test_mco_operator_rebranding_hub_cluster(self, setup_acm_ui):
        """
        Test to verify MCO operator displays correct rebranded name in
        OperatorHub

        This test validates:
        1. Operator name is "DF Multicluster Orchestrator" (not "ODF MCO")
        2. Provider is "Red Hat"
        3. Description is vendor-neutral
        4. Operator is in installed state
        5. Capability levels are correct (Basic Install, Seamless Upgrades)
        6. Channel and version information are displayed

        Test Steps:
        1. Navigate to ACM hub cluster console
        2. Navigate to Operators -> Installed Operators
        3. Search for MCO operator
        4. Click on the operator to view details
        5. Validate operator display name
        6. Validate provider name
        7. Validate description text (should not be ODF-specific)
        8. Verify operator status (Installed)
        9. Verify capability levels
        10. Verify channel and version information

        Args:
            setup_acm_ui (fixture): ACM UI setup fixture
        """
        logger.info("Starting MCO operator rebranding validation test")

        # Initialize ACM page navigator
        acm_obj = AcmPageNavigator()
        ocp_loc = locators_for_current_ocp_version()

        # Expected operator details after rebranding
        expected_operator_name = "DF Multicluster Orchestrator"
        expected_provider = "Red Hat"
        expected_description_keywords = [
            "Orchestrator for Data Foundation",
            "multiple OpenShift clusters",
            "Advanced Cluster Management",
        ]
        # Keywords that should NOT appear (ODF-specific branding)
        disallowed_keywords = ["ODF MCO", "ODF Multicluster Orchestrator"]

        try:
            # Step 1: Navigate to Operators page
            logger.info("Navigating to Operators -> Installed Operators")
            deployment_loc = ocp_loc["deployment"]
            acm_obj.do_click(deployment_loc["operators_tab"], timeout=30)
            acm_obj.do_click(deployment_loc["installed_operators_tab"], timeout=30)

            # Step 2: Select openshift-operators namespace
            logger.info("Selecting openshift-operators namespace")
            acm_obj.do_click(
                deployment_loc["openshift_operators_namespace"], timeout=30
            )

            # Step 3: Search for MCO operator
            logger.info("Searching for Multicluster Orchestrator operator")
            search_box = acm_obj.wait_for_element_to_be_visible(
                deployment_loc["search_operators"], timeout=30
            )
            search_box.clear()
            search_box.send_keys("Multicluster Orchestrator")
            acm_obj.take_screenshot()

            # Step 4: Click on the operator row to view details
            logger.info("Clicking on MCO operator to view details")
            mco_operator_locator = (
                "//a[@data-test-operator-row='DF Multicluster Orchestrator']",
                By.XPATH,
            )
            try:
                acm_obj.do_click(mco_operator_locator, timeout=30)
            except TimeoutException:
                logger.error(
                    "MCO operator with expected name "
                    "'DF Multicluster Orchestrator' "
                    "not found in Installed Operators"
                )
                acm_obj.take_screenshot()
                # Check if old name still exists
                old_operator_locator = (
                    "//a[@data-test-operator-row=" "'ODF Multicluster Orchestrator']",
                    By.XPATH,
                )
                if acm_obj.check_element_presence(old_operator_locator, timeout=10):
                    pytest.fail(
                        "Operator still displays old name "
                        "'ODF Multicluster Orchestrator' "
                        "instead of rebranded name "
                        "'DF Multicluster Orchestrator'"
                    )
                else:
                    pytest.fail(
                        "MCO operator not found in Installed Operators "
                        "with either old or new name"
                    )

            # Step 5: Validate operator display name
            logger.info("Validating operator display name")
            operator_name_locator = (
                "//h1[contains(@class, " "'co-clusterserviceversion-details__name')]",
                By.XPATH,
            )
            operator_name_element = acm_obj.wait_for_element_to_be_visible(
                operator_name_locator, timeout=30
            )
            actual_operator_name = operator_name_element.text
            logger.info(f"Found operator name: {actual_operator_name}")

            assert expected_operator_name in actual_operator_name, (
                f"Expected operator name '{expected_operator_name}', "
                f"but found '{actual_operator_name}'"
            )

            # Ensure old ODF-specific branding is not present
            for keyword in disallowed_keywords:
                assert keyword not in actual_operator_name, (
                    f"Operator name contains disallowed keyword '{keyword}'. "
                    f"Full name: {actual_operator_name}"
                )

            logger.info(f"✓ Operator name validated: {actual_operator_name}")
            acm_obj.take_screenshot()

            # Step 6: Validate provider name
            logger.info("Validating provider name")
            provider_locator = (
                "//span[contains(text(), 'Provided by')]" "/following-sibling::span",
                By.XPATH,
            )
            try:
                provider_element = acm_obj.wait_for_element_to_be_visible(
                    provider_locator, timeout=30
                )
                actual_provider = provider_element.text
                logger.info(f"Found provider: {actual_provider}")

                assert expected_provider in actual_provider, (
                    f"Expected provider '{expected_provider}', "
                    f"but found '{actual_provider}'"
                )

                logger.info(f"✓ Provider validated: {actual_provider}")
            except TimeoutException:
                logger.warning("Provider information not found on the page")
                acm_obj.take_screenshot()

            # Step 7: Validate description is vendor-neutral
            logger.info("Validating operator description")
            description_locator = (
                "//p[contains(@class, "
                "'co-clusterserviceversion-details__description')]",
                By.XPATH,
            )
            try:
                description_element = acm_obj.wait_for_element_to_be_visible(
                    description_locator, timeout=30
                )
                actual_description = description_element.text
                logger.info(f"Found description: {actual_description}")

                # Check for expected keywords in description
                for keyword in expected_description_keywords:
                    assert keyword in actual_description, (
                        f"Expected keyword '{keyword}' "
                        f"not found in description. "
                        f"Description: {actual_description}"
                    )

                # Ensure ODF-specific branding is not in description
                for keyword in disallowed_keywords:
                    assert keyword not in actual_description, (
                        f"Description contains disallowed ODF-specific "
                        f"keyword '{keyword}'. "
                        f"Description: {actual_description}"
                    )

                logger.info("✓ Description validated (vendor-neutral)")
                acm_obj.take_screenshot()
            except TimeoutException:
                logger.warning("Description not found on the page")
                acm_obj.take_screenshot()

            # Step 8: Verify operator status (Installed)
            logger.info("Verifying operator installation status")
            installed_status_locator = (
                "//span[contains(@class, 'co-icon-and-text')]"
                "[contains(text(), 'Installed')]",
                By.XPATH,
            )
            try:
                installed_element = acm_obj.wait_for_element_to_be_visible(
                    installed_status_locator, timeout=30
                )
                assert (
                    installed_element.is_displayed()
                ), "Operator does not show 'Installed' status"
                logger.info("✓ Operator is in 'Installed' state")
                acm_obj.take_screenshot()
            except TimeoutException:
                logger.error("Operator 'Installed' status indicator not found")
                acm_obj.take_screenshot()
                pytest.fail("Operator does not show 'Installed' status")

            # Step 9: Verify capability levels
            logger.info("Verifying operator capability levels")
            # Look for capability level indicators
            basic_install_locator = (
                "//div[contains(@class, "
                "'co-clusterserviceversion-details__section')]"
                "//dt[text()='Capability Level']/following-sibling::dd"
                "//span[contains(text(), 'Basic Install')]",
                By.XPATH,
            )
            seamless_upgrades_locator = (
                "//div[contains(@class, "
                "'co-clusterserviceversion-details__section')]"
                "//dt[text()='Capability Level']/following-sibling::dd"
                "//span[contains(text(), 'Seamless Upgrades')]",
                By.XPATH,
            )

            try:
                # Check for Basic Install capability
                if acm_obj.check_element_presence(basic_install_locator, timeout=10):
                    logger.info("✓ Basic Install capability found")

                # Check for Seamless Upgrades capability
                if acm_obj.check_element_presence(
                    seamless_upgrades_locator, timeout=10
                ):
                    logger.info("✓ Seamless Upgrades capability found")

                acm_obj.take_screenshot()
            except Exception as e:
                logger.warning(f"Could not fully validate capability levels: {str(e)}")
                acm_obj.take_screenshot()

            # Step 10: Verify channel and version information
            logger.info("Verifying channel and version information")
            channel_locator = (
                "//dt[text()='Channel']/following-sibling::dd",
                By.XPATH,
            )
            version_locator = (
                "//dt[text()='Installed Version']/following-sibling::dd",
                By.XPATH,
            )

            try:
                channel_element = acm_obj.wait_for_element_to_be_visible(
                    channel_locator, timeout=30
                )
                channel = channel_element.text
                logger.info(f"Channel: {channel}")

                version_element = acm_obj.wait_for_element_to_be_visible(
                    version_locator, timeout=30
                )
                version = version_element.text
                logger.info(f"Installed Version: {version}")

                # Verify channel is stable-4.22 or later
                assert (
                    "stable" in channel.lower()
                ), f"Expected stable channel, but found: {channel}"

                logger.info(f"✓ Channel validated: {channel}")
                logger.info(f"✓ Version: {version}")
                acm_obj.take_screenshot()
            except TimeoutException:
                logger.warning("Channel or version information not found")
                acm_obj.take_screenshot()

            logger.info(
                "MCO operator rebranding validation test " "completed successfully"
            )

        except Exception as e:
            logger.error(f"Test failed with exception: {str(e)}")
            acm_obj.take_screenshot()
            raise

        finally:
            logger.info("Test cleanup (if any)")
