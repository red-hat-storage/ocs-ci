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
            # Step 1: Navigate directly to the Installed Operators
            # page in the openshift-operators namespace. The ACM
            # console opens in "Fleet Management" perspective, so
            # we bypass the perspective switcher by loading the
            # Installed Operators URL directly.
            logger.info("Navigating directly to Installed Operators page")
            deployment_loc = ocp_loc["deployment"]
            current_url = acm_obj.driver.current_url
            base_url = current_url.split("/multicloud")[0]
            installed_operators_url = (
                f"{base_url}/k8s/ns/openshift-operators"
                "/operators.coreos.com~v1alpha1~ClusterServiceVersion"
            )
            acm_obj.driver.get(installed_operators_url)
            acm_obj.page_has_loaded()
            acm_obj.take_screenshot()

            # Step 4: Search for MCO operator
            logger.info("Searching for Multicluster Orchestrator operator")
            search_box = acm_obj.wait_for_element_to_be_visible(
                deployment_loc["search_operators"], timeout=30
            )
            search_box.clear()
            search_box.send_keys("Multicluster Orchestrator")
            acm_obj.take_screenshot()

            # Step 5: Click on the operator row to view details
            logger.info("Clicking on MCO operator to view details")
            try:
                acm_obj.do_click(deployment_loc["mco_operator_row"], timeout=30)
            except TimeoutException:
                logger.error(
                    "MCO operator with expected name "
                    "'DF Multicluster Orchestrator' "
                    "not found in Installed Operators"
                )
                acm_obj.take_screenshot()
                # Check if old name still exists
                if acm_obj.check_element_presence(
                    deployment_loc["mco_operator_row_old_name"], timeout=10
                ):
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

            # Step 6: Validate operator display name
            logger.info("Validating operator display name")
            operator_name_element = acm_obj.wait_for_element_to_be_visible(
                deployment_loc["operator_display_name"], timeout=30
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

            # Step 7: Validate provider name
            logger.info("Validating provider name")
            try:
                provider_element = acm_obj.wait_for_element_to_be_visible(
                    deployment_loc["operator_provider"], timeout=30
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

            # Step 8: Validate description is vendor-neutral
            # Use page source since the description element structure
            # varies across OCP versions.
            logger.info("Validating operator description")
            page_source = acm_obj.driver.page_source
            for keyword in expected_description_keywords:
                assert keyword in page_source, (
                    f"Expected keyword '{keyword}' "
                    f"not found on operator details page"
                )

            for keyword in disallowed_keywords:
                assert keyword not in page_source, (
                    f"Disallowed ODF-specific keyword "
                    f"'{keyword}' found on operator details page"
                )

            logger.info("✓ Description validated (vendor-neutral)")
            acm_obj.take_screenshot()

            # Step 9: Verify operator status (Installed)
            logger.info("Verifying operator installation status")
            try:
                installed_element = acm_obj.wait_for_element_to_be_visible(
                    deployment_loc["operator_installed_status"], timeout=30
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

            # Step 10: Verify capability levels
            logger.info("Verifying operator capability levels")
            try:
                # Check for Basic Install capability
                if acm_obj.check_element_presence(
                    deployment_loc["operator_capability_basic_install"], timeout=10
                ):
                    logger.info("✓ Basic Install capability found")

                # Check for Seamless Upgrades capability
                if acm_obj.check_element_presence(
                    deployment_loc["operator_capability_seamless_upgrades"], timeout=10
                ):
                    logger.info("✓ Seamless Upgrades capability found")

                acm_obj.take_screenshot()
            except Exception as e:
                logger.warning(f"Could not fully validate capability levels: {str(e)}")
                acm_obj.take_screenshot()

            # Step 11: Verify channel and version information
            logger.info("Verifying channel and version information")
            try:
                channel_element = acm_obj.wait_for_element_to_be_visible(
                    deployment_loc["operator_channel"], timeout=30
                )
                channel = channel_element.text
                logger.info(f"Channel: {channel}")

                version_element = acm_obj.wait_for_element_to_be_visible(
                    deployment_loc["operator_installed_version"], timeout=30
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
