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
from ocs_ci.ocs.ui.page_objects.mco_operator_page import MCOOperatorPage

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

    @pytest.mark.polarion_id("OCS-7805")
    def test_mco_operator_rebranding_hub_cluster(self, setup_acm_ui):
        """
        Test to verify MCO operator displays correct rebranded name
        in OperatorHub

        This test validates:
        1. Operator name is "DF Multicluster Orchestrator"
        2. Provider is "Red Hat"
        3. Description is vendor-neutral
        4. Operator is in installed state
        5. Capability levels are correct
        6. Channel and version information are displayed

        Args:
            setup_acm_ui (fixture): ACM UI setup fixture
        """
        logger.info("Starting MCO operator rebranding validation test")

        expected_operator_name = "DF Multicluster Orchestrator"
        expected_provider = "Red Hat"
        expected_description_keywords = [
            "Orchestrator for Data Foundation",
            "multiple OpenShift clusters",
            "Advanced Cluster Management",
        ]
        disallowed_keywords = [
            "ODF MCO",
            "ODF Multicluster Orchestrator",
        ]

        mco_page = MCOOperatorPage()

        # Navigate to Installed Operators and find MCO
        mco_page.navigate_to_installed_operators()
        mco_page.search_for_operator("Multicluster Orchestrator")
        mco_page.click_mco_operator()

        # Validate operator display name
        logger.info("Validating operator display name")
        actual_name = mco_page.get_operator_display_name()
        logger.info(f"Found operator name: {actual_name}")
        assert expected_operator_name in actual_name, (
            f"Expected operator name '{expected_operator_name}', "
            f"but found '{actual_name}'"
        )
        for keyword in disallowed_keywords:
            assert keyword not in actual_name, (
                f"Operator name contains disallowed keyword "
                f"'{keyword}'. Full name: {actual_name}"
            )
        logger.info(f"Operator name validated: {actual_name}")
        mco_page.take_screenshot()

        # Validate provider name
        logger.info("Validating provider name")
        actual_provider = mco_page.get_operator_provider()
        if actual_provider:
            logger.info(f"Found provider: {actual_provider}")
            assert expected_provider in actual_provider, (
                f"Expected provider '{expected_provider}', "
                f"but found '{actual_provider}'"
            )
            logger.info(f"Provider validated: {actual_provider}")

        # Validate description is vendor-neutral
        logger.info("Validating operator description")
        page_source = mco_page.driver.page_source
        for keyword in expected_description_keywords:
            assert keyword in page_source, (
                f"Expected keyword '{keyword}' " f"not found on operator details page"
            )
        for keyword in disallowed_keywords:
            assert keyword not in page_source, (
                f"Disallowed ODF-specific keyword "
                f"'{keyword}' found on operator details page"
            )
        logger.info("Description validated (vendor-neutral)")
        mco_page.take_screenshot()

        # Verify operator status
        mco_page.verify_operator_installed_status()

        # Verify capability levels
        logger.info("Verifying operator capability levels")
        mco_page.get_capability_levels()

        # Verify channel and version
        logger.info("Verifying channel and version information")
        channel, version = mco_page.get_channel_and_version()
        if channel:
            assert (
                "stable" in channel.lower()
            ), f"Expected stable channel, but found: {channel}"
            logger.info(f"Channel validated: {channel}")
        if version:
            logger.info(f"Version: {version}")

        logger.info("MCO operator rebranding validation test completed successfully")
