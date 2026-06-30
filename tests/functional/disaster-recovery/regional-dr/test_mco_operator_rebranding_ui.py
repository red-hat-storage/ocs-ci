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
from ocs_ci.utility import version
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
        3. Description is vendor-neutral (no ODF-specific branding)
        4. Operator is in installed state
        5. Installed version is 4.22 or higher (rebranding introduced in 4.22)

        Args:
            setup_acm_ui (fixture): ACM UI setup fixture
        """
        logger.test_step("Navigate to Installed Operators page on hub cluster")

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

        mco_page.navigate_to_installed_operators()
        mco_page.search_for_operator("Multicluster Orchestrator")
        mco_page.click_mco_operator()

        logger.test_step("Validate operator display name")
        actual_name = mco_page.get_operator_display_name()
        logger.assertion(
            "Operator name: expected='%s', actual='%s'",
            expected_operator_name,
            actual_name,
        )
        assert expected_operator_name in actual_name, (
            f"Expected operator name '{expected_operator_name}', "
            f"but found '{actual_name}'"
        )
        for keyword in disallowed_keywords:
            assert keyword not in actual_name, (
                f"Operator name contains disallowed keyword "
                f"'{keyword}'. Full name: {actual_name}"
            )
        mco_page.take_screenshot()

        logger.test_step("Validate provider name")
        actual_provider = mco_page.get_operator_provider()
        if actual_provider:
            logger.assertion(
                "Provider: expected='%s', actual='%s'",
                expected_provider,
                actual_provider,
            )
            assert expected_provider in actual_provider, (
                f"Expected provider '{expected_provider}', "
                f"but found '{actual_provider}'"
            )

        logger.test_step("Validate operator description is vendor-neutral")
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
        mco_page.take_screenshot()

        logger.test_step("Verify operator installed status")
        mco_page.verify_operator_installed_status()

        logger.test_step("Verify installed version is 4.22 or higher")
        ocs_ver = version.get_semantic_ocs_version_from_config()
        logger.assertion("OCS version: expected>=4.22, actual='%s'", ocs_ver)
        assert (
            ocs_ver >= version.VERSION_4_22
        ), f"MCO rebranding requires OCS >= 4.22, but found: {ocs_ver}"
