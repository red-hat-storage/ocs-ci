import logging

import pytest
from ocs_ci.framework.testlib import (
    tier1,
    skipif_ui_not_support,
    skipif_ocs_version,
    polarion_id,
    ui,
)
from ocs_ci.framework.pytest_customization.marks import (
    black_squad,
    skipif_external_mode,
    skipif_mcg_only,
)
from ocs_ci.ocs.ui.validation_ui import ValidationUI
from ocs_ci.utility import version

logger = logging.getLogger(__name__)


class TestUserInterfaceValidation(object):
    """
    Test User Interface Validation

    """

    @ui
    @tier1
    @black_squad
    @polarion_id("OCS-2575")
    @skipif_ui_not_support("validation")
    def test_dashboard_validation_ui(self, setup_ui_class):
        """
        Validate User Interface of OCS/ODF dashboard

        Args:
            setup_ui_class: login function on conftest file

        """
        validation_ui_obj = ValidationUI(setup_ui_class)
        ocs_version = version.get_semantic_ocs_version_from_config()
        if ocs_version >= version.VERSION_4_9:
            validation_ui_obj.odf_overview_ui()
        else:
            validation_ui_obj.verification_ui()

    @ui
    @tier1
    @black_squad
    @polarion_id("OCS-4642")
    @skipif_ocs_version("<4.9")
    @skipif_ui_not_support("validation")
    def test_odf_storagesystems_ui(self, setup_ui_class):
        """
        Validate User Interface for ODF Storage Systems Tab for ODF 4.9

        Args:
            setup_ui_class: login function on conftest file

        """
        validation_ui_obj = ValidationUI(setup_ui_class)
        validation_ui_obj.odf_storagesystems_ui()

    @ui
    @tier1
    @black_squad
    # skipped for 4.10 - WONTFIX bz 2189623
    @skipif_ocs_version("<4.11")
    @skipif_external_mode
    @skipif_mcg_only
    @pytest.mark.bugzilla("2096414")
    @polarion_id("OCS-4685")
    def test_odf_cephblockpool_compression_status(self, setup_ui_class):
        """
        Validate Compression status for cephblockpool at StorageSystem details and ocs-storagecluster-cephblockpool
        are matching

         Args:
            setup_ui_class: login function on conftest file

        """

        validation_ui_obj = ValidationUI(setup_ui_class)
        validation_ui_obj.get_blockpools_compression_status_from_storagesystem()
        compression_statuses = (
            validation_ui_obj.get_blockpools_compression_status_from_storagesystem()
        )
        compression_status_expected = "Disabled"
        assert all(
            val == compression_status_expected for val in compression_statuses
        ), (
            "Compression status validation failed:\n"
            f"'Compression status' from StorageSystem details page = {compression_statuses[0]};\n"
            f"'Compression status' from ocs-storagecluster-cephblockpool = {compression_statuses[1]}\n"
            f"Expected: {compression_status_expected}"
        )
