import logging
import pytest
import re

from ocs_ci.ocs.resources.pod import get_pod_logs
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.framework.testlib import (
    tier1,
    skipif_ui_not_support,
    skipif_ocs_version,
    polarion_id,
    ui,
    bugzilla,
)
from ocs_ci.framework.pytest_customization.marks import (
    black_squad,
    skipif_external_mode,
    skipif_mcg_only,
    skipif_ibm_cloud_managed,
    skipif_hci_provider_or_client,
    runs_on_provider,
)
from ocs_ci.ocs.ui.validation_ui import ValidationUI
from ocs_ci.utility import version

logger = logging.getLogger(__name__)


@black_squad
@skipif_ibm_cloud_managed
class TestUserInterfaceValidation(object):
    """
    Test User Interface Validation

    """

    @ui
    @runs_on_provider
    @tier1
    @polarion_id("OCS-4925")
    @skipif_ui_not_support("validation")
    def test_storage_cluster_validation_ui(self, setup_ui_class_factory):
        """
        Validate Storage Cluster status on UI

        Args:
            setup_ui_class: login function on conftest file

        """
        setup_ui_class_factory()

        validation_ui_obj = ValidationUI()
        validation_ui_obj.validate_storage_cluster_ui()

    @ui
    @runs_on_provider
    @tier1
    @bugzilla("2155743")
    @polarion_id("OCS-2575")
    @skipif_ui_not_support("validation")
    def test_dashboard_validation_ui(self, setup_ui_class_factory):
        """
        Validate User Interface of OCS/ODF dashboard
        Verify GET requests initiated by kube-probe on odf-console pod [cover bz-2155743]


        Args:
            setup_ui_class: login function on conftest file

        """
        setup_ui_class_factory()

        ocs_version = version.get_semantic_ocs_version_from_config()
        if ocs_version >= version.VERSION_4_13:
            logger.info(
                "Verify GET requests initiated by kube-probe on odf-console pod"
            )
            pod_odf_console_name = get_pod_name_by_pattern("odf-console")
            pod_odf_console_logs = get_pod_logs(pod_name=pod_odf_console_name[0])
            if (
                re.search(
                    "GET /plugin-manifest.json HTTP.*kube-probe", pod_odf_console_logs
                )
                is None
            ):
                raise ValueError("GET request initiated by kube-probe does not exist")

        validation_ui_obj = ValidationUI()
        if ocs_version >= version.VERSION_4_9:
            validation_ui_obj.odf_overview_ui()
        else:
            validation_ui_obj.verification_ui()

    @ui
    @runs_on_provider
    @tier1
    @polarion_id("OCS-4642")
    @skipif_ocs_version("<4.9")
    @skipif_ui_not_support("validation")
    def test_odf_storagesystems_ui(self, setup_ui_class_factory):
        """
        Validate User Interface for ODF Storage Systems Tab for ODF 4.9

        Args:
            setup_ui_class: login function on conftest file

        """
        setup_ui_class_factory()

        validation_ui_obj = ValidationUI()
        validation_ui_obj.odf_storagesystems_ui()

    @ui
    @tier1
    @runs_on_provider
    @skipif_ocs_version("<4.9")
    @skipif_external_mode
    @skipif_mcg_only
    @pytest.mark.bugzilla("2096414")
    @polarion_id("OCS-4685")
    @skipif_hci_provider_or_client
    def test_odf_cephblockpool_compression_status(self, setup_ui_class_factory):
        """
        Validate Compression status for cephblockpool at StorageSystem details and ocs-storagecluster-cephblockpool
        are matching

         Args:
            setup_ui_class: login function on conftest file

        """
        setup_ui_class_factory()

        storage_system_details = (
            ValidationUI()
            .nav_odf_default_page()
            .nav_storage_systems_tab()
            .nav_storagecluster_storagesystem_details()
        )
        storage_system_details.nav_cephblockpool_verify_statusready()

        compression_statuses = (
            storage_system_details.get_blockpools_compression_status_from_storagesystem()
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

    @ui
    @tier1
    @runs_on_provider
    @pytest.mark.bugzilla("1994584")
    def test_ocs_operator_is_not_present(self, setup_ui_class_factory):
        """
        Validate odf operator is present in the installed operator tab in ui.
        """
        setup_ui_class_factory()

        validation_ui_obj = ValidationUI()
        (
            odf_operator_present,
            ocs_operator_present,
        ) = validation_ui_obj.verify_odf_without_ocs_in_installed_operator()
        assert (
            odf_operator_present
        ), "ODF operator is not present in the installed operator tab"
        assert not ocs_operator_present, (
            "OCS operator is present in the installed operator tab, expected to see only ODF "
            "operator"
        )
