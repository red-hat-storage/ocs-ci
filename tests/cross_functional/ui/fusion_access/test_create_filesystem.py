import logging
import pytest

from ocs_ci.ocs.ui.page_objects.fusion_access_ui import FusionAccessUI

from ocs_ci.framework.testlib import (
    tier1,
    ui,
    ManageTest,
)
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    skipif_ocs_version,
    fusion_access_required,
)
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.ocs.ui.base_ui import BaseUI
from ocs_ci.helpers.helpers import (
    create_unique_resource_name,
)

logger = logging.getLogger(__name__)


@ui
@tier1
@green_squad
@fusion_access_required
@skipif_ocs_version(">=4.21")
class TestFDFSANConnection(ManageTest):
    """
    Test class for FDF SAN (Storage Area Network) connection UI automation.

    This class contains test cases for:
    1. Connecting to external SAN storage
    2. Creating LUN groups
    3. Verifying file system creation
    4. Validating connectivity
    """

    @pytest.fixture(autouse=True)
    def setup_ui(self, setup_ui_class_factory):
        """
        Setup UI session for the test class.

        Args:
            setup_ui_class_factory: Factory fixture to setup UI session
        """
        setup_ui_class_factory()
        self.page_nav = PageNavigator()
        self.base_ui = BaseUI()
        self.fusion_access = FusionAccessUI()

    @tier1
    @pytest.mark.polarion_id("OCS-5500")
    def test_connect_san_storage_and_create_filesystem(
        self,
        teardown_factory,
    ):
        """
        Test to connect SAN storage and create file system via UI.

        Test Steps:
        1. Log into the OpenShift console and navigate to Storage > External systems
        2. On the External systems page, Click on "Create External System" Button
        3. On the Connect to external storage page, select Storage Area Network radio button
        4. Click Next
        5. On Connect Storage Area Network page, select AllNodes (Default) radio button
        6. Provide LUN group name in the Name text field under LUN group details
        7. Select a subset of LUNs from the table
        8. Click on Connect and Create
        9. Navigate to the external systems tab
        10. Wait for the file system to be created
        11. Verify the connection and file system status

        Args:
            teardown_factory: Factory fixture for resource cleanup
        """
        logger.info("Starting FDF SAN connection test")

        # Step 1: Navigate to External Storage Systems page
        logger.info("Step 1: Navigate to Storage > External systems")
        external_systems_page = self.page_nav.nav_external_systems_page()
        self.base_ui.take_screenshot("external_systems_page")

        # Step 2: On the external systems page click on “Create External system” button
        logger.info("Step 2: Click on Create External system")
        self.fusion_access.create_external_systems_page(external_systems_page)

        # Step 3: Select Storage Area Network radio button
        logger.info("Step 3: Select Storage Area Network option")
        self.fusion_access.select_storage_area_network()
        self.base_ui.take_screenshot("san_selected")

        # Step 4: Click Next button
        logger.info("Step 4: Click Next")
        self.fusion_access.click_next_button()
        self.base_ui.take_screenshot("san_configuration_page")

        # Step 5: Select AllNodes (Default) radio button
        logger.info("Step 5: Select AllNodes (Default)")
        self.fusion_access.select_all_nodes_option()
        self.base_ui.take_screenshot("all_nodes_selected")

        # Step 6: Provide LUN group name
        lun_group_name = create_unique_resource_name("test", "lungroup")
        logger.info(f"Step 6: Enter LUN group name: {lun_group_name}")
        self.fusion_access.enter_lun_group_name(lun_group_name)
        self.base_ui.take_screenshot("lun_group_name_entered")

        # Step 7: Select LUNs from the table
        logger.info("Step 7: Select LUNs from the table")
        selected_luns = self.fusion_access.select_luns_from_table()
        logger.info(f"Selected LUNs: {selected_luns}")
        self.base_ui.take_screenshot("luns_selected")

        # Step 8: Click Connect and Create
        logger.info("Step 8: Click Connect and Create")
        self.fusion_access.click_connect_and_create()
        self.base_ui.take_screenshot("connection_initiated")

        # Step 10: Navigate to external systems page and click on SAN_Storage
        logger.info("Step 10: Navigate to Storage > External systems > SAN_Storage")
        external_systems_page = self.page_nav.nav_external_systems_page()
        self.fusion_access.navigate_to_san_storage_tab()
        self.base_ui.take_screenshot("file_systems_tab")

        # Step 11: Wait for LUN group creation and verify SAN Scale state
        logger.info("Step 11: Wait for LUN group creation and verify SAN Scale state")
        try:
            # Wait for filesystem / LUN group row to appear
            logger.info("Waiting for filesystem creation")
            filesystem_name = self.fusion_access.wait_for_filesystem_creation(
                lun_group_name
            )
            logger.info(f"File system created for LUN group: {filesystem_name}")
            self.base_ui.take_screenshot("filesystem_created")

            # Validation: Verify filesystem status
            logger.info("Validating filesystem status")
            assert self.fusion_access.verify_filesystem_status(
                filesystem_name
            ), "filesystem not in OK state"

            # Validation: Verify LUN group is connected
            logger.info("Validating LUN group connection")
            assert self.fusion_access.verify_lun_group_connection(
                lun_group_name
            ), f"LUN group '{lun_group_name}' is not connected"

        except Exception as e:
            logger.error(f"Unexpected error during SAN Scale validation: {e}")
            self.base_ui.take_screenshot("san_scale_validation_failed")
            raise


# Suggested by Bob
