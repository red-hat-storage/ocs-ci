import logging
import re
import pytest
from time import sleep

from ocs_ci.ocs.ui.page_objects.fusion_access_ui import FusionAccessUI
from ocs_ci.framework.testlib import (
    ui,
    ManageTest,
)
from ocs_ci.framework.pytest_customization.marks import (
    magenta_squad,
    fusion_access_required,
    ignore_leftovers,
)
from ocs_ci.framework import config
from ocs_ci.ocs import ocp
from ocs_ci.helpers.helpers import (
    create_unique_resource_name,
)

logger = logging.getLogger(__name__)


@ui
@magenta_squad
@fusion_access_required
@ignore_leftovers
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
    def setup_lun_discovery(self):
        """
        Run LUN discovery on all worker nodes before test execution.

        Discovers and logs in to the iSCSI target on every worker node so that
        the LUNs are visible on the UI when the test runs.

        Reads the following keys from ENV_DATA (set in your private cluster config):
            san_iscsi_ip:  IP of the iSCSI target portal, e.g. "192.168.1.100"
            san_iscsi_iqn: IQN of the iSCSI target, e.g. "iqn.2023-01.com.example:storage"
        """
        iscsi_ip = config.ENV_DATA.get("san_iscsi_ip")
        iscsi_iqn = config.ENV_DATA.get("san_iscsi_iqn")

        # Validate values before interpolating into a shell command.
        # IP must be a dotted-decimal address; IQN must follow the iqn. format.
        if not re.fullmatch(r"[\d]{1,3}(\.[\d]{1,3}){3}", iscsi_ip or ""):
            raise ValueError(f"san_iscsi_ip '{iscsi_ip}' is not a valid IPv4 address")
        if not re.fullmatch(r"iqn\.\d{4}-\d{2}\.[a-zA-Z0-9.\-:]+", iscsi_iqn or ""):
            raise ValueError(f"san_iscsi_iqn '{iscsi_iqn}' is not a valid IQN")

        logger.info(
            f"Starting LUN discovery on all worker nodes "
            f"(portal: {iscsi_ip}, iqn: {iscsi_iqn})"
        )
        ocp_obj = ocp.OCP()
        ocp_obj.exec_oc_cmd(
            f"get nodes -l node-role.kubernetes.io/worker --no-headers -o name "
            f"| xargs -I {{}} -- oc debug {{}} -- bash -c "
            f'\'chroot /host iscsiadm -m discovery -t st -p "{iscsi_ip}"; '
            f'chroot /host iscsiadm --mode node --target "{iscsi_iqn}" '
            f'--portal "{iscsi_ip}" -l\' 2> /dev/null',
            shell=True,
        )
        logger.info("LUN discovery completed on all worker nodes")
        logger.info("Waiting 2 minute for LUNs to become visible on the UI...")
        sleep(120)

    @pytest.fixture(autouse=True)
    def setup_ui(self, setup_ui_class_factory):
        """
        Setup UI session for the test class.

        Args:
            setup_ui_class_factory: Factory fixture to setup UI session
        """
        self.setup_ui_class_factory = setup_ui_class_factory

    @pytest.mark.polarion_id("OCS-5500")
    def test_connect_san_storage_and_create_filesystem(
        self,
    ):
        """
        Test to connect SAN storage and create file system via UI.

        Test Steps:
        1. Log into the OpenShift console and navigate to Storage > External systems
        2. On the External systems page, Click on "Create External System" Button
        3. On the Connect to external storage page, select Storage Area Network radio button
        4. Click Next
        4a. Enter the Image registry URL
        4b. Enter the Image repository name
        4c. Select the Secret key from the dropdown
        4d. Create a docker-registry secret in ibm-spectrum-scale namespace with
            Quay credentials fetched from config (san_quay_server, san_quay_username,
            san_quay_password, san_quay_email)
        5. On Connect Storage Area Network page, select AllNodes (Default) radio button
        6. Provide LUN group name in the Name text field under LUN group details
        7. Select a subset of LUNs from the table
        8. Click on Connect and Create
        9. Navigate to the external systems tab
        10. Wait for the file system to be created
        11. Verify the connection and file system status

        Required config keys in your private cluster config YAML:
            ENV_DATA:
              san_image_registry_url: "quay.io"        # defaults to quay.io if not set
              san_image_repository_name: "org/repo"
              san_quay_server: "quay.io/org"
              san_quay_username: "your-username"
              san_quay_password: "your-password"  # pragma: allowlist secret
              san_quay_email: "your-email@example.com"
              san_iscsi_ip: "19X.16XX.1.1XX"            # iSCSI target portal IP
              san_iscsi_iqn: "iqn.202X-XX.com.example:storage"  # iSCSI target IQN

        """
        logger.info("Starting FDF SAN connection test")

        # Initialise UI session and page object — FusionAccessUI inherits both
        self.setup_ui_class_factory()
        fusion_access = FusionAccessUI()

        # Step 1: Navigate to External Storage Systems page
        logger.info("Step 1: Navigate to Storage > External systems")
        fusion_access.nav_external_systems_page()
        fusion_access.take_screenshot("external_systems_page")

        # Step 2: On the external systems page click on “Create External system” button
        logger.info("Step 2: Click on Connect External system")
        fusion_access.click_connect_external_systems()

        # Step 3: Select Storage Area Network radio button
        logger.info("Step 3: Select Storage Area Network option")
        fusion_access.select_storage_area_network()
        fusion_access.take_screenshot("san_selected")

        # Step 4: Click Next button
        logger.info("Step 4: Click Next")
        fusion_access.click_next_button()
        fusion_access.take_screenshot("san_configuration_page")

        # Step 4a: Enter Image registry URL
        image_registry_url = config.ENV_DATA.get("san_image_registry_url", "quay.io")
        logger.info(f"Step 4a: Enter Image registry URL: {image_registry_url}")
        fusion_access.enter_image_registry_url(image_registry_url)
        fusion_access.take_screenshot("image_registry_url_entered")

        # Step 4b: Enter Image repository name
        image_repository_name = config.ENV_DATA.get("san_image_repository_name")
        logger.info(f"Step 4b: Enter Image repository name: {image_repository_name}")
        fusion_access.enter_image_repository_name(image_repository_name)
        fusion_access.take_screenshot("image_repository_name_entered")

        # Step 4c: Create docker-registry secret in ibm-spectrum-scale namespace
        secret_name = "quayio-secret"
        quay_server = config.ENV_DATA.get("san_quay_server")
        quay_username = config.ENV_DATA.get("san_quay_username")
        quay_password = config.ENV_DATA.get("san_quay_password")
        quay_email = config.ENV_DATA.get("san_quay_email")
        logger.info(
            f"Step 4c: Creating docker-registry secret '{secret_name}' "
            f"in ibm-spectrum-scale namespace"
        )
        ocp_obj = ocp.OCP(kind="secret", namespace="ibm-spectrum-scale")
        ocp_obj.exec_oc_cmd(
            f"create secret docker-registry {secret_name} "
            f"--docker-server='{quay_server}' "
            f"--docker-username='{quay_username}' "
            f"--docker-password='{quay_password}' "
            f"--docker-email='{quay_email}'",
            secrets=[quay_password, quay_username],
        )
        logger.info(f"Waiting for secret '{secret_name}' to be created...")
        ocp_obj.wait_for_resource(
            condition="kubernetes.io/dockerconfigjson",
            resource_name=secret_name,
            column="TYPE",
            timeout=60,
        )
        logger.info(f"Secret '{secret_name}' created successfully")

        # Step 4d: Select Secret key from dropdown
        logger.info("Step 4d: Select Secret key from dropdown")
        fusion_access.select_secret_key()
        fusion_access.take_screenshot("secret_key_selected")

        # Step 5: Select AllNodes (Default) radio button
        logger.info("Step 5: Select All Nodes (Default)")
        fusion_access.select_all_nodes_option()
        fusion_access.take_screenshot("all_nodes_selected")

        # Step 6: Provide LUN group name
        lun_group_name = create_unique_resource_name("test", "lungroup")
        logger.info(f"Step 6: Enter LUN group name: {lun_group_name}")
        fusion_access.enter_lun_group_name(lun_group_name)
        fusion_access.take_screenshot("lun_group_name_entered")

        # Step 7: Select LUNs from the table
        logger.info("Step 7: Select LUNs from the table")
        selected_luns = fusion_access.select_luns_from_table()
        logger.info(f"Selected LUNs: {selected_luns}")
        fusion_access.take_screenshot("luns_selected")

        # Step 8: Click Connect and Create
        logger.info("Step 8: Click Connect and Create")
        fusion_access.click_connect_and_create()
        fusion_access.take_screenshot("connection_initiated")

        # Step 9: Wait for backend to process the request before navigating away
        logger.info("Step 9: Waiting 60 seconds for backend to process the request...")
        sleep(60)

        # Step 10: Refresh browser and wait for page to fully settle
        logger.info("Step 10: Refreshing browser and waiting for page to load...")
        fusion_access.refresh_page()
        fusion_access.page_has_loaded()
        sleep(10)
        fusion_access.take_screenshot("browser_refreshed")

        # Step 10a: Navigate to external systems page and click on SAN_Storage.
        logger.info("Step 10a: Navigate to Storage > External systems > SAN_Storage")
        fusion_access.page_has_loaded()
        fusion_access.nav_external_systems_page()
        fusion_access.navigate_to_san_storage_tab()
        fusion_access.take_screenshot("file_systems_tab")

        # Step 11: Wait for LUN group creation and verify SAN Scale state
        logger.info("Step 11: Wait for LUN group creation and verify SAN Scale state")
        logger.info("Waiting for filesystem creation")
        fusion_access.wait_for_filesystem_and_verify_connection(
            lun_group_name=lun_group_name,
        )
