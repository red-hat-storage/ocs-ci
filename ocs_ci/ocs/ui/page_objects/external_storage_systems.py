from ocs_ci.ocs import constants
from ocs_ci.ocs.ui.base_ui import logger, wait_for_element_to_be_clickable
from ocs_ci.ocs.ui.page_objects.block_and_file import BlockAndFile
from ocs_ci.ocs.ui.page_objects.data_foundation_tabs_common import (
    DataFoundationDefaultTab,
)
from ocs_ci.ocs.ui.page_objects.resource_list import ResourceList
from ocs_ci.ocs.ui.helpers_ui import format_locator
from ocs_ci.utility.utils import exec_cmd


class ExternalSystems(ResourceList):
    """
    UI representation of External Systems page - a resource list of External Storage Cluster(s)
    1. Navigation: PageNavigator (Storage) / Data Foundation Overview / External Systems
    2. Navigation: PageNavigator (Storage) / External Systems

    Possible actions via kebab menu per resource: edit labels, edit annotations, Edit storage system
    Possible to navigate to a resource page via name link
    """

    def __init__(self):
        ResourceList.__init__(self)

    def nav_to_external_storage_cluster(
        self, esc_name=constants.DEFAULT_CLUSTERNAME_EXTERNAL_MODE
    ):
        """
        Navigate to External Storage Cluster page

        Args:
            esc_name (str): External Storage Cluster name

        Returns:
            ExternalStorageCluster: ExternalStorageCluster page object
        """
        logger.info(f"Navigate to External Storage Cluster {esc_name}")
        self.nav_to_resource_via_name(esc_name)
        return ExternalStorageCluster()

    def connect_external_system(self):
        """
        Click Connect external systems button.

        """
        logger.info("Click Connect external system")
        self.do_click(locator=self.external_systems["connect_external_system"])

    def connect_flash(
        self, ip_address, username, password, pool_name, volume_mode="Thick"
    ):
        """
        Connect IBM FlashSystem as External system

        Args:
            ip_address (str): FlashSystem's IP address
            username (str): username
            password (str): password to Flashsystems
            pool_name (str): pool name
            volume_mode (str): volume mode - Thick, Thin, Compressed, Deduplicated etd
        """
        self.connect_external_system()
        logger.info("Choose Flash option")
        self.do_click(locator=self.external_systems["connect_flash"])
        self.do_click(locator=self.external_systems["next_button"])
        logger.info("Fill in the required fields")
        self.do_send_keys(self.external_systems[""], ip_address)
        self.do_click(locator=self.external_systems[""])

    def connect_scale(
        self,
        system_name,
        endpoint,
        port,
        username,
        password,
        filesystem_name,
    ):
        """
        Connect IBM Scale as External system

        Args:
            system_name (str): unique connection name
            endpoint (str): Scale management endpoint
            port (str): Port
            username (str): username
            password (str): password
            filesystem_name (str): name of the filesystem
        """
        self.connect_external_system()
        logger.info("Choose Scale option")
        self.do_click(locator=self.external_systems["connect_scale"])
        self.do_click(locator=self.external_systems["next_button"])
        logger.info("Fill in the required fields")
        self.do_send_keys(self.external_systems["scale_name"], system_name)
        self.do_send_keys(self.external_systems["mandatory_endpoit"], endpoint)
        self.do_send_keys(self.external_systems["mandatory_port"], port)
        self.do_send_keys(self.external_systems["scale_username"], username)
        self.do_send_keys(self.external_systems["scale_password"], password)
        self.do_send_keys(self.external_systems["filesystem_name"], filesystem_name)
        logger.info("Click Connect Scale")
        self.scroll_into_view(locator=self.external_systems["connect_scale_final"])
        self.do_click(
            locator=self.external_systems["connect_scale_final"], enable_screenshot=True
        )
        logger.info("Connect Scale button clicked")
        self.wait_for_element_to_be_present(
            locator=self.external_systems["breadcrumb-link"]
        )
        self.do_click(locator=self.external_systems["breadcrumb-link"])

    def scale_present_on_page(self, scale_name):
        """
        Check that scale connection with the given name
        is present on External Systems page

        Args:
            scale_name (str): scale connection name
        """
        logger.info(f"Checking if {scale_name} is present")
        find_element = self.wait_until_expected_text_is_found(
            locator=self.external_systems["scale_dashboard_link"],
            expected_text=scale_name,
            timeout=20,
        )
        if find_element:
            logger.info(f"{scale_name} found on External Systems page")
            return True
        else:
            logger.info(f"{scale_name} not found on External Systems page")
            return False

    def disconnect_scale(self, scale_name):
        """
        Removing a connection to scale is going to be possible in UI
        but now it's only done via CLI
        """
        logger.info(f"Deleting connection to {scale_name}")
        delete_cmd = "oc delete clusters.scale.spectrum.ibm.com ibm-spectrum-scale"
        exec_cmd(delete_cmd)
        delete_secret_cmd = (
            f"oc delete secret {scale_name}-user-details-secret -n ibm-spectrum-scale"
        )
        exec_cmd(delete_secret_cmd)

    def scale_status_ok(self, scale_name):
        """
        Check the status of the scale operator and connection

        Args:
            scale_name (str): name of the scale cluster

        Returns:
            True if Operator status and Connection status are Healthy
            False othewise

        """
        self.do_clear(self.external_systems["filter"])
        self.do_send_keys(self.external_systems["filter"], scale_name)
        self.do_click(locator=self.external_systems["scale_dashboard_link"])
        operator_status = self.get_element_text(
            locator=self.external_systems["scale_operator_health"]
        )
        logger.info(f"Scale operator status: {operator_status}")
        connection_status = self.get_element_text(
            locator=self.external_systems["scale_connection_health"]
        )
        logger.info(f"Scale connection status: {connection_status}")
        if operator_status == "Healthy" and connection_status == "Healthy":
            return True
        return False

    def connect_scale_filesystem(self, scale_name, filesystem_name):
        """
        Connect an additional scale filesystem

        Args:
            scale_name (str): name of the scale cluster
            filesystem_name (str): name of the additional filesystem
        """
        logger.info(f"Filtering connections to find {scale_name}")
        self.do_clear(self.external_systems["filter"])
        self.do_send_keys(self.external_systems["filter"], scale_name)
        self.do_click(locator=self.external_systems["actions_button"])
        self.do_click(locator=self.external_systems["add_filesystem"])
        logger.info(f"Adding remote filesystem {filesystem_name}")
        self.do_send_keys(
            self.external_systems["filesystem_name_input"], filesystem_name
        )
        self.do_click(locator=self.external_systems["add_button"])

    def delete_scale_filesystem(self, scale_name, filesystem_name):
        """
        Delete a scale filesystem

        Args:
            scale_name (str): name of the scale cluster
            filesystem_name (str): name of the  filesystem
        """
        self.do_clear(self.external_systems["filter"])
        wait_for_element_to_be_clickable(self.external_systems["filter"])
        self.do_send_keys(self.external_systems["filter"], scale_name)
        wait_for_element_to_be_clickable(self.external_systems["scale_dashboard_link"])
        self.do_click(self.external_systems["scale_dashboard_link"])
        self.do_click(
            format_locator(self.external_systems["filesystem_link"]), filesystem_name
        )
        self.do_click(locator=self.external_systems["actions_button"])
        self.do_click(locator=self.external_systems["delete_filesystem"])
        self.do_click(locator=self.external_systems["confirm_delete"])
        self.wait_for_element_to_be_present(
            locator=self.external_systems["breadcrumb-link"]
        )
        self.do_click(locator=self.external_systems["breadcrumb-link"])


class ExternalStorageCluster(DataFoundationDefaultTab, BlockAndFile):
    """
    UI representation of External Storage Cluster page - a resource from the list of External Systems
    1. Navigation: PageNavigator (Storage) / Data Foundation Overview / External Systems / External Storage Cluster
    from the list
    2. Navigation: PageNavigator (Storage) / External Systems / External Storage Cluster from the list
    """

    def validate_block_and_file_tab_active(self) -> bool:
        """
        Validate Overview tab is active

        Returns:
            bool: True if active, False otherwise
        """
        logger.info("Validate Block and File tab is active")
        is_default = self.is_block_and_file_tab()
        if not is_default:
            logger.warning("Block and File tab is not active")

        return is_default
