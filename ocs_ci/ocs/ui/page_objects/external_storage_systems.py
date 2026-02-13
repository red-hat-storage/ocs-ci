from ocs_ci.ocs import constants
from ocs_ci.ocs.ui.base_ui import logger
from ocs_ci.ocs.ui.page_objects.block_and_file import BlockAndFile
from ocs_ci.ocs.ui.page_objects.data_foundation_tabs_common import (
    DataFoundationDefaultTab,
)
from ocs_ci.ocs.ui.page_objects.resource_list import ResourceList
from ocs_ci.ocs.ui.helpers_ui import format_locator


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
        It looks different depending on whether an external system
        is already connected or not
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

        Returns:
            "Success" if there was no alert raised
            Text of the alert otherwise

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
        self.do_click(locator=self.external_systems["connect_scale_final"])
        try:
            self.wait_for_element_to_be_present(
                locator=self.external_systems["alert_description"],
                timeout=30,
            )
            alert_text = self.get_element_text(
                locator=self.external_systems["alert_description"]
            )
            return f"{alert_text}"
        except TimeoutError:
            return "Success"

    def connect_scale_filesystem(self, scale_name, filesystem_name):
        """
        Connect an additional scale filesystem

        Args:
            scale_name (str): name of the scale cluster
            filesystem_name (str): name of the additional filesystem
        """
        self.do_send_keys(self.external_systems["filter"], scale_name)
        self.do_click(locator=self.external_systems["actions_button"])
        self.do_click(locator=self.external_systems["add_filesystem"])
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
        self.do_send_keys(self.external_systems["filter"], scale_name)
        self.do_click(locator=self.external_systems["scale_link"])
        self.do_click(
            format_locator(self.external_systems["filesystem_link"]), filesystem_name
        )
        self.do_click(locator=self.external_systems["actions_button"])
        self.do_click(locator=self.external_systems["delete_filesystem"])
        self.do_click(locator=self.external_systems["confirm_delete"])


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
