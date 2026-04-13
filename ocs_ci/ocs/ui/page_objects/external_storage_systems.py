from ocs_ci.ocs import constants
from ocs_ci.ocs.ui.base_ui import logger, wait_for_element_to_be_clickable
from ocs_ci.ocs.ui.page_objects.block_and_file import BlockAndFile
from ocs_ci.ocs.ui.page_objects.data_foundation_tabs_common import (
    DataFoundationDefaultTab,
)
from ocs_ci.ocs.ui.page_objects.resource_list import ResourceList
from ocs_ci.ocs.ui.helpers_ui import format_locator
from ocs_ci.ocs import ocp
from ocs_ci.ocs.exceptions import UnexpectedNameException, UnexpectedStatusException
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
        filesystem_name,
        username=None,
        password=None,
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
        if not username:
            username = f"{system_name}_user"
            password = f"{system_name}_passw0rd"
            create_user_cmd = f"ssh root@{endpoint} -i ~/.ssh/openshift-dev.pem"
            " /usr/lpp/mmfs/gui/cli/mkuser {username} -p {password} -g CsiAdmin,ContainerOperator"
            exec_cmd(create_user_cmd)
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

    def disconnect_scale(
        self, scale_name, delete_user=False, endpoint=None, username=None
    ):
        """
        Removing a connection to scale is going to be possible in UI
        but now it's only done via CLI

        Args:
            scale_name (str): scale connection name
            delete_user (bool): True if scale user needs to be deleted, False otherwise
            endpoint (str): Scale management endpoint
            username (str): username
        """
        logger.info(f"Deleting connection to {scale_name}")
        delete_cmd = "delete clusters.scale.spectrum.ibm.com ibm-spectrum-scale"
        ocp.OCP().exec_oc_cmd(delete_cmd)
        delete_secret_cmd = (
            f"delete secret {scale_name}-user-details-secret -n ibm-spectrum-scale"
        )
        ocp.OCP().exec_oc_cmd(delete_secret_cmd)
        if delete_user:
            delete_cmd = f"ssh root@{endpoint} -i ~/.ssh/openshift-dev.pem /usr/lpp/mmfs/gui/cli/rmuser {username}"
            exec_cmd(delete_cmd)

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
        logger.info(f"Filtering connections to find {scale_name}")
        self.do_clear(self.external_systems["filter"])
        wait_for_element_to_be_clickable(self.external_systems["filter"])
        self.do_send_keys(self.external_systems["filter"], scale_name)
        logger.info(f"Clicking on {scale_name} to go to Scale dashboard")
        wait_for_element_to_be_clickable(self.external_systems["scale_dashboard_link"])
        self.do_click(self.external_systems["scale_dashboard_link"])
        logger.info(f"Clicking on {filesystem_name}")
        self.do_click(
            format_locator(self.external_systems["filesystem_link"], filesystem_name)
        )
        logger.info(f"Deleting {filesystem_name}")
        self.do_click(locator=self.external_systems["actions_button"])
        self.do_click(locator=self.external_systems["delete_filesystem"])
        self.do_click(locator=self.external_systems["confirm_delete"])
        logger.info("Deletion confirmed")

    def check_filesystem_details(self, scale_name, filesystem_name, status="Connected"):
        """
        Check filesystem details on the Filesystems card of Scale dashboard

        Args:
            scale_name (str): name of the scale cluster
            filesystem_name (str): name of the  filesystem
            status (str): the expected status of the filesystem
        """
        logger.info(f"Filtering connections to find {scale_name}")
        self.do_clear(self.external_systems["filter"])
        wait_for_element_to_be_clickable(self.external_systems["filter"])
        self.do_send_keys(self.external_systems["filter"], scale_name)
        logger.info(f"Clicking on {scale_name} to go to Scale dashboard")
        wait_for_element_to_be_clickable(self.external_systems["scale_dashboard_link"])
        self.do_click(self.external_systems["scale_dashboard_link"])
        logger.info(
            f"Checking the name of the filesystem is {scale_name}-{filesystem_name}"
        )
        filesystem_name_on_card = self.get_element_text(
            format_locator(self.external_systems["filesystem_link"], filesystem_name)
        )
        if filesystem_name_on_card != f"{scale_name}-{filesystem_name}":
            self.do_click(
                locator=self.page_nav["external_systems_page"],
            )
            raise UnexpectedNameException(
                f"Expected name: {scale_name}-{filesystem_name}."
                f" Name found on the card: {filesystem_name_on_card}"
            )
        filesystem_status_on_card = self.get_element_text(
            format_locator(self.external_systems["filesystem_status"], filesystem_name)
        )
        if filesystem_status_on_card != status:
            self.do_click(
                locator=self.page_nav["external_systems_page"],
            )
            raise UnexpectedStatusException(
                f"Expected status: {status}. Status found on the card: {filesystem_status_on_card}"
            )
        self.do_click(
            locator=self.page_nav["external_systems_page"],
        )

    def get_scale_version_from_dashboard(self, scale_name):
        """
        Get scale version from the dashboard

        Args:
            scale_name (str): name of the scale cluster

        Returns:
            str: scale version found on the scale dashboard

        """
        logger.info(f"Filtering connections to find {scale_name}")
        self.do_clear(self.external_systems["filter"])
        wait_for_element_to_be_clickable(self.external_systems["filter"])
        self.do_send_keys(self.external_systems["filter"], scale_name)
        logger.info(f"Clicking on {scale_name} to go to Scale dashboard")
        wait_for_element_to_be_clickable(self.external_systems["scale_dashboard_link"])
        self.do_click(self.external_systems["scale_dashboard_link"])
        scale_version_ui = self.get_element_text(
            locator=self.external_systems["scale_version"]
        )
        logger.info(f"Scale version on the dashboard is {scale_version_ui}")
        self.do_click(
            locator=self.page_nav["external_systems_page"],
        )
        return scale_version_ui

    def get_scale_version_from_remotecluster(self, scale_name):
        """
        Get scale version from the remotecluster CR

        Args:
            scale_name (str): name of the scale cluster

        Returns:
            str: scale version found in the remotecluster CR

        """
        remotecluster_obj = ocp.OCP(
            kind="remotecluster",
            namespace="ibm-spectrum-scale",
            resource_name=scale_name,
        )
        scale_version_from_remotecluster = (
            remotecluster_obj.get().get("status").get("guiVersion")
        )
        scale_version_cli = scale_version_from_remotecluster.split("-")[0]
        logger.info(f"Scale version in Remotecluster is {scale_version_cli}")
        return scale_version_cli


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
