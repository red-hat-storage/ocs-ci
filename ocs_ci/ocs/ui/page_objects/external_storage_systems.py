import time

from selenium.common.exceptions import NoSuchElementException, TimeoutException
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import CommandFailed, TimeoutExpiredError
from ocs_ci.ocs.ui.base_ui import logger, wait_for_element_to_be_clickable
from ocs_ci.ocs.ui.helpers_ui import format_locator
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.ui.page_objects.block_and_file import BlockAndFile
from ocs_ci.ocs.ui.page_objects.data_foundation_tabs_common import (
    DataFoundationDefaultTab,
)
from ocs_ci.ocs.ui.page_objects.resource_list import ResourceList


class ExternalSystems(ResourceList):
    """
    UI representation of External Systems page - a resource list of External Storage Cluster(s)
    1. Navigation: PageNavigator (Storage) / Data Foundation Overview / External Systems
    2. Navigation: PageNavigator (Storage) / External Systems

    Possible actions via kebab menu per resource: edit labels, edit annotations, Edit storage system
    Possible to navigate to a resource page via name link
    """

    def __init__(self):
        super().__init__()

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

    def get_page_title(self):
        """
        Get the page title text from the External Systems page.

        Returns:
            str: The page title text
        """
        logger.info("Getting External Systems page title")
        return self.get_element_text(self.external_systems["page_title"])

    def get_empty_state_elements(self):
        """
        Collect empty state element details when no external systems
        are connected. Uses locators defined in views.py.

        Returns:
            dict: Keys:
                - heading_text (str or None): Heading text, None if not found
                - description_text (str or None): Description text, None if not found
                - connect_button_enabled (bool): True if Connect button is visible and enabled
                - explore_link_text (str or None): Explore link text, None if not found
        """
        logger.info("Collecting empty state element details")
        info = {}

        try:
            heading = self.wait_for_element_to_be_visible(
                self.external_systems["empty_state_heading"], timeout=10
            )
            info["heading_text"] = heading.text
        except TimeoutException:
            logger.warning("Empty state heading not found")
            info["heading_text"] = None

        try:
            description = self.wait_for_element_to_be_visible(
                self.external_systems["empty_state_description"], timeout=5
            )
            info["description_text"] = description.text
        except TimeoutException:
            logger.warning("Empty state description not found")
            info["description_text"] = None

        try:
            connect_button = self.wait_for_element_to_be_visible(
                self.external_systems["connect_external_system_button"], timeout=5
            )
            info["connect_button_enabled"] = connect_button.is_enabled()
        except TimeoutException:
            logger.warning("Connect external systems button not found")
            info["connect_button_enabled"] = False

        try:
            explore_link = self.wait_for_element_to_be_visible(
                self.external_systems["explore_external_systems_link"], timeout=5
            )
            info["explore_link_text"] = explore_link.text
        except TimeoutException:
            logger.warning("Explore external systems link not found")
            info["explore_link_text"] = None

        logger.info(f"Empty state elements collected: {info}")
        return info

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
        self.do_send_keys(self.external_systems["ip_address_input"], ip_address)
        self.do_click(locator=self.external_systems["next_button"])

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
        self.do_click(locator=self.external_systems["scale_next_button"])
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

        # do_click only waits for the click itself, not for the server response.
        # The breadcrumb is already present on the form page, so navigating away
        # immediately would race the connection creation. Poll until the connect
        # reaches a definitive state: either an error alert appears (failure), or
        # the console navigates to the newly created system's dashboard
        # (URL/link contains 'scale.spectrum.ibm', or the Connect button leaves
        # the form) which confirms the connection was actually created.
        connected = False
        for _ in range(30):
            if self.get_elements(self.external_systems["modal_error_alert"]):
                error_msg = self.get_element_text(
                    self.external_systems["modal_error_alert"]
                )
                raise CommandFailed(
                    f"Failed to connect scale system '{system_name}': {error_msg}"
                )
            if "scale.spectrum.ibm" in self.driver.current_url or self.get_elements(
                self.external_systems["scale_dashboard_link"]
            ):
                connected = True
                break
            # Connect button gone from the form also indicates the submit went through
            if not self.get_elements(self.external_systems["connect_scale_final"]):
                connected = True
                break
            time.sleep(2)

        if not connected:
            raise CommandFailed(
                f"Scale system '{system_name}' was not created within the expected time."
            )

        # Navigate back to the External Systems list only after the connection
        # is confirmed created.
        if self.get_elements(self.external_systems["breadcrumb-link"]):
            self.do_click(locator=self.external_systems["breadcrumb-link"])
        else:
            from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator

            PageNavigator().nav_external_systems_page()
        self.page_has_loaded(retries=10)

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

    def disconnect_scale(self, scale_name, filesystem_names):
        """
        Disconnecting a Storage Scale cluster is currently only supported via the CLI,
        which removes the connection, associated secrets, and filesystems.
        UI support for this action will be added in a future release.

        Args:
            scale_name (str): name of the scale connection
            filesystem_names (list): names of all filesystems associated with the
                connection that must be deleted (e.g. ["fs1", "fs2"])
        """
        logger.info(f"Deleting connection to {scale_name}")
        delete_cmd = "delete clusters.scale.spectrum.ibm.com ibm-spectrum-scale"
        ocp.OCP().exec_oc_cmd(delete_cmd)
        delete_secret_cmd = (
            f"delete secret {scale_name}-user-details-secret -n ibm-spectrum-scale"
        )
        ocp.OCP().exec_oc_cmd(delete_secret_cmd)
        for filesystem_name in filesystem_names:
            logger.info(f"Deleting filesystem {scale_name}-{filesystem_name}")
            delete_file_system = (
                f"delete filesystem {scale_name}-{filesystem_name} "
                "-n ibm-spectrum-scale"
            )
            ocp.OCP().exec_oc_cmd(delete_file_system)

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
        return operator_status == "Healthy" and connection_status == "Healthy"

    def connect_scale_filesystem(self, scale_name, filesystem_name):
        """
        Connect an additional scale filesystem
        """
        logger.info(f"Filtering connections to find {scale_name}")
        self.wait_for_element_to_be_visible(self.external_systems["filter"], timeout=15)
        self.do_clear(self.external_systems["filter"])
        self.do_send_keys(self.external_systems["filter"], scale_name)
        self.do_click(locator=self.external_systems["actions_button"])
        self.do_click(locator=self.external_systems["add_filesystem"])
        logger.info(f"Adding remote filesystem {filesystem_name}")
        self.do_send_keys(
            self.external_systems["filesystem_name_input"], filesystem_name
        )
        self.do_click(locator=self.external_systems["scale_add_button"])

        # do_click only waits for the click itself, not for the server response.
        # Poll until the add operation reaches a definitive state: either the
        # error modal appears, or the modal closes (Add button gone) on success.
        error_present = False
        for _ in range(15):
            if self.get_elements(self.external_systems["modal_error_alert"]):
                error_present = True
                break
            # Modal dismissed (Add button no longer present) indicates success
            if not self.get_elements(self.external_systems["scale_add_button"]):
                break
            time.sleep(2)

        # Check specifically if an error alert popped up inside the modal
        if error_present:
            error_msg = self.get_element_text(
                self.external_systems["modal_error_alert"]
            )
            logger.warning(
                f"Filesystem creation returned an error modal: {error_msg}; dismissing modal"
            )
            if self.get_elements(self.external_systems["modal_cancel_button"]):
                self.do_click(self.external_systems["modal_cancel_button"])
            raise CommandFailed(
                f"Failed to add scale filesystem '{filesystem_name}': {error_msg}"
            )

        self.page_has_loaded(retries=10)

    def delete_scale_filesystem(self, scale_name, filesystem_name):
        logger.info(f"Filtering connections to find {scale_name}")

        # Ensure modal overlay is dismissed if present using stored locator
        if self.get_elements(self.external_systems["modal_cancel_button"]):
            self.do_click(self.external_systems["modal_cancel_button"])

        if (
            "/odf/external-systems" not in self.driver.current_url
            or "scale.spectrum.ibm.com" in self.driver.current_url
        ):
            from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator

            PageNavigator().nav_external_systems_page()

        wait_for_element_to_be_clickable(self.external_systems["filter"])

        self.do_clear(self.external_systems["filter"])
        self.do_send_keys(self.external_systems["filter"], scale_name)

        # Stored views.py locator formatted without '|' OR-operator
        resource_locator = format_locator(
            self.external_systems["scale_connection_name_link"], scale_name
        )
        logger.info(f"Clicking on {scale_name} to go to Scale dashboard")
        wait_for_element_to_be_clickable(resource_locator)
        self.do_click(resource_locator)

        logger.info(f"Clicking on {filesystem_name}")
        self.do_click(
            format_locator(self.external_systems["filesystem_link"], filesystem_name)
        )
        logger.info(f"Deleting {filesystem_name}")
        self.do_click(locator=self.external_systems["actions_button"])
        self.do_click(locator=self.external_systems["delete_filesystem"])
        self.do_click(locator=self.external_systems["confirm_delete"])
        logger.info("Deletion confirmed")


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

    # --- RHSTOR-8585 Scale Dashboard enhancements --------------------------

    def _wait_for_scale_elements(self, locator, timeout=60, sleep=3):
        """
        Poll until elements matching the locator are present, tolerating absence.

        Scale dashboard and modal content renders asynchronously, so reading it
        immediately after navigation/click is racy. This polls ``get_elements``
        (which does not wait) instead of ``wait_for_element_to_be_visible`` so the
        AI locator fallback is not triggered for checks that may legitimately find
        nothing.

        Args:
            locator (tuple): (selector, By) to look for.
            timeout (int): how long to poll before giving up.
            sleep (int): seconds between polls.

        Returns:
            list: matching web elements, or an empty list if none appear in time.
        """
        try:
            for elements in TimeoutSampler(
                timeout=timeout, sleep=sleep, func=self.get_elements, locator=locator
            ):
                if elements:
                    logger.info(
                        f"Found {len(elements)} element(s) for locator {locator}"
                    )
                    return elements
        except TimeoutExpiredError:
            logger.warning(f"No elements found for locator {locator} in {timeout}s")
        return []

    def get_local_node_count(self):
        """
        Read the node count shown on the local StorageCluster card.

        Returns:
            int or None: number of nodes, or None if it cannot be parsed.
        """
        try:
            element = self.wait_for_element_to_be_visible(
                self.external_systems["scale_local_node_count_link"], timeout=30
            )
            text = element.text
        except (TimeoutException, NoSuchElementException):
            logger.warning("Local node count link not found on Scale dashboard")
            return None
        for token in text.split():
            if token.isdigit():
                return int(token)
        logger.warning(f"Could not parse node count from '{text}'")
        return None

    def open_add_nodes_modal(self, timeout=30):
        """
        Open the "Add nodes to local cluster" modal via the node-count edit pencil.

        Args:
            timeout (int): wait for the modal to become visible.

        Returns:
            bool: True if the modal opened, False otherwise.
        """
        if self.get_elements(self.external_systems["add_nodes_modal_title"]):
            return True
        logger.info("Opening 'Add nodes to local cluster' modal")
        self.do_click(self.external_systems["scale_node_count_edit_pencil"])
        try:
            self.wait_for_element_to_be_visible(
                self.external_systems["add_nodes_modal_title"], timeout=timeout
            )
            return True
        except TimeoutException:
            logger.warning("'Add nodes to local cluster' modal did not open")
            return False

    def enable_control_plane_nodes(self):
        """
        Tick "Include control plane nodes" so master node rows appear in the
        "Add nodes to local cluster" modal (TC5).
        """
        logger.info("Enabling 'Include control plane nodes'")
        self.select_checkbox_status(
            True, self.external_systems["include_control_plane_checkbox"]
        )

    def get_control_plane_node_names(self):
        """
        Read the control plane (master) node names listed in the "Add nodes to
        local cluster" modal.

        "Include control plane nodes" must be enabled first so the master rows
        are rendered. Worker rows are already part of the local cluster (their
        checkboxes are disabled), so only the control plane rows are returned.

        Returns:
            list: control plane node names present in the modal table.
        """
        elements = self._wait_for_scale_elements(
            self.external_systems["control_plane_node_name_links"]
        )
        names = [e.text.strip() for e in elements if e.text.strip()]
        logger.info(f"Control plane nodes available to add: {names}")
        return names

    def is_node_selectable(self, node_name):
        """
        Whether the node's row checkbox is enabled in the "Add nodes to local
        cluster" modal. A node already part of the local cluster has a disabled
        checkbox, so this distinguishes an addable node from one already added.

        Args:
            node_name (str): node name identifying the row.

        Returns:
            bool: True if the row checkbox exists and is enabled (node not yet
                part of the local cluster).
        """
        elements = self.get_elements(
            format_locator(self.external_systems["node_row_checkbox"], node_name)
        )
        return bool(elements) and elements[0].is_enabled()

    def select_node(self, node_name, select=True):
        """
        Select or deselect a node row checkbox by node name (idempotent).

        Control plane node rows are unchecked by default once "Include control
        plane nodes" is enabled, so a master node must be explicitly selected
        before it can be added. Using the checkbox status (rather than a raw
        click) makes this safe to call again after re-opening the modal without
        accidentally toggling the row off.

        Args:
            node_name (str): node name identifying the row.
            select (bool): True to select the row, False to deselect.
        """
        logger.info(f"{'Selecting' if select else 'Deselecting'} node '{node_name}'")
        self.select_checkbox_status(
            select,
            format_locator(self.external_systems["node_row_checkbox"], node_name),
        )

    def add_nodes_to_local_cluster(
        self, node_names=None, include_control_plane=False, timeout=60
    ):
        """
        Add nodes to the local cluster (TC1 for worker nodes, TC5 for control
        plane nodes).

        Args:
            node_names (list): node names to select; when None the console's
                default row selection is left untouched.
            include_control_plane (bool): tick "Include control plane nodes"
                before selecting rows (TC5).
            timeout (int): wait for the modal to open.

        Returns:
            bool: True if the Add button was clicked, False if the modal never
                opened.
        """
        if not self.open_add_nodes_modal(timeout=timeout):
            return False

        if include_control_plane:
            self.enable_control_plane_nodes()

        for node in node_names or []:
            self.select_node(node)

        logger.info("Clicking Add to confirm node selection")
        self.do_click(
            self.external_systems["add_nodes_add_button"], enable_screenshot=True
        )
        return True

    def cancel_add_nodes_modal(self):
        """Dismiss the "Add nodes to local cluster" modal without saving."""
        if self.get_elements(self.external_systems["add_nodes_cancel_button"]):
            self.do_click(self.external_systems["add_nodes_cancel_button"])

    def is_kernel_devel_verified(self):
        """
        Whether the "Add nodes to local cluster" modal reports the kernel-devel
        packages as verified for the selected node (used by TC5 to decide whether
        the master MCO still needs to be applied).

        Returns:
            bool: True if the verified message is present.
        """
        return bool(
            self.get_elements(self.external_systems["wizard_kernel_devel_verified"])
        )

    def is_kernel_devel_status_shown(self):
        """
        Whether the "Add nodes to local cluster" modal shows a kernel-devel
        validation status for the target nodes (either the verified state or a
        missing/warning hint). Its presence proves the UI validated kernel-devel
        packages on the selected nodes.

        Returns:
            bool: True if any kernel-devel validation status is present.
        """
        return bool(
            self.get_elements(self.external_systems["wizard_kernel_devel_status"])
        )

    def wait_for_kernel_devel_status(self, timeout=60):
        """
        Wait until the modal renders a kernel-devel validation status for the
        selected node. The status appears asynchronously after a node is
        selected, so callers must wait for it before reading the specific state
        (verified vs missing); otherwise "not yet rendered" is misread as
        "packages missing".

        Args:
            timeout (int): how long to wait for the status to appear.

        Returns:
            bool: True if a kernel-devel validation status is shown in time.
        """
        return bool(
            self._wait_for_scale_elements(
                self.external_systems["wizard_kernel_devel_status"], timeout=timeout
            )
        )

    # ---------------------------------------------------------------------- TC3

    def filesystem_capacity_columns_present(self):
        """
        Whether the file systems table exposes the Used/Total capacity columns.

        Returns:
            bool: True only when both column headers are present.
        """
        used = self._wait_for_scale_elements(
            self.external_systems["fs_used_capacity_header"]
        )
        total = self._wait_for_scale_elements(
            self.external_systems["fs_total_capacity_header"]
        )
        return bool(used) and bool(total)

    def get_filesystem_capacity(self, fs_name):
        """
        Read the Used/Total capacity cells for a given filesystem row (TC3).

        Args:
            fs_name (str): filesystem name identifying the row.

        Returns:
            dict: {"used": str or None, "total": str or None}
        """
        capacity = {"used": None, "total": None}
        try:
            capacity["used"] = self.wait_for_element_to_be_visible(
                format_locator(self.external_systems["fs_used_capacity_cell"], fs_name),
                timeout=30,
            ).text
        except (TimeoutException, NoSuchElementException):
            logger.warning(f"Used capacity cell not found for '{fs_name}'")
        try:
            capacity["total"] = self.wait_for_element_to_be_visible(
                format_locator(
                    self.external_systems["fs_total_capacity_cell"], fs_name
                ),
                timeout=30,
            ).text
        except (TimeoutException, NoSuchElementException):
            logger.warning(f"Total capacity cell not found for '{fs_name}'")
        return capacity

    # ---------------------------------------------------------------------- TC4

    def open_encryption_modal(self, timeout=30):
        """
        Open the "Enable data encryption" modal via the encryption edit pencil.

        Args:
            timeout (int): wait for the modal to become visible.

        Returns:
            bool: True if the modal opened, False otherwise.
        """
        logger.info("Opening 'Enable data encryption' modal")
        self.do_click(self.external_systems["scale_encryption_edit_pencil"])
        try:
            self.wait_for_element_to_be_visible(
                self.external_systems["encryption_modal_title"], timeout=timeout
            )
            return True
        except TimeoutException:
            logger.warning("'Enable data encryption' modal did not open")
            return False

    def enable_data_encryption(self, kms_params, timeout=30):
        """
        Enable data encryption backed by an external KMS (TC4).

        Args:
            kms_params (dict): any subset of the following KMS connection fields
                is filled if present: ``username``, ``password``, ``port``,
                ``client``, ``remote_rkm``, ``server_info``, ``tenant_id``,
                ``ca_cert`` (absolute path to a certificate file to upload).
            timeout (int): wait for the modal to open.

        Returns:
            bool: True if Save was clicked, False if the modal never opened.
        """
        if not self.open_encryption_modal(timeout=timeout):
            return False

        logger.info("Enabling the 'Enable data encryption' checkbox")
        self.select_checkbox_status(
            True, self.external_systems["enable_encryption_checkbox"]
        )

        field_locators = {
            "username": "enc_username",
            "password": "enc_password",
            "port": "enc_port",
            "client": "enc_client",
            "remote_rkm": "enc_remote_rkm",
            "server_info": "enc_server_info",
            "tenant_id": "enc_tenant_id",
        }
        for key, locator_key in field_locators.items():
            value = kms_params.get(key)
            if value is not None:
                logger.info(f"Filling encryption field '{key}'")
                self.do_send_keys(self.external_systems[locator_key], str(value))

        ca_cert = kms_params.get("ca_cert")
        if ca_cert:
            logger.info(f"Uploading CA certificate from '{ca_cert}'")
            self.do_send_keys(self.external_systems["enc_ca_cert_browse"], ca_cert)

        logger.info("Clicking Save to enable data encryption")
        self.do_click(
            self.external_systems["encryption_save_button"], enable_screenshot=True
        )
        return True

    # ---------------------------------------------------------------------- TC6

    def open_dashboard_actions(self):
        """Open the Scale Dashboard Actions dropdown (TC6)."""
        logger.info("Opening Scale dashboard Actions dropdown")
        self.do_click(self.external_systems["scale_dashboard_actions_button"])

    def is_remove_action_disabled(self):
        """
        Whether the Remove/offboard action is disabled because filesystems still
        exist, and the explanatory subtext is shown (TC6).

        Returns:
            bool: True when the Remove action is disabled and the
                "Cannot be removed if file systems exist" subtext is present.
        """
        self.open_dashboard_actions()
        remove_elements = self._wait_for_scale_elements(
            self.external_systems["dashboard_remove_action"]
        )
        if not remove_elements:
            logger.warning("Remove action not found in Actions dropdown")
            return False

        remove = remove_elements[0]
        disabled = (
            not remove.is_enabled()
            or remove.get_attribute("aria-disabled") == "true"
            or remove.get_attribute("disabled") is not None
        )
        subtext_present = bool(
            self.get_elements(self.external_systems["remove_disabled_subtext"])
        )
        return disabled and subtext_present
