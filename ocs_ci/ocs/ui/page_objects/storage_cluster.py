from ocs_ci.ocs import constants
from ocs_ci.ocs.ui.base_ui import (
    logger,
    wait_for_element_to_be_clickable,
)
from ocs_ci.ocs.ui.page_objects.block_and_file import BlockAndFile
from ocs_ci.ocs.ui.page_objects.block_pools import StoragePools
from ocs_ci.ocs.ui.page_objects.encryption_module import EncryptionModule
from ocs_ci.ocs.ui.page_objects.object_storage import ObjectStorage
from ocs_ci.ocs.ui.page_objects.odf_topology_tab import TopologyTab


class StorageClusterPage(
    BlockAndFile, ObjectStorage, StoragePools, TopologyTab, EncryptionModule
):
    """
    Storage cluster tab Class
    Content of Storage cluster (navigation link) / Storage cluster page (default for Storage / Storage cluster)
    """

    def __init__(self):
        BlockAndFile.__init__(self)
        ObjectStorage.__init__(self)
        StoragePools.__init__(self)
        TopologyTab.__init__(self)
        EncryptionModule.__init__(self)

    def validate_block_and_file_tab_active(self) -> bool:
        """
        Validate Block and File tab is active and encryption summary is loaded.

        Returns:
            bool: True if active, False otherwise
        """
        logger.info("Validate Block and File tab is active")
        if not self.is_block_and_file_tab():
            logger.warning("Block and File tab is not active, navigating to it")
            self.nav_block_and_file_tab()

        is_active = self.is_block_and_file_tab()
        if is_active:
            self.wait_for_encryption_summary_ready("file_and_block")
        return is_active

    def nav_cephblockpool_verify_statusready(self):
        """
        Initial page - Data Foundation / Storage pools / block pool page
        Navigate to ocs-storagecluster-cephblockpool
        Verify cephblockpool status is 'Ready'

        Raises:
            CephHealthException if cephblockpool_status != 'Ready'
        """
        self.page_has_loaded(5, 5)
        self.nav_storage_pools_tab().verify_cephblockpool_status()

    def nav_storage_pools(self):
        """
        Navigate to Block pools (for version 4.16 or lower)/Storage pools tab
        """
        logger.info("Click on 'Storage pools' tab")
        self.do_click(self.validation_loc["blockpools"], enable_screenshot=True)
        self.page_has_loaded(retries=15, sleep_time=2)

        from ocs_ci.ocs.ui.page_objects.block_pools import StoragePools

        return StoragePools()

    def set_storagecluster_annotation(self, key, value):
        """
        Set (add or update) an annotation on the StorageCluster CR via the
        OCP Console Edit Annotations dialog.

        Opens Actions → Edit Annotations, then either:
        - Edits the existing key row if the key is already present, or
        - Adds a new row and types the key and value.

        Saves and closes the dialog when done.

        Args:
            key (str): Annotation key (e.g. "uninstall.ocs.openshift.io/confirm-deletion")
            value (str): Annotation value (e.g. "false")
        """
        logger.info("Click Actions menu on StorageCluster detail page")
        self.do_click(
            self.attach_storage_loc["storage_cluster_actions"],
            enable_screenshot=True,
        )
        logger.info("Click 'Edit Annotations' menu item")
        self.do_click(
            self.attach_storage_loc["edit_annotations_menu_item"],
            enable_screenshot=True,
        )

        # Wait for the Edit Annotations dialog to open
        self.wait_for_element_to_be_visible(
            self.attach_storage_loc["annotation_key_input"],
            timeout=15,
        )

        # Find if key already exists among the key inputs
        key_inputs = self.get_elements(self.attach_storage_loc["annotation_key_input"])
        target_index = None
        for i, inp in enumerate(key_inputs):
            if inp.get_attribute("value") == key:
                target_index = i
                break

        if target_index is None:
            # Key not present — click Add More to create a new row
            logger.info(f"Annotation key '{key}' not found; adding a new row")
            self.do_click(
                self.attach_storage_loc["annotation_add_row_btn"],
                enable_screenshot=False,
            )
            # Re-fetch inputs after the new row appears
            key_inputs = self.get_elements(
                self.attach_storage_loc["annotation_key_input"]
            )
            target_index = len(key_inputs) - 1
            # Type into the specific new row — get_elements returns all inputs,
            # use the last one which is the newly added row
            key_inputs[target_index].clear()
            key_inputs[target_index].send_keys(key)
            logger.info(f"Typed annotation key '{key}' into row {target_index}")
        else:
            logger.info(
                f"Annotation key '{key}' found at row {target_index}; editing value"
            )

        # Locate the value input at the same row index and set the value
        value_inputs = self.get_elements(
            self.attach_storage_loc["annotation_value_input"]
        )
        value_inputs[target_index].clear()
        value_inputs[target_index].send_keys(value)
        logger.info(f"Set annotation {key}={value}")

        # Save
        logger.info("Click Save to apply annotations")
        self.do_click(
            self.attach_storage_loc["annotation_save_btn"],
            enable_screenshot=True,
        )
        # Wait for the page to fully reload after save before returning
        self.page_has_loaded(retries=15, sleep_time=2)
        return self

    def initiate_storagecluster_delete(self):
        """
        Open the Actions menu on the StorageCluster detail page and click
        Delete StorageCluster.

        Returns:
            ConfirmDialog: dialog instance so the caller can confirm or cancel.
        """
        from ocs_ci.ocs.ui.page_objects.confirm_dialog import ConfirmDialog

        # Ensure page is fully loaded and any post-save overlay is gone
        self.page_has_loaded(retries=15, sleep_time=2)
        logger.info("Click Actions menu on StorageCluster detail page")
        self.do_click(
            self.attach_storage_loc["storage_cluster_actions"],
            enable_screenshot=True,
        )
        logger.info("Actions menu opened")
        logger.info("Click 'Delete StorageCluster' menu item")
        self.do_click(
            self.generic_locators["delete_resource"],
            enable_screenshot=True,
        )
        logger.info("Delete StorageCluster menu item clicked")
        logger.info("Waiting for delete-action button in confirmation dialog")
        wait_for_element_to_be_clickable(
            self.generic_locators["confirm_delete_resource"], timeout=30
        )
        return ConfirmDialog()

    def get_blockpools_compression_status_from_storagesystem(self) -> tuple:
        """
        Initial page - Data Foundation / Storage Cluster / Storage pools / ocs-storagecluster-cephblockpool
        Get compression status from storagesystem details and ocs-storagecluster-cephblockpool

        Returns:
            tuple: String representation of 'Compression status' from the block pool page
            String representation of 'Compression status' from ocs-storagecluster-cephblockpool page

        """

        logger.info(
            f"Get the 'Compression status' of '{constants.DEFAULT_CEPHBLOCKPOOL}'"
        )
        compression_status_blockpools_tab = self.get_element_text(
            self.validation_loc["storagesystem-details-compress-state"]
        )
        logger.info(
            f"Click on '{constants.DEFAULT_CEPHBLOCKPOOL}' link under BlockPools tab"
        )
        self.do_click(
            self.validation_loc[constants.DEFAULT_CEPHBLOCKPOOL],
            enable_screenshot=True,
        )
        compression_status_blockpools_details = self.get_element_text(
            self.validation_loc["storagecluster-blockpool-details-compress-status"]
        )
        return compression_status_blockpools_tab, compression_status_blockpools_details
