from ocs_ci.ocs import constants
from ocs_ci.ocs.ui.base_ui import logger
from ocs_ci.ocs.ui.page_objects.InfraHealth import InfraHealthModal
from ocs_ci.ocs.ui.page_objects.block_and_file import BlockAndFile
from ocs_ci.ocs.ui.page_objects.block_pools import StoragePools
from ocs_ci.ocs.ui.page_objects.encryption_module import EncryptionModule
from ocs_ci.ocs.ui.page_objects.object_storage import ObjectStorage
from ocs_ci.ocs.ui.page_objects.odf_topology_tab import TopologyTab


class StorageClusterPage(
    BlockAndFile,
    ObjectStorage,
    StoragePools,
    TopologyTab,
    EncryptionModule,
    InfraHealthModal,
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
        Validate Overview tab is active

        Returns:
            bool: True if active, False otherwise
        """
        logger.info("Validate Block and File tab is active")
        is_default = self.is_block_and_file_tab()
        if not is_default:
            logger.warning("Block and File tab is not active")

        return is_default

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
