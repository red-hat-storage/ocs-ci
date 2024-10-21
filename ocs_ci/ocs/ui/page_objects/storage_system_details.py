from ocs_ci.ocs import constants
from ocs_ci.ocs.ui.base_ui import logger, BaseUI
from ocs_ci.ocs.ui.page_objects.storage_system_tab import StorageSystemTab
from ocs_ci.utility import version


class StorageSystemDetails(StorageSystemTab):
    def __init__(self):
        StorageSystemTab.__init__(self)

    def nav_details_overview(self):
        logger.info("Click on Overview tab")
        if self.ocp_version_semantic >= version.VERSION_4_14:
            self.do_click(
                self.validation_loc["storagesystems_overview"], enable_screenshot=True
            )
        else:
            self.do_click(self.validation_loc["odf-overview"], enable_screenshot=True)

    def nav_details_object(self):
        """
        Accessible only at StorageSystems / StorageSystem details / Overview
        ! At 'StorageSystems / StorageSystem details / BlockPools' Object page is not accessible
        """
        logger.info("Click on 'Object' tab")
        if (
            self.ocp_version_semantic == version.VERSION_4_11
            and self.ocs_version_semantic == version.VERSION_4_10
        ):
            self.do_click(
                self.validation_loc["object-odf-4-10"], enable_screenshot=True
            )
        else:
            self.do_click(self.validation_loc["object"], enable_screenshot=True)

    def nav_block_and_file(self):
        """
        Accessible only at StorageSystems / StorageSystem details / Overview
        ! At 'StorageSystems / StorageSystem details / BlockPools' Block and file page is not accessible
        """
        logger.info("Click on 'Block and File' tab")
        if (
            self.ocp_version_semantic == version.VERSION_4_11
            and self.ocs_version_semantic == version.VERSION_4_10
        ):
            self.do_click(
                self.validation_loc["blockandfile-odf-4-10"], enable_screenshot=True
            )
        else:
            self.do_click(self.validation_loc["blockandfile"], enable_screenshot=True)

        from ocs_ci.ocs.ui.page_objects.block_and_file import BlockAndFile

        return BlockAndFile()

    def nav_cephblockpool_verify_statusready(self):
        """
        Initial page - Data Foundation / Storage Systems tab / StorageSystem details
        Navigate to ocs-storagecluster-cephblockpool
        Verify cephblockpool status is 'Ready'

        Raises:
            CephHealthException if cephblockpool_status != 'Ready'
        """
        self.page_has_loaded(5, 5)
        self.nav_ceph_blockpool().verify_cephblockpool_status()

    def nav_ceph_blockpool(self):
        """
        Navigate to Block pools (for version 4.16 or lower)/Storage pools tab
        """
        logger.info("Click on 'BlockPools' tab")
        if (
            self.ocp_version_semantic == version.VERSION_4_11
            and self.ocs_version_semantic == version.VERSION_4_10
        ):
            self.do_click(
                self.validation_loc["blockpools-odf-4-10"],
                enable_screenshot=True,
            )
        else:
            self.do_click(self.validation_loc["blockpools"], enable_screenshot=True)
        self.page_has_loaded(retries=15, sleep_time=2)

        from ocs_ci.ocs.ui.page_objects.block_pools import BlockPools

        return BlockPools()

    def get_blockpools_compression_status_from_storagesystem(self) -> tuple:
        """
        Initial page - Data Foundation / Storage Systems tab / StorageSystem details / ocs-storagecluster-cephblockpool
        Get compression status from storagesystem details and ocs-storagecluster-cephblockpool

        Returns:
            tuple: String representation of 'Compression status' from StorageSystem details page and
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

    def navigate_backward(self):
        BaseUI.navigate_backward(self)
        return StorageSystemTab()

    def nav_storage_systems_via_breadcrumb(self):
        """
        Navigate to StorageSystems via breadcrumb

        Returns:
            StorageSystemTab: StorageSystemTab page object
        """
        logger.info("Click on StorageSystems breadcrumb")
        self.do_click((self.validation_loc["storagesystems"]))
        return StorageSystemTab()

    def check_only_one_block_pools_tab(self):
        """
        Verify that only one BlockPools tab is present. BZ #2096513
        """
        logger.info("Verify that only one BlockPools tab is present. BZ #2096513")
        blockpools_tabs = self.get_elements(self.validation_loc["blockpools"])
        return len(blockpools_tabs) == 1

    def is_storage_system_details_breadcrumb_present(self):
        """
        Verify that Storage System Details breadcrumb is present
        """
        logger.info("Verify that Storage System Details breadcrumb is present")
        is_present = (
            len(self.get_elements(self.validation_loc["storagesystem-details"])) == 1
        )
        if not is_present:
            logger.warning("Storage System Details breadcrumb is not present")
        return is_present
