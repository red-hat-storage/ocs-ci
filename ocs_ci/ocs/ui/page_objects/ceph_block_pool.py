from ocs_ci.ocs.ui.page_objects.block_and_file import BlockAndFile


class CephBlockPool(BlockAndFile):
    """
    Ceph Block Pool page Class
    Navigation: PageNavigator / Storage Cluster / Storage pools / Block pool <any>
    """

    def __init__(self):
        BlockAndFile.__init__(self)

    def get_storage_cluster_status(self):
        """
        Verify status of the Storage Cluster on ceph blockpool page, reading from the Status Card

        Returns:
            bool: True if status is Healthy, False otherwise

        """
        return self.get_element_text(self.bp_loc["status_text_in_pool"])

    def block_pool_ready(self):
        """
        Verify that the Block Pool is in 'Ready' state.

        Returns:
            bool: True if the Block Pool is in 'Ready' state, False otherwise.

        """
        return self.get_storage_cluster_status() == "Ready"

    def navigate_storage_pools_via_breadcrumb(self):
        """
        Navigate to Storage Pools page via breadcrumb link.

        Returns:
            StoragePools: StoragePools page object

        """
        self.do_click(
            self.validation_loc["breadcrumb-storage-pools"],
            enable_screenshot=True,
        )
        from ocs_ci.ocs.ui.page_objects.block_pools import StoragePools

        return StoragePools()
