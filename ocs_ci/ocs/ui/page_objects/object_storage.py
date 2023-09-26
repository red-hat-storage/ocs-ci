from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator, logger
from ocs_ci.ocs.ui.page_objects.resource_list import ResourceList


class ObjectStorage(PageNavigator, ResourceList):
    """
    Object Service page object under PageNavigator / Storage (version 4.14 and above)
    """

    def __init__(self):
        super().__init__()

    def nav_backing_store_tab(self):
        """
        Navigate to Backing Store tab. Accessible from Object Storage page

        Returns:
            BackingStoreTab: BackingStoreTab page object
        """
        logger.info("Navigate to Data Foundation - Backing Store tab")
        self.do_click(self.validation_loc["osc_backing_store_tab"])

        from ocs_ci.ocs.ui.page_objects.backing_store_tab import BackingStoreTab

        return BackingStoreTab()

    def nav_bucket_class_tab(self):
        """
        Navigate to Bucket class tab. Accessible from Object Storage page

        Returns:
            BucketClassTab: BucketClassTab page object
        """
        logger.info("Navigate to Data Foundation - Bucket class tab")
        self.do_click(locator=self.validation_loc["osc_bucket_class_tab"])

        from ocs_ci.ocs.ui.page_objects.bucket_class_tab import BucketClassTab

        return BucketClassTab()

    def nav_namespace_store_tab(self):
        """
        Navigate to Namespace Store tab. Accessible from Object Storage page

        Returns:
            NameSpaceStoreTab: NameSpaceStoreTab page object
        """
        logger.info("Navigate to Data Foundation - Namespace Store tab")
        self.do_click(
            locator=self.validation_loc["namespacestore_page"], enable_screenshot=True
        )

        from ocs_ci.ocs.ui.page_objects.namespace_store_tab import NameSpaceStoreTab

        return NameSpaceStoreTab()

    def nav_object_buckets_tab(self):
        """
        Navigate to Object Buckets tab. Accessible from Object Storage page

        Returns:
            ObjectBucketTab: ObjectBucketTab page object
        """
        self.do_click(
            locator=self.page_nav["object_buckets_tab"], enable_screenshot=False
        )

        from ocs_ci.ocs.ui.page_objects.object_buckets_tab import ObjectBucketsTab

        return ObjectBucketsTab()

    def nav_object_buckets_claims_tab(self):
        """
        Navigate to Object Buckets Claims tab. Accessible from Object Storage page

        Returns:
            ObjectBucketClaimTab: ObjectBucketClaimTab page object
        """
        self.do_click(locator=self.obc_loc["obc_menu_name"], enable_screenshot=False)

        from ocs_ci.ocs.ui.page_objects.object_bucket_claims_tab import (
            ObjectBucketClaimsTab,
        )

        return ObjectBucketClaimsTab()

    def select_project(self, cluster_namespace):
        """
        Helper function to select a project via UI. e.g. 'openshift-storage'

        Args:
            cluster_namespace (str): project name will be selected from the list

        """
        logger.info("Select openshift-storage project")
        self.do_click(self.generic_locators["project_selector"])
        self.wait_for_namespace_selection(project_name=cluster_namespace)
