from ocs_ci.ocs import constants
from ocs_ci.ocs.ui.page_objects.page_navigator import logger
from ocs_ci.ocs.ui.page_objects.resource_list import ResourceList
from ocs_ci.ocs.ui.page_objects.encryption_module import EncryptionModule


class ObjectStorage(EncryptionModule, ResourceList):
    """
    Object Service page object under EncryptionModule(PageNavigator) / Storage
    version 4.14 and above
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

    def nav_buckets_tab(self):
        """
        Navigate to Object Buckets tab. Accessible from Object Storage page

        Returns:
            ObjectBucketTab: ObjectBucketTab page object
        """
        self.do_click(locator=self.page_nav["buckets_tab"], enable_screenshot=False)

        from ocs_ci.ocs.ui.page_objects.buckets_tab import BucketsTab

        return BucketsTab()

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

    def select_storage_provider(self, provider: str) -> None:
        """
        Select S3 storage provider (MCG or RGW) on the Object Storage page.

        Args:
            provider (str): Provider ID - constants.S3_PROVIDER_NOOBAA or constants.S3_PROVIDER_RGW_INTERNAL.

        Raises:
            ValueError: If provider is not recognized.

        """
        provider_map = {
            constants.S3_PROVIDER_NOOBAA: "provider_card_mcg",
            constants.S3_PROVIDER_RGW_INTERNAL: "provider_card_rgw",
        }
        locator_key = provider_map.get(provider)
        if not locator_key:
            raise ValueError(
                f"Unknown provider: {provider}. "
                f"Use '{constants.S3_PROVIDER_NOOBAA}' or '{constants.S3_PROVIDER_RGW_INTERNAL}'"
            )

        logger.info(f"Selecting storage provider: {provider}")
        self.do_click(self.bucket_tab[locator_key])
        self.page_has_loaded(retries=10)

    def is_rgw_provider_available(self) -> bool:
        """
        Check if RGW provider card is present and enabled.

        Returns:
            bool: True if RGW provider is available and selectable.

        """
        if not self.check_element_presence(self.bucket_tab["provider_radio_rgw"][::-1]):
            logger.info("RGW provider card not found in DOM")
            return False

        is_disabled = self.get_element_attribute(
            self.bucket_tab["provider_radio_rgw"], "disabled", safe=True
        )
        available = is_disabled is None
        logger.info(f"RGW provider available: {available}")
        return available

    def select_project(self, cluster_namespace):
        """
        Helper function to select a project via UI. e.g. 'openshift-storage'

        Args:
            cluster_namespace (str): project name will be selected from the list

        """
        logger.info(f"Select '{cluster_namespace}' project")
        self.select_namespace(project_name=cluster_namespace)
