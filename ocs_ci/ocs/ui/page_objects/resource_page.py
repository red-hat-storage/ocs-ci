from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from ocs_ci.ocs.exceptions import TimeoutExpiredError, IncorrectUiOptionRequested
from ocs_ci.ocs.ui.base_ui import BaseUI, logger, take_screenshot
from ocs_ci.utility.utils import TimeoutSampler


class ResourcePage(BaseUI):
    """
    Class represents Resource List UI module - the body of pages ObjectBucketClaim tab, Object Bucket tab,
    Namespace store tab, Persistent Volume Claim page, Volume Snapshots, etc.
    """

    def verify_current_page_resource_status(self, status_to_check, timeout=30):
        """
        Compares a given status string to the one shown in the resource's UI page

        Args:
            status_to_check (str): The status that will be compared with the one in the UI
            timeout (int): How long should the check run before moving on

        Returns:
            bool: True if the resource was found, False otherwise
        """

        def _retrieve_current_status_from_ui():
            resource_status = WebDriverWait(self.driver, timeout).until(
                ec.visibility_of_element_located(
                    self.generic_locators["resource_status"][::-1]
                )
            )
            logger.info(f"Resource status is {resource_status.text}")
            return resource_status

        logger.info(
            f"Verifying that the resource has reached a {status_to_check} status"
        )
        try:
            for resource_ui_status in TimeoutSampler(
                timeout,
                3,
                _retrieve_current_status_from_ui,
            ):
                if resource_ui_status.text.lower() == status_to_check.lower():
                    return True
        except TimeoutExpiredError:
            logger.error(
                "The resource did not reach the expected state within the time limit."
            )
            return False

    def is_backing_store_open(self):
        """
        Check if backing store is open

        Returns:
            bool: True if backing store is open, False otherwise
        """
        logger.info("Check if backing store page is open")
        return (
            self.get_element_text(
                self.generic_locators["resource_list_breadcrumbs"]
            ).lower()
            == "BackingStores".lower()
        )

    def is_namespace_store_open(self):
        """
        Check if namespace store is open

        Returns:
            bool: True if namespace store is open, False otherwise
        """
        logger.info("Check if namespace store page is open")
        return (
            self.get_element_text(
                self.generic_locators["resource_list_breadcrumbs"]
            ).lower()
            == "NamespaceStores".lower()
        )

    def is_obc_open(self):
        """
        Check if namespace store is open

        Returns:
            bool: True if namespace store is open, False otherwise
        """
        logger.info("Check if ObjectBucketClaim page is open")
        return (
            self.get_element_text(
                self.generic_locators["resource_list_breadcrumbs"]
            ).lower()
            == "ObjectBucketClaims".lower()
        )

    def is_ob_open(self):
        """
        Check if namespace store is open

        Returns:
            bool: True if namespace store is open, False otherwise
        """
        logger.info("Check if ObjectBucket page is open")
        return (
            self.get_element_text(
                self.generic_locators["resource_list_breadcrumbs"]
            ).lower()
            == "ObjectBuckets".lower()
        )

    def nav_resource_list_via_breadcrumbs(self):
        """
        Navigate to Resource List page via breadcrumbs

        Returns:
            object: ResourceList page object, depends on which page was open before
        """
        logger.info("Navigate to Resource List page via breadcrumbs")

        from ocs_ci.ocs.ui.page_objects.backing_store_tab import BackingStoreTab
        from ocs_ci.ocs.ui.page_objects.namespace_store_tab import NameSpaceStoreTab
        from ocs_ci.ocs.ui.page_objects.object_bucket_claims_tab import (
            ObjectBucketClaimsTab,
        )
        from ocs_ci.ocs.ui.page_objects.object_buckets_tab import ObjectBucketsTab

        if self.is_namespace_store_open():
            resource_list_page = NameSpaceStoreTab
        elif self.is_backing_store_open():
            resource_list_page = BackingStoreTab
        elif self.is_obc_open():
            resource_list_page = ObjectBucketClaimsTab
        elif self.is_ob_open():
            resource_list_page = ObjectBucketsTab
        else:
            raise IncorrectUiOptionRequested(
                "Wrong page is open after resource created", func=take_screenshot
            )

        self.do_click(
            self.generic_locators["resource_list_breadcrumbs"], enable_screenshot=True
        )

        return resource_list_page()
