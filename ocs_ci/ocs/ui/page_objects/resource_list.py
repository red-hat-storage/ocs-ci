from selenium.webdriver.common.by import By

from ocs_ci.ocs.exceptions import IncorrectUiOptionRequested
from ocs_ci.ocs.ui.helpers_ui import format_locator
from ocs_ci.ocs.ui.page_objects.searchbar import SearchBar
from ocs_ci.ocs.ui.base_ui import logger


class ResourceList(SearchBar):
    """
    Resource List module presented in OBC, OB, PVC, PV, BucketClass, BackingStore, StorageClass, VolumeSnapshotClasses,
    BlockPools, etc.

    This module is for selecting resource, navigation into, filtering, deletion, edition, etc.
    """

    def nav_to_resource(self, resource_name: str = None, resource_label: str = None):
        """
        Navigate to resource. Should be passed either resource_name or resource_label
        Args:
            resource_name (str): Resource name - optional argument
            resource_label (str): Resource label - optional argument
        """
        if not (resource_name or resource_label):
            raise IncorrectUiOptionRequested(
                "Either resource_name or resource_label should have value"
            )

        if resource_name:
            logger.info(f"Navigate to resource by name '{resource_name}'")
            self.select_search_by("name")
            self.search(resource_name)
            self.do_click(
                format_locator(self.generic_locators["resource_link"], resource_name),
                enable_screenshot=True,
            )
        elif resource_label:
            logger.info(f"Navigate to resource by label '{resource_label}'")
            raise NotImplementedError(
                "TODO: select resource by label, first dropdown matching to entered label"
            )

    def select_resource_number(self, resource_number: int):
        """
        Select resource number
        Args:
            resource_number (int): Resource number (index)
        """
        logger.info(f"Select resource number {resource_number}")

    def delete_resource(self, delete_via, resource):
        """
        Delete Object Bucket or Object bucket claim

        Args:
            delete_via (str): delete using 'three dots' icon, from the Object Bucket page/Object Bucket Claims page
                or click on specific Object Bucket/Object Bucket Claim and delete it using 'Actions' dropdown list
            resource (str): resource name to delete. It may be Object Bucket Claim name both for OBC or OB,
                and it may be Object Bucket Name. Object Bucket name consists from Object Bucket Claim and prefix
        """
        logger.info(f"Find resource by name '{resource}' using search-bar")
        self.page_has_loaded()
        self.do_send_keys(self.generic_locators["search_resource_field"], resource)

        if delete_via == "Actions":
            logger.info(f"Go to {resource} Page")
            # delete specific resource by its dynamic name. Works both for
            self.nav_to_resource(resource_name=resource)

            logger.info(f"Click on '{delete_via}'")
            self.do_click(self.generic_locators["actions"], enable_screenshot=True)
        else:
            logger.info(f"Click on '{delete_via}'")

            self.do_click(
                (
                    format_locator(
                        self.generic_locators["three_dots_specific_resource"], resource
                    ),
                    By.XPATH,
                ),
                enable_screenshot=True,
            )

        logger.info(f"Click on 'Delete {resource}'")
        # works both for OBC and OB, both from three_dots icon and Actions dropdown list
        self.do_click(self.obc_loc["delete_resource"], enable_screenshot=True)

        logger.info(f"Confirm {resource} Deletion")
        # same PopUp both for OBC and OB
        self.do_click(self.generic_locators["confirm_action"], enable_screenshot=True)
