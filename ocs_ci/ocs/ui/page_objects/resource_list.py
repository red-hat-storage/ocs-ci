from ocs_ci.ocs.ui.helpers_ui import format_locator
from ocs_ci.ocs.ui.page_objects.searchbar import SearchBar
from ocs_ci.ocs.ui.base_ui import logger
from ocs_ci.utility import version


class ResourceList(SearchBar):
    """
    Resource List module presented in OBC, OB, PVC, PV, BucketClass, BackingStore, StorageClass, VolumeSnapshotClasses,
    BlockPools, etc.

    This module is for selecting resource, navigation into, filtering, deletion, edition, etc.
    """

    def nav_to_resource_via_name(self, resource_name: str = None):
        """
        Navigate to resource searching it via name

        Args:
            resource_name (str): Resource name
        """
        logger.info(f"Navigate to resource by name '{resource_name}'")
        self.select_search_by("name")
        self.do_clear(self.generic_locators["searchbar_input"])
        self.search(resource_name)

        loc = format_locator(
            self.generic_locators["resource_link"],
            resource_name,
            resource_name,
            resource_name,
            resource_name,
        )

        self.wait_for_element_to_be_visible(loc)
        self.do_click(
            loc,
            enable_screenshot=True,
        )

    def nav_to_resource_via_label(self, resource_label: str):
        """
        Navigate to resource searching it via label

        Args:
            resource_label (str): Resource label
        """
        logger.info(f"Navigate to resource via label '{resource_label}'")
        raise NotImplementedError(
            "TODO: select resource by label, first dropdown matching to entered label. Not implemented yet."
        )

    def delete_resource(self, delete_via, resource):
        """
        Delete Object Bucket, Object Bucket Claim, PVC, PV, BucketClass, BackingStore, StorageClass, Namespace, etc.

        Args:
            delete_via (str): supported values: 'three_dots' or 'Actions'
                delete using 'three dots' icon, from the Resource List page
                or click on specific resource and delete it using 'Actions' dropdown list
            resource (str): resource name to delete.
        """
        logger.info(f"Find resource by name '{resource}' using search-bar")
        self.page_has_loaded()

        if delete_via == "Actions":
            logger.info(f"Go to {resource} Page")
            # delete specific resource by its dynamic name. Works both for
            self.nav_to_resource_via_name(resource_name=resource)

            logger.info(f"Click on '{delete_via}'")
            self.do_click(self.generic_locators["actions"], enable_screenshot=True)
        else:
            logger.info(f"Click on '{delete_via}'")

            self.do_send_keys(self.generic_locators["search_resource_field"], resource)
            self.do_click(
                format_locator(
                    self.generic_locators["three_dots_specific_resource"],
                    resource,
                    resource,
                ),
                enable_screenshot=True,
            )

        logger.info(f"Click on 'Delete {resource}'")
        # works both for OBC and OB, both from three_dots icon and Actions dropdown list
        self.do_click(self.obc_loc["delete_resource"], enable_screenshot=True)

        logger.info(f"Confirm {resource} Deletion")
        # same PopUp both for OBC and OB
        # check if we are on the Bucket page, it has different UI starting from 4.18
        from ocs_ci.ocs.ui.page_objects.buckets_tab import BucketsTab

        if (
            isinstance(self, BucketsTab)
            and self.ocp_version_semantic >= version.VERSION_4_18
        ):
            self.dialog_confirm_delete(resource)
        else:
            self.do_click(
                self.generic_locators["confirm_action"], enable_screenshot=True
            )
