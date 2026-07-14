from time import sleep
from ocs_ci.ocs.exceptions import IncorrectUiOptionRequested
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.ocs.ui.page_objects.object_storage import ObjectStorage, logger
from ocs_ci.ocs.ui.helpers_ui import get_element_by_text


class BucketClassUI(PageNavigator):
    """
    A class representation for abstraction of BC-related OpenShift UI actions

    """

    def __init__(self):
        super().__init__()

    def create_standard_bucketclass_ui(self, bc_name, policy, store_list):
        """
        Create a standard BC via the UI

        Args:
            bc_name (str): The name to grant the BC
            policy (str): The policy type to use. Spread/Mirror
            store_list (list[str]): A list of backingstore names to be used by the bucketclass

        """
        self.navigate_to_ocs_operator_page()

        logger.info("Enter the BC section")
        self.do_click(self.ocs_loc["bucketclass_page"])

        logger.info("Create a new BC")
        self.do_click(self.generic_locators["create_resource_button"])

        logger.info("Pick type")
        self.do_click(self.bucketclass["standard_type"])

        logger.info("Enter BC name")
        self.do_send_keys(self.bucketclass["bucketclass_name"], bc_name)
        self.do_click(self.generic_locators["submit_form"])

        logger.info("Pick policy ({policy})")
        self.do_click(self.bucketclass[f"{policy}_policy"])
        self.do_click(self.generic_locators["submit_form"])

        logger.info("Pick backingstore(s)")
        for backingstore_name in store_list:
            self.do_send_keys(
                self.generic_locators["search_resource_field"], backingstore_name
            )
            sleep(0.3)
            self.do_click(self.generic_locators["check_first_row_checkbox"])
            sleep(0.3)
            self.do_click(self.generic_locators["remove_search_filter"])

        self.do_click(self.generic_locators["submit_form"])

        logger.info("Submit")
        self.do_click(self.generic_locators["submit_form"])

    def set_single_namespacestore_policy(self, nss_name_lst):
        self.do_click(self.bucketclass["nss_dropdown"])
        self.do_click_by_id(nss_name_lst[0])

    def set_multi_namespacestore_policy(self, nss_name_lst):
        for nss_name in nss_name_lst:
            self.do_send_keys(self.generic_locators["search_resource_field"], nss_name)
            sleep(1)
            self.do_click(self.generic_locators["check_first_row_checkbox"])
            sleep(1)
            self.do_click(self.generic_locators["remove_search_filter"])
            sleep(2)

        self.do_click(self.bucketclass["nss_dropdown"])
        self.do_click_by_id(nss_name_lst[0])

    def set_cache_namespacestore_policy(self, nss_name_lst, bs_name_lst):
        self.do_click(self.bucketclass["nss_dropdown"])
        self.do_click_by_id(nss_name_lst[0])

        self.do_click(self.bucketclass["bs_dropdown"])
        self.do_click_by_id(bs_name_lst[0])

        self.do_send_keys(self.bucketclass["ttl_input"], "5")
        self.do_click(self.bucketclass["ttl_time_unit_dropdown"])
        self.do_click(self.bucketclass["ttl_minute_time_unit_button"])

    set_namespacestore_policy = {
        "single": set_single_namespacestore_policy,
        "multi": set_multi_namespacestore_policy,
        "cache": set_cache_namespacestore_policy,
    }

    def create_namespace_bucketclass_ui(
        self, bc_name, policy, nss_name_lst, bs_name_lst
    ):
        """
        Create a namespace BC via the UI

        Args:
            bc_name (str): The name to grant the BC
            policy (str): The policy type to use. Single/Multi/Cache
            nss_name_lst (list[str]): A list of namespacestore names to be used by the bucketclass
            bs_name_lst (list[str]): A list of backingstore names to be used by the bucketclass

        """
        self.navigate_to_ocs_operator_page()

        logger.info("Enter the BC section")
        self.do_click(self.ocs_loc["bucketclass_page"])

        logger.info("Create a new BC")
        self.do_click(self.generic_locators["create_resource_button"])

        logger.info("Pick type")
        self.do_click(self.bucketclass["namespace_type"])

        logger.info("Enter BC name")
        self.do_send_keys(self.bucketclass["bucketclass_name"], bc_name)
        self.do_click(self.generic_locators["submit_form"])

        logger.info(f"Pick policy ({policy})")
        self.do_click(self.bucketclass[f"{policy}_policy"])
        self.do_click(self.generic_locators["submit_form"])

        logger.info("Pick resources")
        if policy == "cache":
            self.set_namespacestore_policy[policy](self, nss_name_lst, bs_name_lst)
        else:
            self.set_namespacestore_policy[policy](self, nss_name_lst)
        self.do_click(self.generic_locators["submit_form"])

        logger.info("Submit")
        self.do_click(self.generic_locators["submit_form"])

    def delete_bucketclass_ui(self, bc_name):
        """
        Delete a BC via the UI

        bc_name (str): Name of the BC to be deleted

        """
        self.navigate_to_ocs_operator_page()

        logger.info("Enter the BC section")
        self.do_click(self.ocs_loc["bucketclass_page"])

        logger.info("Search for the BC")
        self.do_send_keys(self.generic_locators["search_resource_field"], bc_name)

        logger.info("Open BC kebab menu")
        self.do_click(self.generic_locators["kebab_button"])

        logger.info("Click on 'Delete Bucket Class'")
        self.do_click(self.generic_locators["delete_resource_kebab_button"])

        logger.info("Confirm BC Deletion")
        self.do_click(self.generic_locators["confirm_action"])


class NamespaceStoreUI(ObjectStorage):
    def __init__(self):
        super().__init__()
        self.sc_loc = self.obc_loc

    def create_namespace_store(
        self,
        namespace_store_name,
        namespace_store_provider,
        namespace_store_pvc_name,
        namespace_store_folder,
    ):
        """

        Args:
            namespace_store_name (str): the namespace store
            namespace_store_provider (str):  the provider [aws, filesystem, azure]
            namespace_store_pvc_name (str): pvc name for file system mode
            namespace_store_folder (str): the folder name for mount point to fs.

        """
        logger.info("Create namespace-store via UI")

        self.nav_object_storage_page().nav_namespace_store_tab()
        self.do_click(self.sc_loc["namespace_store_create"])
        self.do_send_keys(self.sc_loc["namespace_store_name"], namespace_store_name)

        if namespace_store_provider == "fs":
            self.do_click(self.sc_loc["namespace_store_provider"])
            self.do_click(self.sc_loc["namespace_store_filesystem"])
            sleep(2)
            self.do_click(self.sc_loc["namespace_store_pvc_expand"])
            self.do_click(get_element_by_text(namespace_store_pvc_name))
            self.do_send_keys(
                self.sc_loc["namespace_store_folder"], namespace_store_folder
            )
        else:
            raise IncorrectUiOptionRequested(
                "Only fs is supported with this method. Rest options are supported with "
                "'object_storage.nav_namespace_store_tab().create_store()'"
            )

        self.take_screenshot()
        self.do_click(self.sc_loc["namespace_store_create_item"])
        self.take_screenshot()
