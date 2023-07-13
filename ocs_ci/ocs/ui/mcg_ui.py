import logging
from time import sleep
from selenium.webdriver.support.wait import WebDriverWait
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.ocs.ui.helpers_ui import get_element_by_text

logger = logging.getLogger(__name__)


class MCGStoreUI(PageNavigator):
    """
    A class representation for abstraction of MCG store related OpenShift UI actions

    """

    def __init__(self):
        super().__init__()
        self.wait = WebDriverWait(self.driver, 30)

    def create_store_ui(self, kind, store_name, secret_name, target_bucket):
        """
        Create an MCG store via the UI

        Args:
            kind (str): The store kind - backingstore | namespacestore
            store_name (str): The name to grant to the store
            secret_name (str): The name of the secret to used to connect the store to AWS
            target_bucket (str): The AWS S3 bucket to use as a host for the store

        """
        self.navigate_to_ocs_operator_page()

        logger.info("Enter the store section")
        self.do_click(self.ocs_loc[f"{kind}_page"])

        logger.info("Create a new store")
        self.do_click(self.generic_locators["create_resource_button"])

        logger.info("Enter store name")
        self.do_send_keys(self.mcg_stores["store_name"], store_name)

        logger.info("Pick AWS as the provider")
        self.do_click(self.mcg_stores["provider_dropdown"])
        self.do_click(self.mcg_stores["aws_provider"])

        logger.info("Pick the us-east-2 region")
        self.do_click(self.mcg_stores["aws_region_dropdown"])
        self.do_click(self.mcg_stores["us_east_2_region"])

        logger.info("Pick secret")
        self.do_click(self.mcg_stores["aws_secret_dropdown"])
        self.do_send_keys(self.mcg_stores["aws_secret_search_field"], secret_name)
        self.do_click(self.generic_locators["first_dropdown_option"])

        logger.info("Enter target bucket name")
        self.do_send_keys(self.mcg_stores["target_bucket"], target_bucket)
        logger.info("Submit form")
        self.do_click(self.generic_locators["submit_form"])

    def delete_store_ui(self, kind, store_name):
        """
        Delete an MCG store via the UI

        store_name (str): Name of the store to be deleted

        """
        self.navigate_to_ocs_operator_page()

        logger.info("Enter the store section")
        self.do_click(self.ocs_loc[f"{kind}_page"])

        logger.info("Search for the store")
        self.do_send_keys(self.generic_locators["search_resource_field"], store_name)

        logger.info("Open store kebab menu")
        self.do_click(self.generic_locators["kebab_button"])

        logger.info(f"Click on 'Delete {kind}'")
        self.do_click(self.generic_locators["delete_resource_kebab_button"])

        logger.info("Confirm store Deletion")
        self.do_click(self.generic_locators["confirm_action"])


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


class NamespaceStoreUI(PageNavigator):
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

        self.nav_odf_default_page().nav_namespace_store_tab()
        self.do_click(self.sc_loc["namespace_store_create"])
        self.do_send_keys(self.sc_loc["namespace_store_name"], namespace_store_name)

        if namespace_store_provider == "fs":
            self.do_click(self.sc_loc["namespace_store_provider"])
            self.do_click(self.sc_loc["namespace_store_filesystem"])
            sleep(2)
            self.do_click(self.sc_loc["namespace_store_pvc_expand"])
            self.do_click(get_element_by_text(namespace_store_pvc_name))

        self.do_send_keys(self.sc_loc["namespace_store_folder"], namespace_store_folder)
        self.take_screenshot()
        self.do_click(self.sc_loc["namespace_store_create_item"])
