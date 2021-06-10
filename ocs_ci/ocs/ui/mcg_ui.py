import logging

from selenium.webdriver.support.wait import WebDriverWait

from ocs_ci.ocs.ui.base_ui import PageNavigator
from ocs_ci.ocs.ui.views import locators
from ocs_ci.utility.utils import get_ocp_version

logger = logging.getLogger(__name__)


class MCGStoreUI(PageNavigator):
    """
    A class representation for abstraction of BS-related OpenShift UI actions

    """

    def __init__(self, driver):
        super().__init__(driver)
        self.wait = WebDriverWait(self.driver, 30)
        ocp_version = get_ocp_version()
        self.ocs_loc = locators[ocp_version]["ocs_operator"]
        self.mcg_stores = locators[ocp_version]["mcg_stores"]

    def create_store_ui(self, kind, store_name, secret_name, target_bucket):
        """
        Create an MCG store via the UI

        Args:
            kind (str): The store kind - backing | namespace
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

        logger.info("Open BS kebab menu")
        self.do_click(self.generic_locators["kebab_button"])

        logger.info(f"Click on 'Delete {kind}'")
        self.do_click(self.generic_locators["delete_resource_kebab_button"])

        logger.info("Confirm store Deletion")
        self.do_click(self.generic_locators["confirm_action"])


class BucketClassUI(PageNavigator):
    """
    A class representation for abstraction of BC-related OpenShift UI actions

    """

    def __init__(self, driver):
        super().__init__(driver)
        ocp_version = get_ocp_version()
        self.ocs_loc = locators[ocp_version]["ocs_operator"]
        self.bucketclass = locators[ocp_version]["bucketclass"]

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
            self.do_click(self.generic_locators["check_first_row_checkbox"])
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
            self.do_click(self.generic_locators["check_first_row_checkbox"])
            self.do_click(self.generic_locators["remove_search_filter"])

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
            bs_name_lst (list[str]): A list of namespacestore names to be used by the bucketclass

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

        logger.info("Search for the BS")
        self.do_send_keys(self.generic_locators["search_resource_field"], bc_name)

        logger.info("Open BC kebab menu")
        self.do_click(self.generic_locators["kebab_button"])

        logger.info("Click on 'Delete Bucket Class'")
        self.do_click(self.generic_locators["delete_resource_kebab_button"])

        logger.info("Confirm BC Deletion")
        self.do_click(self.generic_locators["confirm_action"])


class ObcUI(PageNavigator):
    """
    A class representation for abstraction of OBC-related OpenShift UI actions

    """

    def __init__(self, driver):
        super().__init__(driver)
        ocp_version = get_ocp_version()
        self.obc_loc = locators[ocp_version]["obc"]

    def create_obc_ui(self, obc_name, storageclass, bucketclass=None):
        """
        Create an OBC via the UI

        Args:
            obc_name (str): The name to grant the OBC
            storageclass (str): The storageclass to be used by the OBC
            bucketclass (str): The bucketclass to be used by the OBC

        """
        self.navigate_object_bucket_claims_page()

        logger.info("Select openshift-storage project")
        self.do_click(self.generic_locators["project_selector"])
        self.do_click(self.generic_locators["select_openshift-storage_project"])

        logger.info("Click on 'Create Object Bucket Claim'")
        self.do_click(self.generic_locators["create_resource_button"])

        logger.info("Enter OBC name")
        self.do_send_keys(self.obc_loc["obc_name"], obc_name)

        logger.info("Select Storage Class")
        self.do_click(self.obc_loc["storageclass_dropdown"])
        self.do_send_keys(self.obc_loc["storageclass_text_field"], storageclass)
        self.do_click(self.generic_locators["first_dropdown_option"])

        if bucketclass:
            logger.info("Select BucketClass")
            self.do_click(self.obc_loc["bucketclass_dropdown"])
            self.do_send_keys(self.obc_loc["bucketclass_text_field"], bucketclass)
            self.do_click(self.generic_locators["first_dropdown_option"])

        logger.info("Create OBC")
        self.do_click(self.generic_locators["submit_form"])

    def delete_obc_ui(self, obc_name):
        """
        Delete an OBC via the UI

        obc_name (str): Name of the OBC to be deleted

        """
        self.navigate_object_bucket_claims_page()

        logger.info("Select openshift-storage project")
        self.do_click(self.generic_locators["project_selector"])
        self.do_click(self.generic_locators["select_openshift-storage_project"])

        self.do_send_keys(self.generic_locators["search_resource_field"], text=obc_name)

        logger.info(f"Go to OBC {obc_name} Page")
        self.do_click(self.obc_loc["first_obc_link"])

        logger.info("Click on Actions")
        self.do_click(self.generic_locators["actions"])

        logger.info("Click on 'Delete OBC'")
        self.do_click(self.obc_loc["delete_obc"])

        logger.info("Confirm OBC Deletion")
        self.do_click(self.generic_locators["confirm_action"])
