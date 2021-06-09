import logging

from selenium.webdriver.support.wait import WebDriverWait

from ocs_ci.ocs.ui.base_ui import PageNavigator
from ocs_ci.ocs.ui.views import locators
from ocs_ci.utility.utils import get_ocp_version

logger = logging.getLogger(__name__)


class BackingstoreUI(PageNavigator):
    """
    A class representation of BS-related OpenShift UI elements

    """

    def __init__(self, driver):
        super().__init__(driver)
        self.wait = WebDriverWait(self.driver, 30)
        ocp_version = get_ocp_version()
        self.ocs_loc = locators[ocp_version]["ocs_operator"]
        self.backingstore = locators[ocp_version]["backingstore"]

    def create_backingstore_ui(self, bs_name, secret_name, target_bucket):
        """
        Create a BC via the UI

        bc_name (str): The name to grant the OBC

        """
        self.navigate_to_ocs_operator_page()

        logger.info("Enter the BS section")
        self.do_click(self.ocs_loc["backingstore_page"])

        logger.info("Create a new BS")
        self.do_click(self.generic_loc["create_resource_button"])

        logger.info("Enter backingstore name")
        self.do_send_keys(self.backingstore["backingstore_name"], bs_name)

        logger.info("Pick AWS as the provider")
        self.do_click(self.backingstore["provider_dropdown"])
        self.do_click(self.backingstore["aws_provider"])

        logger.info("Pick the us-east-2 region")
        self.do_click(self.backingstore["aws_region_dropdown"])
        self.do_click(self.backingstore["us_east_2_region"])

        logger.info("Pick secret")
        self.do_click(self.backingstore["aws_secret_dropdown"])
        self.do_send_keys(self.backingstore["aws_secret_search_field"], secret_name)
        self.do_click(self.generic_loc["first_dropdown_option"])

        logger.info("Enter target bucket name")
        self.do_send_keys(self.backingstore["target_bucket"], target_bucket)

        logger.info("Submit form")
        self.do_click(self.generic_loc["submit_form"])

        print(5)

    def delete_backingstore_ui(self, bs_name):
        """
        Delete an OBC via the UI

        obc_name (str): Name of the OBC to be deleted

        """
        self.navigate_to_ocs_operator_page()

        logger.info("Enter the BS section")
        self.do_click(self.ocs_loc["backingstore_page"])

        logger.info("Search for the BS")
        self.do_send_keys(self.generic_loc["search_resource_field"], bs_name)

        logger.info("Open BS kebab menu")
        self.do_click(self.generic_loc["kebab_button"])

        logger.info("Click on 'Delete Backingstore'")
        self.do_click(self.generic_loc["delete_resource_kebab_button"])

        logger.info("Confirm BS Deletion")
        self.do_click(self.generic_loc["confirm_action"])


class BucketClassUI(PageNavigator):
    """
    A class representation of BC-related OpenShift UI elements

    """

    def __init__(self, driver):
        super().__init__(driver)
        ocp_version = get_ocp_version()
        self.ocs_loc = locators[ocp_version]["ocs_operator"]
        self.bucketclass = locators[ocp_version]["bucketclass"]

    def create_standard_bucketclass_ui(self, bc_name, policy, store_list):
        """
        Create a BC via the UI

        bc_name (str): The name to grant the OBC

        """
        self.navigate_to_ocs_operator_page()

        logger.info("Enter the BC section")
        self.do_click(self.ocs_loc["bucketclass_page"])

        logger.info("Create a new BC")
        self.do_click(self.generic_loc["create_resource_button"])

        logger.info(f"Pick type")
        self.do_click(self.bucketclass[f"standard_type"])

        logger.info("Enter BC name")
        self.do_send_keys(self.bucketclass["bucketclass_name"], bc_name)
        self.do_click(self.generic_loc["submit_form"])

        logger.info(f"Pick policy ({policy})")
        self.do_click(self.bucketclass[f"{policy}_policy"])
        self.do_click(self.generic_loc["submit_form"])

        logger.info("Pick store(s)")
        for backingstore_name in store_list:
            self.do_send_keys(
                self.generic_loc["search_resource_field"], backingstore_name
            )
            self.do_click(self.generic_loc["check_first_row_checkbox"])
            self.do_click(self.generic_loc["remove_search_filter"])

        self.do_click(self.generic_loc["submit_form"])

        logger.info("Submit")
        self.do_click(self.generic_loc["submit_form"])

    def set_single_namespacestore_policy(self, nss_name_lst):
        self.do_click(self.bucketclass["nss_dropdown"])
        self.do_click_by_id(nss_name_lst[0])

    def set_multi_namespacestore_policy(self, nss_name_lst):
        for nss_name in nss_name_lst:
            self.do_send_keys(self.generic_loc["search_resource_field"], nss_name)
            self.do_click(self.generic_loc["check_first_row_checkbox"])
            self.do_click(self.generic_loc["remove_search_filter"])

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
        Create a BC via the UI

        bc_name (str): The name to grant the OBC

        """
        self.navigate_to_ocs_operator_page()

        logger.info("Enter the BC section")
        self.do_click(self.ocs_loc["bucketclass_page"])

        logger.info("Create a new BC")
        self.do_click(self.generic_loc["create_resource_button"])

        logger.info(f"Pick type")
        self.do_click(self.bucketclass[f"namespace_type"])

        logger.info("Enter BC name")
        self.do_send_keys(self.bucketclass["bucketclass_name"], bc_name)
        self.do_click(self.generic_loc["submit_form"])

        logger.info(f"Pick policy ({policy})")
        self.do_click(self.bucketclass[f"{policy}_policy"])
        self.do_click(self.generic_loc["submit_form"])

        logger.info("Pick resources")
        if policy == "cache":
            self.set_namespacestore_policy[policy](self, nss_name_lst, bs_name_lst)
        else:
            self.set_namespacestore_policy[policy](self, nss_name_lst)
        self.do_click(self.generic_loc["submit_form"])

        logger.info("Submit")
        self.do_click(self.generic_loc["submit_form"])

    def delete_bucketclass_ui(self, bc_name):
        """
        Delete an OBC via the UI

        obc_name (str): Name of the OBC to be deleted

        """
        self.navigate_to_ocs_operator_page()

        logger.info("Enter the BC section")
        self.do_click(self.ocs_loc["bucketclass_page"])

        logger.info("Search for the BS")
        self.do_send_keys(self.generic_loc["search_resource_field"], bc_name)

        logger.info("Open BC kebab menu")
        self.do_click(self.generic_loc["kebab_button"])

        logger.info("Click on 'Delete Bucket Class'")
        self.do_click(self.generic_loc["delete_resource_kebab_button"])

        logger.info("Confirm BC Deletion")
        self.do_click(self.generic_loc["confirm_action"])


class ObcUI(PageNavigator):
    """
    A class representation of OBC-related OpenShift UI elements

    """

    def __init__(self, driver):
        super().__init__(driver)
        ocp_version = get_ocp_version()
        self.obc_loc = locators[ocp_version]["obc"]

    def create_obc_ui(self, obc_name, storageclass, bucketclass=None):
        """
        Create an OBC via the UI

        obc_name (str): The name to grant the OBC
        storageclass (str): The storageclass to be used by the OBC
        bucketclass (str): The bucketclass to be used by the OBC

        """
        self.navigate_object_bucket_claims_page()

        logger.info("Select openshift-storage project")
        self.do_click(self.generic_loc["project_selector"])
        self.do_click(self.generic_loc["select_openshift-storage_project"])

        logger.info("Click on 'Create Object Bucket Claim'")
        self.do_click(self.generic_loc["create_resource_button"])

        logger.info("Enter OBC name")
        self.do_send_keys(self.obc_loc["obc_name"], obc_name)

        logger.info("Select Storage Class")
        self.do_click(self.obc_loc["storageclass_dropdown"])
        self.do_send_keys(self.obc_loc["storageclass_text_field"], storageclass)
        self.do_click(self.generic_loc["first_dropdown_option"])

        if bucketclass:
            logger.info("Select BucketClass")
            self.do_click(self.obc_loc["bucketclass_dropdown"])
            self.do_send_keys(self.obc_loc["bucketclass_text_field"], bucketclass)
            self.do_click(self.generic_loc["first_dropdown_option"])

        logger.info("Create OBC")
        self.do_click(self.generic_loc["submit_form"])

    def delete_obc_ui(self, obc_name):
        """
        Delete an OBC via the UI

        obc_name (str): Name of the OBC to be deleted

        """
        self.navigate_object_bucket_claims_page()

        logger.info("Select openshift-storage project")
        self.do_click(self.generic_loc["project_selector"])
        self.do_click(self.generic_loc["select_openshift-storage_project"])

        self.do_send_keys(self.generic_loc["search_resource_field"], text=obc_name)

        logger.info(f"Go to OBC {obc_name} Page")
        self.do_click(self.obc_loc["first_obc_link"])

        logger.info("Click on Actions")
        self.do_click(self.generic_loc["actions"])

        logger.info("Click on 'Delete OBC'")
        self.do_click(self.obc_loc["delete_obc"])

        logger.info("Confirm OBC Deletion")
        self.do_click(self.generic_loc["confirm_action"])
