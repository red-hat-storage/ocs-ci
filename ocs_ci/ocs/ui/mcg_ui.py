import logging

from ocs_ci.ocs import constants
from time import sleep
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework import config
from selenium.webdriver.support.wait import WebDriverWait
from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.utility import version
from ocs_ci.ocs.ui.base_ui import PageNavigator
from ocs_ci.ocs.ui.views import locators
from tests.conftest import delete_projects

logger = logging.getLogger(__name__)


class MCGStoreUI(PageNavigator):
    """
    A class representation for abstraction of MCG store related OpenShift UI actions

    """

    def __init__(self):
        super().__init__()
        self.wait = WebDriverWait(self.driver, 30)
        ocs_version = f"{version.get_ocs_version_from_csv(only_major_minor=True)}"
        self.ocs_loc = locators[ocs_version]["ocs_operator"]
        self.mcg_stores = locators[ocs_version]["mcg_stores"]

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
        ocs_version = f"{version.get_ocs_version_from_csv(only_major_minor=True)}"
        self.ocs_loc = locators[ocs_version]["ocs_operator"]
        self.bucketclass = locators[ocs_version]["bucketclass"]

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


class ObcUI(PageNavigator):
    """
    A class representation for abstraction of OBC-related OpenShift UI actions

    """

    def __init__(self):
        super().__init__()
        ocs_version = f"{version.get_ocs_version_from_csv(only_major_minor=True)}"
        self.obc_loc = locators[ocs_version]["obc"]

    def create_obc_ui(self, obc_name, storageclass, bucketclass=None):
        """
        Create an OBC via the UI

        Args:
            obc_name (str): The name to grant the OBC
            storageclass (str): The storageclass to be used by the OBC
            bucketclass (str): The bucketclass to be used by the OBC

        """
        self.navigate_object_bucket_claims_page()

        self.select_openshift_storage_default_project()

        logger.info("Click on 'Create Object Bucket Claim'")
        self.do_click(self.generic_locators["create_resource_button"])

        logger.info("Enter OBC name")
        self.do_send_keys(self.obc_loc["obc_name"], obc_name)

        logger.info("Select Storage Class")
        self.do_click(self.obc_loc["storageclass_dropdown"])

        if self.ocp_version_full <= version.VERSION_4_8:
            self.do_send_keys(self.obc_loc["storageclass_text_field"], storageclass)

        if self.ocp_version_full <= version.VERSION_4_8 or (
            self.ocp_version_full > version.VERSION_4_8 and not bucketclass
        ):
            self.do_click(self.generic_locators["first_dropdown_option"])
        else:
            self.do_click(self.generic_locators["second_dropdown_option"])

        if bucketclass:
            logger.info("Select BucketClass")
            self.do_click(self.obc_loc["bucketclass_dropdown"])
            self.do_send_keys(self.obc_loc["bucketclass_text_field"], bucketclass)
            self.do_click(self.generic_locators["first_dropdown_option"])

        logger.info("Create OBC")
        self.do_click(self.generic_locators["submit_form"])

    def select_openshift_storage_default_project(self):
        """
        Helper function to select openshift-storage project

        Notice: the func works from PersistantVolumeClaims, VolumeSnapshots and OBC pages
        """
        logger.info("Select openshift-storage project")
        self.do_click(self.generic_locators["project_selector"])
        self.wait_for_namespace_selection(
            project_name=config.ENV_DATA["cluster_namespace"]
        )

    def delete_obc_ui(self, obc_name, delete_via):
        """
        Delete an OBC via the UI

        obc_name (str): Name of the OBC to be deleted
        delete_via (str): delete via 'OBC/Actions' or via 'three dots'
        """
        self.navigate_object_bucket_claims_page()

        self.select_openshift_storage_default_project()

        if delete_via == "Actions":
            logger.info(f"Go to OBC {obc_name} Page")
            self.do_click(self.obc_loc["resource_name"])
            logger.info(f"Click on '{delete_via}'")

            self.do_click(self.generic_locators["actions"])
        else:
            logger.info(f"Click on '{delete_via}'")
            self.do_click(self.generic_locators["three_dots"])

        logger.info("Click on 'Delete OBC'")
        self.do_click(self.obc_loc["delete_obc"])

        logger.info("Confirm OBC Deletion")
        self.do_click(self.generic_locators["confirm_action"])


class ObcUi(ObcUI):
    def __init__(self):
        super().__init__()
        self.sc_loc = locators[self.ocp_version]["obc"]

    def check_obc_option(self, text="Object Bucket Claims"):
        """check OBC is visible to user after giving admin access"""

        sc_name = create_unique_resource_name("namespace-", "interface")
        self.do_click(self.sc_loc["Developer_dropdown"])
        self.do_click(self.sc_loc["select_administrator"], timeout=5)
        self.do_click(self.sc_loc["create_project"])
        self.do_send_keys(self.sc_loc["project_name"], sc_name)
        self.do_click(self.sc_loc["save_project"])
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Storage"])
        obc_found = self.wait_until_expected_text_is_found(
            locator=self.sc_loc["obc_menu_name"], expected_text=text, timeout=10
        )
        if not obc_found:
            logger.info("user is not able to access OBC")
            self.take_screenshot()
            return None
        else:
            logger.info("user is able to access OBC")

        namespaces = []
        namespace_obj = OCP(kind=constants.NAMESPACE, namespace=sc_name)
        namespaces.append(namespace_obj)
        delete_projects(namespaces)
