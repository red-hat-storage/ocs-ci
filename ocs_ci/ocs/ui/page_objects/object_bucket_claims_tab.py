import random
import string
import time

from ocs_ci.framework import config
from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.ui.page_objects.data_foundation_tabs_common import CreateResourceForm
from ocs_ci.ocs.ui.page_objects.object_storage import ObjectStorage, logger
from ocs_ci.ocs.ui.page_objects.resource_page import ResourcePage
from tests.conftest import delete_projects


class ObjectBucketClaimsTab(ObjectStorage, CreateResourceForm):
    """
    A class representation for abstraction of OBC-related OpenShift UI actions

    """

    def __init__(self):
        ObjectStorage.__init__(self)
        self.name_input_loc = self.obc_loc["obc_name"]
        self.rules = {
            constants.UI_INPUT_RULES_OBJECT_BUCKET_CLAIM[
                "rule1"
            ]: self._check_max_length_backing_store_rule,
            constants.UI_INPUT_RULES_OBJECT_BUCKET_CLAIM[
                "rule2"
            ]: self._check_start_end_char_rule,
            constants.UI_INPUT_RULES_OBJECT_BUCKET_CLAIM[
                "rule3"
            ]: self._check_only_lower_case_numbers_periods_hyphens_rule,
            constants.UI_INPUT_RULES_OBJECT_BUCKET_CLAIM[
                "rule4"
            ]: self._check_obc_cannot_be_used_before,
        }
        self.sc_loc = self.obc_loc

    def check_obc_option(self, username, text="Object Bucket Claims"):
        """
        Check OBC is visible to user after giving admin access

        Args:
            username (str): user's username
            text (str): text to be found on OBC page

        """

        sc_name = create_unique_resource_name("namespace-", "interface")

        self.select_administrator_user()

        self.do_click(self.sc_loc["create_project"])
        self.do_send_keys(self.sc_loc["project_name"], sc_name)
        self.do_click(self.sc_loc["save_project"])
        ocp_obj = OCP()
        ocp_obj.exec_oc_cmd(
            f"adm policy add-role-to-user admin {username} -n {sc_name}"
        )
        logger.info(
            f"Waiting for project {sc_name} to be created and roles assigned 10sec"
        )
        time.sleep(10)
        self.navigate_object_bucket_claims_page()
        obc_found = self.wait_until_expected_text_is_found(
            locator=self.sc_loc["obc_menu_name"], expected_text=text, timeout=10
        )
        if not obc_found:
            logger.info("user is not able to access OBC")
            self.take_screenshot()
        else:
            logger.info("user is able to access OBC")

        namespaces = []
        namespace_obj = OCP(kind=constants.NAMESPACE, namespace=sc_name)
        namespaces.append(namespace_obj)
        delete_projects(namespaces)
        return obc_found

    def _check_obc_cannot_be_used_before(self, rule_exp):
        """
        Check whether the given rule expression can be used before creating an OBC.

        Args:
            rule_exp (str): the rule requested to be checked. rule_exp text should match the text from validation popup.

        Returns:
            bool: True if the input text length not violated, False otherwise.
        """
        existing_obc = OCP().exec_oc_cmd(
            "get obc --all-namespaces -o custom-columns=':metadata.name'"
        )
        if not existing_obc:
            obc_name = create_unique_resource_name(
                resource_description="bucket", resource_type="s3"
            )
            logger.info(f"create new OBC with name '{obc_name}'")
            self.create_obc_ui(
                obc_name, "openshift-storage.noobaa.io", "noobaa-default-bucket-class"
            )
            self.navigate_object_bucket_claims_page()
            self.proceed_resource_creation()
            existing_obc = str(
                OCP().exec_oc_cmd(
                    "get obc --all-namespaces -o custom-columns=':metadata.name'"
                )
            )

        name_exist = existing_obc.split()[0]
        random_char = random.choice(string.ascii_lowercase + string.digits)
        name_does_not_exist = random_char + name_exist[1:]

        params_list = [
            (rule_exp, name_exist, self.status_error),
            (rule_exp, name_does_not_exist, self.status_success),
        ]

        return all(self._check_rule_case(*params) for params in params_list)

    def create_obc_ui(self, obc_name, storageclass, bucketclass=None):
        """
        Create an OBC via the UI

        Args:
            obc_name (str): The name to grant the OBC
            storageclass (str): The storageclass to be used by the OBC
            bucketclass (str): The bucketclass to be used by the OBC

        Returns:
            ResourcePage: The page object of the newly created OBC
        """
        # create_obc_ui procedure should start from home page even if prev test failed in the middle
        self.navigate_OCP_home_page()
        self.navigate_object_bucket_claims_page()

        self.select_project(config.ENV_DATA["cluster_namespace"])

        logger.info("Click on 'Create Object Bucket Claim'")
        self.do_click(self.generic_locators["create_resource_button"])

        logger.info("Enter OBC name")
        self.do_send_keys(self.obc_loc["obc_name"], obc_name)

        logger.info("Select Storage Class")
        self.do_click(self.obc_loc["storageclass_dropdown"])
        self.do_send_keys(self.obc_loc["storageclass_text_field"], storageclass)

        from ocs_ci.ocs.ui.helpers_ui import format_locator

        self.do_click(
            locator=format_locator(self.generic_locators["storage_class"], storageclass)
        )

        if bucketclass:
            logger.info("Select BucketClass")
            self.do_click(self.obc_loc["bucketclass_dropdown"])
            self.do_send_keys(self.obc_loc["bucketclass_text_field"], bucketclass)
            self.do_click(self.generic_locators["first_dropdown_option"])

        logger.info("Create OBC")
        self.do_click(self.generic_locators["submit_form"])

        return ResourcePage()

    def select_openshift_storage_default_project(self):
        """
        Helper function to select openshift-storage project

        Notice: the func works from PersistantVolumeClaims, VolumeSnapshots and OBC pages
        """
        logger.info("Select 'openshift-storage' project")
        self.select_namespace(project_name=config.ENV_DATA["cluster_namespace"])

    def delete_obc_ui(self, obc_name, delete_via):
        """
        Delete an OBC via the UI

        obc_name (str): Name of the OBC to be deleted
        delete_via (str): delete via 'OBC/Actions' or via 'three dots'
        """
        self.navigate_object_bucket_claims_page()

        self.select_project(config.ENV_DATA["cluster_namespace"])

        self.delete_resource(delete_via, obc_name)

    def attach_deployment_to_obc_ui(self, deployment, obc_name):
        """
        Attach deployment to obc

        Args:
            deployment (str): Name of the deployment to attach with
            obc_name (str): Name of the obc to be attached

        """
        self.navigate_object_bucket_claims_page()
        self.select_openshift_storage_default_project()
        logger.info("Click on search bar")
        self.do_click(self.generic_locators["search_resource_field"])
        logger.info("Clear existing text from search bar if any")
        self.do_clear(self.generic_locators["search_resource_field"])
        logger.info("Enter the obc to be searched")
        self.do_send_keys(self.generic_locators["search_resource_field"], text=obc_name)
        logger.info("Click on the kebab menu")
        self.do_click(self.obc_loc["kebab_action"])
        logger.info("Select attach to deployment option")
        self.do_click(self.obc_loc["attach_to_deployment"])
        logger.info("Click on the dropdown and search for the existing deployment")
        odf_deployment_dropdown = self.find_an_element_by_xpath(
            "/html/body/div[5]/div/div/div/div/div[2]/button"
        )
        self.driver.execute_script("arguments[0].click();", odf_deployment_dropdown)
        self.do_click(self.obc_loc["search_bar"])
        self.do_send_keys(self.obc_loc["search_bar"], text=deployment)
        logger.info("Select the deployment and attach it")
        self.do_click(self.obc_loc["odf_resource_item"])
        self.do_click(self.obc_loc["attach"])

    def attach_obc_to_deployment_ui(self, deployment, obc_name):
        """
        Attach obc to deployment

        Args:
            deployment (str): Name of the deployment
            obc_name (str): Name of the obc to attach

        """
        self.navigate_deployments_page()
        self.select_openshift_storage_default_project()
        logger.info("Click on search bar")
        self.do_click(self.generic_locators["search_resource_field"])
        logger.info("Clear existing text from search bar if any")
        self.do_clear(self.generic_locators["search_resource_field"])
        logger.info("Enter the obc to be searched")
        self.do_send_keys(
            self.generic_locators["search_resource_field"], text=deployment
        )
        logger.info("Click the kebab menu")
        self.do_click(self.obc_loc["kebab_action"])
        logger.info("Add Storage")
        self.do_click(self.obc_loc["add_storage"])
        logger.info("Check ObjectBucketClaim option")
        self.do_click(self.obc_loc["obc_radiobutton"])
        logger.info("Select use existing claim")
        self.do_click(self.obc_loc["use_existing_claim"])
        logger.info("Click on the dropdown menu")
        obc_dropdown = self.find_an_element_by_xpath(
            "/html[1]/body[1]/div[2]/div[1]/div[1]/div[1]/div[1]/div[1]/div[1]/main[1]"
            "/div[1]/div[1]/div[1]/div[1]/div[1]/section[1]/div[1]/form[1]/div[1]/div[1]/div[2]/div[2]/div[1]/button[1]"
        )
        self.driver.execute_script("arguments[0].click();", obc_dropdown)
        logger.info("Search for the existing obc and click on it")
        self.do_click(self.obc_loc["search_bar"])
        self.do_send_keys(self.obc_loc["search_bar"], text=obc_name)
        self.do_click(self.obc_loc["odf_resource_item"])
        self.do_click(self.generic_locators["submit_form"])
