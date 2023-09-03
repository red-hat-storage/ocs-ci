import random
import string

from ocs_ci.framework import config
from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.ui.mcg_ui import logger
from ocs_ci.ocs.ui.page_objects.data_foundation_tabs_common import CreateResourceForm
from ocs_ci.ocs.ui.page_objects.object_service import ObjectService
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.ocs.ui.page_objects.resource_list import ResourceList
from tests.conftest import delete_projects


class BucketsUI(PageNavigator, ResourceList):
    """
    A class representation for abstraction of OBC or OB-related OpenShift UI actions

    """

    def __init__(self):
        super().__init__()

    def select_openshift_storage_project(self, cluster_namespace):
        """
        Helper function to select openshift-storage project

        Args:
            cluster_namespace (str): project name will be selected from the list

        Notice: the func works from PersistantVolumeClaims, VolumeSnapshots and OBC pages
        """
        logger.info("Select openshift-storage project")
        self.do_click(self.generic_locators["project_selector"])
        self.wait_for_namespace_selection(project_name=cluster_namespace)

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

        from ocs_ci.ocs.ui.helpers_ui import format_locator

        if delete_via == "Actions":
            logger.info(f"Go to {resource} Page")
            # delete specific resource by its dynamic name. Works both for OBC and OB

            resource_from_list = format_locator(
                self.generic_locators["resource_from_list_by_name"], resource
            )
            self.do_click(
                resource_from_list,
                enable_screenshot=True,
            )

            logger.info(f"Click on '{delete_via}'")
            self.do_click(self.generic_locators["actions"], enable_screenshot=True)
        else:
            logger.info(f"Click on '{delete_via}'")
            # delete specific resource by its dynamic name. Works both for OBC and OB
            resource_actions_loc = format_locator(
                self.generic_locators["actions_of_resource_from_list"], resource
            )
            self.do_click(resource_actions_loc, enable_screenshot=True)

        logger.info(f"Click on 'Delete {resource}'")
        # works both for OBC and OB, both from three_dots icon and Actions dropdown list
        self.do_click(self.generic_locators["delete_resource"], enable_screenshot=True)

        logger.info(f"Confirm {resource} Deletion")
        # same PopUp both for OBC and OB
        self.do_click(self.generic_locators["confirm_action"], enable_screenshot=True)


class ObjectBucketClaimsTab(ObjectService, BucketsUI, CreateResourceForm):
    """
    A class representation for abstraction of OBC-related OpenShift UI actions

    """

    def __init__(self):
        BucketsUI.__init__(self)
        CreateResourceForm.__init__(self)
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

        """
        # create_obc_ui procedure should start from home page even if prev test failed in the middle
        self.navigate_OCP_home_page()
        self.navigate_object_bucket_claims_page()

        self.select_openshift_storage_project(config.ENV_DATA["cluster_namespace"])

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

        self.select_openshift_storage_project(config.ENV_DATA["cluster_namespace"])

        self.delete_resource(delete_via, obc_name)
