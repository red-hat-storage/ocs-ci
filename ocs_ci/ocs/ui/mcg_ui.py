import logging

from ocs_ci.ocs.ui.base_ui import PageNavigator
from ocs_ci.ocs.ui.views import locators
from ocs_ci.utility.utils import get_ocp_version

logger = logging.getLogger(__name__)


class ObcUI(PageNavigator):
    """
    User Interface Selenium

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
