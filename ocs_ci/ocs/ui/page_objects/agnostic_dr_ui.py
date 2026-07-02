"""
Page Object for the ACM Disaster Recovery UI — agnostic DR DRPolicy creation wizard.
"""

import logging

from ocs_ci.ocs import constants
from ocs_ci.ocs.ui.acm_ui import AcmPageNavigator
from ocs_ci.ocs.ui.helpers_ui import format_locator
from ocs_ci.ocs.ui.views import locators_for_current_ocp_version

logger = logging.getLogger(__name__)


class AgnosticDRPolicyPage(AcmPageNavigator):
    """
    Page Object for the ACM 'Create DRPolicy' wizard used in agnostic DR.

    Locators are loaded from the ``agnostic_dr_page`` key in views.py.
    Inherits navigation helpers from AcmPageNavigator (navigate_data_services,
    do_click, do_send_keys, etc.).
    """

    def __init__(self):
        super().__init__()
        self.agnostic_dr_loc = locators_for_current_ocp_version()["agnostic_dr_page"]

    def navigate_to_policies_tab(self):
        """
        Navigate to Data Services > Disaster Recovery > Policies tab.
        """
        acm_loc = locators_for_current_ocp_version()["acm_page"]
        self.navigate_data_services()
        logger.info("Clicking Policies tab under Disaster Recovery")
        self.do_click(
            acm_loc["Policies"],
            avoid_stale=True,
            enable_screenshot=True,
            timeout=120,
        )

    def click_create_drpolicy(self):
        """
        Click the 'Create DRPolicy' button to open the creation wizard.
        """
        logger.info("Clicking 'Create DRPolicy' button")
        self.do_click(
            self.agnostic_dr_loc["create-drpolicy-btn"],
            enable_screenshot=True,
            timeout=60,
        )

    def fill_policy_name(self, policy_name):
        """
        Enter the policy name in the wizard.

        Args:
            policy_name (str): Name for the DRPolicy resource.
        """
        logger.info("Filling policy name: %s", policy_name)
        self.do_send_keys(self.agnostic_dr_loc["drpolicy-name-input"], policy_name)

    def select_managed_cluster(self, cluster_name):
        """
        Select a managed cluster by clicking its row checkbox.

        Args:
            cluster_name (str): Name of the managed cluster to select.
        """
        logger.info("Selecting managed cluster '%s' in DRPolicy wizard", cluster_name)
        self.do_click(
            format_locator(self.agnostic_dr_loc["cluster-row-checkbox"], cluster_name),
            enable_screenshot=True,
        )

    def set_scheduling_interval(self, interval_minutes):
        """
        Set the replication scheduling interval in minutes.

        Clears the default value before entering the new one.

        Args:
            interval_minutes (int): Scheduling interval in minutes.
        """
        logger.info("Setting scheduling interval to %d minutes", interval_minutes)
        locator = self.agnostic_dr_loc["scheduling-interval-input"]
        self.do_clear(locator)
        self.do_send_keys(locator, str(interval_minutes))

    def fill_s3_profile(self, prefix, cluster_name, endpoint):
        """
        Fill the S3 profile fields for one cluster.

        Args:
            prefix (str): Field prefix — ``'c1'`` for the first cluster,
                ``'c2'`` for the second.
            cluster_name (str): Used as the S3 profile name.
            endpoint (str): MinIO external endpoint URL
                (e.g. ``http://minio-minio.apps.<domain>``).
        """
        loc = self.agnostic_dr_loc
        logger.info(
            "Filling S3 profile for cluster '%s' (prefix=%s)",
            cluster_name,
            prefix,
        )
        self.do_send_keys(loc[f"{prefix}-bucket-name"], constants.MINIO_BUCKET)
        self.do_send_keys(loc[f"{prefix}-endpoint"], endpoint)
        self.do_send_keys(loc[f"{prefix}-access-key-id"], constants.MINIO_ACCESS_KEY)
        self.do_send_keys(loc[f"{prefix}-secret-key"], constants.MINIO_SECRET_KEY)
        self.do_send_keys(loc[f"{prefix}-region"], "us-east-1")
        self.do_send_keys(loc[f"{prefix}-s3-profile-name"], cluster_name)

    def submit_create_drpolicy(self):
        """
        Click the final Create button to submit the DRPolicy wizard.
        """
        logger.info("Submitting DRPolicy creation wizard")
        self.do_click(
            self.agnostic_dr_loc["drpolicy-create-btn"],
            enable_screenshot=True,
            timeout=60,
        )
        logger.info("DRPolicy creation form submitted via UI")
