"""
Helper functions specific for DR User Interface
"""

import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.helpers.dr_helpers import (
    get_current_primary_cluster_name,
    get_current_secondary_cluster_name,
)
from ocs_ci.ocs.ui.acm_ui import AcmPageNavigator
from ocs_ci.ocs.acm.acm import AcmAddClusters
from ocs_ci.ocs.acm import acm
from ocs_ci.ocs.ui.views import locators
from ocs_ci.ocs.ui.helpers_ui import format_locator
from ocs_ci.utility.utils import get_ocp_version

log = logging.getLogger(__name__)


def dr_ui_validation_before_operation():
    """
     This function calls other functions to navigate to ACM UI from OCP console of Hub and does pre-checks
     such as Submariner validation from ACM console for Regional DR.
     Submariner check will not be done for Metro DR and only navigation to ACM console will take place.

    This function is only applicable for Regional DR.

    """
    ui_driver = acm.login_to_acm()
    # acm_page_nav_obj = AcmPageNavigator(ui_driver)
    acm_add_clusters_obj = AcmAddClusters(ui_driver)

    # acm_page_nav_obj.navigate_infrastructure_env_page()

    multicluster_mode = config.MULTICLUSTER.get("multicluster_mode", None)

    # TODO: remove the cluster_set_name name for Jenkins runs to auto fetch it.
    if multicluster_mode == constants.RDR_MODE:
        acm_add_clusters_obj.submariner_validation_ui(cluster_set_name="myclusterset")


def check_cluster_status_on_acm_console(
    down_cluster_name=None,
    cluster_names=None,
    timeout=900,
    expected_text=constants.STATUS_READY,
    wait=False,
):
    """
    This function checks the current status of imported clusters on the ACM console.
    These clusters are the managed OCP clusters and the ACM Hub cluster.

    Args:
        down_cluster_name (str): If Failover is performed when a cluster goes down, wait is set to True & the updated
                            status of cluster unavailability is checked on the ACM console.
                            It takes the cluster name which is down.
        cluster_names (list): This is a list of cluster names involved in a DR setup. You can either pass the cluster
                            names as args in the form of list, but if not passed, it fetches the primary & secondary
                            cluster names passed at run time for context setting
                            (max. 3 for now including ACM Hub cluster).
                            ACM Hub cluster name is hard coded as "local-cluster" as the name is constant & isn't
                            expected to change.
        timeout (int): Timeout to wait for certain elements to be found on the ACM UI
        expected_text (str): Any particular string/status of the cluster to be checked on the ACM console.
        wait (bool): When True, cluster status check will be done for the expected text/status in the given timeout.

    """

    ocp_version = get_ocp_version()
    ui_driver = acm.login_to_acm()
    acm_obj = AcmPageNavigator(ui_driver)
    acm_loc = locators[ocp_version]["acm_page"]

    acm_obj.navigate_clusters_page()

    if down_cluster_name:
        wait = True
        check_cluster_unavailability = acm_obj.wait_until_expected_text_is_found(
            format_locator(acm_loc["cluster_status_check"], down_cluster_name),
            expected_text="Unavailable",
            timeout=timeout,
        )
        if check_cluster_unavailability:
            log.info(f"Down cluster {down_cluster_name} is {expected_text}")
            return True
        else:
            cluster_status = acm_obj.get_element_text(acm_loc["cluster_status_check"])
            assert cluster_status == constants.STATUS_READY, (
                f"Down cluster {down_cluster_name} is still in {cluster_status} state after {timeout} seconds,"
                f"expected status is {expected_text}"
            )
            if (
                not check_cluster_unavailability
                and cluster_status != constants.STATUS_READY
            ):
                # Overall cluster status should change as per BZ 2155203, hence the below code is written
                # and can be further modified depending upon the fix
                other_expected_status = ["NotReady", "Offline", "Error", "Unknown"]
                for status in other_expected_status:
                    check_cluster_unavailability = (
                        acm_obj.wait_until_expected_text_is_found(
                            format_locator(
                                acm_loc["cluster_status_check"], down_cluster_name
                            ),
                            expected_text=status,
                            timeout=30,
                        )
                    )
                    if check_cluster_unavailability:
                        f"Cluster {down_cluster_name} is in {status} state"
                        return True
            else:
                log.error(
                    f"Down cluster {down_cluster_name} status check failed, actual status is {cluster_status}"
                )
                return False
    if not cluster_names:
        primary_cluster = get_current_primary_cluster_name
        secondary_cluster = get_current_secondary_cluster_name
        cluster_names = ["local-cluster", primary_cluster, secondary_cluster]
    for cluster in cluster_names:
        acm_obj.do_click(format_locator(acm_loc["cluster_name"], cluster))
        cluster_status = acm_obj.get_element_text(acm_loc["cluster_status_check"])
        log.info(f"Cluster {cluster} status is {cluster_status} on ACM UI")
        if wait:
            wait_cluster_readiness = acm_obj.wait_until_expected_text_is_found(
                format_locator(acm_loc["cluster_status_check"], cluster),
                expected_text=expected_text,
                timeout=timeout,
            )
            log.info(f"Status of {cluster} is {cluster_status}")
            log.info("Navigate back to Clusters page to check status of other clusters")
            acm_obj.do_click(acm_loc["clusters-page"], enable_screenshot=True)
            if not wait_cluster_readiness:
                return False
    return True


def failover_relocate_ui(
    workload_to_move=None,
    policy_name=None,
    failover_or_preferred_cluster=None,
    action=constants.ACTION_FAILOVER,
    timeout=30,
):
    """
    Function to perform Failover/Relocate operations via ACM UI

    Args:
        workload_to_move (str): Name of running workloads on which action to be taken
        policy_name (str): Name of the DR policy applied to the running workloads
        failover_or_preferred_cluster (str): Name of the failover cluster or preferred cluster to which workloads
                                            will be moved
        action (str): action could be "Failover" or "Relocate", "Failover" is set to default
        timeout (int): timeout to wait for certain elements to be found on the ACM UI

    Returns:
            bool: True if the action is triggered, raises Exception if any of the mandatory argument is not provided

    """

    if workload_to_move and policy_name and failover_or_preferred_cluster:
        ocp_version = get_ocp_version()
        acm_loc = locators[ocp_version]["acm_page"]
        ui_driver = acm.login_to_acm()
        acm_obj = AcmPageNavigator(ui_driver)
        acm_loc = locators[ocp_version]["acm_page"]
        acm_obj.navigate_data_services()
        acm_obj.do_click(acm_loc["applications-page"], enable_screenshot=True)
        acm_obj.do_click(acm_loc["apply-filter"])
        acm_obj.do_click(acm_loc["subscription"], enable_screenshot=True)
        workload_check = acm_obj.wait_until_expected_text_is_found(
            format_locator(acm_loc["workload-name"], workload_to_move),
            expected_text=workload_to_move,
            timeout=timeout,
        )
        assert workload_check, f"Workload {workload_to_move} not found on ACM UI"
        log.info(f"Workload found on ACM UI is {workload_to_move}")
        acm_obj.do_click(acm_loc["kebab-action"], enable_screenshot=True)
        if action == constants.ACTION_FAILOVER:
            acm_obj.do_click(acm_loc["failover-app"], enable_screenshot=True)
        else:
            acm_obj.do_click(acm_loc["relocate-app"], enable_screenshot=True)
        acm_obj.do_click(acm_loc["policy-dropdown"], enable_screenshot=True)
        acm_obj.do_click(
            format_locator(acm_loc["select-policy"], policy_name),
            enable_screenshot=True,
        )
        acm_obj.do_click(acm_loc["target-cluster-dropdown"], enable_screenshot=True)
        acm_obj.do_click(
            format_locator(
                acm_loc["failover-preferred-cluster-name"],
                failover_or_preferred_cluster,
            ),
            enable_screenshot=True,
        )
        if action == constants.ACTION_FAILOVER:
            assert acm_obj.wait_until_expected_text_is_found(
                locator=acm_loc["operation-readiness"],
                expected_text=constants.STATUS_READY,
            ), "Failover Operation readiness check failed"
        else:
            assert acm_obj.wait_until_expected_text_is_found(
                locator=acm_loc["operation-readiness"],
                expected_text=constants.STATUS_READY,
            ), "Relocate Operation readiness check failed"
        acm_obj.do_click(acm_loc["subscription-dropdown"], enable_screenshot=True)
        log.info("Click on Initiate button to failover/relocate")
        acm_obj.do_click(acm_loc["initiate-action"], enable_screenshot=True)
        if action == constants.ACTION_FAILOVER:
            log.info("Failover trigerred")
        else:
            log.info("Relocate trigerred")
        title_alert_after_action = acm_obj.get_element_text(
            acm_loc["title-alert-after-action"]
        )
        if action == constants.ACTION_FAILOVER:
            assert (
                title_alert_after_action == "Failover initiated"
            ), "Issue initiating Failover"
        else:
            assert (
                title_alert_after_action == "Relocate initiated"
            ), "Issue initiating Relocate"
        log.info("Close the action modal")
        acm_obj.do_click(acm_loc["close-action-modal"], enable_screenshot=True)
        return True
    else:
        log.error(
            "Incorrect params or version to perform Failover/Relocate operations from ACM console"
        )
        raise NotImplementedError


def verify_failover_relocate_status_ui(action=constants.ACTION_FAILOVER, timeout=900):
    """
    Function to verify if Failover/Relocate was successfully triggered from ACM UI or not

    Args:
        action (str): action "Failover" or "Relocate" which was taken on the workloads and to be verified,
                    "Failover" is set to default
        timeout (int): timeout to wait for certain elements to be found on the ACM UI
    """

    ocp_version = get_ocp_version()
    ui_driver = acm.login_to_acm()
    acm_obj = AcmPageNavigator(ui_driver)
    acm_loc = locators[ocp_version]["acm_page"]

    log.info(
        "Click on drpolicy hyperlink under Data policy column on Applications page"
    )
    acm_obj.do_click(acm_loc["data-policy-hyperlink"], enable_screenshot=True)
    log.info("Click on View more details")
    acm_obj.do_click(acm_loc["view-more-details"], enable_screenshot=True)
    if action == constants.ACTION_FAILOVER:
        action_status = acm_obj.wait_until_expected_text_is_found(
            acm_loc["action-status-failover"],
            expected_text="FailedOver",
            timeout=timeout,
        )
        assert action_status, "Failover verification from UI failed"
    else:
        action_status = acm_obj.wait_until_expected_text_is_found(
            acm_loc["action-status-relocate"],
            expected_text="Relocated",
            timeout=timeout,
        )
        assert action_status, "Relocate verification from UI failed"
