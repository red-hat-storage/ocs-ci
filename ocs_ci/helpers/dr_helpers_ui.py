"""
Helper functions specific to DR User Interface
"""

import logging

from selenium.common.exceptions import NoSuchElementException
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.ui.views import locators
from ocs_ci.ocs.ui.helpers_ui import format_locator
from ocs_ci.utility.utils import get_ocp_version
from ocs_ci.ocs.utils import get_non_acm_cluster_config

log = logging.getLogger(__name__)


def dr_submariner_validation_from_ui(acm_obj):
    """
    This function is only applicable for Regional DR.

    This function calls other function and does pre-checks on ACM UI
    such as Submariner validation from ACM console for Regional DR.

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class

    """
    multicluster_mode = config.MULTICLUSTER.get("multicluster_mode", None)
    if multicluster_mode == constants.RDR_MODE:
        # Add an arg to below function and pass the cluster_set_name created on your cluster
        # when running the test locally.
        acm_obj.submariner_validation_ui()


def check_cluster_status_on_acm_console(
    acm_obj,
    down_cluster_name=None,
    cluster_names=None,
    timeout=900,
    expected_text=constants.STATUS_READY,
):
    """
    This function checks the current status of imported clusters on the ACM console.
    These clusters are the managed OCP clusters and the ACM Hub cluster.

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        down_cluster_name (str): If Failover is performed when a cluster goes down, it waits and checks the updated
                            status of cluster unavailability on the ACM console.
                            It takes the cluster name which is down.
        cluster_names (list): This is a list of cluster names involved in a DR setup. You can either pass the cluster
                            names as args in the form of list, but if not passed, it fetches the primary & secondary
                            cluster names passed at run time for context setting
                            (max. 3 for now including ACM Hub cluster).
                            ACM Hub cluster name is hard coded as "local-cluster" as the name is constant & isn't
                            expected to change.
        timeout (int): Timeout to wait for certain elements to be found on the ACM UI
        expected_text (str): Any particular string/status of the cluster to be checked on the ACM console.
                            Default is set to ready

    """
    ocp_version = get_ocp_version()
    acm_loc = locators[ocp_version]["acm_page"]
    acm_obj.navigate_clusters_page()
    if down_cluster_name:
        log.info(
            "Down cluster name is provided, checking it's updated status on ACM console"
        )
        acm_obj.do_click(format_locator(acm_loc["cluster_name"], down_cluster_name))
        check_cluster_unavailability = acm_obj.wait_until_expected_text_is_found(
            format_locator(acm_loc["cluster_status_check"], expected_text),
            expected_text=expected_text,
            timeout=timeout,
        )
        if check_cluster_unavailability:
            log.info(f"Down cluster {down_cluster_name} is '{expected_text}'")
            acm_obj.take_screenshot()
            log.info("Navigate back to Clusters page")
            acm_obj.do_click(acm_loc["clusters-page"])
            return True
        else:
            check_cluster_availability = acm_obj.wait_until_expected_text_is_found(
                format_locator(acm_loc["cluster_status_check"], "Ready"),
                expected_text="Ready",
                timeout=30,
            )
            assert check_cluster_availability, (
                f"Down cluster {down_cluster_name} is still in {constants.STATUS_READY} state after {timeout} seconds,"
                f"expected status is {expected_text}"
            )
            log.info(
                f"Checking other expected statuses cluster {down_cluster_name} could be in on ACM UI "
                f"due to Node shutdown"
            )
            # Overall cluster status should change when only a few nodes of the cluster are down
            # as per BZ 2155203, hence the below code is written
            # and can be further implemented depending upon the fix.
            other_expected_status = ["Unavailable", "NotReady", "Offline", "Error"]
            for status in other_expected_status:
                check_cluster_unavailability_again = (
                    acm_obj.wait_until_expected_text_is_found(
                        format_locator(acm_loc["cluster_status_check"], status),
                        expected_text=status,
                        timeout=10,
                    )
                )
                if check_cluster_unavailability_again:
                    f"Cluster {down_cluster_name} is in {status} state on ACM UI"
                    acm_obj.take_screenshot()
                    log.info("Navigate back to Clusters page")
                    acm_obj.do_click(acm_loc["clusters-page"])
                    return True
            log.error(f"Down cluster {down_cluster_name} status check failed")
            acm_obj.take_screenshot()
            return False
    else:
        if not cluster_names:
            cluster_names = ["local-cluster"]
            for cluster in get_non_acm_cluster_config():
                cluster_names.append(cluster.ENV_DATA["cluster_name"])
        for cluster in cluster_names:
            log.info(f"Checking status of cluster {cluster} on ACM UI")
            acm_obj.do_click(format_locator(acm_loc["cluster_name"], cluster))
            cluster_status = acm_obj.get_element_text(
                format_locator(acm_loc["cluster_status_check"], expected_text)
            )
            if cluster_status == expected_text:
                log.info(f"Cluster {cluster} status is {cluster_status} on ACM UI")
                log.info("Navigate back to Clusters page")
                acm_obj.do_click(acm_loc["clusters-page"], enable_screenshot=True)
            else:
                wait_for_expected_status = acm_obj.wait_until_expected_text_is_found(
                    format_locator(acm_loc["cluster_status_check"], expected_text),
                    timeout=900,
                )
                if wait_for_expected_status:
                    log.info(f"Cluster {cluster} status is {expected_text} on ACM UI")
                    log.info("Navigate back to Clusters page")
                    acm_obj.do_click(acm_loc["clusters-page"], enable_screenshot=True)
                else:
                    log.error(
                        f"Cluster {cluster} status is not {expected_text} on ACM UI"
                    )
                    log.info("Navigate back to Clusters page")
                    acm_obj.do_click(acm_loc["clusters-page"], enable_screenshot=True)
                    return False


def verify_drpolicy_ui(acm_obj, scheduling_interval):
    """
    Function to verify DRPolicy status and replication policy on Data Policies page of ACM console

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        scheduling_interval (int): Scheduling interval in the DRPolicy to be verified on ACM UI

    """
    ocp_version = get_ocp_version()
    acm_loc = locators[ocp_version]["acm_page"]
    acm_obj.navigate_data_services()
    log.info("Verify status of DRPolicy on ACM UI")
    policy_status = acm_obj.wait_until_expected_text_is_found(
        acm_loc["drpolicy-status"], expected_text="Validated"
    )
    if policy_status:
        log.info(f"DRPolicy status on ACM UI is {constants.DRPOLICY_STATUS}")
    else:
        log.error(
            f"DRPolicy status on ACM UI is not {constants.DRPOLICY_STATUS}, can not proceed"
        )
        raise NoSuchElementException
    log.info("Verify Replication policy on ACM UI")
    replication_policy = acm_obj.get_element_text(acm_loc["replication-policy"])
    multicluster_mode = config.MULTICLUSTER.get("multicluster_mode", None)
    if multicluster_mode == constants.RDR_MODE:
        assert (
            replication_policy
            == f"{constants.RDR_REPLICATION_POLICY}, interval: {scheduling_interval}m"
        ), f"Replication policy on ACM UI is {replication_policy}, can not proceed"
    log.info("DRPolicy successfully validated on ACM UI")


def failover_relocate_ui(
    acm_obj,
    scheduling_interval=0,
    workload_to_move=None,
    policy_name=None,
    failover_or_preferred_cluster=None,
    action=constants.ACTION_FAILOVER,
    timeout=120,
    move_workloads_to_same_cluster=False,
):
    """
    Function to perform Failover/Relocate operations via ACM UI

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        scheduling_interval (int): scheduling interval value from DRPolicy
        workload_to_move (str): Name of running workloads on which action to be taken
        policy_name (str): Name of the DR policy applied to the running workload
        failover_or_preferred_cluster (str): Name of the failover cluster or preferred cluster to which workloads
                                            will be moved
        action (str): action could be "Failover" or "Relocate", "Failover" is set to default
        timeout (int): timeout to wait for certain elements to be found on the ACM UI
        move_workloads_to_same_cluster (bool): Bool condition to test negative failover/relocate scenarios to move
                                            running workloads to same cluster
    Returns:
            bool: True if the action is triggered, raises Exception if any of the mandatory argument is not provided

    """
    if workload_to_move and policy_name and failover_or_preferred_cluster:
        ocp_version = get_ocp_version()
        acm_loc = locators[ocp_version]["acm_page"]
        verify_drpolicy_ui(acm_obj, scheduling_interval=scheduling_interval)
        acm_obj.navigate_applications_page()
        log.info("Click on search bar")
        acm_obj.do_click(acm_loc["search-bar"])
        log.info("Clear existing text from search bar if any")
        acm_obj.do_clear(acm_loc["search-bar"])
        log.info("Enter the workload to be searched")
        acm_obj.do_send_keys(acm_loc["search-bar"], text=workload_to_move)
        log.info("Click on kebab menu option")
        acm_obj.do_click(acm_loc["kebab-action"], enable_screenshot=True)
        if action == constants.ACTION_FAILOVER:
            log.info("Selecting action as Failover from ACM UI")
            acm_obj.do_click(
                acm_loc["failover-app"], enable_screenshot=True, timeout=timeout
            )
        else:
            log.info("Selecting action as Relocate from ACM UI")
            acm_obj.do_click(
                acm_loc["relocate-app"], enable_screenshot=True, timeout=timeout
            )
        log.info("Click on policy dropdown")
        acm_obj.do_click(acm_loc["policy-dropdown"], enable_screenshot=True)
        log.info("Select policy from policy dropdown")
        acm_obj.do_click(
            format_locator(acm_loc["select-policy"], policy_name),
            enable_screenshot=True,
        )
        log.info("Click on target cluster dropdown")
        acm_obj.do_click(acm_loc["target-cluster-dropdown"], enable_screenshot=True)
        if move_workloads_to_same_cluster:
            log.info("Select target cluster same as current primary cluster on ACM UI")
            acm_obj.do_click(
                format_locator(
                    acm_loc["failover-preferred-cluster-name"],
                    failover_or_preferred_cluster,
                ),
                enable_screenshot=True,
            )
        else:
            log.info("Select target cluster on ACM UI")
            acm_obj.do_click(
                format_locator(
                    acm_loc["failover-preferred-cluster-name"],
                    failover_or_preferred_cluster,
                ),
                enable_screenshot=True,
            )
        log.info("Check operation readiness")
        if action == constants.ACTION_FAILOVER:
            if move_workloads_to_same_cluster:
                assert not acm_obj.wait_until_expected_text_is_found(
                    locator=acm_loc["operation-readiness"],
                    expected_text=constants.STATUS_READY,
                    timeout=30,
                ), "Failover Operation readiness check failed"
                log.info("Failover readiness is False as expected")
            else:
                assert acm_obj.wait_until_expected_text_is_found(
                    locator=acm_loc["operation-readiness"],
                    expected_text=constants.STATUS_READY,
                ), "Failover Operation readiness check failed"
        else:
            if move_workloads_to_same_cluster:
                assert not acm_obj.wait_until_expected_text_is_found(
                    locator=acm_loc["operation-readiness"],
                    expected_text=constants.STATUS_READY,
                    timeout=30,
                ), "Relocate Operation readiness check failed"
                log.info("Relocate readiness is False as expected")
            else:
                assert acm_obj.wait_until_expected_text_is_found(
                    locator=acm_loc["operation-readiness"],
                    expected_text=constants.STATUS_READY,
                ), "Relocate Operation readiness check failed"
        initiate_btn = acm_obj.find_an_element_by_xpath(
            "//button[@id='modal-intiate-action']"
        )
        aria_disabled = initiate_btn.get_attribute("aria-disabled")
        if move_workloads_to_same_cluster:
            if aria_disabled == "false":
                log.error(
                    "Initiate button in enabled to failover/relocate on the same cluster"
                )
                acm_obj.take_screenshot()
                return False
            else:
                log.info(
                    "As expected, initiate button is disabled to failover/relocate on the same cluster"
                )
                acm_obj.take_screenshot()
                return True
        log.info("Click on subscription dropdown")
        acm_obj.do_click(acm_loc["subscription-dropdown"], enable_screenshot=True)
        # TODO: Commented below lines due to Regression BZ2208637
        # log.info("Check peer readiness")
        # assert acm_obj.wait_until_expected_text_is_found(
        #     locator=acm_loc["peer-ready"],
        #     expected_text=constants.PEER_READY,
        # ), f"Peer is not ready, can not initiate {action}"
        acm_obj.take_screenshot()
        if aria_disabled == "true":
            log.error("Initiate button in not enabled to failover/relocate")
            return False
        else:
            log.info("Click on Initiate button to failover/relocate")
            acm_obj.do_click(acm_loc["initiate-action"], enable_screenshot=True)
            if action == constants.ACTION_FAILOVER:
                log.info(
                    f"Failover for workload {workload_to_move} triggered from ACM UI"
                )
            else:
                log.info(
                    f"Relocate for workload {workload_to_move} triggered from ACM UI"
                )
            acm_obj.take_screenshot()
            log.info("Close the action modal")
            acm_obj.do_click(acm_loc["close-action-modal"], enable_screenshot=True)
            return True
    else:
        log.error(
            "Incorrect or missing params to perform Failover/Relocate operation from ACM UI"
        )
        raise NotImplementedError


def verify_failover_relocate_status_ui(
    acm_obj, action=constants.ACTION_FAILOVER, timeout=120, workload_to_check=None
):
    """
    Function to verify current status of in progress Failover/Relocate operation on ACM UI

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        action (str): action "Failover" or "Relocate" which was taken on the workloads,
                    "Failover" is set to default
        timeout (int): timeout to wait for certain elements to be found on the ACM UI
        workload_to_check (str): Name of workload whose status needs to be checked on the ACM UI

    """
    ocp_version = get_ocp_version()
    acm_loc = locators[ocp_version]["acm_page"]
    acm_obj.navigate_applications_page()
    log.info("Click on search bar")
    acm_obj.do_click(acm_loc["search-bar"])
    log.info("Clear existing text from search bar if any")
    acm_obj.do_clear(acm_loc["search-bar"])
    log.info("Enter the workload to be searched")
    acm_obj.do_send_keys(acm_loc["search-bar"], text=workload_to_check)
    data_policy_hyperlink = acm_obj.wait_until_expected_text_is_found(
        locator=acm_loc["data-policy-hyperlink"],
        expected_text="1 policy",
        timeout=timeout,
    )
    if data_policy_hyperlink:
        log.info(
            "Click on drpolicy hyperlink under Data policy column on Applications page"
        )
        acm_obj.do_click(acm_loc["data-policy-hyperlink"], enable_screenshot=True)
    else:
        log.error(
            "drpolicy hyperlink under Data policy column on Applications page not found,"
            "can not proceed with verification"
        )
        raise NoSuchElementException
    log.info("Click on View more details")
    acm_obj.do_click(acm_loc["view-more-details"], enable_screenshot=True)
    log.info("Verifying failover/relocate status on ACM UI")
    if action == constants.ACTION_FAILOVER:
        action_status = acm_obj.wait_until_expected_text_is_found(
            acm_loc["action-status-failover"],
            expected_text="FailedOver",
            timeout=timeout,
        )
        fetch_status = acm_obj.get_element_text(acm_loc["action-status-failover"])
        assert action_status, "Failover verification from ACM UI failed"
        log.info(
            f"{action} successfully verified for workload {workload_to_check} on ACM UI, status is {fetch_status}"
        )
    else:
        action_status = acm_obj.wait_until_expected_text_is_found(
            acm_loc["action-status-relocate"],
            expected_text="Relocated",
            timeout=timeout,
        )
        fetch_status = acm_obj.get_element_text(acm_loc["action-status-relocate"])
        assert action_status, "Relocate verification from ACM UI failed"
        log.info(
            f"{action} successfully verified for workload {workload_to_check} on ACM UI, status is {fetch_status}"
        )
    close_action_modal = acm_obj.wait_until_expected_text_is_found(
        acm_loc["close-action-modal"], expected_text="Close", timeout=120
    )
    if close_action_modal:
        log.info("Close button found")
        acm_obj.take_screenshot()
        acm_obj.do_click_by_xpath("//*[text()='Close']")
        log.info(
            f"Data policy modal page closed, {action} on workload {workload_to_check} completed"
        )
    else:
        log.error("Close button not found, next iteration may fail")
        acm_obj.take_screenshot()
