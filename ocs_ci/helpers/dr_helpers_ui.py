"""
Helper functions specific to DR User Interface
"""

import logging

from selenium.common.exceptions import NoSuchElementException
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.ui.acm_ui import AcmPageNavigator
from ocs_ci.ocs.acm.acm import AcmAddClusters
from ocs_ci.ocs.acm import acm
from ocs_ci.ocs.ui.views import locators
from ocs_ci.ocs.ui.helpers_ui import format_locator
from ocs_ci.utility.utils import get_ocp_version
from ocs_ci.ocs.utils import get_non_acm_cluster_config

log = logging.getLogger(__name__)


def dr_submariner_validation_from_ui():
    """
    This function is only applicable for Regional DR.

    This function calls other functions and does pre-checks on ACM UI
    such as Submariner validation from ACM console for Regional DR.

    """
    ui_driver = acm.login_to_acm()
    acm_add_clusters_obj = AcmAddClusters(ui_driver)
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
                            Default is set to ready
        wait (bool): When True, additional cluster status check will be done for the expected text/status
                    in the given timeout.

    """

    ocp_version = get_ocp_version()
    ui_driver = acm.login_to_acm()
    acm_obj = AcmPageNavigator(ui_driver)
    acm_loc = locators[ocp_version]["acm_page"]
    acm_obj.navigate_clusters_page()
    if down_cluster_name:
        wait = True
        acm_obj.do_click(format_locator(acm_loc["cluster_name"], down_cluster_name))
        cluster_status = acm_obj.get_element_text(
            format_locator(acm_loc["cluster_status_check"], expected_text)
        )
        log.info(f"Cluster {down_cluster_name} status is {cluster_status} on ACM UI")
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
                timeout=timeout,
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
            # and can be further modified depending upon the fix.
            other_expected_status = ["Unavailable", "NotReady", "Offline", "Error"]
            for status in other_expected_status:
                check_cluster_unavailability = (
                    acm_obj.wait_until_expected_text_is_found(
                        format_locator(acm_loc["cluster_status_check"], status),
                        expected_text=status,
                        timeout=30,
                    )
                )
                if check_cluster_unavailability:
                    f"Cluster {down_cluster_name} is in {status} state on ACM UI"
                    acm_obj.take_screenshot()
                    log.info("Navigate back to Clusters page")
                    acm_obj.do_click(acm_loc["clusters-page"])
                    return True
                else:
                    log.error(f"Down cluster {down_cluster_name} status check failed")
                    acm_obj.take_screenshot()
                    return False
    else:
        if not cluster_names:
            cluster_names = ["local-cluster"]
            for cluster in get_non_acm_cluster_config():
                cluster_names.append(cluster.ENV_DATA["cluster_name"])
        for cluster in cluster_names:
            acm_obj.do_click(format_locator(acm_loc["cluster_name"], cluster))
            cluster_status = acm_obj.get_element_text(
                format_locator(acm_loc["cluster_status_check"], expected_text)
            )
            log.info(f"Cluster {cluster} status is {cluster_status} on ACM UI")
            if wait:
                wait_cluster_readiness = acm_obj.wait_until_expected_text_is_found(
                    format_locator(acm_loc["cluster_status_check"], expected_text),
                    expected_text=expected_text,
                    timeout=timeout,
                )
                log.info(f"Status of {cluster} is {cluster_status}")
                log.info("Navigate back to Clusters page")
                acm_obj.do_click(acm_loc["clusters-page"], enable_screenshot=True)
                if not wait_cluster_readiness:
                    return False
        return True


def verify_drpolicy_ui(scheduling_interval):
    """
    Function to verify DRPolicy status and replication policy on Data Policies page of ACM console

    Args:
        scheduling_interval (int): Scheduling interval in the DRPolicy to be verified on ACM UI

    """
    ocp_version = get_ocp_version()
    ui_driver = acm.login_to_acm()
    acm_obj = AcmPageNavigator(ui_driver)
    acm_loc = locators[ocp_version]["acm_page"]
    acm_obj.navigate_clusters_page()
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
    scheduling_interval,
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
        ui_driver = acm.login_to_acm()
        acm_obj = AcmPageNavigator(ui_driver)
        acm_loc = locators[ocp_version]["acm_page"]
        verify_drpolicy_ui(scheduling_interval=scheduling_interval)
        acm_obj.navigate_applications_page()
        log.info("Apply Filter on Applications page")
        acm_obj.do_click(acm_loc["apply-filter"])
        log.info("Select subscription from filters")
        acm_obj.do_click(acm_loc["subscription"], enable_screenshot=True)
        workload_check = acm_obj.wait_until_expected_text_is_found(
            format_locator(acm_loc["workload-name"], workload_to_move),
            expected_text=workload_to_move,
            timeout=timeout,
        )
        assert workload_check, f"Workload {workload_to_move} not found on ACM UI"
        log.info(f"Workload {workload_to_move} found on ACM UI")
        log.info("Click on kebab menu option")
        acm_obj.do_click(acm_loc["kebab-action"], enable_screenshot=True)
        if action == constants.ACTION_FAILOVER:
            log.info("Selecting action as Failover from ACM UI")
            acm_obj.do_click(acm_loc["failover-app"], enable_screenshot=True)
        else:
            log.info("Selecting action as Relocate from ACM UI")
            acm_obj.do_click(acm_loc["relocate-app"], enable_screenshot=True)
        log.info("Click on policy dropdown")
        acm_obj.do_click(acm_loc["policy-dropdown"], enable_screenshot=True)
        log.info("Select policy from policy dropdown")
        acm_obj.do_click(
            format_locator(acm_loc["select-policy"], policy_name),
            enable_screenshot=True,
        )
        log.info("Click on target cluster dropdown")
        acm_obj.do_click(acm_loc["target-cluster-dropdown"], enable_screenshot=True)
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
            assert acm_obj.wait_until_expected_text_is_found(
                locator=acm_loc["operation-readiness"],
                expected_text=constants.STATUS_READY,
            ), "Failover Operation readiness check failed"
        else:
            assert acm_obj.wait_until_expected_text_is_found(
                locator=acm_loc["operation-readiness"],
                expected_text=constants.STATUS_READY,
            ), "Relocate Operation readiness check failed"
        log.info("Click on subscription dropdown")
        acm_obj.do_click(acm_loc["subscription-dropdown"], enable_screenshot=True)
        log.info("Check peer readiness")
        assert acm_obj.wait_until_expected_text_is_found(
            locator=acm_loc["peer-ready"],
            expected_text=constants.PEER_READY,
        ), f"Peer is not ready, can not initiate {action}"
        acm_obj.take_screenshot()
        log.info("Click on Initiate button to failover/relocate")
        acm_obj.do_click(acm_loc["initiate-action"], enable_screenshot=True)
        if action == constants.ACTION_FAILOVER:
            log.info("Failover trigerred from ACM UI")
        else:
            log.info("Relocate trigerred from ACM UI")
        acm_obj.take_screenshot()
        log.info("Close the action modal")
        acm_obj.do_click(acm_loc["close-action-modal"], enable_screenshot=True)
        return True
    else:
        log.error(
            "Incorrect or missing params to perform Failover/Relocate operation from ACM UI"
        )
        raise NotImplementedError


def verify_failover_relocate_status_ui(action=constants.ACTION_FAILOVER, timeout=900):
    """
    Function to verify current status of in progress Failover/Relocate operation on ACM UI

    Args:
        action (str): action "Failover" or "Relocate" which was taken on the workloads,
                    "Failover" is set to default
        timeout (int): timeout to wait for certain elements to be found on the ACM UI
    """

    ocp_version = get_ocp_version()
    ui_driver = acm.login_to_acm()
    acm_obj = AcmPageNavigator(ui_driver)
    acm_loc = locators[ocp_version]["acm_page"]
    acm_obj.navigate_clusters_page()
    acm_obj.navigate_applications_page()
    log.info("Apply Filter on Applications page")
    acm_obj.do_click(acm_loc["apply-filter"])
    log.info("Select subscription from filters")
    acm_obj.do_click(acm_loc["subscription"], enable_screenshot=True)
    data_policy_hyperlink = acm_obj.wait_until_expected_text_is_found(
        locator=acm_loc["data-policy-hyperlink"], expected_text="1 policy", timeout=30
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
        assert action_status, "Failover verification from ACM UI failed"
    else:
        action_status = acm_obj.wait_until_expected_text_is_found(
            acm_loc["action-status-relocate"],
            expected_text="Relocated",
            timeout=timeout,
        )
        assert action_status, "Relocate verification from ACM UI failed"
    log.info(f"{action} successfully verified on ACM UI, status is {action_status}")
