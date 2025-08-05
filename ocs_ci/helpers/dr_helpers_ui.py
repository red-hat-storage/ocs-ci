"""
Helper functions specific to DR User Interface
"""

import logging
import time

from typing import List

from selenium.common.exceptions import (
    NoSuchElementException,
    StaleElementReferenceException,
)

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import ResourceWrongStatusException, TimeoutException
from ocs_ci.ocs.ui.base_ui import (
    wait_for_element_to_be_clickable,
    wait_for_element_to_be_visible,
)
from ocs_ci.ocs.ui.views import locators_for_current_ocp_version
from ocs_ci.ocs.ui.helpers_ui import format_locator
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
    acm_loc = locators_for_current_ocp_version()["acm_page"]
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
    acm_loc = locators_for_current_ocp_version()["acm_page"]
    acm_obj.navigate_data_services()
    log.info("Click on 'Policies' tab under Disaster recovery")
    acm_obj.do_click(
        acm_loc["Policies"], avoid_stale=True, enable_screenshot=True, timeout=120
    )
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
    log.info("DRPolicy and replication policy successfully validated on ACM UI")
    log.info("Navigate back to Disaster recovery Overview page")
    acm_obj.do_click(
        acm_loc["disaster-recovery-overview"], avoid_stale=True, enable_screenshot=True
    )


def failover_relocate_ui(
    acm_obj,
    scheduling_interval=0,
    workload_to_move=None,
    policy_name=None,
    failover_or_preferred_cluster=None,
    action=constants.ACTION_FAILOVER,
    timeout=120,
    move_workloads_to_same_cluster=False,
    workload_type=constants.SUBSCRIPTION,
    do_not_trigger=False,
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
        workload_type (str): Type of workload, appset or subscription
        do_not_trigger (bool): If in case you do not want to click on the Initiate button
                            so as not to initiate the operation, set it to True. It's False by default.
    Returns:
            bool: True if the action is triggered, raises Exception if any of the mandatory argument is not provided

    """
    if workload_to_move and policy_name and failover_or_preferred_cluster:
        acm_loc = locators_for_current_ocp_version()["acm_page"]
        verify_drpolicy_ui(acm_obj, scheduling_interval=scheduling_interval)
        acm_obj.navigate_applications_page()
        clear_filter = acm_obj.wait_until_expected_text_is_found(
            locator=acm_loc["clear-filter"],
            expected_text="Clear all filters",
            timeout=10,
        )
        if clear_filter:
            log.info("Clear existing filters")
            acm_obj.do_click(acm_loc["clear-filter"])
        if workload_type == constants.SUBSCRIPTION:
            log.info(f"Apply filter for workload type {constants.SUBSCRIPTION}")
            acm_obj.do_click(acm_loc["apply-filter"], enable_screenshot=True)
            acm_obj.do_click(acm_loc["sub-checkbox"], enable_screenshot=True)
        elif workload_type == constants.APPLICATION_SET:
            log.info(f"Apply filter for workload type {constants.APPLICATION_SET}")
            acm_obj.do_click(acm_loc["apply-filter"], enable_screenshot=True)
            acm_obj.do_click(acm_loc["appset-checkbox"], enable_screenshot=True)
        log.info("Click on search bar")
        acm_obj.do_click(acm_loc["search-bar"])
        log.info("Clear existing text from search bar if any")
        acm_obj.do_clear(acm_loc["search-bar"])
        log.info(f"Enter the workload to be searched {workload_to_move}")
        acm_obj.do_send_keys(acm_loc["search-bar"], text=workload_to_move)
        if action == constants.ACTION_FAILOVER:
            log.info("Selecting action as Failover from ACM UI")
            for _ in range(10):
                try:
                    log.info("Click on kebab menu option")
                    kebab_action = wait_for_element_to_be_clickable(
                        acm_loc["kebab-action"]
                    )
                    acm_obj.driver.execute_script("arguments[0].click();", kebab_action)
                    kebab_state = kebab_action.get_attribute("aria-expanded")
                    log.info(f"Kebab state: {kebab_state}")
                    if not kebab_state:
                        acm_obj.driver.execute_script(
                            "arguments[0].click();", kebab_action
                        )
                        log.info("Kebab menu options are open")
                    failover_app = wait_for_element_to_be_visible(
                        acm_loc["failover-app"]
                    )
                    acm_obj.driver.execute_script("arguments[0].click();", failover_app)
                    break
                except (TimeoutException, StaleElementReferenceException):
                    log.warning("Failover option not found, retrying...")
                    time.sleep(1)
            log.info("Failover option is selected")
        else:
            log.info("Selecting action as Relocate from ACM UI")
            for _ in range(10):
                try:
                    log.info("Click on kebab menu option")
                    kebab_action = wait_for_element_to_be_clickable(
                        acm_loc["kebab-action"]
                    )
                    acm_obj.driver.execute_script("arguments[0].click();", kebab_action)
                    kebab_state = kebab_action.get_attribute("aria-expanded")
                    log.info(f"Kebab state: {kebab_state}")
                    if not kebab_state:
                        acm_obj.driver.execute_script(
                            "arguments[0].click();", kebab_action
                        )
                        log.info("Kebab menu options are open")
                    relocate_app = wait_for_element_to_be_visible(
                        acm_loc["relocate-app"]
                    )
                    acm_obj.driver.execute_script("arguments[0].click();", relocate_app)
                    break
                except (TimeoutException, StaleElementReferenceException):
                    log.warning("Relocate option not found, retrying...")
                    time.sleep(1)
            log.info("Relocate option is selected")
        if workload_type == constants.SUBSCRIPTION:
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
                log.info(
                    "Select target cluster same as current primary cluster on ACM UI"
                )
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
                log.info("Failover readiness is Ready as expected")
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
                log.info("Relocate readiness is Ready as expected")
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
            # DRPC name is by default selected, hence no code is needed for subscription dropdown
        if aria_disabled == "true":
            log.error("Initiate button in not enabled to failover/relocate")
            return False
        else:
            if do_not_trigger:
                log.info(
                    "Failover/Relocate operation will not be triggered as intended"
                )
            else:
                log.info("Click on Initiate button to failover/relocate")
                acm_obj.do_click(
                    acm_loc["initiate-action"], enable_screenshot=True, avoid_stale=True
                )
                if action == constants.ACTION_FAILOVER:
                    log.info("Failover trigerred from ACM UI")
                else:
                    log.info("Relocate trigerred from ACM UI")
        if not do_not_trigger:
            acm_obj.take_screenshot()
            acm_obj.page_has_loaded()
            if workload_type == constants.SUBSCRIPTION:
                log.info("Close the action modal")
                acm_obj.do_click(
                    acm_loc["close-action-modal"],
                    enable_screenshot=True,
                    avoid_stale=True,
                )
                log.info(
                    f"Action modal successfully closed for {constants.SUBSCRIPTION} type workload"
                )
                # It automatically closes for Appset based workload
            return True
    else:
        log.error(
            "Incorrect or missing params to perform Failover/Relocate operation from ACM UI"
        )
        raise NotImplementedError


def verify_failover_relocate_status_ui(
    acm_obj, action=constants.ACTION_FAILOVER, timeout=120
):
    """
    Function to verify current status of in progress Failover/Relocate operation on ACM UI

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        action (str): action "Failover" or "Relocate" which was taken on the workloads,
                    "Failover" is set to default
        timeout (int): timeout to wait for certain elements to be found on the ACM UI

    """
    acm_loc = locators_for_current_ocp_version()["acm_page"]
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
        log.info(f"{action} successfully verified on ACM UI, status is {fetch_status}")
    else:
        action_status = acm_obj.wait_until_expected_text_is_found(
            acm_loc["action-status-relocate"],
            expected_text="Relocated",
            timeout=timeout,
        )
        fetch_status = acm_obj.get_element_text(acm_loc["action-status-relocate"])
        assert action_status, "Relocate verification from ACM UI failed"
        log.info(f"{action} successfully verified on ACM UI, status is {fetch_status}")
    close_action_modal = acm_obj.wait_until_expected_text_is_found(
        acm_loc["close-action-modal"], expected_text="Close", timeout=120
    )
    if close_action_modal:
        log.info("Close button found")
        acm_obj.do_click_by_xpath("//*[text()='Close']")
        log.info("Data policy modal page closed")


def check_cluster_operator_status(acm_obj, timeout=30):
    """
    The function verifies the cluster operator status on the DR monitoring dashboard

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        timeout (int): Timeout for which status check should be done
    Returns:
        bool: False if expected text Degraded is found, True otherwise

    """
    acm_loc = locators_for_current_ocp_version()["acm_page"]
    log.info("Check the Cluster operator status")
    cluster_operator_status = acm_obj.wait_until_expected_text_is_found(
        locator=acm_loc["cluster-operator-status"],
        expected_text="Degraded",
        timeout=timeout,
    )
    if cluster_operator_status:
        log.error("Cluster operator status on DR monitoring dashboard is degraded")
        acm_obj.take_screenshot()
        return False
    else:
        log.info("Text 'Degraded' for Cluster operator is not found, validation passed")
        acm_obj.take_screenshot()
        return True


def clusters_in_dr_relationship(
    acm_obj,
    locator: tuple,
    timeout=30,
    expected_text=None,
):
    """
    This function is to verify there are 2 clusters in a healthy DR relationship

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        locator (tuple): Locator for the element to be searched
        timeout (int): Timeout for which status check should be done
        expected_text (str): Text to be searched

    Returns:
        bool: True if expected_text is found, False otherwise

    """
    log.info("Check the healthy clusters count")
    healthy_clusters = acm_obj.wait_until_expected_text_is_found(
        locator=locator,
        expected_text=expected_text,
        timeout=timeout,
    )
    if healthy_clusters:
        log.info(
            f"Text '{expected_text}' for clusters in disaster recovery relationship found"
        )
        acm_obj.take_screenshot()
        return True
    else:
        log.error(
            "Cluster operator status on DR monitoring dashboard is not as expected"
        )
        acm_obj.take_screenshot()
        return False


def application_count_on_ui(acm_obj):
    """
    The function fetches the total application count on the DR monitoring dashboard


    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class

    Returns:
        app_count_list (list): Number of ACM managed applications and total applications
        enrolled in disaster recovery on DR dashboard

    """
    acm_loc = locators_for_current_ocp_version()["acm_page"]
    log.info("Fetch the ACM managed applications count on ACM UI")
    managed_app_text = acm_obj.get_element_text(acm_loc["managed_app_count"])
    log.info(f"Text on managed app count is '{managed_app_text}'")
    number_of_managed_applications = int(managed_app_text.split(": ")[1])
    total_app_count = int(acm_obj.get_element_text(acm_loc["total_app_count"]))
    log.info(f"Total app count is {total_app_count}")
    app_count_list = []
    app_count_list.extend([number_of_managed_applications, total_app_count])
    log.info(f"ACM managed and total count list is {app_count_list}")
    return app_count_list


def health_and_peer_connection_check_on_ui(
    acm_obj, cluster1, cluster2, timeout=15, expected_text="Degraded"
):
    """
    The function checks the cluster and operator health, peer connection of both the managed clusters in a DR
    relationship

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        cluster1 (str): Name of managed cluster one (primary preferably)
        cluster2 (str): Name of managed cluster two (secondary is most cases)
        timeout (int): Timeout for which the expected text would be checked
        expected_text (str): Text available on DR monitoring dashboard for Cluster and Operator status

    Returns:
        False if text Degraded is found either for cluster or operator health for any of the managed clusters,
        True if it is not found for both of them

    """

    acm_loc = locators_for_current_ocp_version()["acm_page"]
    for cluster in [cluster1, cluster2]:
        log.info(f"Select managed cluster {cluster} from cluster dropdown")
        acm_obj.do_click(acm_loc["cluster-dropdown"], enable_screenshot=True)
        acm_obj.do_click(format_locator(acm_loc["cluster"], cluster))
        peer_connection = acm_obj.wait_until_expected_text_is_found(
            locator=acm_loc["peer-connection"],
            expected_text="1 Connected",
            timeout=timeout,
        )
        if peer_connection:
            log.info(
                f"Text '1 Connected' for cluster {cluster} found, validation passed"
            )
            acm_obj.take_screenshot()
        else:
            log.error(
                f"Text '1 Connected' for cluster {cluster} not found, validation failed"
            )
            acm_obj.take_screenshot()
            raise ResourceWrongStatusException
        locator_list = ["cluster-health-status", "cluster-operator-health-status"]
        for locator in locator_list:
            cluster_health_status = acm_obj.wait_until_expected_text_is_found(
                locator=acm_loc[locator],
                expected_text=expected_text,
                timeout=timeout,
            )
            if not cluster_health_status:
                log.info(
                    f"Text {expected_text} for locator {locator} for cluster {cluster} not found"
                )
                acm_obj.take_screenshot()
            else:
                log.warning(
                    f"Text {expected_text} for locator {locator} for cluster {cluster} found"
                )
                acm_obj.take_screenshot()
                return False
    return True


def protected_volume_count_per_cluster(acm_obj, cluster_name):
    """
    Function to check total protected volume count on selected cluster
    Args:
        cluster_name (str): Name of the managed cluster where apps are primary

    Returns:
        DR protected total volume count on the selected cluster

    """
    acm_loc = locators_for_current_ocp_version()["acm_page"]
    log.info(f"Select managed cluster {cluster_name} from cluster dropdown")
    acm_obj.do_click(acm_loc["cluster-dropdown"], enable_screenshot=True)
    acm_obj.do_click(format_locator(acm_loc["cluster"], cluster_name))
    log.info(f"Fetch protected pvc count on cluster {cluster_name}")
    total_pvc_count = acm_obj.get_element_text(
        format_locator(acm_loc["total-vol-count"])
    )
    return int(total_pvc_count)


def check_apps_running_on_selected_cluster(
    acm_obj, cluster_name, app_names: List[str], timeout=10
):
    """
    Function to check the apps running on selected managed cluster on DR monitoring dashboard

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        cluster_name (str): Name of the managed cluster where apps are primary
        app_names (list): Name of the multiple apps from CLI in the form of a list to iterate over it
        timeout (int): Timeout for which an element on UI should be checked for

    Returns:
        True if all the apps are found on selected managed cluster, False if any of the apps are missing

    """
    if not app_names or any(not app.strip() for app in app_names):
        raise ValueError(
            "Parameter 'app_names' is required and must be a non-empty list"
        )
    acm_loc = locators_for_current_ocp_version()["acm_page"]
    log.info(f"Select managed cluster {cluster_name} from cluster dropdown")
    acm_obj.do_click(acm_loc["cluster-dropdown"], enable_screenshot=True)
    acm_obj.do_click(format_locator(acm_loc["cluster"], cluster_name))
    log.info("Check application names in application dropdown")
    acm_obj.do_click(acm_loc["app-dropdown"], enable_screenshot=True)
    locator_list = ["app-name-1", "app-name-2"]
    for locator, app in zip(locator_list, app_names):
        app_presence = acm_obj.wait_until_expected_text_is_found(
            locator=acm_loc[locator],
            expected_text=app,
            timeout=timeout,
        )
        if app_presence:
            log.info(f"App {app} found on cluster {cluster_name} on DR dashboard")
        else:
            log.error(f"App {app} not found on cluster {cluster_name} on DR dashboard")
            return False
    return True


def verify_application_present_in_ui(acm_obj, workloads_to_check=[], timeout=60):
    """
    Verify if application is present in UI

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        workloads_to_check (list): Specify the workloads to check if they exist
        timeout (int): timeout to wait for certain elements to be found on the ACM UI

    Returns:
        bool: True if the application is present, false otherwise

    """
    if workloads_to_check:
        acm_loc = locators_for_current_ocp_version()["acm_page"]
        acm_obj.navigate_applications_page()
        for app in workloads_to_check:
            log.info("Click on search bar")
            acm_obj.do_click(acm_loc["search-bar"])
            log.info("Clear existing text from search bar if any")
            acm_obj.do_clear(acm_loc["search-bar"])
            log.info("Enter the workload to be searched")
            acm_obj.do_send_keys(acm_loc["search-bar"], text=app)
            action_status = acm_obj.wait_until_expected_text_is_found(
                locator=acm_loc["no-results-found"],
                expected_text="No results found",
                timeout=timeout,
            )
            if action_status:
                fetch_status = acm_obj.get_element_text(
                    locator=acm_loc["no-results-found"]
                )
                assert action_status, "Application present in UI"
                log.info(f"{fetch_status} for application {app} in UI")
                return False
        return True


def delete_application_ui(acm_obj, workloads_to_delete=[], timeout=70):
    """
    Function to delete specified workloads on ACM UI

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        workloads_to_delete (list): Specify the workloads to delete
        timeout (int): timeout to wait for certain elements to be found on the ACM UI

    Returns:
        bool: True if the application is deleted successfully, false otherwise

    """
    log.info(f"workloads_to_delete {workloads_to_delete}")
    if verify_application_present_in_ui(
        acm_obj, workloads_to_check=workloads_to_delete, timeout=timeout
    ):
        acm_loc = locators_for_current_ocp_version()["acm_page"]
        acm_obj.navigate_applications_page()
        for app in workloads_to_delete:
            log.info("Click on search bar")
            acm_obj.do_click(acm_loc["search-bar"])
            log.info("Clear existing text from search bar if any")
            acm_obj.do_clear(acm_loc["search-bar"])
            log.info("Enter the workload to be searched")
            acm_obj.do_send_keys(acm_loc["search-bar"], text=app)
            log.info("Click on kebab menu option")
            acm_obj.do_click(
                acm_loc["kebab-action"], enable_screenshot=True, timeout=timeout
            )
            acm_obj.do_click(
                acm_loc["delete-app"], enable_screenshot=True, timeout=timeout
            )
            log.info(f"Deleting application {app}")
            if not acm_obj.get_checkbox_status(acm_loc["remove-app-resources"]):
                acm_obj.select_checkbox_status(
                    status=True, locator=acm_loc["remove-app-resources"]
                )
            acm_obj.do_click(acm_loc["delete"], enable_screenshot=True, timeout=timeout)
            # Check if the workload got deleted
            assert not verify_application_present_in_ui(
                acm_obj, workloads_to_check=app, timeout=timeout
            ), f"Application {app} still exists"
            log.info(f"Application {app} got deleted successfully")
        return True
    else:
        log.error("Applications not present to delete from UI")
        return False


def assign_drpolicy_for_discovered_vms_via_ui(
    acm_obj, vms: List[str], standalone=True, protection_name=None, namespace=None
):
    """
    This function can be used to assign Data Policy via UI to Discovered VMs via Virtual machines page
    of the ACM console.
    With ACM 2.14 and above, Data Policy can be assigned as Standalone or Shared Protection type (if there is an
    existing DRPC for another VM workload, and you want to club it together)

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        vms (list): Specify the names of VMs for DR protection in the form of a list
        standalone (bool): True by default, will switch to Shared Protection type when False
        protection_name (str): Protection name used to DR protect the workload using which
                                DRPC and Placement would be created
        namespace (str): None by default, namespace of the workload
     Returns:
         True if function executes successfully

    """
    if not vms or any(not vm.strip() for vm in vms):
        raise ValueError("Parameter 'vms' is required and must be a non-empty list")
    acm_loc = locators_for_current_ocp_version()["acm_page"]
    acm_obj.navigate_clusters_page(vms=True)
    for vm in vms:
        existing_filter = acm_obj.check_element_presence(
            acm_loc["remove-existing-filter"][::-1]
        )
        if existing_filter:
            acm_obj.do_click(acm_loc["remove-existing-filter"])
            log.info("Existing filter removed")
        else:
            log.info("No filter exists")
        log.info("Select name as filter")
        acm_obj.do_click(acm_loc["filter-vms"], enable_screenshot=True)
        acm_obj.do_click(acm_loc["filter-with-name"], enable_screenshot=True)
        log.info("Select the name of the VM and apply filter")
        acm_obj.do_click(format_locator(acm_loc["vm_name"], vm))
        log.info("Select namespace as filter")
        acm_obj.do_click(acm_loc["filter-vms-2"], enable_screenshot=True)
        acm_obj.do_click(acm_loc["filter-with-namespace"], enable_screenshot=True)
        log.info("Select the name of the namespace where VM is running")
        acm_obj.do_click(format_locator(acm_loc["vm-namespace"], namespace))
        log.info("Click on forward arrow to apply filter")
        acm_obj.do_click(acm_loc["click-forward-arrow"], enable_screenshot=True)
        log.info("Check the status of the VM")
        vm_current_status = acm_obj.get_element_text(acm_loc["vm-status"])
        if vm_current_status != "Running":
            wait_for_status = acm_obj.wait_until_expected_text_is_found(
                acm_loc["vm-status"], expected_text="Running", timeout=300
            )
            assert (
                wait_for_status
            ), f"Expected VM status is 'Running', but got '{vm_current_status}'"
        log.info("Click on the kebab menu option")
        acm_obj.do_click(acm_loc["vm-kebab-menu"], enable_screenshot=True)
        log.info("Click on Manage disaster recovery")
        acm_obj.do_click(acm_loc["manage-dr"], enable_screenshot=True)
        log.info("Click on Enroll virtual machine")
        acm_obj.do_click(acm_loc["enroll-vm"], enable_screenshot=True)
        if standalone:
            log.info("Send Protection name")
            acm_obj.do_click(acm_loc["name-input-btn"])
            acm_obj.do_send_keys(acm_loc["name-input-btn"], text=protection_name)
        else:
            log.info("Protecting VM with Shared Protection type")
            acm_obj.do_click(acm_loc["select-shared"], enable_screenshot=True)
            radio_buttons = acm_obj.get_elements(acm_loc["select-drpc"])
            # Assert that exactly one element is found
            assert (
                len(radio_buttons) == 1
            ), f"Expected 1 radio button but found {len(radio_buttons)}"
            log.info("Expected 1 radio button found, select existing DRPC")
            acm_obj.do_click(acm_loc["select-drpc"], enable_screenshot=True)
        log.info("Click next")
        acm_obj.do_click(acm_loc["vm-page-next-btn"], enable_screenshot=True)
        if standalone:
            log.info("Select policy")
            acm_obj.do_click(acm_loc["dr-policy"], enable_screenshot=True)
            acm_obj.do_click(acm_loc["select-policy"], enable_screenshot=True)
        log.info("Click next")
        acm_obj.do_click(acm_loc["vm-page-next-btn"], enable_screenshot=True)
        log.info("Verify selected protection type")
        selected_protection_type = "Standalone" if standalone else "Shared"
        protection_type = acm_obj.get_element_text(
            format_locator(
                acm_loc["selected-protection-type"], selected_protection_type
            )
        )
        assert protection_type == (
            "Standalone" if standalone else "Shared"
        ), f"Expected {'Standalone' if standalone else 'Shared'}, but got '{protection_type}'"
        log.info("Assign DRPolicy")
        acm_obj.do_click(acm_loc["assign"], enable_screenshot=True)
        time.sleep(2)
        log.info("Policy confirmation")
        conf_msg = acm_obj.get_element_text(acm_loc["conf-msg"])
        log.info(f"Confirmation message is {conf_msg}")
        expected_conf_msg = conf_msg.split("\n", 1)[1].strip()
        assert expected_conf_msg == "New policy assigned to application"
        log.info("Close page")
        acm_obj.do_click(acm_loc["close-page"], enable_screenshot=True)
        return True
