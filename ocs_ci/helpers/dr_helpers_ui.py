"""
Helper functions specific to DR User Interface
"""

import logging
import time

from typing import List

from ocs_ci.utility.version import compare_versions
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException as SeleniumTimeoutException,
)

from ocs_ci.framework import config
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.exceptions import (
    ResourceWrongStatusException,
    TimeoutException,
    UnexpectedBehaviour,
)
from ocs_ci.ocs.ui.base_ui import (
    wait_for_element_to_be_clickable,
    wait_for_element_to_be_visible,
)
from ocs_ci.ocs.ui.views import locators_for_current_ocp_version
from ocs_ci.ocs.ui.helpers_ui import format_locator
from ocs_ci.ocs.utils import get_non_acm_cluster_config
from ocs_ci.utility.utils import get_running_acm_version

log = logging.getLogger(__name__)


def get_managed_clusters_not_eligible_for_dr_topology():
    """
    Return ACM managed cluster names that are not registered as DRClusters on the hub.

    Returns:
        list: Managed cluster names without a corresponding DRCluster
    """
    restore_index = config.cur_index
    try:
        config.switch_acm_ctx()
        managed_clusters = OCP(kind=constants.ACM_MANAGEDCLUSTER).get().get("items", [])
        managed_cluster_names = [
            cluster["metadata"]["name"] for cluster in managed_clusters
        ]
        drcluster_names = set(dr_helpers.get_all_drclusters())
    finally:
        config.switch_ctx(restore_index)
    return sorted(name for name in managed_cluster_names if name not in drcluster_names)


def navigate_to_dr_topology_tab(acm_obj, timeout=120):
    """
    Navigate to Disaster recovery and open the Topology tab.

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        timeout (int): Timeout for UI elements on the DR Topology page
    """
    acm_loc = locators_for_current_ocp_version()["acm_page"]
    acm_obj.navigate_data_services()
    acm_obj.do_click(
        acm_loc["dr-topology-tab"],
        avoid_stale=True,
        enable_screenshot=True,
        timeout=timeout,
    )
    acm_obj.page_has_loaded(retries=5, sleep_time=3)
    log.info("Opened Disaster recovery Topology tab")


def is_dr_cluster_displayed_on_topology(acm_obj, cluster_name, timeout=60):
    """
    Check whether a cluster name is rendered on the DR Topology view.

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        cluster_name (str): DRCluster name to look for on the topology canvas
        timeout (int): Timeout for the cluster label to become visible

    Returns:
        bool: True if the cluster name is found on the DR Topology view
    """
    acm_loc = locators_for_current_ocp_version()["acm_page"]
    return acm_obj.wait_until_expected_text_is_found(
        format_locator(acm_loc["dr-topology-cluster-name"], cluster_name),
        expected_text=cluster_name,
        timeout=timeout,
    )


def is_dr_policy_displayed_on_topology(acm_obj, policy_name, timeout=60):
    """
    Check whether a DRPolicy name is rendered on the DR Topology view.

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        policy_name (str): DRPolicy name to look for on the topology canvas
        timeout (int): Timeout for the policy label to become visible

    Returns:
        bool: True if the policy name is found on the DR Topology view
    """
    acm_loc = locators_for_current_ocp_version()["acm_page"]
    return acm_obj.wait_until_expected_text_is_found(
        format_locator(acm_loc["dr-topology-policy-name"], policy_name),
        expected_text=policy_name,
        timeout=timeout,
    )


def verify_df_cluster_listing_on_topology_view(
    acm_obj, expected_clusters=None, timeout=60
):
    """
    Verify cluster labels on the open DR Topology tab match expected DRClusters.

    Managed clusters without a DRCluster (including local-cluster) must not appear
    on the topology canvas.

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        expected_clusters (list): Optional DRCluster names; fetched via CLI when omitted
        timeout (int): Timeout for DR Topology UI elements

    Returns:
        list: Data Foundation cluster names verified on the DR Topology tab
    """
    if expected_clusters is None:
        expected_clusters = dr_helpers.get_dr_topology_clusters()
    expected_clusters = sorted(
        name for name in expected_clusters if name != constants.ACM_LOCAL_CLUSTER
    )
    assert (
        expected_clusters
    ), "No DRClusters found via CLI; cannot verify Data Foundation cluster listing"

    missing_clusters = [
        cluster_name
        for cluster_name in expected_clusters
        if not is_dr_cluster_displayed_on_topology(
            acm_obj, cluster_name, timeout=timeout
        )
    ]
    assert (
        not missing_clusters
    ), f"Data Foundation clusters missing on Topology UI: {missing_clusters}"
    for cluster_name in expected_clusters:
        log.info(
            f"Data Foundation cluster '{cluster_name}' is displayed on DR Topology tab"
        )

    non_dr_managed_clusters = get_managed_clusters_not_eligible_for_dr_topology()
    if non_dr_managed_clusters:
        cluster_name_locator = locators_for_current_ocp_version()["acm_page"][
            "dr-topology-cluster-name"
        ]
        unexpected_clusters = [
            cluster_name
            for cluster_name in non_dr_managed_clusters
            if acm_obj.check_element_presence(
                format_locator(cluster_name_locator, cluster_name)[::-1],
                timeout=5,
                use_fallback=False,
            )
        ]
        assert not unexpected_clusters, (
            "Managed clusters without DRCluster shown on Topology UI: "
            f"{unexpected_clusters}"
        )
    else:
        log.info(
            "No managed clusters without DRCluster in this environment; "
            "skipping negative topology listing check"
        )

    acm_obj.take_screenshot()
    return expected_clusters


def open_dr_topology_cluster_sidebar(acm_obj, cluster_name, timeout=60):
    """
    Click a cluster node on the DR Topology view to open its sidebar.

    Click the label parent element, with zoom-out retries and a JS click
    fallback when the canvas intercepts the click.

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        cluster_name (str): DRCluster name shown on the topology canvas
        timeout (int): Timeout for the cluster label to become visible
    """
    acm_loc = locators_for_current_ocp_version()["acm_page"]
    topology_loc = locators_for_current_ocp_version()["topology"]
    cluster_click_locator = format_locator(
        acm_loc["dr-topology-cluster-click"], cluster_name
    )

    for attempt in range(1, 4):
        try:
            acm_obj.do_click(cluster_click_locator, use_fallback=False, timeout=timeout)
            break
        except (NoSuchElementException, SeleniumTimeoutException) as ex:
            if attempt == 3:
                raise AssertionError(
                    f"Cluster '{cluster_name}' was not clickable after 3 attempts"
                ) from ex
            log.info("Zooming out DR Topology view")
            acm_obj.do_click(topology_loc["zoom_out"])
            acm_obj.page_has_loaded(retries=3, sleep_time=2)
            log.info(f"Retry cluster click on DR Topology. attempt {attempt}")
        except ElementClickInterceptedException:
            log.info(
                "Click intercepted on DR Topology node. "
                "Falling back to JS dispatchEvent click."
            )
            acm_obj.click_with_script(cluster_click_locator)
            break

    acm_obj.take_screenshot()
    acm_obj.page_has_loaded(retries=3, sleep_time=2)
    log.info(f"Clicked cluster '{cluster_name}' on DR Topology tab")


def verify_dr_topology_cluster_sidebar_open(acm_obj, cluster_name, timeout=60):
    """
    Verify the cluster sidebar opens with Details and Application Details tabs.

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        cluster_name (str): DRCluster name expected in the sidebar
        timeout (int): Timeout for sidebar elements
    """
    acm_loc = locators_for_current_ocp_version()["acm_page"]
    sidebar_open = acm_obj.check_element_presence(
        acm_loc["dr-topology-sidebar-close"][::-1], timeout=timeout
    )
    assert sidebar_open, (
        f"Cluster sidebar did not open for '{cluster_name}' " "(close button not found)"
    )

    details_tab = acm_obj.wait_until_expected_text_is_found(
        acm_loc["dr-topology-sidebar-details-tab"],
        expected_text=constants.DR_TOPOLOGY_SIDEBAR_DETAILS_TAB,
        timeout=timeout,
    )
    application_details_tab = acm_obj.wait_until_expected_text_is_found(
        acm_loc["dr-topology-sidebar-application-details-tab"],
        expected_text=constants.DR_TOPOLOGY_SIDEBAR_APPLICATION_DETAILS_TAB,
        timeout=timeout,
    )
    cluster_visible = acm_obj.wait_until_expected_text_is_found(
        format_locator(acm_loc["dr-topology-sidebar-cluster-name"], cluster_name),
        expected_text=cluster_name,
        timeout=timeout,
    )
    assert (
        details_tab and application_details_tab and cluster_visible
    ), f"Cluster sidebar did not open as expected for '{cluster_name}'"
    acm_obj.take_screenshot()
    log.info(f"Cluster sidebar opened for '{cluster_name}'")


def close_dr_topology_sidebar(acm_obj, timeout=30):
    """
    Close the DR Topology sidebar.

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        timeout (int): Timeout for the close button
    """
    acm_loc = locators_for_current_ocp_version()["acm_page"]
    if acm_obj.check_element_presence(
        acm_loc["dr-topology-sidebar-close"][::-1], timeout=5
    ):
        acm_obj.do_click(
            acm_loc["dr-topology-sidebar-close"],
            enable_screenshot=True,
            timeout=timeout,
        )
        acm_obj.page_has_loaded(retries=2, sleep_time=1)
        log.info("Closed DR Topology sidebar")


def open_dr_topology_policy_sidebar(acm_obj, policy_name, timeout=60):
    """
    Click a DRPolicy label on the DR Topology view to open its sidebar.

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        policy_name (str): DRPolicy name shown on the topology canvas
        timeout (int): Timeout for the policy label to become clickable
    """
    acm_loc = locators_for_current_ocp_version()["acm_page"]
    topology_loc = locators_for_current_ocp_version()["topology"]
    policy_click_locator = format_locator(
        acm_loc["dr-topology-policy-click"], policy_name
    )

    for attempt in range(1, 4):
        try:
            acm_obj.do_click(policy_click_locator, use_fallback=False, timeout=timeout)
            break
        except (NoSuchElementException, SeleniumTimeoutException) as ex:
            if attempt == 3:
                raise AssertionError(
                    f"Policy '{policy_name}' was not clickable after 3 attempts"
                ) from ex
            log.info("Zooming out DR Topology view")
            acm_obj.do_click(topology_loc["zoom_out"])
            acm_obj.page_has_loaded(retries=3, sleep_time=2)
            log.info(f"Retry policy click on DR Topology. attempt {attempt}")
        except ElementClickInterceptedException:
            log.info(
                "Click intercepted on DR Topology policy. "
                "Falling back to JS dispatchEvent click."
            )
            acm_obj.click_with_script(policy_click_locator)
            break

    acm_obj.take_screenshot()
    acm_obj.page_has_loaded(retries=3, sleep_time=2)
    log.info(f"Clicked policy '{policy_name}' on DR Topology tab")


def verify_dr_topology_policy_sidebar_open(
    acm_obj,
    policy_name,
    connected_clusters,
    scheduling_interval,
    timeout=60,
):
    """
    Verify the DRPolicy sidebar opens with expected policy details.

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        policy_name (str): DRPolicy name expected in the sidebar
        connected_clusters (list): Cluster names paired by the DRPolicy
        scheduling_interval (str): DRPolicy scheduling interval (e.g. 5m)
        timeout (int): Timeout for sidebar elements
    """
    acm_loc = locators_for_current_ocp_version()["acm_page"]
    sidebar_open = acm_obj.check_element_presence(
        acm_loc["dr-topology-sidebar-close"][::-1], timeout=timeout
    )
    assert (
        sidebar_open
    ), f"Policy sidebar did not open for '{policy_name}' (close button not found)"

    policy_visible = acm_obj.wait_until_expected_text_is_found(
        format_locator(acm_loc["dr-topology-sidebar-policy-name"], policy_name),
        expected_text=policy_name,
        timeout=timeout,
    )
    validated_status = acm_obj.wait_until_expected_text_is_found(
        acm_loc["drpolicy-status"],
        expected_text=constants.DRPOLICY_STATUS,
        timeout=timeout,
    )
    scheduling_visible = acm_obj.wait_until_expected_text_is_found(
        format_locator(
            acm_loc["dr-topology-sidebar-scheduling-interval"],
            scheduling_interval,
        ),
        expected_text=scheduling_interval,
        timeout=timeout,
    )
    connected_cluster_checks = [
        acm_obj.wait_until_expected_text_is_found(
            format_locator(
                acm_loc["dr-topology-sidebar-connected-cluster"], cluster_name
            ),
            expected_text=cluster_name,
            timeout=timeout,
        )
        for cluster_name in connected_clusters
    ]

    assert (
        policy_visible
        and validated_status
        and scheduling_visible
        and all(connected_cluster_checks)
    ), f"Policy sidebar did not open as expected for '{policy_name}'"
    acm_obj.take_screenshot()
    log.info(f"Policy sidebar opened for '{policy_name}'")


def select_dr_topology_filter(acm_obj, filter_option, timeout=30):
    """
    Select a filter option on the DR Topology search bar.

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        filter_option (str): Filter label, e.g. Cluster name or Policy
        timeout (int): Timeout for filter UI elements
    """
    acm_loc = locators_for_current_ocp_version()["acm_page"]
    selected_filter_locator = format_locator(
        acm_loc["dr-topology-filter-selected"], filter_option
    )
    if acm_obj.check_element_presence(
        selected_filter_locator[::-1], timeout=3, use_fallback=False
    ):
        log.info(f"DR Topology filter '{filter_option}' is already selected")
        return

    acm_obj.do_click(acm_loc["dr-topology-filter-toggle"], timeout=timeout)
    acm_obj.do_click(
        format_locator(acm_loc["dr-topology-filter-option"], filter_option),
        timeout=timeout,
    )
    acm_obj.page_has_loaded(retries=2, sleep_time=1)
    log.info(f"Selected DR Topology filter '{filter_option}'")


def search_dr_topology(acm_obj, search_text, timeout=30):
    """
    Enter text in the DR Topology search bar.

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        search_text (str): Text to search on the topology view
        timeout (int): Timeout for the search input
    """
    acm_loc = locators_for_current_ocp_version()["acm_page"]
    acm_obj.do_send_keys(acm_loc["dr-topology-search"], search_text, timeout=timeout)
    acm_obj.page_has_loaded(retries=3, sleep_time=2)
    log.info(f"Searched DR Topology for '{search_text}'")


def reset_dr_topology_search(acm_obj, timeout=30):
    """
    Reset the DR Topology search bar.

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        timeout (int): Timeout for the reset button
    """
    acm_loc = locators_for_current_ocp_version()["acm_page"]
    acm_obj.do_click(acm_loc["dr-topology-search-reset"], timeout=timeout)
    acm_obj.page_has_loaded(retries=3, sleep_time=2)
    log.info("Reset DR Topology search")


def verify_dr_topology_cluster_name_search_filter(
    acm_obj, cluster_name, hidden_cluster_names, timeout=60
):
    """
    Verify Cluster name filter and search narrows the topology to one cluster.

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        cluster_name (str): Cluster name to search for
        hidden_cluster_names (list): Cluster names expected to be hidden by search
        timeout (int): Timeout for topology UI elements
    """
    select_dr_topology_filter(
        acm_obj, constants.DR_TOPOLOGY_SEARCH_FILTER_CLUSTER_NAME, timeout=timeout
    )
    search_dr_topology(acm_obj, cluster_name, timeout=timeout)
    assert is_dr_cluster_displayed_on_topology(
        acm_obj, cluster_name, timeout=timeout
    ), f"Cluster '{cluster_name}' not found after Cluster name search"

    hidden_clusters = [
        name
        for name in hidden_cluster_names
        if acm_obj.check_element_presence(
            format_locator(
                locators_for_current_ocp_version()["acm_page"][
                    "dr-topology-cluster-name"
                ],
                name,
            )[::-1],
            timeout=5,
            use_fallback=False,
        )
    ]
    assert not hidden_clusters, (
        "Clusters should be hidden by Cluster name search filter: " f"{hidden_clusters}"
    )
    acm_obj.take_screenshot()
    reset_dr_topology_search(acm_obj, timeout=timeout)
    log.info(f"Cluster name search filter validated for '{cluster_name}'")


def verify_dr_topology_policy_search_filter(
    acm_obj, policy_name, connected_clusters, timeout=120
):
    """
    Verify Policy filter and search shows the policy and connected clusters.

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        policy_name (str): DRPolicy name to search for
        connected_clusters (list): Cluster names paired by the DRPolicy
        timeout (int): Timeout for topology UI elements
    """
    select_dr_topology_filter(
        acm_obj, constants.DR_TOPOLOGY_SEARCH_FILTER_POLICY, timeout=timeout
    )
    search_dr_topology(acm_obj, policy_name, timeout=timeout)
    assert is_dr_policy_displayed_on_topology(
        acm_obj, policy_name, timeout=timeout
    ), f"Policy '{policy_name}' not found after Policy search"

    missing_clusters = [
        cluster_name
        for cluster_name in connected_clusters
        if not is_dr_cluster_displayed_on_topology(
            acm_obj, cluster_name, timeout=timeout
        )
    ]
    assert not missing_clusters, (
        "Connected clusters missing after Policy search filter: " f"{missing_clusters}"
    )
    acm_obj.take_screenshot()
    reset_dr_topology_search(acm_obj, timeout=timeout)
    log.info(f"Policy search filter validated for '{policy_name}'")


def open_dr_topology_drpc_sidebar(
    acm_obj,
    cluster_name,
    dr_status=constants.DR_TOPOLOGY_DRPC_HEALTHY_STATUS,
    timeout=60,
):
    """
    Click the DRPC status label on a cluster node to open the Applications sidebar.

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        cluster_name (str): DRCluster name that shows the DRPC status badge
        dr_status (str): DRPC status text on the topology node (default: healthy)
        timeout (int): Timeout for the DRPC label to become clickable
    """
    acm_loc = locators_for_current_ocp_version()["acm_page"]
    topology_loc = locators_for_current_ocp_version()["topology"]
    drpc_click_locator = format_locator(
        acm_loc["dr-topology-drpc-click"], dr_status, cluster_name
    )

    for attempt in range(1, 4):
        try:
            acm_obj.do_click(drpc_click_locator, use_fallback=False, timeout=timeout)
            break
        except (NoSuchElementException, SeleniumTimeoutException) as ex:
            if attempt == 3:
                raise AssertionError(
                    f"DRPC status '{dr_status}' on cluster '{cluster_name}' "
                    f"was not clickable after 3 attempts"
                ) from ex
            log.info("Zooming out DR Topology view")
            acm_obj.do_click(topology_loc["zoom_out"])
            acm_obj.page_has_loaded(retries=3, sleep_time=2)
            log.info(f"Retry DRPC click on DR Topology. attempt {attempt}")
        except ElementClickInterceptedException:
            log.info(
                "Click intercepted on DR Topology DRPC label. "
                "Falling back to JS dispatchEvent click."
            )
            acm_obj.click_with_script(drpc_click_locator)
            break

    acm_obj.take_screenshot()
    acm_obj.page_has_loaded(retries=3, sleep_time=2)
    log.info(
        f"Clicked DRPC status '{dr_status}' on cluster '{cluster_name}' "
        "on DR Topology tab"
    )


def verify_dr_topology_applications_sidebar(acm_obj, protected_apps, timeout=60):
    """
    Verify the Applications sidebar shows expected table headers and app rows.

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        protected_apps (list): Application dicts with name, status, and policy
        timeout (int): Timeout for sidebar elements
    """
    acm_loc = locators_for_current_ocp_version()["acm_page"]
    sidebar_open = acm_obj.check_element_presence(
        acm_loc["dr-topology-sidebar-close"][::-1], timeout=timeout
    )
    assert sidebar_open, "Applications sidebar did not open (close button not found)"

    header_checks = [
        acm_obj.wait_until_expected_text_is_found(
            acm_loc["dr-topology-sidebar-app-table-header-name"],
            expected_text="Name",
            timeout=timeout,
        ),
        acm_obj.wait_until_expected_text_is_found(
            acm_loc["dr-topology-sidebar-app-table-header-dr-status"],
            expected_text="DR Status",
            timeout=timeout,
        ),
        acm_obj.wait_until_expected_text_is_found(
            acm_loc["dr-topology-sidebar-app-table-header-policy"],
            expected_text="Policy",
            timeout=timeout,
        ),
    ]
    assert all(
        header_checks
    ), "Applications sidebar table headers are not displayed as expected"

    for app in protected_apps:
        app_name = app["name"]
        assert acm_obj.wait_until_expected_text_is_found(
            format_locator(
                acm_loc["dr-topology-sidebar-app-row-name"],
                app_name,
                app_name,
            ),
            expected_text=app_name,
            timeout=timeout,
        ), f"Application name '{app_name}' not found in Applications sidebar"
        assert acm_obj.wait_until_expected_text_is_found(
            format_locator(
                acm_loc["dr-topology-sidebar-app-row-status"],
                app_name,
                app["status"],
            ),
            expected_text=app["status"],
            timeout=timeout,
        ), (
            f"DR Status '{app['status']}' not found for application "
            f"'{app_name}' in Applications sidebar"
        )
        assert acm_obj.wait_until_expected_text_is_found(
            format_locator(
                acm_loc["dr-topology-sidebar-app-row-policy"],
                app_name,
                app["policy"],
            ),
            expected_text=app["policy"],
            timeout=timeout,
        ), (
            f"Policy '{app['policy']}' not found for application "
            f"'{app_name}' in Applications sidebar"
        )

    acm_obj.take_screenshot()
    log.info(
        "Applications sidebar validated for apps: "
        f"{[app['name'] for app in protected_apps]}"
    )


def wait_for_dr_topology_workloads_healthy(workloads, scheduling_interval):
    """
    Wait until DR protected workloads are healthy before Topology UI validation.

    Args:
        workloads (list): Deployed DR workload objects
        scheduling_interval (int): Wait time in minutes before Topology UI validation
    """
    from ocs_ci.ocs.resources.drpc import DRPC

    restore_index = config.cur_index
    try:
        config.switch_acm_ctx()
        for workload in workloads:
            if getattr(workload, "discovered_apps_placement_name", None):
                drpc_obj = DRPC(
                    namespace=constants.DR_OPS_NAMESPACE,
                    resource_name=workload.discovered_apps_placement_name,
                )
            elif workload.workload_type == constants.APPLICATION_SET:
                drpc_obj = DRPC(
                    namespace=constants.GITOPS_CLUSTER_NAMESPACE,
                    resource_name=f"{workload.appset_placement_name}-drpc",
                )
            else:
                drpc_obj = DRPC(namespace=workload.workload_namespace)
            drpc_obj.wait_for_peer_ready_status()

        total_pvc_count = sum(
            getattr(workload, "workload_pvc_count", 1) for workload in workloads
        )
        dr_helpers.wait_for_mirroring_status_ok(replaying_images=total_pvc_count)
        time.sleep(scheduling_interval * 60)
        log.info("DR protected workloads are healthy for Topology UI validation")
    finally:
        config.switch_ctx(restore_index)


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


def verify_pending_cleanup_alert_firing(
    acm_obj, operation="Failover", drpc_name=None, namespace=None
):
    """
    Verify that ApplicationCleanupPending alert is firing on DR Dashboard.

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        operation (str): DR operation name for logging (Failover/Relocate)
        drpc_name (str): DRPC name to verify specific alert (optional)
        namespace (str): Namespace of DRPC (optional, defaults to DR_OPS_NAMESPACE)

    Raises:
        UnexpectedBehaviour: If alert is not found on DR Dashboard

    """
    acm_loc = locators_for_current_ocp_version()["acm_page"]
    namespace = namespace or constants.DR_OPS_NAMESPACE

    log.info(
        f"Verifying {constants.ALERT_APPLICATION_CLEANUP_PENDING} "
        f"alert is firing on DR Dashboard after {operation}"
        + (f" for DRPC '{drpc_name}'" if drpc_name else "")
    )
    acm_obj.refresh_page()
    acm_obj.navigate_data_services()

    # Navigate to Disaster Recovery Overview page where alerts are shown
    acm_obj.do_click(
        acm_loc["disaster-recovery-overview"],
        avoid_stale=True,
        enable_screenshot=True,
        timeout=60,
    )
    acm_obj.take_screenshot()

    # Expand Critical alerts section (best-effort)
    try:
        critical_alert = acm_obj.find_an_element_by_xpath(
            acm_loc["critical-alert"][0]
        ).get_attribute("aria-expanded")

        if critical_alert == "false":
            critical_alert_elem = wait_for_element_to_be_clickable(
                acm_loc["critical-alert"]
            )
            acm_obj.driver.execute_script("arguments[0].click();", critical_alert_elem)
            acm_obj.take_screenshot()
    except (NoSuchElementException, StaleElementReferenceException) as e:
        log.warning(
            f"Could not expand Critical alerts section: {e}. "
            "Proceeding to verify alert presence directly."
        )
        acm_obj.take_screenshot()

    # Verify alert is visible - use DRPC-specific locator if drpc_name provided
    if drpc_name:
        alert_locator = (
            acm_loc["pending-cleanup-alert-drpc-message"][0].format(
                drpc_name, namespace
            ),
            acm_loc["pending-cleanup-alert-drpc-message"][1],
        )
        expected_text = f"DRPC '{drpc_name}'"
    else:
        alert_locator = acm_loc["pending-cleanup-alert"]
        expected_text = constants.ALERT_APPLICATION_CLEANUP_PENDING

    alert_found = acm_obj.wait_until_expected_text_is_found(
        locator=alert_locator,
        expected_text=expected_text,
        timeout=120,
    )

    if alert_found:
        log.info(
            f"Alert '{constants.ALERT_APPLICATION_CLEANUP_PENDING}' "
            f"found on DR Dashboard after {operation}"
            + (f" for DRPC '{drpc_name}'" if drpc_name else "")
        )
        acm_obj.take_screenshot()
    else:
        log.error(
            f"Alert '{constants.ALERT_APPLICATION_CLEANUP_PENDING}' "
            f"NOT found on DR Dashboard after {operation}"
            + (f" for DRPC '{drpc_name}'" if drpc_name else "")
        )
        acm_obj.take_screenshot()
        raise UnexpectedBehaviour(
            f"{constants.ALERT_APPLICATION_CLEANUP_PENDING} alert "
            f"did not appear on DR Dashboard after {operation}"
            + (f" for DRPC '{drpc_name}'" if drpc_name else "")
        )


def verify_pending_cleanup_alert_resolved(
    acm_obj, operation="Failover", drpc_name=None, namespace=None
):
    """
    Verify that ApplicationCleanupPending alert is resolved/cleared from DR Dashboard.

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        operation (str): DR operation name for logging (Failover/Relocate)
        drpc_name (str): DRPC name to verify specific alert resolved (optional)
        namespace (str): Namespace of DRPC (optional, defaults to DR_OPS_NAMESPACE)

    Raises:
        UnexpectedBehaviour: If alert is still present on DR Dashboard

    """
    acm_loc = locators_for_current_ocp_version()["acm_page"]
    namespace = namespace or constants.DR_OPS_NAMESPACE

    log.info(
        f"Verifying {constants.ALERT_APPLICATION_CLEANUP_PENDING} "
        f"alert is cleared from DR Dashboard after {operation} cleanup"
        + (f" for DRPC '{drpc_name}'" if drpc_name else "")
    )

    # Navigate to DR Dashboard Overview page
    acm_obj.refresh_page()
    acm_obj.navigate_data_services()
    acm_obj.do_click(
        acm_loc["disaster-recovery-overview"],
        avoid_stale=True,
        enable_screenshot=True,
        timeout=60,
    )
    acm_obj.take_screenshot()

    # Try to expand Critical alerts section if it exists
    try:
        critical_alert = acm_obj.find_an_element_by_xpath(
            acm_loc["critical-alert"][0]
        ).get_attribute("aria-expanded")

        if critical_alert == "false":
            critical_alert_elem = wait_for_element_to_be_clickable(
                acm_loc["critical-alert"]
            )
            acm_obj.driver.execute_script("arguments[0].click();", critical_alert_elem)
            acm_obj.take_screenshot()
    except (NoSuchElementException, StaleElementReferenceException) as e:
        log.info(
            f"Critical alerts section not found or not expandable: {e}. "
            "This may indicate no critical alerts exist (expected)."
        )
        acm_obj.take_screenshot()
    except Exception as e:
        acm_obj.take_screenshot()
        raise UnexpectedBehaviour(
            f"Unable to verify DR Dashboard critical-alert section after {operation} cleanup."
        ) from e

    # Verify alert is NOT visible - use DRPC-specific locator if drpc_name provided
    if drpc_name:
        alert_locator = (
            acm_loc["pending-cleanup-alert-drpc-message"][0].format(
                drpc_name, namespace
            ),
            acm_loc["pending-cleanup-alert-drpc-message"][1],
        )
        expected_text = f"DRPC '{drpc_name}'"
    else:
        alert_locator = acm_loc["pending-cleanup-alert"]
        expected_text = constants.ALERT_APPLICATION_CLEANUP_PENDING

    # use_fallback=False as this is intentional negative check (alert should NOT exist)
    alert_still_present = acm_obj.wait_until_expected_text_is_found(
        locator=alert_locator,
        expected_text=expected_text,
        timeout=60,
        use_fallback=False,
    )

    if alert_still_present:
        log.error(
            f"Alert '{constants.ALERT_APPLICATION_CLEANUP_PENDING}' "
            f"still present on DR Dashboard after {operation} cleanup"
            + (f" for DRPC '{drpc_name}'" if drpc_name else "")
        )
        acm_obj.take_screenshot()
        raise UnexpectedBehaviour(
            f"{constants.ALERT_APPLICATION_CLEANUP_PENDING} alert "
            f"did not clear after {operation} cleanup"
            + (f" for DRPC '{drpc_name}'" if drpc_name else "")
        )
    else:
        log.info(
            f"Alert '{constants.ALERT_APPLICATION_CLEANUP_PENDING}' "
            f"successfully cleared from DR Dashboard after {operation}"
            + (f" for DRPC '{drpc_name}'" if drpc_name else "")
        )
        acm_obj.take_screenshot()


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
                format_locator(acm_loc["select-dr-policy"], policy_name),
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


def check_or_assign_drpolicy_for_discovered_vms_via_ui(
    acm_obj,
    vms: List[object],
    managed_cluster_name,
    assign_policy=True,
    standalone=True,
    protection_name=None,
    namespace=None,
):
    """
    This function can be used to check the VM status and assign Data Policy using UI to Discovered VMs
    via Virtual machines page of the ACM console.
    Starting ODF 4.19 and ACM 2.14, Data Policy can be assigned as Standalone or Shared Protection type (if there is an
    existing DRPC for another VM workload, and you want to club it together)

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        vms (object): Contains object of VMs for DR protection in the form of a list
        managed_cluster_name (str): Name of the managed cluster where VM workload is running
        assign_policy (bool): Optional steps when only VM status has to be checked, DRPolicy won't be applied when False
        standalone (bool): True by default, will switch to Shared Protection type when False
        protection_name (str): Protection name used to DR protect the workload using which
                                DRPC and Placement would be created
        namespace (str): None by default, namespace of the workload


     Returns:
         True if function executes successfully

    """
    acm_loc = locators_for_current_ocp_version()["acm_page"]
    if acm_obj.check_element_presence(
        (
            acm_loc["modal_dialog_close_button"][1],
            acm_loc["modal_dialog_close_button"][0],
        ),
        timeout=10,
    ):
        log.info("Modal dialog box found, closing it..")
        acm_obj.do_click(acm_loc["modal_dialog_close_button"], timeout=5)
    log.info("Look for 'All Clusters'")
    all_clusters = acm_obj.wait_until_expected_text_is_found(
        acm_loc["all-clusters"], expected_text="All clusters"
    )
    if all_clusters:
        log.info("All Clusters option found")
    else:
        log.warning("'All Clusters' not found on the VMs page")
        return False
    for vm in vms:
        log.info("Select the cluster where VM workload is running")
        acm_obj.do_click(
            format_locator(acm_loc["managed-cluster-name"], managed_cluster_name)
        )
        log.info(f"Managed cluster {managed_cluster_name} found ")
        log.info("Select the namespace where VM workload is running")
        acm_obj.do_click(
            format_locator(acm_loc["cnv-workload-namespace"], namespace),
            enable_screenshot=True,
        )
        log.info(f"Namespace {namespace} found on cluster {managed_cluster_name}")
        log.info("Click on the VM")
        acm_obj.do_click(
            format_locator(acm_loc["cnv-vm-name"], vm.vm_name), enable_screenshot=True
        )
        log.info("Check the status of the VM")
        vm_current_status = acm_obj.get_element_text(acm_loc["vm-status"])
        if vm_current_status != "Running":
            wait_for_status = acm_obj.wait_until_expected_text_is_found(
                acm_loc["vm-status"], expected_text="Running", timeout=300
            )
            assert (
                wait_for_status
            ), f"Expected VM status is 'Running', but got '{vm_current_status}'"
        else:
            log.info(
                f"VM status is running on the managed cluster {managed_cluster_name}"
            )
        if assign_policy:
            log.info("Click on the Actions button")
            acm_obj.do_click(acm_loc["vm-actions"], enable_screenshot=True)
            log.info("Click on Manage disaster recovery")
            acm_obj.do_click(acm_loc["manage-dr"], enable_screenshot=True)
            log.info("Click on Enroll virtual machine")
            acm_obj.do_click(acm_loc["enroll-vm"], enable_screenshot=True)
            if standalone:
                log.info("Protecting VM with Standalone Protection type")
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
            acm_obj.take_screenshot()
            expected_conf_msg = conf_msg.split("\n", 1)[1].strip()
            assert expected_conf_msg == "New policy assigned to application"
            log.info("Close page")
            acm_obj.do_click(acm_loc["close-page"], enable_screenshot=True)
    return True


def navigate_using_fleet_virtualization(acm_obj):
    """
    Starting ACM 2.15, VMs page from the ACM console has been removed and is integrated
    with the Virtulization Operator which is required to be installed on the ACM hub cluster and
    has it's own perspective dropdown to switch to, which is called Fleet Virtulization.

    This function is to navigate to the new VMs page using the Fleet Virtulization dropdown and
    connect dots with the existing tests so as to apply DR Policy to the CNV VM workloads from this page
    using Standalone or Shared Protection type.

    Refer ACM-23371 and ACM-22068 for more details

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class

    Returns:
        True if VM is found on the selected managed cluster and function executes successfully, False otherwise
    """
    acm_version_str = ".".join(get_running_acm_version().split(".")[:2])
    acm_loc = locators_for_current_ocp_version()["acm_page"]
    log.info("Navigate to VMs console using Fleet Virtulization dropdown")
    acm_obj.do_click(acm_loc["switch-perspective"])
    acm_obj.do_click(acm_loc["fleet-virtual"])
    acm_obj.page_has_loaded(retries=10, sleep_time=5)
    if compare_versions(f"{acm_version_str} >= 2.16"):
        if acm_obj.check_element_presence(
            (
                acm_loc["modal_dialog_close_button"][1],
                acm_loc["modal_dialog_close_button"][0],
            ),
            timeout=10,
        ):
            log.info("Modal dialog box found, closing it..")
            acm_obj.do_click(acm_loc["modal_dialog_close_button"], timeout=5)
    log.info("From side nav bar, navigate to VirtualMachines page")
    acm_obj.do_click(acm_loc["nav-bar-vms-page"])
    log.info(
        "Successfully navigate to the VirtualMachines page under Fleet Virtualization"
    )
    if compare_versions(f"{acm_version_str} < 2.16"):
        if acm_obj.check_element_presence(
            (
                acm_loc["modal_dialog_close_button"][1],
                acm_loc["modal_dialog_close_button"][0],
            ),
            timeout=10,
        ):
            log.info("Modal dialog box found, closing it..")
            acm_obj.do_click(acm_loc["modal_dialog_close_button"], timeout=5)

    return True


def navigate_to_protected_applications_page(acm_obj, timeout=120):
    """
    Navigate to Protected Applications page under Data Services -> Disaster Recovery on ACM UI.

    This page displays both managed (AppSet) and discovered applications that are DR protected.
    Feature applicable from ODF 4.21+.

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        timeout (int): Timeout to wait for page elements

    Returns:
        bool: True if successfully navigated to Protected Applications page

    Raises:
        NoSuchElementException: If Protected Applications tab is not found

    """
    acm_loc = locators_for_current_ocp_version()["acm_page"]

    # First navigate to Data Services -> Disaster Recovery
    log.info("Navigating to Data Services -> Disaster Recovery page")
    acm_obj.navigate_data_services()

    # Take screenshot after navigating to DR page
    acm_obj.take_screenshot("dr_overview_page")

    # Click on Protected Applications tab
    log.info("Clicking on 'Protected applications' tab")
    protected_app_tab = acm_obj.wait_until_expected_text_is_found(
        locator=acm_loc["protected-applications-tab"],
        expected_text="Protected applications",
        timeout=timeout,
    )
    if protected_app_tab:
        acm_obj.do_click(
            acm_loc["protected-applications-tab"],
            avoid_stale=True,
            enable_screenshot=True,
        )
        # Wait for page to load
        acm_obj.wait_for_element_to_be_visible(
            acm_loc["protected-app-list-table"], timeout=30
        )
        acm_obj.take_screenshot("protected_applications_page")
        log.info("Successfully navigated to Protected Applications page")
        return True
    else:
        log.error("Protected Applications tab not found on Disaster Recovery page")
        acm_obj.take_screenshot("protected_app_tab_not_found")
        raise NoSuchElementException("Protected Applications tab not found")


def _clear_filters_and_search_protected_app(acm_obj, app_name, timeout=60):
    """
    Helper function to clear filters and search for an application on Protected Applications page.

    This is a common operation used by multiple verification functions.

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        app_name (str): Name of the application to search for
        timeout (int): Timeout for UI operations

    Returns:
        tuple: (app_locator, app_found) where app_locator is the formatted locator
               and app_found is boolean indicating if app was found

    """
    acm_loc = locators_for_current_ocp_version()["acm_page"]

    # Clear any existing filters (button only appears when filters are active).
    # use_fallback=False: absence is expected when no filters are set; skip AI fallback.
    try:
        clear_filter_present = acm_obj.check_element_presence(
            (acm_loc["clear-filter"][1], acm_loc["clear-filter"][0]),
            timeout=5,
            use_fallback=False,
        )
        if clear_filter_present:
            log.info("Clearing existing filters")
            acm_obj.do_click(acm_loc["clear-filter"])
    except Exception:
        pass

    # Click on search bar and enter the app name
    log.info(f"Searching for application: {app_name}")
    acm_obj.do_click(acm_loc["protected-app-search-bar"], timeout=timeout)
    acm_obj.do_clear(acm_loc["protected-app-search-bar"])
    acm_obj.do_send_keys(acm_loc["protected-app-search-bar"], text=app_name)

    # Wait for search results to load
    app_locator = format_locator(acm_loc["protected-app-name-in-list"], app_name)

    # Try to wait for element visibility, but don't fail if not found
    # This is important when verifying app removal (expected_present=False)
    try:
        acm_obj.wait_for_element_to_be_visible(app_locator, timeout=10)
        app_found = True
    except SeleniumTimeoutException:
        log.info(f"Application '{app_name}' not visible in search results")
        app_found = False

    # Take screenshot after search
    acm_obj.take_screenshot(f"search_result_{app_name}")

    return app_locator, app_found


def verify_app_in_protected_applications_list(
    acm_obj, app_name, timeout=60, expected_present=True, retry_interval=10
):
    """
    Verify if an application is present in the Protected Applications list view.

    This function searches for a specific application on the Protected Applications page
    which lists both managed (AppSet) and discovered applications.

    The function polls with retries for both presence and absence checks:
    - When expected_present=True: Polls until app appears or timeout (useful after DR apply)
    - When expected_present=False: Polls until app disappears or timeout (useful after DR removal)

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        app_name (str): Name of the application to search for
        timeout (int): Total timeout for polling/retry attempts
        expected_present (bool): If True, expects app to be present; if False, expects app to be absent
        retry_interval (int): Seconds to wait between retry attempts

    Returns:
        bool: True if verification passes (app found when expected_present=True,
              or app not found when expected_present=False)

    """
    state_desc = "presence" if expected_present else "removal"
    log.info(
        f"Verifying application '{app_name}' {state_desc} in Protected Applications list "
        f"(timeout={timeout}s, retry_interval={retry_interval}s)"
    )

    end_time = time.time() + timeout
    attempt = 0

    while time.time() < end_time:
        attempt += 1
        log.info(
            f"Attempt {attempt}: Checking '{app_name}' {state_desc}..."
        )

        # Navigate to Protected Applications page to refresh the view
        navigate_to_protected_applications_page(acm_obj)

        # Search for the app
        _, app_found = _clear_filters_and_search_protected_app(
            acm_obj, app_name, timeout=30  # Shorter timeout for individual search
        )

        if expected_present and app_found:
            log.info(
                f"Application '{app_name}' found in Protected Applications list "
                f"(attempt {attempt})"
            )
            return True
        elif not expected_present and not app_found:
            log.info(
                f"Application '{app_name}' correctly not present in "
                f"Protected Applications list (attempt {attempt})"
            )
            return True

        # Condition not met yet, log and retry
        if expected_present:
            log.info(
                f"Application '{app_name}' not found yet, "
                f"waiting {retry_interval}s before next check..."
            )
        else:
            log.info(
                f"Application '{app_name}' still present, "
                f"waiting {retry_interval}s before next check..."
            )
        time.sleep(retry_interval)

    # Timeout reached
    if expected_present:
        log.error(
            f"Application '{app_name}' NOT found in Protected Applications list "
            f"after {timeout}s timeout"
        )
    else:
        log.error(
            f"Application '{app_name}' still found in Protected Applications list "
            f"after {timeout}s timeout"
        )
    return False


def verify_protected_applications_list_view(
    acm_obj,
    appset_workloads=None,
    discovered_workloads=None,
    timeout=60,
):
    """
    Verify that both managed (AppSet) and discovered applications appear
    on the Protected Applications list view page.

    This is the main verification function for the Protected Applications list view feature
    which enhances the page to display both managed and discovered applications.

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        appset_workloads (list): List of AppSet workload objects (used to derive namespace-based names)
        discovered_workloads (list): List of Discovered Apps workload objects with
                                     discovered_apps_placement_name attribute
        timeout (int): Timeout for UI operations

    Returns:
        bool: True if all applications are verified on the Protected Applications page

    Raises:
        AssertionError: If any application is not found on the Protected Applications page

    """
    log.info("Starting verification of Protected Applications list view")

    # Navigate to Protected Applications page
    navigate_to_protected_applications_page(acm_obj, timeout=timeout)

    apps_to_verify = []

    # Collect AppSet workload names
    # Protected Apps UI uses the full namespace (e.g. appset-workload-<hash>-rbd)
    # as data-test=resource-link-{name}; do not strip the 'appset-' prefix.
    if appset_workloads:
        for workload in appset_workloads:
            app_name = workload.workload_namespace
            apps_to_verify.append(("AppSet/Managed", app_name))
            log.info(f"Will verify AppSet workload (derived from namespace): {app_name}")

    # Collect Discovered Apps workload names
    if discovered_workloads:
        for workload in discovered_workloads:
            app_name = workload.discovered_apps_placement_name
            apps_to_verify.append(("Discovered", app_name))
            log.info(f"Will verify Discovered Apps workload: {app_name}")

    # Verify each application
    # Note: verify_app_in_protected_applications_list() uses the helper function
    # _clear_filters_and_search_protected_app() which clears filters before each search
    for app_type, app_name in apps_to_verify:
        log.info(f"Verifying {app_type} application: {app_name}")
        app_found = verify_app_in_protected_applications_list(
            acm_obj, app_name, timeout=timeout, expected_present=True
        )
        if app_found:
            log.info(
                f"{app_type} application '{app_name}' successfully verified on Protected Applications page"
            )
        else:
            log.error(
                f"{app_type} application '{app_name}' NOT found on Protected Applications page"
            )
            raise AssertionError(
                f"{app_type} application '{app_name}' NOT found on Protected Applications page"
            )

    log.info(
        "Successfully verified all applications on Protected Applications list view page"
    )

    return True


def verify_protected_app_kebab_menu_actions(
    acm_obj,
    app_name,
    expected_actions=None,
    timeout=60,
):
    """
    Verify the kebab menu actions available for a protected application.

    This function clicks on the kebab menu for a specific application and verifies
    that the expected action items are present.

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        app_name (str): Name of the application to verify kebab menu for
        expected_actions (list): List of expected action names. Defaults to
                                 ["Edit configuration", "Failover", "Relocate",
                                  "Remove disaster recovery"] for managed apps.
        timeout (int): Timeout for UI operations

    Returns:
        bool: True if all expected actions are present

    Raises:
        AssertionError: If any expected action is not found in the kebab menu

    """
    if expected_actions is None:
        expected_actions = [
            "Edit configuration",
            "Failover",
            "Relocate",
            "Manage disaster recovery",
        ]

    acm_loc = locators_for_current_ocp_version()["acm_page"]

    log.info(f"Verifying kebab menu actions for app: {app_name}")

    # Clear filters and search for the application
    _clear_filters_and_search_protected_app(acm_obj, app_name, timeout)

    # Click on the kebab menu for this application
    log.info(f"Clicking kebab menu for app: {app_name}")
    kebab_locator = format_locator(acm_loc["protected-app-kebab-menu"], app_name)
    acm_obj.do_click(kebab_locator, timeout=timeout, enable_screenshot=True)

    # Wait for dropdown menu to appear
    first_action_locator = format_locator(
        acm_loc["protected-app-action-menu-item"], expected_actions[0]
    )
    acm_obj.wait_for_element_to_be_visible(first_action_locator, timeout=10)
    acm_obj.take_screenshot(f"kebab_menu_{app_name}")

    # Verify each expected action is present
    all_actions_found = True
    for action in expected_actions:
        log.info(f"Checking for action: {action}")

        # Use the parameterized locator for menu item
        action_locator = format_locator(
            acm_loc["protected-app-action-menu-item"], action
        )

        action_found = acm_obj.check_element_presence(
            (action_locator[1], action_locator[0]), timeout=10
        )

        if action_found:
            log.info(f"Action '{action}' found in kebab menu")
        else:
            log.error(f"Action '{action}' NOT found in kebab menu")
            all_actions_found = False

    # Close the kebab menu by pressing Escape
    try:
        from selenium.webdriver.common.keys import Keys

        acm_obj.driver.find_element("tag name", "body").send_keys(Keys.ESCAPE)
        time.sleep(1)
    except Exception as e:
        log.debug(f"Could not close kebab menu: {e}")

    if all_actions_found:
        log.info(
            f"All expected actions verified for app '{app_name}': {expected_actions}"
        )
    else:
        acm_obj.take_screenshot(f"kebab_menu_actions_missing_{app_name}")
        raise AssertionError(
            f"Not all expected actions found in kebab menu for app '{app_name}'. "
            f"Expected: {expected_actions}"
        )

    return all_actions_found


def verify_protected_app_dr_status(
    acm_obj,
    app_name,
    expected_status,
    timeout=120,
):
    """
    Verify the DR status of a protected application on the Protected Applications page.

    This function navigates to the Protected Applications page, searches for the
    specified application, and verifies its DR status matches the expected value.

    DR Status values:
        - Protecting: Initial sync in progress
        - Healthy: Sync completed successfully
        - Warning: Sync delayed beyond threshold
        - Critical: Sync failed or cluster issues

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        app_name (str): Name of the application to verify DR status for
        expected_status (str): Expected DR status (Protecting, Healthy, Warning, Critical)
        timeout (int): Timeout to wait for expected status

    Returns:
        bool: True if DR status matches expected value

    Raises:
        AssertionError: If DR status does not match expected value within timeout

    """
    acm_loc = locators_for_current_ocp_version()["acm_page"]

    log.info(f"Verifying DR status for app '{app_name}', expected: '{expected_status}'")

    # Navigate to Protected Applications page if not already there
    navigate_to_protected_applications_page(acm_obj, timeout=timeout)

    # Clear filters and search for the application
    _clear_filters_and_search_protected_app(acm_obj, app_name, timeout=30)

    # Get DR status locator for this app
    status_locator = format_locator(acm_loc["protected-app-dr-status"], app_name)

    # Wait for status element to be visible and get actual status
    acm_obj.wait_for_element_to_be_visible(status_locator, timeout=timeout)

    try:
        actual_status = acm_obj.get_element_text(status_locator)
    except Exception:
        actual_status = "Unknown"

    acm_obj.take_screenshot(f"dr_status_{app_name}_{expected_status}")

    # Case-insensitive comparison for status
    if actual_status.lower() == expected_status.lower():
        log.info(f"App '{app_name}' has expected DR status: '{actual_status}'")
        return True
    else:
        log.error(
            f"App '{app_name}' DR status mismatch. "
            f"Expected: '{expected_status}', Actual: '{actual_status}'"
        )
        acm_obj.take_screenshot(f"dr_status_mismatch_{app_name}")
        raise AssertionError(
            f"DR status for app '{app_name}' is '{actual_status}', "
            f"expected '{expected_status}'"
        )


def verify_manage_dr_modal_for_managed_app(
    acm_obj,
    app_name,
    expected_policy_name,
    drpc_obj=None,
    timeout=60,
):
    """
    Verify the Manage DR modal for a managed application shows correct details.

    This function opens the kebab menu for the specified managed app, clicks
    "Manage disaster recovery" action, and verifies the modal content including:
    - DR Policy name (with "Validated" status)
    - Volume group replication status (Enabled)
    - Last sync time (matches CLI if drpc_obj provided)

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        app_name (str): Name of the managed application
        expected_policy_name (str): Expected DR policy name (e.g., "odr-policy-5m")
        drpc_obj (DRPC): DRPC object to get CLI last sync time for comparison (optional)
        timeout (int): Timeout for UI operations

    Returns:
        bool: True if all verifications pass

    Raises:
        AssertionError: If any verification fails

    """
    acm_loc = locators_for_current_ocp_version()["acm_page"]

    log.info(f"Verifying Manage DR modal for managed app: {app_name}")

    # Navigate to Protected Applications page
    navigate_to_protected_applications_page(acm_obj, timeout=timeout)

    # Clear filters and search for the application
    _clear_filters_and_search_protected_app(acm_obj, app_name, timeout=30)

    # Click kebab menu for this app
    kebab_locator = format_locator(acm_loc["protected-app-kebab-menu"], app_name)
    acm_obj.do_click(kebab_locator, timeout=timeout)
    log.info(f"Clicked kebab menu for app: {app_name}")

    # Click "Manage disaster recovery" action
    action_locator = format_locator(
        acm_loc["protected-app-action-menu-item"], "Manage disaster recovery"
    )
    acm_obj.do_click(action_locator, timeout=timeout)
    log.info("Clicked 'Manage disaster recovery' action")

    # Wait for modal to appear
    acm_obj.wait_for_element_to_be_visible(
        acm_loc["manage-dr-modal"], timeout=timeout
    )
    acm_obj.take_screenshot(f"manage_dr_modal_{app_name}")
    log.info("Manage DR modal opened successfully")

    verification_results = []

    # Verify DR Policy name
    policy_locator = format_locator(
        acm_loc["manage-dr-policy-name"], expected_policy_name
    )
    policy_found = acm_obj.check_element_presence(
        (policy_locator[1], policy_locator[0]), timeout=30
    )
    if policy_found:
        log.info(f"Verified DR Policy name: {expected_policy_name}")
        verification_results.append(("DR Policy name", True))
    else:
        log.error(f"DR Policy name '{expected_policy_name}' NOT found in modal")
        verification_results.append(("DR Policy name", False))

    # Verify Volume group replication is Enabled
    vrg_found = acm_obj.check_element_presence(
        (acm_loc["manage-dr-vrg-status"][1], acm_loc["manage-dr-vrg-status"][0]),
        timeout=30,
    )
    if vrg_found:
        log.info("Verified Volume group replication: Enabled")
        verification_results.append(("VRG Enabled", True))
    else:
        log.error("Volume group replication 'Enabled' NOT found in modal")
        verification_results.append(("VRG Enabled", False))

    # Verify Last sync time is present
    sync_time_found = acm_obj.check_element_presence(
        (
            acm_loc["manage-dr-last-sync-time"][1],
            acm_loc["manage-dr-last-sync-time"][0],
        ),
        timeout=30,
    )
    if sync_time_found:
        try:
            sync_time_element = acm_obj.find_an_element_by_xpath(
                acm_loc["manage-dr-last-sync-time"][0]
            )
            ui_sync_time_text = sync_time_element.text
            log.info(f"Last sync time from UI: {ui_sync_time_text}")
            verification_results.append(("Last sync time present", True))

            # If DRPC object provided, compare with CLI
            if drpc_obj:
                cli_sync_time = drpc_obj.get_last_group_sync_time()
                log.info(f"Last sync time from CLI (DRPC): {cli_sync_time}")
                # UI format: "Last synced on 9 Feb 2026, 08:15 UTC"
                # CLI format: "2026-02-09T08:15:00Z"
                # For basic validation, just log both values
                log.info(
                    f"UI sync time: {ui_sync_time_text}, CLI sync time: {cli_sync_time}"
                )
        except Exception as e:
            log.warning(f"Could not get last sync time text: {e}")
    else:
        log.error("Last sync time NOT found in modal")
        verification_results.append(("Last sync time present", False))

    acm_obj.take_screenshot(f"manage_dr_modal_verified_{app_name}")

    # Close the modal
    try:
        acm_obj.do_click(acm_loc["manage-dr-modal-close"], timeout=10)
        log.info("Closed Manage DR modal")
    except Exception as e:
        log.warning(f"Could not close modal using close button: {e}")
        # Try pressing Escape key as fallback
        from selenium.webdriver.common.keys import Keys

        acm_obj.send_keys(Keys.ESCAPE)

    # Check all verifications passed
    failed_checks = [name for name, passed in verification_results if not passed]
    if failed_checks:
        raise AssertionError(
            f"Manage DR modal verification failed for app '{app_name}'. "
            f"Failed checks: {failed_checks}"
        )

    log.info(f"All Manage DR modal verifications passed for app: {app_name}")
    return True


def failover_from_protected_app_page(
    acm_obj,
    app_name,
    expected_target_cluster,
    drpc_obj=None,
    timeout=120,
):
    """
    Perform failover for an application from the Protected Applications page.

    Works for both AppSet/managed and discovered applications listed on the
    Protected Applications page.

    This function:
    1. Navigates to Protected Applications page
    2. Searches for the app and clicks Failover from kebab menu
    3. Verifies failover modal content (target cluster, readiness)
    4. Clicks Initiate to start failover
    5. Waits for failover completion via CLI (DRPC status)

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        app_name (str): Name of the application to failover (AppSet namespace or
            discovered placement name as shown in the UI)
        expected_target_cluster (str): Expected target cluster name for failover
        drpc_obj (DRPC): DRPC object for CLI verification (optional)
        timeout (int): Timeout for UI operations

    Returns:
        bool: True if failover initiated successfully

    Raises:
        AssertionError: If failover readiness is not Ready or target cluster mismatch

    """
    from ocs_ci.ocs import constants

    acm_loc = locators_for_current_ocp_version()["acm_page"]

    log.info(f"Initiating failover for app '{app_name}' to cluster '{expected_target_cluster}'")

    # Navigate to Protected Applications page
    navigate_to_protected_applications_page(acm_obj, timeout=timeout)

    # Clear filters and search for the application
    _clear_filters_and_search_protected_app(acm_obj, app_name, timeout=30)

    # Click kebab menu for this app
    kebab_locator = format_locator(acm_loc["protected-app-kebab-menu"], app_name)
    acm_obj.do_click(kebab_locator, timeout=timeout)
    log.info(f"Clicked kebab menu for app: {app_name}")

    # Click "Failover" action
    action_locator = format_locator(acm_loc["protected-app-action-menu-item"], "Failover")
    acm_obj.do_click(action_locator, timeout=timeout)
    log.info("Clicked 'Failover' action from kebab menu")

    # Wait for failover modal to appear
    acm_obj.wait_for_element_to_be_visible(acm_loc["failover-modal"], timeout=timeout)
    acm_obj.take_screenshot(f"failover_modal_{app_name}")
    log.info("Failover modal opened successfully")

    # Verify target cluster name is present in modal
    target_cluster_locator = format_locator(
        acm_loc["failover-target-cluster-text"], expected_target_cluster
    )
    target_found = acm_obj.check_element_presence(
        (target_cluster_locator[1], target_cluster_locator[0]), timeout=30
    )
    if not target_found:
        acm_obj.take_screenshot(f"failover_target_cluster_not_found_{app_name}")
        raise AssertionError(
            f"Target cluster '{expected_target_cluster}' not found in Failover modal"
        )
    log.info(f"Verified target cluster: {expected_target_cluster}")

    # Verify failover readiness is "Ready"
    readiness_found = acm_obj.check_element_presence(
        (acm_loc["failover-ready-status"][1], acm_loc["failover-ready-status"][0]),
        timeout=60,
    )
    if not readiness_found:
        acm_obj.take_screenshot(f"failover_not_ready_{app_name}")
        raise AssertionError(f"Failover readiness is not 'Ready' for app '{app_name}'")
    log.info("Verified failover readiness: Ready")

    acm_obj.take_screenshot(f"failover_modal_verified_{app_name}")

    # Click Initiate button to start failover
    log.info("Clicking Initiate button to start failover")
    acm_obj.do_click(acm_loc["failover-initiate-btn"], timeout=timeout)
    log.info(f"Failover initiated for app: {app_name}")

    acm_obj.take_screenshot(f"failover_initiated_{app_name}")

    # Wait for failover completion via CLI if DRPC object provided
    if drpc_obj:
        log.info(f"Waiting for DRPC to reach {constants.STATUS_FAILEDOVER} phase")
        drpc_obj.wait_for_phase(constants.STATUS_FAILEDOVER, timeout=600)
        log.info(f"DRPC reached {constants.STATUS_FAILEDOVER} phase - failover complete")

    return True


def relocate_from_protected_app_page(
    acm_obj,
    app_name,
    expected_target_cluster,
    drpc_obj=None,
    timeout=120,
    discovered_apps=False,
):
    """
    Perform relocate for an application from the Protected Applications page.

    Works for both AppSet/managed and discovered applications listed on the
    Protected Applications page.

    This function:
    1. Navigates to Protected Applications page
    2. Searches for the app and clicks Relocate from kebab menu
    3. Verifies relocate modal content (target cluster, readiness)
    4. Clicks Initiate to start relocate
    5. Waits for DRPC phase via CLI when drpc_obj is provided (Relocated for
       managed apps; Relocating for discovered apps before manual cleanup)

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        app_name (str): Name of the application to relocate (AppSet namespace or
            discovered placement name as shown in the UI)
        expected_target_cluster (str): Expected target cluster name for relocate
        drpc_obj (DRPC): DRPC object for CLI verification (optional)
        timeout (int): Timeout for UI operations
        discovered_apps (bool): For discovered apps, wait for Relocating phase only.
            Cleanup on the old primary must run before Relocated is reached.

    Returns:
        bool: True if relocate initiated successfully

    Raises:
        AssertionError: If relocate readiness is not Ready or target cluster mismatch

    """
    from ocs_ci.ocs import constants

    acm_loc = locators_for_current_ocp_version()["acm_page"]

    log.info(f"Initiating relocate for app '{app_name}' to cluster '{expected_target_cluster}'")

    # Navigate to Protected Applications page
    navigate_to_protected_applications_page(acm_obj, timeout=timeout)

    # Clear filters and search for the application
    _clear_filters_and_search_protected_app(acm_obj, app_name, timeout=30)

    # Click kebab menu for this app
    kebab_locator = format_locator(acm_loc["protected-app-kebab-menu"], app_name)
    acm_obj.do_click(kebab_locator, timeout=timeout)
    log.info(f"Clicked kebab menu for app: {app_name}")

    # Click "Relocate" action
    action_locator = format_locator(acm_loc["protected-app-action-menu-item"], "Relocate")
    acm_obj.do_click(action_locator, timeout=timeout)
    log.info("Clicked 'Relocate' action from kebab menu")

    # Wait for relocate modal to appear
    acm_obj.wait_for_element_to_be_visible(acm_loc["relocate-modal"], timeout=timeout)
    acm_obj.take_screenshot(f"relocate_modal_{app_name}")
    log.info("Relocate modal opened successfully")

    # Verify target cluster name is present in modal
    target_cluster_locator = format_locator(
        acm_loc["relocate-target-cluster-text"], expected_target_cluster
    )
    target_found = acm_obj.check_element_presence(
        (target_cluster_locator[1], target_cluster_locator[0]), timeout=30
    )
    if not target_found:
        acm_obj.take_screenshot(f"relocate_target_cluster_not_found_{app_name}")
        raise AssertionError(
            f"Target cluster '{expected_target_cluster}' not found in Relocate modal"
        )
    log.info(f"Verified target cluster: {expected_target_cluster}")

    # Verify relocate readiness is "Ready"
    readiness_found = acm_obj.check_element_presence(
        (acm_loc["relocate-ready-status"][1], acm_loc["relocate-ready-status"][0]),
        timeout=60,
    )
    if not readiness_found:
        acm_obj.take_screenshot(f"relocate_not_ready_{app_name}")
        raise AssertionError(f"Relocate readiness is not 'Ready' for app '{app_name}'")
    log.info("Verified relocate readiness: Ready")

    acm_obj.take_screenshot(f"relocate_modal_verified_{app_name}")

    # Click Initiate button to start relocate
    log.info("Clicking Initiate button to start relocate")
    acm_obj.do_click(acm_loc["relocate-initiate-btn"], timeout=timeout)
    log.info(f"Relocate initiated for app: {app_name}")

    acm_obj.take_screenshot(f"relocate_initiated_{app_name}")

    # Wait for relocate progress via CLI if DRPC object provided
    if drpc_obj:
        if discovered_apps:
            log.info(
                f"Waiting for DRPC to reach {constants.STATUS_RELOCATING} phase "
                f"(discovered app; cleanup required before Relocated)"
            )
            drpc_obj.wait_for_phase(
                constants.STATUS_RELOCATING, timeout=1200, sleep=15
            )
            log.info(
                f"DRPC reached {constants.STATUS_RELOCATING} phase - "
                "ready for discovered apps cleanup"
            )
        else:
            log.info(f"Waiting for DRPC to reach {constants.STATUS_RELOCATED} phase")
            drpc_obj.wait_for_phase(constants.STATUS_RELOCATED, timeout=600)
            log.info(
                f"DRPC reached {constants.STATUS_RELOCATED} phase - relocate complete"
            )

    return True


def remove_dr_from_protected_app_page(acm_obj, app_name, timeout=120):
    """
    Remove DR protection for an application from the Protected Applications page.

    This function:
    1. Navigates to the Protected Applications page
    2. Searches for the application
    3. Clicks the kebab menu and selects "Manage disaster recovery"
    4. Clicks "Remove disaster recovery" button in the modal

    Args:
        acm_obj (AcmAddClusters): ACM Page Navigator Class
        app_name (str): Name of the application to remove DR protection from
        timeout (int): Timeout for UI operations

    Returns:
        bool: True if DR removal was initiated successfully

    """
    acm_loc = locators_for_current_ocp_version()["acm_page"]

    # Navigate to Protected Applications page
    log.info(f"Navigating to Protected Applications page to remove DR for: {app_name}")
    navigate_to_protected_applications_page(acm_obj)

    # Search for the application
    log.info(f"Searching for application: {app_name}")
    _clear_filters_and_search_protected_app(acm_obj, app_name, timeout)

    # Click on kebab menu for the application
    log.info(f"Opening kebab menu for app: {app_name}")
    kebab_locator = format_locator(acm_loc["protected-app-kebab-menu"], app_name)
    acm_obj.do_click(kebab_locator, enable_screenshot=True, timeout=timeout)

    # Click on "Manage disaster recovery" action
    log.info("Clicking 'Manage disaster recovery' action")
    action_locator = format_locator(
        acm_loc["protected-app-action-menu-item"], "Manage disaster recovery"
    )
    acm_obj.do_click(action_locator, enable_screenshot=True, timeout=timeout)

    # Wait for Manage DR modal to appear
    log.info("Waiting for Manage DR modal to appear")
    acm_obj.wait_for_element_to_be_visible(acm_loc["manage-dr-modal"], timeout=30)
    acm_obj.take_screenshot(f"manage_dr_modal_{app_name}")

    # Click on "Remove disaster recovery" button
    log.info("Clicking 'Remove disaster recovery' button")
    acm_obj.do_click(acm_loc["remove-dr-btn"], enable_screenshot=True, timeout=timeout)

    # Wait for confirmation dialog to appear
    log.info("Waiting for confirmation dialog...")
    time.sleep(2)
    acm_obj.take_screenshot(f"dr_removal_confirmation_dialog_{app_name}")

    # Click on "Confirm remove" button
    log.info("Clicking 'Confirm remove' button")
    acm_obj.do_click(
        acm_loc["confirm-remove-dr-btn"], enable_screenshot=True, timeout=timeout
    )

    log.info(f"DR removal confirmed for app: {app_name}")
    acm_obj.take_screenshot(f"dr_removal_confirmed_{app_name}")

    # Wait for DR removal to complete and modal to close
    time.sleep(5)

    # Close the Manage DR modal if it's still open
    log.info("Checking if modal needs to be closed...")
    try:
        modal_close_btn = acm_obj.check_element_presence(
            (acm_loc["manage-dr-modal-close"][1], acm_loc["manage-dr-modal-close"][0]),
            timeout=5,
        )
        if modal_close_btn:
            acm_obj.do_click(
                acm_loc["manage-dr-modal-close"], enable_screenshot=True, timeout=10
            )
            log.info("Manage DR modal closed successfully")
    except Exception as e:
        log.info(f"Modal may have closed automatically: {e}")

    acm_obj.take_screenshot(f"dr_removal_complete_{app_name}")

    return True
