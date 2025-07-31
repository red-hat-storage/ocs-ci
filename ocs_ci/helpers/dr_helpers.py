"""
Helper functions specific for DR
"""

import json
import logging
import tempfile
import time
from datetime import datetime

from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.cluster import is_hci_cluster
from ocs_ci.ocs.defaults import RBD_NAME
from ocs_ci.ocs.exceptions import (
    TimeoutExpiredError,
    UnexpectedBehaviour,
    NotFoundError,
    UnexpectedDeploymentConfiguration,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.drpc import DRPC
from ocs_ci.ocs.resources.pod import get_all_pods, get_ceph_tools_pod
from ocs_ci.ocs.resources.pv import get_all_pvs
from ocs_ci.ocs.resources.pvc import get_all_pvc_objs
from ocs_ci.ocs.node import gracefully_reboot_nodes, get_node_objs
from ocs_ci.ocs.utils import (
    get_non_acm_cluster_config,
    get_active_acm_index,
    get_primary_cluster_config,
    get_passive_acm_index,
    enable_mco_console_plugin,
    set_recovery_as_primary,
    get_all_acm_indexes,
)
from ocs_ci.utility import version, templating
from ocs_ci.utility.retry import retry

from ocs_ci.utility.utils import (
    TimeoutSampler,
    CommandFailed,
    run_cmd,
    exec_cmd,
    is_cluster_y_version_upgraded,
)
from ocs_ci.helpers.helpers import (
    run_cmd_verify_cli_output,
    find_cephblockpoolradosnamespace,
    find_cephfilesystemsubvolumegroup,
    create_unique_resource_name,
)

logger = logging.getLogger(__name__)


def get_current_primary_cluster_name(
    namespace,
    workload_type=constants.SUBSCRIPTION,
    discovered_apps=False,
    resource_name=None,
):
    """
    Get current primary cluster name based on workload namespace

    Args:
        namespace (str): Name of the namespace
        workload_type (str): Type of workload, i.e., Subscription or ApplicationSet
        discovered_apps (bool): If true then deployed workload is discovered_apps
        resource_name (str): DRPC NAME Only Used for discovered apps

    Returns:
        str: Current primary cluster name

    """
    restore_index = config.cur_index
    if workload_type == constants.APPLICATION_SET:
        namespace = constants.GITOPS_CLUSTER_NAMESPACE
    if discovered_apps:
        if not resource_name:
            raise ValueError("Resource name is expected")
        namespace = constants.DR_OPS_NAMESAPCE
        drpc_data = DRPC(namespace=namespace, resource_name=resource_name).get()
    else:
        drpc_data = DRPC(namespace=namespace).get()
    if drpc_data.get("spec").get("action") == constants.ACTION_FAILOVER:
        cluster_name = drpc_data["spec"]["failoverCluster"]
    else:
        cluster_name = drpc_data["spec"]["preferredCluster"]
    config.switch_ctx(restore_index)
    return cluster_name


def get_current_secondary_cluster_name(
    namespace,
    workload_type=constants.SUBSCRIPTION,
    discovered_apps=False,
    resource_name=None,
):
    """
    Get current secondary cluster name based on workload namespace

    Args:
        namespace (str): Name of the namespace
        workload_type (str): Type of workload, i.e., Subscription or ApplicationSet
        discovered_apps (bool): If true then deployed workload is discovered_apps
        resource_name (str): DRPC NAME Only Used for discovered apps


    Returns:
        str: Current secondary cluster name

    """
    restore_index = config.cur_index
    if workload_type == constants.APPLICATION_SET:
        namespace = constants.GITOPS_CLUSTER_NAMESPACE
    if discovered_apps:
        namespace = constants.DR_OPS_NAMESAPCE
        primary_cluster_name = get_current_primary_cluster_name(
            namespace=namespace,
            resource_name=resource_name,
            discovered_apps=discovered_apps,
        )
        drpolicy_data = DRPC(
            namespace=namespace, resource_name=resource_name
        ).drpolicy_obj.get()
    else:
        primary_cluster_name = get_current_primary_cluster_name(namespace)
        drpolicy_data = DRPC(namespace=namespace).drpolicy_obj.get()
    config.switch_ctx(restore_index)
    for cluster_name in drpolicy_data["spec"]["drClusters"]:
        if not cluster_name == primary_cluster_name:
            return cluster_name


def set_current_primary_cluster_context(
    namespace, workload_type=constants.SUBSCRIPTION
):
    """
    Set current primary cluster context based on workload namespace

    Args:
        namespace (str): Name of the namespace
        workload_type (str): Type of workload, i.e., Subscription or ApplicationSet

    """
    if workload_type == constants.APPLICATION_SET:
        namespace = constants.GITOPS_CLUSTER_NAMESPACE
    cluster_name = get_current_primary_cluster_name(namespace)
    config.switch_to_cluster_by_name(cluster_name)


def set_current_secondary_cluster_context(
    namespace, workload_type=constants.SUBSCRIPTION
):
    """
    Set secondary cluster context based on workload namespace

    Args:
        namespace (str): Name of the namespace
        workload_type (str): Type of workload, i.e., Subscription or ApplicationSet

    """
    if workload_type == constants.APPLICATION_SET:
        namespace = constants.GITOPS_CLUSTER_NAMESPACE
    cluster_name = get_current_secondary_cluster_name(namespace)
    config.switch_to_cluster_by_name(cluster_name)


def get_scheduling_interval(
    namespace,
    workload_type=constants.SUBSCRIPTION,
    discovered_apps=False,
    resource_name=None,
):
    """
    Get scheduling interval for the workload in the given namespace

    Args:
        namespace (str): Name of the namespace
        workload_type (str): Type of workload, i.e., Subscription or ApplicationSet
        discovered_apps (bool): If true then deployed workload is discovered_apps

    Returns:
        int: scheduling interval value from DRPolicy

    """
    restore_index = config.cur_index
    if workload_type == constants.APPLICATION_SET:
        namespace = constants.GITOPS_CLUSTER_NAMESPACE
    if discovered_apps:
        if not resource_name:
            raise ValueError("Resource name is expected")
        namespace = constants.DR_OPS_NAMESAPCE
        drpolicy_obj = DRPC(
            namespace=namespace, resource_name=resource_name
        ).drpolicy_obj
    else:
        drpolicy_obj = DRPC(namespace=namespace).drpolicy_obj
    interval_value = int(drpolicy_obj.get()["spec"]["schedulingInterval"][:-1])
    config.switch_ctx(restore_index)
    return interval_value


def failover(
    failover_cluster,
    namespace,
    workload_type=constants.SUBSCRIPTION,
    workload_placement_name=None,
    switch_ctx=None,
    discovered_apps=False,
    old_primary=None,
):
    """
    Initiates Failover action to the specified cluster

    Args:
        failover_cluster (str): Cluster name to which the workload should be failed over
        namespace (str): Namespace where workload is running
        workload_type (str): Type of workload, i.e., Subscription or ApplicationSet
        workload_placement_name (str): Placement name
        switch_ctx (int): The cluster index by the cluster name
        discovered_apps (bool): True when cluster is failing over DiscoveredApps
        old_primary (str): Name of cluster where workload were running

    """
    restore_index = config.cur_index
    config.switch_ctx(switch_ctx) if switch_ctx else config.switch_acm_ctx()
    failover_params = f'{{"spec":{{"action":"{constants.ACTION_FAILOVER}","failoverCluster":"{failover_cluster}"}}}}'
    if workload_type == constants.APPLICATION_SET:
        namespace = constants.GITOPS_CLUSTER_NAMESPACE
        drpc_obj = DRPC(
            namespace=namespace,
            resource_name=f"{workload_placement_name}-drpc",
            switch_ctx=switch_ctx,
        )
    elif discovered_apps:
        failover_params = (
            f'{{"spec":{{"action":"{constants.ACTION_FAILOVER}",'
            f'"failoverCluster":"{failover_cluster}",'
            f'"preferredCluster":"{old_primary}"}}}}'
        )
        namespace = constants.DR_OPS_NAMESAPCE
        drpc_obj = DRPC(namespace=namespace, resource_name=f"{workload_placement_name}")
    else:
        drpc_obj = DRPC(namespace=namespace, switch_ctx=switch_ctx)
    drpc_obj.wait_for_peer_ready_status()
    logger.info(f"Initiating Failover action with failoverCluster:{failover_cluster}")
    assert drpc_obj.patch(
        params=failover_params, format_type="merge"
    ), f"Failed to patch {constants.DRPC}: {drpc_obj.resource_name}"

    logger.info(
        f"Wait for {constants.DRPC}: {drpc_obj.resource_name} to reach {constants.STATUS_FAILEDOVER} phase"
    )

    drpc_obj.wait_for_phase(
        constants.STATUS_FAILEDOVER,
        timeout=360,
    )
    config.switch_ctx(restore_index)


def relocate(
    preferred_cluster,
    namespace,
    workload_type=constants.SUBSCRIPTION,
    workload_placement_name=None,
    switch_ctx=None,
    discovered_apps=False,
    old_primary=None,
    workload_instance=None,
    multi_ns=False,
    workload_instances_shared=None,
):
    """
    Initiates Relocate action to the specified cluster

    Args:
        preferred_cluster (str): Cluster name to which the workload should be relocated
        namespace (str): Namespace where workload is running
        workload_type (str): Type of workload, i.e., Subscription or ApplicationSet
        workload_placement_name (str): Placement name
        switch_ctx (int): The cluster index by the cluster name
        discovered_apps (bool): If true then deployed workload is discovered_apps
        old_primary (str): Name of cluster where workload were running
        workload_instance (object): Discovered App instance to get namespace and dir location
        multi_ns (bool): Multi Namespace
        workload_instances_shared (list): List of workloads tied to a single DRPC using Shared Protection type

    """
    restore_index = config.cur_index
    config.switch_ctx(switch_ctx) if switch_ctx else config.switch_acm_ctx()
    relocate_params = f'{{"spec":{{"action":"{constants.ACTION_RELOCATE}","preferredCluster":"{preferred_cluster}"}}}}'
    if workload_type == constants.APPLICATION_SET:
        namespace = constants.GITOPS_CLUSTER_NAMESPACE
        drpc_obj = DRPC(
            namespace=namespace,
            resource_name=f"{workload_placement_name}-drpc",
            switch_ctx=switch_ctx,
        )
    elif discovered_apps:
        relocate_params = (
            f'{{"spec":{{"action":"{constants.ACTION_RELOCATE}",'
            f'"failoverCluster":"{old_primary}",'
            f'"preferredCluster":"{preferred_cluster}"}}}}'
        )
        namespace = constants.DR_OPS_NAMESAPCE
        drpc_obj = DRPC(namespace=namespace, resource_name=f"{workload_placement_name}")
    else:
        drpc_obj = DRPC(namespace=namespace, switch_ctx=switch_ctx)
    drpc_obj.wait_for_peer_ready_status()
    logger.info(f"Initiating Relocate action with preferredCluster:{preferred_cluster}")
    assert drpc_obj.patch(
        params=relocate_params, format_type="merge"
    ), f"Failed to patch {constants.DRPC}: {drpc_obj.resource_name}"

    logger.info(
        f"Wait for {constants.DRPC}: {drpc_obj.resource_name} to reach {constants.STATUS_RELOCATED} phase"
    )
    relocate_condition = constants.STATUS_RELOCATED
    if discovered_apps:
        relocate_condition = constants.STATUS_RELOCATING
    drpc_obj.wait_for_phase(relocate_condition)

    if multi_ns:
        logger.info("Doing Cleanup Operations")
        do_discovered_apps_cleanup_multi_ns(
            old_primary=old_primary, workload_instance=workload_instance
        )
    else:
        if discovered_apps and workload_instance and not workload_instances_shared:
            logger.info("Doing Cleanup Operations")
            do_discovered_apps_cleanup(
                drpc_name=workload_placement_name,
                old_primary=old_primary,
                workload_namespace=workload_instance.workload_namespace,
                workload_dir=workload_instance.workload_dir,
                vrg_name=workload_instance.discovered_apps_placement_name,
            )
        elif discovered_apps and workload_instance and workload_instances_shared:
            logger.info("Doing Cleanup Operations for relocate operation of Shared VMs")
            for cnv_wl in workload_instances_shared:
                do_discovered_apps_cleanup(
                    drpc_name=workload_placement_name,
                    old_primary=old_primary,
                    workload_namespace=workload_instances_shared[0].workload_namespace,
                    workload_dir=cnv_wl.workload_dir,
                    vrg_name=workload_instances_shared[
                        0
                    ].discovered_apps_placement_name,
                    skip_resource_deletion_verification=True,
                )

    config.switch_ctx(restore_index)


def check_mirroring_status_ok(
    replaying_images=None, cephblockpoolradosns=None, storageclient_uid=None
):
    """
    Check if mirroring status has health OK and expected number of replaying images

    Args:
        replaying_images (int): Expected number of images in replaying state
        cephblockpoolradosns (string): The name of the cephblockpoolradosnamespace
        storageclient_uid(string): The uid of the storageclient in the client cluster where the application is running.
            Applicable for provider - client configuration.

    Returns:
        bool: True if status contains expected health and states values, False otherwise

    Raises:
        NotFoundError: If the configuration is provider mode and the name of the cephblockpoolradosnamespace
            is not obtained
    """
    ocs_version = version.get_semantic_ocs_version_from_config()
    if is_hci_cluster():
        logger.info("Get the cephblockpoolradosnamespace associated with storageclient")
        cephbpradosns = (
            cephblockpoolradosns
            or config.ENV_DATA.get("radosnamespace_name", None)
            or find_cephblockpoolradosnamespace(storageclient_uid=storageclient_uid)
        )

        if not cephbpradosns:
            raise NotFoundError("Couldn't identify the cephblockpoolradosnamespace")

        cbp_obj = ocp.OCP(
            kind=constants.CEPHBLOCKPOOLRADOSNS,
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=cephbpradosns,
        )
    else:
        if ocs_version >= version.VERSION_4_19:
            cephbpradosns = "ocs-storagecluster-cephblockpool-builtin-implicit"
            cbp_obj = ocp.OCP(
                kind=constants.CEPHBLOCKPOOLRADOSNS,
                namespace=config.ENV_DATA["cluster_namespace"],
                resource_name=cephbpradosns,
            )
        else:
            cbp_obj = ocp.OCP(
                kind=constants.CEPHBLOCKPOOL,
                resource_name=constants.DEFAULT_CEPHBLOCKPOOL,
                namespace=config.ENV_DATA["cluster_namespace"],
            )

    mirroring_status = cbp_obj.get().get("status").get("mirroringStatus").get("summary")
    logger.info(f"Mirroring status: {mirroring_status}")
    health_keys = ["daemon_health", "health", "image_health"]
    for key in health_keys:
        expected_value = "OK"
        current_value = mirroring_status.get(key)
        if current_value not in expected_value:
            logger.warning(
                f"Unexpected {key}. Current status is {current_value} but expected {expected_value}"
            )
            return False

    if replaying_images:
        # Replaying images count can be higher due to presence of dummy images
        # This does not apply for clusters with ODF 4.12 and above.
        # See https://bugzilla.redhat.com/show_bug.cgi?id=2132359
        if ocs_version >= version.VERSION_4_12:
            expected_value = [replaying_images]
        else:
            expected_value = range(replaying_images, replaying_images + 3)

        current_value = mirroring_status.get("states").get("replaying")

        if current_value not in expected_value:
            logger.warning(
                f"Unexpected states. Current replaying count is {current_value} but expected {expected_value}"
            )
            # Continue test if the bug https://issues.redhat.com/browse/DFBUGS-1525 is hit
            current_stopped_value = mirroring_status.get("states").get("stopped")

            if current_stopped_value in expected_value:
                logger.warning("Counting 'stopped' value due to the bug DFBUGS-1525")
                return True
            else:
                return False

    return True


def wait_for_mirroring_status_ok(replaying_images=None, timeout=600):
    """
    Wait for mirroring status to reach health OK and expected number of replaying
    images for each of the ODF cluster

    Args:
        replaying_images (int): Expected number of images in replaying state
        timeout (int): time in seconds to wait for mirroring status reach OK

    Returns:
        bool: True if status contains expected health and states values

    Raises:
        TimeoutExpiredError: In case of unexpected mirroring status

    """
    restore_index = config.cur_index
    for cluster in get_non_acm_cluster_config():
        config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
        logger.info(
            f"Validating mirroring status on cluster {cluster.ENV_DATA['cluster_name']}"
        )
        sample = TimeoutSampler(
            timeout=timeout,
            sleep=5,
            func=check_mirroring_status_ok,
            replaying_images=replaying_images,
        )
        if not sample.wait_for_func_status(result=True):
            error_msg = (
                "The mirroring status does not have expected values within the time"
                f" limit on cluster {cluster.ENV_DATA['cluster_name']}"
            )
            logger.error(error_msg)
            raise TimeoutExpiredError(error_msg)

    config.switch_ctx(restore_index)
    return True


@retry(ValueError, tries=10)
def check_mirroring_status_for_custom_pool(
    pool_name, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE, min_replaying=1
):
    """
    Check the health and mirroring status of a custom CephBlockPoolRadosNamespace resource.
    Refer For OCSQE-2294 or RHSTOR-5129 in ODF 4.19 for details

    This function verifies that:
    - At least two such resources exist in the given namespace
    - The specified pool has all health fields set to 'OK'
    - The replaying count in both 'image_states' and 'states' meets the minimum threshold

    Args:
        pool_name (str): Base name of the Ceph block pool (without '-builtin-implicit' suffix) whose
        mirroring status has to be validated.
        namespace (str): Namespace to look for the resource. Default is 'openshift-storage'.
        min_replaying (int): Minimum expected value for replaying count. Default is 1.

    Returns:
        bool: True if all checks pass, otherwise False.

    Raises:
        ValueError: If custom Pool is missing, insufficient Pool count, or summary is not found.
    """
    restore_index = config.cur_index
    managed_clusters = get_non_acm_cluster_config()
    for cluster in managed_clusters:
        index = cluster.MULTICLUSTER["multicluster_index"]
        config.switch_ctx(index)
        logger.info("Checking count of CephBlockPoolRadosNamespace resource")
        custom_pool_name = f"{pool_name}-builtin-implicit"
        ocp = OCP(kind="CephBlockPoolRadosNamespace", namespace=namespace)
        items = ocp.get().get("items", [])

        if len(items) < 2:
            raise ValueError(
                f"Expected at least 2 resources, found {len(items)} in {namespace}"
            )

        for obj in items:
            if obj.get("metadata", {}).get("name") != custom_pool_name:
                continue
            logger.info("Validate if mirroring status summary is present or not")
            summary = obj.get("status", {}).get("mirroringStatus", {}).get("summary")
            if not summary:
                raise ValueError(f"No summary found for {custom_pool_name}")
            logger.info("Validate health")
            for key in ("health", "daemon_health", "group_health", "image_health"):
                val = summary.get(key)
                logger.info(f"{custom_pool_name} - {key}: {val}")
                if val != "OK":
                    logger.error(f"{key} is not OK: {val}")
                    raise ValueError(
                        f"Health check for {key} is not OK: {val} for pool {custom_pool_name}"
                    )

            img = summary.get("image_states", {}).get("replaying", 0)
            state = summary.get("states", {}).get("replaying", 0)
            logger.info(
                f"{custom_pool_name} - replaying counts: image_states={img}, states={state}"
            )

            if img < min_replaying or state < min_replaying:
                logger.error(
                    f"Replaying count too low: image_states={img}, states={state}"
                )
                raise ValueError(
                    f"Replaying count too low: image_states={img}, states={state} for pool {custom_pool_name}"
                )

            return True

        raise ValueError(f"Custom Pool {custom_pool_name} not found in {namespace}")
    config.switch_ctx(restore_index)
    return False


def get_pv_count(namespace):
    """
    Gets PV resource count in the given namespace

    Args:
        namespace (str): the namespace of the workload

    Returns:
         int: PV resource count

    """
    all_pvs = get_all_pvs()["items"]
    workload_pvs = [
        pv
        for pv in all_pvs
        if pv.get("spec").get("claimRef", {}).get("namespace") == namespace
    ]
    return len(workload_pvs)


def get_vr_count(namespace):
    """
    Gets VR resource count in given namespace

    Args:
        namespace (str): the namespace of the VR resources

    Returns:
         int: VR resource count

    """
    vr_obj = ocp.OCP(kind=constants.VOLUME_REPLICATION, namespace=namespace)
    vr_items = vr_obj.get().get("items")
    return len(vr_items)


def get_replicationsources_count(namespace):
    """
    Gets ReplicationSource resource count in given namespace

    Args:
        namespace (str): the namespace of the ReplicationSource resources

    Returns:
         int: ReplicationSource resource count

    """
    rs_obj = ocp.OCP(kind=constants.REPLICATION_SOURCE, namespace=namespace)
    rs_items = rs_obj.get().get("items")
    return len(rs_items)


def get_replicationdestinations_count(namespace):
    """
    Gets ReplicationDestination resource count in given namespace

    Args:
        namespace (str): the namespace of the ReplicationDestination resources

    Returns:
         int: ReplicationDestination resource count

    """
    rd_obj = ocp.OCP(kind=constants.REPLICATIONDESTINATION, namespace=namespace)
    rd_items = rd_obj.get().get("items")
    return len(rd_items)


def check_vr_state(state, namespace):
    """
    Check if all VRs in the given namespace are in expected state

    Args:
        state (str): The VRs state to check for (e.g. 'primary', 'secondary')
        namespace (str): the namespace of the VR resources

    Returns:
        bool: True if all VRs are in expected state or were deleted, False otherwise

    """
    vr_obj = ocp.OCP(kind=constants.VOLUME_REPLICATION, namespace=namespace)
    vr_list = vr_obj.get().get("items")

    # Skip state check if resource was deleted
    if len(vr_list) == 0 and state.lower() == "secondary":
        logger.info("VR resources not found, skipping state check")
        return True

    vr_state_mismatch = []
    for vr in vr_list:
        vr_name = vr["metadata"]["name"]
        desired_state = vr["spec"]["replicationState"]
        current_state = vr["status"]["state"]
        logger.info(
            f"VR: {vr_name} desired state is {desired_state}, current state is {current_state}"
        )

        if not (
            state.lower() == desired_state.lower()
            and state.lower() == current_state.lower()
        ):
            vr_state_mismatch.append(vr_name)

    if not vr_state_mismatch:
        logger.info(f"All {len(vr_list)} VR are in expected state {state}")
        return True
    else:
        logger.warning(
            f"Following {len(vr_state_mismatch)} VR are not in expected state {state}: {vr_state_mismatch}"
        )
        return False


def check_vrg_existence(namespace, vrg_name=""):
    """
    Check if VRG resource exists in the given namespace

    Args:
        namespace (str): the namespace of the VRG resource
        vrg_name (str): Name of VRG

    """
    vrg_list = []
    try:

        vrg_list = (
            ocp.OCP(
                kind=constants.VOLUME_REPLICATION_GROUP,
                namespace=namespace,
                resource_name=vrg_name,
            )
            .get()
            .get("items")
        )
    except Exception as e:
        if (
            f'Error from server (NotFound): volumereplicationgroups.ramendr.openshift.io "{vrg_name}" not found'
            in str(e)
        ):
            logger.info(f"VRG {vrg_name} not found in namespace {namespace}.")
        else:
            logger.warning(
                f"Exception raised when fetching Volume Replication Group: {e}"
            )

    if len(vrg_list) > 0:
        return True
    else:
        return False


def check_vrg_state(state, namespace, resource_name=None):
    """
    Check if VRG in the given namespace is in expected state

    Args:
        state (str): The VRG state to check for (e.g. 'primary', 'secondary')
        namespace (str): the namespace of the VRG resources
        resource_name (str): Name of VRG resource

    Returns:
        bool: True if VRG is in expected state or was deleted, False otherwise

    """

    vrg_obj = ocp.OCP(
        kind=constants.VOLUME_REPLICATION_GROUP,
        namespace=namespace,
    )
    if resource_name:
        vrg_list = vrg_obj.get(resource_name=resource_name)
        vrg_list_index = vrg_list
    else:
        vrg_list = vrg_obj.get().get("items")
        vrg_list_index = vrg_list[0]

    # Skip state check if resource was deleted
    if len(vrg_list) == 0 and state.lower() == "secondary":
        ocs_version = version.get_semantic_ocs_version_from_config()
        if ocs_version <= version.VERSION_4_17:
            logger.info("VRG resource not found, skipping state check")
            return True
        else:
            logger.info("VRG resource not found")
            return False
    vrg_name = vrg_list_index["metadata"]["name"]
    desired_state = vrg_list_index["spec"]["replicationState"]
    current_state = vrg_list_index["status"]["state"]
    logger.info(
        f"VRG: {vrg_name} desired state is {desired_state}, current state is {current_state}"
    )
    if (
        state.lower() == desired_state.lower()
        and state.lower() == current_state.lower()
    ):
        return True
    else:
        logger.warning(f"VRG is not in expected state {state}")
        return False


def wait_for_replication_resources_creation(
    vr_count,
    namespace,
    timeout,
    discovered_apps=False,
    vrg_name=None,
    skip_vrg_check=False,
):
    """
    Wait for replication resources to be created

    Args:
        vr_count (int): Expected number of VR resources or ReplicationSource count
        namespace (str): the namespace of the VR or ReplicationSource resources
        timeout (int): time in seconds to wait for VR or ReplicationSource resources to be created
            or reach expected state
        discovered_apps (bool): If true then deployed workload is discovered_apps
        vrg_name (str): Name of VRG
        skip_vrg_check (bool): If true vrg check will be skipped

    Raises:
        TimeoutExpiredError: In case replication resources not created

    """
    logger.info("Waiting for VRG to be created")
    vrg_namespace = constants.DR_OPS_NAMESAPCE if discovered_apps else namespace
    sample = TimeoutSampler(
        timeout=timeout, sleep=5, func=check_vrg_existence, namespace=vrg_namespace
    )
    if not sample.wait_for_func_status(result=True):
        error_msg = "VRG resource is not created"
        logger.error(error_msg)
        raise TimeoutExpiredError(error_msg)

    # TODO: Improve the parameter for condition
    if "cephfs" in namespace:
        resource_kind = constants.REPLICATION_SOURCE
        count_function = get_replicationsources_count
    else:
        resource_kind = constants.VOLUME_REPLICATION
        count_function = get_vr_count
    if config.MULTICLUSTER["multicluster_mode"] != "metro-dr":
        logger.info(f"Waiting for {vr_count} {resource_kind}s to be created")
        sample = TimeoutSampler(
            timeout=timeout,
            sleep=5,
            func=count_function,
            namespace=namespace,
        )
        sample.wait_for_func_value(vr_count)

        if resource_kind == constants.VOLUME_REPLICATION:
            logger.info(
                f"Waiting for {vr_count} {resource_kind}s to reach primary state"
            )
            sample = TimeoutSampler(
                timeout=timeout,
                sleep=5,
                func=check_vr_state,
                state="primary",
                namespace=namespace,
            )
            if not sample.wait_for_func_status(result=True):
                error_msg = "One or more VR haven't reached expected state primary within the time limit."
                logger.error(error_msg)
                raise TimeoutExpiredError(error_msg)
    if not skip_vrg_check:
        wait_for_vrg_state(
            vrg_state="primary",
            vrg_namespace=vrg_namespace,
            resource_name=vrg_name,
            timeout=timeout,
        )


def wait_for_replication_resources_deletion(
    namespace,
    timeout,
    check_state=True,
    discovered_apps=False,
    vrg_name=None,
    skip_vrg_check=False,
):
    """
    Wait for replication resources to be deleted

    Args:
        namespace (str): the namespace of the resources'
        timeout (int): time in seconds to wait for resources to reach expected
            state or deleted
        check_state (bool): True for checking resources state before deletion, False otherwise
        discovered_apps (bool): If true then deployed workload is discovered_apps
        vrg_name (str): Name of VRG
        skip_vrg_check (bool): If true vrg check will be skipped

    Raises:
        TimeoutExpiredError: In case replication resources not deleted

    """
    vrg_namespace = constants.DR_OPS_NAMESAPCE if discovered_apps else namespace
    # TODO: Improve the parameter for condition
    if "cephfs" in namespace:
        resource_kind = constants.REPLICATION_SOURCE
        count_function = get_replicationsources_count
    else:
        resource_kind = constants.VOLUME_REPLICATION
        count_function = get_vr_count

    if check_state:
        if resource_kind == constants.VOLUME_REPLICATION:
            logger.info("Waiting for all VRs to reach secondary state")
            sample = TimeoutSampler(
                timeout=timeout,
                sleep=5,
                func=check_vr_state,
                state="secondary",
                namespace=namespace,
            )
            if not sample.wait_for_func_status(result=True):
                error_msg = "One or more VR haven't reached expected state secondary within the time limit."
                logger.error(error_msg)
                raise TimeoutExpiredError(error_msg)

        if not skip_vrg_check:
            wait_for_vrg_state(
                vrg_state="secondary",
                vrg_namespace=vrg_namespace,
                resource_name=vrg_name,
                timeout=timeout,
            )

    ocs_version = version.get_semantic_ocs_version_from_config()
    if (
        not check_state
        or (ocs_version <= version.VERSION_4_17 and "cephfs" not in namespace)
        and not skip_vrg_check
    ):
        logger.info("Waiting for VRG to be deleted")
        sample = TimeoutSampler(
            timeout=timeout,
            sleep=5,
            func=check_vrg_existence,
            namespace=vrg_namespace,
            vrg_name=vrg_name,
        )
        if not sample.wait_for_func_status(result=False):
            error_msg = "VRG resource not deleted"
            logger.info(error_msg)
            raise TimeoutExpiredError(error_msg)

    if config.MULTICLUSTER["multicluster_mode"] != "metro-dr" and not skip_vrg_check:
        logger.info(f"Waiting for all {resource_kind} to be deleted")
        sample = TimeoutSampler(
            timeout=timeout,
            sleep=5,
            func=count_function,
            namespace=namespace,
        )
        sample.wait_for_func_value(0)


def wait_for_all_resources_creation(
    pvc_count,
    pod_count,
    namespace,
    timeout=900,
    skip_replication_resources=False,
    discovered_apps=False,
    vrg_name=None,
    skip_vrg_check=False,
):
    """
    Wait for workload and replication resources to be created

    Args:
        pvc_count (int): Expected number of PVCs
        pod_count (int): Expected number of Pods
        namespace (str): the namespace of the workload
        timeout (int): time in seconds to wait for resource creation
        skip_replication_resources (bool): if true vr status wont't be check
        discovered_apps (bool): If true then deployed workload is discovered_apps
        vrg_name (str): Name of VRG
        skip_vrg_check (bool): If true vrg check will be skipped



    """
    logger.info(f"Waiting for {pvc_count} PVCs to reach {constants.STATUS_BOUND} state")
    ocp.OCP(kind=constants.PVC, namespace=namespace).wait_for_resource(
        condition=constants.STATUS_BOUND,
        resource_count=pvc_count,
        timeout=timeout,
        sleep=5,
    )
    logger.info(
        f"Waiting for {pod_count} pods to reach {constants.STATUS_RUNNING} state"
    )
    ocp.OCP(kind=constants.POD, namespace=namespace).wait_for_resource(
        condition=constants.STATUS_RUNNING,
        resource_count=pod_count,
        timeout=timeout,
        sleep=5,
    )
    if not skip_replication_resources:
        wait_for_replication_resources_creation(
            pvc_count, namespace, timeout, discovered_apps, vrg_name, skip_vrg_check
        )


def wait_for_all_resources_deletion(
    namespace,
    timeout=1000,
    discovered_apps=False,
    workload_cleanup=False,
    vrg_name=None,
    skip_vrg_check=False,
):
    """
    Wait for workload and replication resources to be deleted

    Args:
        namespace (str): the namespace of the workload
        timeout (int): time in seconds to wait for resource deletion
        discovered_apps (bool): If true then deployed workload is discovered_apps
        workload_cleanup (bool): Set to True when performing final workload cleanup.
            If True:
            - PVC and PV deletion will always be checked
            - Replication resources state check will be skipped.
        vrg_name (str): Name of VRG
        skip_vrg_check (bool): If true vrg check will be skipped


    """
    logger.info("Waiting for all pods to be deleted")
    all_pods = get_all_pods(namespace=namespace)
    for pod_obj in all_pods:
        if "volsync-rsync-tls-dst" not in pod_obj.name:
            pod_obj.ocp.wait_for_delete(
                resource_name=pod_obj.name, timeout=timeout, sleep=5
            )

    check_state = not workload_cleanup
    wait_for_replication_resources_deletion(
        namespace, timeout, check_state, discovered_apps, vrg_name, skip_vrg_check
    )

    if workload_cleanup or not (
        config.MULTICLUSTER["multicluster_mode"] == "regional-dr"
        and "cephfs" in namespace
    ):
        logger.info("Waiting for all PVCs to be deleted")
        all_pvcs = get_all_pvc_objs(namespace=namespace)

        for pvc_obj in all_pvcs:
            pvc_obj.ocp.wait_for_delete(
                resource_name=pvc_obj.name, timeout=timeout, sleep=5
            )

    if config.MULTICLUSTER["multicluster_mode"] != "metro-dr":
        if workload_cleanup or "cephfs" not in namespace:
            logger.info("Waiting for all PVs to be deleted")
            sample = TimeoutSampler(
                timeout=timeout,
                sleep=5,
                func=get_pv_count,
                namespace=namespace,
            )
            sample.wait_for_func_value(0)


def wait_for_cnv_workload(
    vm_name, namespace, phase=constants.STATUS_RUNNING, timeout=600
):
    """
    Wait for VM to reach a phase

    Args:
        vm_name (str): Name of the VM
        namespace (str): Namespace of the vm workload
        phase (str): Phase of the vm resource to wait for. example: Running, Stopped
        timeout (int): time in seconds to wait for resource deletion

    """
    logger.info(f"Wait for VM: {vm_name} to reach {phase} state")
    vm_obj = ocp.OCP(
        kind=constants.VIRTUAL_MACHINE_INSTANCES,
        resource_name=vm_name,
        namespace=namespace,
    )
    vm_obj._has_phase = True
    vm_obj.wait_for_phase(phase=constants.STATUS_RUNNING, timeout=timeout)


def wait_for_replication_destinations_creation(rep_dest_count, namespace, timeout=900):
    """
    Wait for ReplicationDestination resources to be created

    Args:
        rep_dest_count (int): Expected number of ReplicationDestination resource
        namespace (str): The namespace of the ReplicationDestination resources
        timeout (int): Time in seconds to wait for ReplicationDestination resources to be created

    Raises:
        TimeoutExpiredError: If expected number of ReplicationDestination resources not created

    """

    logger.info(
        f"Waiting for {rep_dest_count} {constants.REPLICATIONDESTINATION} to be created"
    )
    sample = TimeoutSampler(
        timeout=timeout,
        sleep=5,
        func=get_replicationdestinations_count,
        namespace=namespace,
    )
    sample.wait_for_func_value(rep_dest_count)


def wait_for_replication_destinations_deletion(namespace, timeout=900):
    """
    Wait for ReplicationDestination resources to be deleted

    Args:
        namespace (str): The namespace of the ReplicationDestination resources
        timeout (int): Time in seconds to wait for ReplicationDestination resources to be deleted

    Raises:
        TimeoutExpiredError: If expected number of ReplicationDestination resources not deleted

    """

    logger.info(f"Waiting for all {constants.REPLICATIONDESTINATION} to be deleted")
    sample = TimeoutSampler(
        timeout=timeout,
        sleep=5,
        func=get_replicationdestinations_count,
        namespace=namespace,
    )
    sample.wait_for_func_value(0)


def get_backend_volumes_for_pvcs(namespace):
    """
    Gets list of RBD images or CephFS subvolumes associated with the PVCs in the given namespace

    Args:
        namespace (str): The namespace of the PVC resources

    Returns:
        list: List of RBD images or CephFS subvolumes

    """
    backend_volumes = []
    for cluster in get_non_acm_cluster_config():
        config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
        logger.info(f"Fetching backend volume names for PVCs in namespace: {namespace}")
        all_pvcs = get_all_pvc_objs(namespace=namespace)
        for pvc_obj in all_pvcs:
            if pvc_obj.name.startswith("volsync"):
                continue

            if pvc_obj.backed_sc in [
                constants.DEFAULT_STORAGECLASS_RBD,
                constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_RBD,
                constants.DEFAULT_CNV_CEPH_RBD_SC,
            ]:
                backend_volume = pvc_obj.get_rbd_image_name
            elif pvc_obj.backed_sc in [
                constants.DEFAULT_STORAGECLASS_CEPHFS,
                constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_CEPHFS,
            ]:
                backend_volume = pvc_obj.get_cephfs_subvolume_name

            backend_volumes.append(backend_volume)

    backend_volumes = list(set(backend_volumes))
    logger.info(f"Found {len(backend_volumes)} backend volumes: {backend_volumes}")
    return backend_volumes


def verify_backend_volume_deletion(
    backend_volumes,
    cephblockpoolradosns=None,
    cephfssubvolumegroup=None,
    storageclient_uid=None,
):
    """
    Check whether RBD images/CephFS subvolumes are deleted in the backend.

    Args:
        backend_volumes (list): List of RBD images or CephFS subvolumes
        cephblockpoolradosns (str): The name of the cephblockpoolradosnamespace
        cephfssubvolumegroup (str): The name of the cephfilesystemsubvolumegroup
        storageclient_uid(string): The uid of the storageclient in the client cluster where the application is running.
            Applicable for provider - client configuration.

    Returns:
        bool: True if volumes are deleted and False if volumes are not deleted

    Raises:
        NotFoundError: If the configuration is provider mode and the name of the cephblockpoolradosnamespace
            is not obtained
    """
    ocs_version = version.get_semantic_ocs_version_from_config()

    ct_pod = get_ceph_tools_pod()
    rbd_pool_name = (
        (config.ENV_DATA.get("rbd_name") or RBD_NAME)
        if config.DEPLOYMENT["external_mode"]
        else constants.DEFAULT_CEPHBLOCKPOOL
    )

    # TODO: Condition is valid if both is_hci_cluster() and "upgraded cluster from 4.18"
    # Condition is not valid if fresh installed 4.19 and then upgraded
    if is_hci_cluster() and (
        (ocs_version == version.VERSION_4_18)
        or (config.ENV_DATA["cluster_type"] == constants.HCI_CLIENT)
        or is_cluster_y_version_upgraded()
    ):
        cephbpradosns = (
            cephblockpoolradosns
            or config.ENV_DATA.get("radosnamespace_name", None)
            or find_cephblockpoolradosnamespace(storageclient_uid=storageclient_uid)
        )

        if not cephbpradosns:
            raise NotFoundError("Could not identify the cephblockpoolradosnamespace")
        namespace_param = f"--namespace {cephbpradosns}"

        subvolumegroup = (
            config.ENV_DATA.get("subvolumegroup_name", False) or cephfssubvolumegroup
        )
        if not subvolumegroup:
            subvolumegroup = find_cephfilesystemsubvolumegroup(
                storageclient_uid=storageclient_uid
            )

        if not subvolumegroup:
            raise NotFoundError("Couldn't identify the cephfilesystemsubvolumegroup")
    else:
        namespace_param = ""
        subvolumegroup = "csi"

    rbd_images = ct_pod.exec_cmd_on_pod(
        f"rbd ls {rbd_pool_name} {namespace_param} --format json"
    )

    fs_name = ct_pod.exec_ceph_cmd("ceph fs ls")[0]["name"]
    cephfs_cmd_output = ct_pod.exec_cmd_on_pod(
        f"ceph fs subvolume ls {fs_name} --group_name {subvolumegroup}"
    )
    cephfs_subvolumes = [subvolume["name"] for subvolume in cephfs_cmd_output]

    ceph_volumes = rbd_images + cephfs_subvolumes
    logger.info(f"All backend volumes present in the cluster: {ceph_volumes}")
    not_deleted_volumes = []
    for backend_volume in backend_volumes:
        if backend_volume in ceph_volumes:
            not_deleted_volumes.append(backend_volume)
    if not_deleted_volumes:
        logger.info(
            f"The following backend volumes were not deleted: {not_deleted_volumes}"
        )

    return len(not_deleted_volumes) == 0


def wait_for_backend_volume_deletion(backend_volumes, timeout=600):
    """
    Verify that RBD image/CephFS subvolume are deleted in the backend.

    Args:
        backend_volumes (list): List of RBD images or CephFS subvolumes
        timeout (int): time in seconds to wait

    Raises:
        TimeoutExpiredError: In case backend volumes are not deleted
    """
    sample = TimeoutSampler(
        timeout=timeout,
        sleep=10,
        func=verify_backend_volume_deletion,
        backend_volumes=backend_volumes,
    )
    if not sample.wait_for_func_status(result=True):
        error_msg = "Backend RBD images or CephFS subvolumes were not deleted"
        logger.error(error_msg)
        raise TimeoutExpiredError(error_msg)


def get_all_drpolicy():
    """
    Gets all DRPolicy from hub cluster

    Returns:
        list: List of all DRPolicy

    """
    config.switch_acm_ctx()
    drpolicy_obj = ocp.OCP(kind=constants.DRPOLICY)
    drpolicy_list = drpolicy_obj.get(all_namespaces=True).get("items")
    return drpolicy_list


def verify_last_group_sync_time(
    drpc_obj,
    scheduling_interval,
    initial_last_group_sync_time=None,
):
    """
    Verifies that the lastGroupSyncTime for a given DRPC object is within the expected range.

    Args:
        drpc_obj (obj): DRPC object
        scheduling_interval (int): The scheduling interval in minutes
        initial_last_group_sync_time (str): Previous lastGroupSyncTime value (optional).

    Returns:
        str: Current lastGroupSyncTime

    Raises:
        AssertionError: If the lastGroupSyncTime is outside the expected range
            (greater than or equal to three times the scheduling interval)

    """
    restore_index = config.cur_index
    config.switch_acm_ctx()
    if initial_last_group_sync_time:
        for last_group_sync_time in TimeoutSampler(
            (3 * scheduling_interval * 60), 15, drpc_obj.get_last_group_sync_time
        ):
            if last_group_sync_time:
                if last_group_sync_time != initial_last_group_sync_time:
                    logger.info(
                        f"Verified: Current lastGroupSyncTime {last_group_sync_time} is different from "
                        f"previous value {initial_last_group_sync_time}"
                    )
                    break
            logger.info(
                "The value of lastGroupSyncTime in drpc is not updated. Retrying..."
            )
    else:
        last_group_sync_time = drpc_obj.get_last_group_sync_time()

    # Verify lastGroupSyncTime
    time_format = "%Y-%m-%dT%H:%M:%SZ"
    last_group_sync_time_formatted = datetime.strptime(
        last_group_sync_time, time_format
    )
    current_time = datetime.strptime(
        datetime.utcnow().strftime(time_format), time_format
    )
    time_since_last_sync = (
        current_time - last_group_sync_time_formatted
    ).total_seconds() / 60
    logger.info(f"Time in minutes since the last sync {time_since_last_sync}")
    assert (
        time_since_last_sync < 3 * scheduling_interval
    ), "The syncing of volumes is exceeding three times the scheduled snapshot interval"
    logger.info("Verified lastGroupSyncTime value within expected range")
    config.switch_ctx(restore_index)
    return last_group_sync_time


def get_all_drclusters():
    """
    Get all DRClusters

    Returns:
        list: List of all DRClusters
    """
    restore_index = config.cur_index
    config.switch_acm_ctx()
    drclusters_obj = ocp.OCP(kind=constants.DRCLUSTER)
    drclusters = []
    for cluster in drclusters_obj.get().get("items"):
        drclusters.append(cluster.get("metadata").get("name"))
    logger.info(f"The DRClusters are {drclusters}")
    config.switch_ctx(restore_index)
    return drclusters


def get_managed_cluster_node_ips():
    """
    Gets node ips of individual managed clusters for enabling fencing on MDR DRCluster configuration

    Returns:
        cluster (list): Returns list of managed cluster, indexes and their node IPs

    """
    primary_index = get_primary_cluster_config().MULTICLUSTER["multicluster_index"]
    secondary_index = [
        s.MULTICLUSTER["multicluster_index"]
        for s in get_non_acm_cluster_config()
        if s.MULTICLUSTER["multicluster_index"] != primary_index
    ][0]
    cluster_name_primary = config.clusters[primary_index].ENV_DATA["cluster_name"]
    cluster_name_secondary = config.clusters[secondary_index].ENV_DATA["cluster_name"]
    cluster_data = [
        [cluster_name_primary, primary_index],
        [cluster_name_secondary, secondary_index],
    ]
    for cluster in cluster_data:
        config.switch_ctx(cluster[1])
        logger.info(f"Getting node IPs on managed cluster: {cluster[0]}")
        node_obj = ocp.OCP(kind=constants.NODE).get()
        external_ips = []
        for node in node_obj.get("items"):
            addresses = node.get("status").get("addresses")
            for address in addresses:
                if address.get("type") == "ExternalIP":
                    external_ips.append(address.get("address"))
        external_ips_with_cidr = [f"{ip}/32" for ip in external_ips]
        cluster.append(external_ips_with_cidr)
    return cluster_data


def enable_fence(drcluster_name, switch_ctx=None):
    """
    Once the managed cluster is fenced, all communication
    from applications to the ODF external storage cluster will fail

    Args:
        drcluster_name (str): Name of the DRcluster which needs to be fenced
        switch_ctx (int): The cluster index by the cluster name

    """

    logger.info(
        f"Edit the DRCluster resource for {drcluster_name} cluster on the Hub cluster"
    )
    restore_index = config.cur_index
    config.switch_ctx(switch_ctx) if switch_ctx else config.switch_acm_ctx()
    fence_params = f'{{"spec":{{"clusterFence":"{constants.ACTION_FENCE}"}}}}'
    drcluster_obj = ocp.OCP(resource_name=drcluster_name, kind=constants.DRCLUSTER)
    if not drcluster_obj.patch(params=fence_params, format_type="merge"):
        raise CommandFailed(f"Failed to patch {constants.DRCLUSTER}: {drcluster_name}")
    logger.info(f"Successfully fenced {constants.DRCLUSTER}: {drcluster_name}")
    config.switch_ctx(restore_index)


def configure_drcluster_for_fencing():
    """
    Configures DRClusters for enabling fencing

    """
    old_ctx = config.cur_index
    cluster_ip_list = get_managed_cluster_node_ips()
    config.switch_acm_ctx()
    for cluster in cluster_ip_list:
        fence_ip_data = json.dumps({"spec": {"cidrs": cluster[2]}})
        fence_ip_cmd = (
            f"oc patch drcluster {cluster[0]} --type merge -p '{fence_ip_data}'"
        )
        logger.info(f"Patching DRCluster: {cluster[0]} to add node IP addresses")
        run_cmd(fence_ip_cmd)

        fence_annotation_data = """{"metadata": {"annotations": {
        "drcluster.ramendr.openshift.io/storage-clusterid": "openshift-storage",
        "drcluster.ramendr.openshift.io/storage-driver": "openshift-storage.rbd.csi.ceph.com",
        "drcluster.ramendr.openshift.io/storage-secret-name": "rook-csi-rbd-provisioner",
        "drcluster.ramendr.openshift.io/storage-secret-namespace": "openshift-storage" } } }"""
        fencing_annotation_cmd = (
            f"oc patch drcluster {cluster[0]} --type merge -p '{fence_annotation_data}'"
        )
        logger.info(f"Patching DRCluster: {cluster[0]} to add fencing annotations")
        run_cmd(fencing_annotation_cmd)

    config.switch_ctx(old_ctx)


def enable_unfence(drcluster_name, switch_ctx=None):
    """
    The OpenShift cluster to be Unfenced is the one where applications
    are not currently running and the cluster that was Fenced earlier.

    Args:
        drcluster_name (str): Name of the DRcluster which needs to be fenced
        switch_ctx (int): The cluster index by the cluster name

    """

    logger.info(
        f"Edit the DRCluster resource for {drcluster_name} cluster on the Hub cluster"
    )
    restore_index = config.cur_index
    config.switch_ctx(switch_ctx) if switch_ctx else config.switch_acm_ctx()
    unfence_params = f'{{"spec":{{"clusterFence":"{constants.ACTION_UNFENCE}"}}}}'
    drcluster_obj = ocp.OCP(resource_name=drcluster_name, kind=constants.DRCLUSTER)
    if not drcluster_obj.patch(params=unfence_params, format_type="merge"):
        raise CommandFailed(f"Failed to patch {constants.DRCLUSTER}: {drcluster_name}")
    logger.info(f"Successfully unfenced {constants.DRCLUSTER}: {drcluster_name}")
    config.switch_ctx(restore_index)


def fence_state(drcluster_name, fence_state, switch_ctx=None):
    """
    Sets the specified clusterFence state

    Args:
       drcluster_name (str): Name of the DRcluster which needs to be fenced
       fence_state (str): Specify the clusterfence state either constants.ACTION_UNFENCE and ACTION_FENCE
       switch_ctx (int): The cluster index by the cluster name

    """

    logger.info(
        f"Edit the DRCluster {drcluster_name} cluster clusterfence state {fence_state}  "
    )
    restore_index = config.cur_index
    config.switch_ctx(switch_ctx) if switch_ctx else config.switch_acm_ctx()
    params = f'{{"spec":{{"clusterFence":"{fence_state}"}}}}'
    drcluster_obj = ocp.OCP(resource_name=drcluster_name, kind=constants.DRCLUSTER)
    if not drcluster_obj.patch(params=params, format_type="merge"):
        raise CommandFailed(f"Failed to patch {constants.DRCLUSTER}: {drcluster_name}")
    logger.info(
        f"Successfully changed clusterfence state to {fence_state} {constants.DRCLUSTER}: {drcluster_name}"
    )
    config.switch_ctx(restore_index)


def get_fence_state(drcluster_name, switch_ctx=None):
    """
    Returns the clusterfence state of given drcluster

    Args:
        drcluster_name (str): Name of the DRcluster
        switch_ctx (int): The cluster index by the cluster name

    Returns:
        state (str): If drcluster are fenced: Fenced or Unfenced, else None if not defined

    """
    restore_index = config.cur_index
    config.switch_ctx(switch_ctx) if switch_ctx else config.switch_acm_ctx()
    drcluster_obj = ocp.OCP(resource_name=drcluster_name, kind=constants.DRCLUSTER)
    state = drcluster_obj.get().get("status").get("phase")
    config.switch_ctx(restore_index)
    return state


@retry(UnexpectedBehaviour, tries=25, delay=5, backoff=5)
def verify_fence_state(drcluster_name, state, switch_ctx=None):
    """
    Verify the specified drcluster is in expected state

    Args:
        drcluster_name (str): Name of the DRcluster
        state (str): The fence state it is either constants.ACTION_FENCE or constants.ACTION_UNFENCE
        switch_ctx (int): The cluster index by the cluster name

    Raises:
        Raises exception Unexpected-behaviour if the specified drcluster is not in the given state condition
    """
    sample = get_fence_state(drcluster_name=drcluster_name, switch_ctx=switch_ctx)
    if sample == state:
        logger.info(f"Primary managed cluster {drcluster_name} reached {state} state")
    else:
        logger.error(
            f"Primary managed cluster {drcluster_name} not reached {state} state"
        )
        raise UnexpectedBehaviour(
            f"Primary managed cluster {drcluster_name} not reached {state} state"
        )


def create_backup_schedule():
    """
    Create backupschedule resource only on active hub

    """
    old_ctx = config.cur_index
    config.switch_ctx(get_active_acm_index())
    backup_schedule = templating.load_yaml(constants.BACKUP_SCHEDULE_YAML)
    backup_schedule_yaml = tempfile.NamedTemporaryFile(
        mode="w+", prefix="bkp", delete=False
    )
    templating.dump_data_to_temp_yaml(backup_schedule, backup_schedule_yaml.name)
    run_cmd(f"oc create -f {backup_schedule_yaml.name}")
    config.switch_ctx(old_ctx)


def gracefully_reboot_ocp_nodes(drcluster_name, disable_eviction=False):
    """
    Gracefully reboot OpenShift Container Platform
    nodes which was fenced before

    Args:
        drcluster_name (str): Name of the drcluster which needs to be rebooted
        disable_eviction (bool): On True will delete pod that is protected by PDB, False by default

    """
    config.switch_to_cluster_by_name(drcluster_name)
    gracefully_reboot_nodes(disable_eviction=disable_eviction)


def restore_backup():
    """
    Restores the backup in new hub and make it as active

    """

    restore_index = config.cur_index
    config.switch_ctx(get_passive_acm_index())
    restore_schedule = templating.load_yaml(constants.DR_RESTORE_YAML)
    restore_schedule["metadata"]["name"] = create_unique_resource_name(
        resource_description="acm", resource_type="restore"
    )
    restore_schedule_yaml = tempfile.NamedTemporaryFile(
        mode="w+", prefix="restore", delete=False
    )
    templating.dump_data_to_temp_yaml(restore_schedule, restore_schedule_yaml.name)
    run_cmd(f"oc create -f {restore_schedule_yaml.name}")
    config.switch_ctx(restore_index)


@retry(UnexpectedBehaviour, tries=25, delay=5, backoff=5)
def verify_restore_is_completed():
    """
    Function to verify restore is completed or finished

    """
    restore_index = config.cur_index
    config.switch_ctx(get_passive_acm_index())
    restore_obj = ocp.OCP(
        kind=constants.ACM_HUB_RESTORE, namespace=constants.ACM_HUB_BACKUP_NAMESPACE
    )
    cmd_output = restore_obj.exec_oc_cmd(command="get restore -oyaml")
    status = cmd_output["items"][0]["status"]["phase"]
    if status == "Finished":
        logger.info("Restore completed successfully")
    else:
        logger.error(f"Restore failed with some errors: {cmd_output}")
        raise UnexpectedBehaviour("Restore failed with some errors")
    config.switch_ctx(restore_index)


@retry(UnexpectedBehaviour, tries=25, delay=5, backoff=2)
def verify_drpolicy_cli(switch_ctx=None):
    """
    Function to verify DRPolicy status

    Returns:
        bool: True if the status is in succeed state, else raise exception
        switch_ctx (int): The cluster index by the cluster name

    """

    restore_index = config.cur_index
    config.switch_ctx(switch_ctx) if switch_ctx else config.switch_acm_ctx()
    drpolicy_obj = ocp.OCP(kind=constants.DRPOLICY)
    status = drpolicy_obj.get().get("items")[0].get("status").get("conditions")[0]
    if status.get("reason") == "Succeeded":
        logger.info("DRPolicy validation succeeded")
        config.switch_ctx(restore_index)
        return True
    else:
        logger.warning(f"DRPolicy is not in succeeded or validated state: {status}")
        config.switch_ctx(restore_index)
        raise UnexpectedBehaviour(
            f"DRPolicy is not in succeeded or validated state: {status}"
        )


@retry(UnexpectedBehaviour, tries=25, delay=5, backoff=5)
def verify_backup_is_taken():
    """
    Function to verify backup is taken

    """
    backup_index = config.cur_index
    config.switch_ctx(get_active_acm_index())
    backup_obj = ocp.OCP(
        kind=constants.ACM_BACKUP_SCHEDULE, namespace=constants.ACM_HUB_BACKUP_NAMESPACE
    )
    cmd_output = backup_obj.exec_oc_cmd(command="get BackupSchedule -oyaml")
    status = cmd_output["items"][0]["status"]["phase"]
    if status == "Enabled":
        logger.info("Backup enabled successfully")
    else:
        logger.error(f"Backup failed with some errors: {cmd_output}")
        raise UnexpectedBehaviour("Backup failed with some errors")
    config.switch_ctx(backup_index)


def get_nodes_from_active_zone(namespace):
    """
    Get the nodes list and index from active zone

    Args:
        namespace (str): Namespace of the app workload

    Returns:
        tuple: contains index and the node_objs list of the cluster
            active_hub_index (int): Index of the active hub cluster
            active_hub_cluster_node_objs (list): Node list of the active hub nodes
            managed_cluster_index (int): Index of the active zone managed cluster
            managed_cluster_node_objs (list): Node list of the active zone managed cluster
            ceph_node_ips (list): Ceph node list which are running in active zone

    """

    # Get nodes from zone where active hub running
    config.switch_ctx(get_active_acm_index())
    active_hub_index = config.cur_index
    zone = config.ENV_DATA.get("zone")
    active_hub_cluster_node_objs = get_node_objs()
    set_current_primary_cluster_context(namespace)
    if config.ENV_DATA.get("zone") == zone:
        managed_cluster_index = config.cur_index
        managed_cluster_node_objs = get_node_objs()
    else:
        set_current_secondary_cluster_context(namespace)
        managed_cluster_index = config.cur_index
        managed_cluster_node_objs = get_node_objs()
    external_cluster_node_roles = config.EXTERNAL_MODE.get(
        "external_cluster_node_roles"
    )
    zone = "zone-b" if zone == "b" else "zone-c"
    ceph_node_ips = []
    for ceph_node in external_cluster_node_roles:
        if (
            external_cluster_node_roles[ceph_node].get("location").get("datacenter")
            != zone
        ):
            continue
        else:
            ceph_node_ips.append(
                external_cluster_node_roles[ceph_node].get("ip_address")
            )

    return (
        active_hub_index,
        active_hub_cluster_node_objs,
        managed_cluster_index,
        managed_cluster_node_objs,
        ceph_node_ips,
    )


def create_klusterlet_config():
    """
    Create klusterletconfig after hub recovery to avoid eviction
    of resources by adding "AppliedManifestWork" eviction grace period

    """
    old_ctx = config.cur_index
    config.switch_ctx(get_passive_acm_index())
    klusterlet_config = templating.load_yaml(constants.KLUSTERLET_CONFIG_YAML)
    klusterlet_config_yaml = tempfile.NamedTemporaryFile(
        mode="w+", prefix="klusterlet_config", delete=False
    )
    templating.dump_data_to_temp_yaml(klusterlet_config, klusterlet_config_yaml.name)
    run_cmd(f"oc create -f {klusterlet_config_yaml.name}")
    logger.info("Klusterletconfig is successfully created on the passive hub")
    config.switch_ctx(old_ctx)


def remove_parameter_klusterlet_config():
    """
    Edit the global KlusterletConfig on the new hub and
    remove the parameter appliedManifestWorkEvictionGracePeriod and its value

    """
    old_ctx = config.cur_index
    config.switch_ctx(get_passive_acm_index())
    klusterlet_config_obj = ocp.OCP(kind=constants.KLUSTERLET_CONFIG)
    name = klusterlet_config_obj.get().get("items")[0].get("metadata").get("name")
    remove_op = [{"op": "remove", "path": "/spec"}]
    klusterlet_config_obj.patch(
        resource_name=name, params=json.dumps(remove_op), format_type="json"
    )
    logger.info(
        "appliedManifestWorkEvictionGracePeriod and it's value is successfully removed from KlusterletConfig"
    )
    config.switch_ctx(old_ctx)


def add_label_to_appsub(workloads, label="test", value="test1"):
    """
    Function to add new label with any value to the AppSub on the hub.
    This is needed as WA for sub app pods to show up after failover in ACM 2.11 post hub recovery (bz: 2295782)

    Args:
        workloads (list): List of workloads created
        label (str): Name of label to be added
        value (str): Value to be added

    """
    old_ctx = config.cur_index
    config.switch_ctx(get_passive_acm_index())
    for wl in workloads:
        if wl.workload_type == constants.SUBSCRIPTION:
            sub_obj = ocp.OCP(
                kind=constants.SUBSCRIPTION, namespace=wl.workload_namespace
            )
            name = sub_obj.get().get("items")[0].get("metadata").get("name")
            run_cmd(
                f"oc label appsub -n {wl.workload_namespace} {name} {label}={value}"
            )
    config.switch_ctx(old_ctx)


def disable_dr_from_app(secondary_cluster_name):
    """
    Function to disable DR from app

    Args:
        secondary_cluster_name(str): cluster where application is running

    """
    old_ctx = config.cur_index
    config.switch_acm_ctx()

    # get all placement and replace value with surviving cluster
    placement_obj = ocp.OCP(kind=constants.PLACEMENT)
    placements = placement_obj.get(all_namespaces=True).get("items")
    for placement in placements:
        name = placement["metadata"]["name"]
        if (name != "all-openshift-clusters") and (name != "global"):
            namespace = placement["metadata"]["namespace"]
            params = (
                f"""[{{"op": "replace", "path": "{constants.CLUSTERSELECTORPATH}","""
                f""""value": "{secondary_cluster_name}"}}]"""
            )
            cmd = f"oc patch placement {name} -n {namespace}  -p '{params}' --type=json"
            run_cmd(cmd)

    # Delete all drpc
    run_cmd("oc delete drpc --all -A")
    sample = TimeoutSampler(
        timeout=300,
        sleep=5,
        func=run_cmd_verify_cli_output,
        cmd="oc get drpc -A",
        expected_output_lst="No resources found",
    )
    if not sample.wait_for_func_status(result=False):
        raise Exception("All drpcs are not deleted")

    time.sleep(10)

    # Remove annotation from placements
    for placement in placements:
        name = placement["metadata"]["name"]
        if (name != "all-openshift-clusters") and (name != "global"):
            namespace = placement["metadata"]["namespace"]
            params = f"""[{{"op": "remove", "path": "{constants.EXPERIMENTAL_ANNOTATION_PATH}"}}]"""
            cmd = f"oc patch {constants.PLACEMENT} {name} -n {namespace} -p '{params}' --type=json"
            run_cmd(cmd)

    config.switch_ctx(old_ctx)


def apply_drpolicy_to_workload(workload, drcluster_name):
    """
    Function for applying drpolicy to indiviusual workload

    Args:
        workload(List): List of workload objects
        drcluster_name(str): Name of the DRcluster on which workloads belongs

    """
    for wl in workload:
        drpc_yaml_data = templating.load_yaml(wl.drcp_data_yaml.name)
        logger.info(drpc_yaml_data)
        if wl.workload_type == constants.SUBSCRIPTION:
            drpc_yaml_data["metadata"]["namespace"] = wl.workload_namespace
        drpc_yaml_data["spec"]["preferredCluster"] = drcluster_name
        templating.dump_data_to_temp_yaml(drpc_yaml_data, wl.drcp_data_yaml.name)
        config.switch_acm_ctx()
        wl.add_annotation_to_placement()
        run_cmd(f"oc create -f {wl.drcp_data_yaml.name}")


def replace_cluster(workload, primary_cluster_name, secondary_cluster_name):
    """
    Function to do core replace cluster task

    Args:
        workload(List): List of workload objects
        primary_cluster_name (str): Name of the primary DRcluster
        secondary_cluster_name(str): Name of the secondary DRcluster

    """

    # Delete dr cluster
    config.switch_acm_ctx()
    run_cmd(cmd=f"oc delete drcluster {primary_cluster_name} --wait=false")

    # Disable DR on hub for each app
    disable_dr_from_app(secondary_cluster_name)
    logger.info("DR configuration is successfully disabled on each app")

    # Remove DR configuration from hub and surviving cluster
    logger.info("Running Remove DR configuration script..")
    run_cmd(cmd=f"chmod +x {constants.REMOVE_DR_EACH_MANAGED_CLUSTER}")
    run_cmd(cmd=f"sh {constants.REMOVE_DR_EACH_MANAGED_CLUSTER}")

    sample = TimeoutSampler(
        timeout=300,
        sleep=5,
        func=run_cmd_verify_cli_output,
        cmd="oc get namespace openshift-operators",
        expected_output_lst={"openshift-operators", "Active"},
    )
    if not sample.wait_for_func_status(result=True):
        raise Exception("Namespace openshift-operators is not created")

    # add label to openshift-opeartors namespace
    ocp_obj = ocp.OCP(kind="Namespace")
    label = "openshift.io/cluster-monitoring='true'"
    ocp_obj.add_label(resource_name=constants.OPENSHIFT_OPERATORS, label=label)

    # Detach old primary
    run_cmd(cmd=f"oc delete managedcluster {primary_cluster_name}")

    # Verify old primary cluster is dettached
    expected_output = primary_cluster_name
    out = run_cmd(cmd="oc get managedcluster")
    if expected_output in out:
        raise Exception("Old primary cluster is not dettached.")
    else:
        logger.info("Old primary cluster is dettached")

    # Import Recovery cluster
    from ocs_ci.ocs.acm.acm import (
        import_recovery_clusters_with_acm,
        validate_cluster_import,
    )

    cluster_name_recoevry = import_recovery_clusters_with_acm()

    # Verify recovery cluster is imported
    validate_cluster_import(cluster_name_recoevry)

    # Set recovery cluster as primary context wise
    set_recovery_as_primary()

    config.switch_acm_ctx()

    # Install MCO on active hub again
    from ocs_ci.deployment.deployment import MultiClusterDROperatorsDeploy

    dr_conf = dict()
    dep_mco = MultiClusterDROperatorsDeploy(dr_conf)
    dep_mco.deploy()
    # Enable MCO console plugin
    enable_mco_console_plugin()
    config.switch_acm_ctx()

    # Configure mirror peer
    dep_mco.configure_mirror_peer()

    # Create DR policy
    dep_mco.deploy_dr_policy()

    # Validate drpolicy
    verify_drpolicy_cli(switch_ctx=get_active_acm_index())

    # Apply dr policy on all app on secondary cluster
    apply_drpolicy_to_workload(workload, secondary_cluster_name)

    # Configure DRClusters for fencing automation
    configure_drcluster_for_fencing()


def do_discovered_apps_cleanup(
    drpc_name,
    old_primary,
    workload_namespace,
    workload_dir,
    vrg_name,
    skip_resource_deletion_verification=False,
    ignore_resource_not_found=False,
):
    """
    Function to clean up Resources

    Args:
        drpc_name (str): Name of DRPC
        old_primary (str): Name of old primary where cleanup will happen
        workload_namespace (str): Workload namespace
        workload_dir (str): Dir location of workload
        vrg_name (str): Name of VRG
        skip_resource_deletion_verification (bool): False by default and runs always, else resource verification is
                                                    handled separately in the test when Shared protection type is used
                                                    for DR protection via ACM UI

        ignore_resource_not_found (bool): False by default, resource not found is ignored when the workload which was
                                        DR protected via ACM UI is deleted, refer DFBUGS-3706

    """
    restore_index = config.cur_index
    config.switch_acm_ctx()
    drpc_obj = DRPC(namespace=constants.DR_OPS_NAMESAPCE, resource_name=drpc_name)
    drpc_obj.wait_for_progression_status(status=constants.STATUS_WAITFORUSERTOCLEANUP)
    time.sleep(90)
    assert drpc_obj.get_progression_status(
        status_to_check=constants.STATUS_WAITFORUSERTOCLEANUP
    )
    logger.info(
        f'Progression status after 90 seconds is {drpc_obj.get()["status"]["progression"]}'
    )
    config.switch_to_cluster_by_name(old_primary)
    workload_path = constants.DR_WORKLOAD_REPO_BASE_DIR + "/" + workload_dir
    if not ignore_resource_not_found:

        # --ignore-not-found is needed to avoid https://issues.redhat.com/browse/DFBUGS-3706
        logger.info("Using '--ignore-not-found' during workload deletion")
        run_cmd(
            f"oc delete -k {workload_path} -n {workload_namespace} --wait=false --ignore-not-found --force "
        )
    else:
        run_cmd(
            f"oc delete -k {workload_path} -n {workload_namespace} --wait=false --force "
        )
    if not skip_resource_deletion_verification:
        wait_for_all_resources_deletion(
            namespace=workload_namespace, discovered_apps=True, vrg_name=vrg_name
        )
        config.switch_acm_ctx()
        drpc_obj.wait_for_progression_status(status=constants.STATUS_COMPLETED)
    config.switch_ctx(restore_index)


def do_discovered_apps_cleanup_multi_ns(
    old_primary, workload_instance, vrg_state="secondary"
):
    """
    Function to clean up Resources

    Args:
        old_primary (str): Name of old primary where cleanup will happen
        workload_instance (list): Workload instance

    """
    restore_index = config.cur_index
    config.switch_acm_ctx()

    drpc_obj = DRPC(
        namespace=constants.DR_OPS_NAMESAPCE,
        resource_name=workload_instance[0].discovered_apps_placement_name,
    )
    drpc_obj.wait_for_progression_status(status=constants.STATUS_WAITFORUSERTOCLEANUP)
    config.switch_to_cluster_by_name(old_primary)
    vrg_name = workload_instance[0].discovered_apps_placement_name
    for workload_instance_index in workload_instance:
        workload_dir = workload_instance_index.workload_dir
        workload_namespace = workload_instance_index.workload_namespace
        workload_path = constants.DR_WORKLOAD_REPO_BASE_DIR + "/" + workload_dir
        run_cmd(
            f"oc delete -k {workload_path} -n {workload_namespace} --wait=false --force "
        )
    for workload_instance_index in workload_instance:
        workload_namespace = workload_instance_index.workload_namespace
        wait_for_all_resources_deletion(
            namespace=workload_namespace,
            discovered_apps=True,
            vrg_name=vrg_name,
            skip_vrg_check=True,
        )

    wait_for_vrg_state(
        vrg_state=vrg_state,
        vrg_namespace=constants.DR_OPS_NAMESAPCE,
        resource_name=vrg_name,
    )
    config.switch_acm_ctx()
    drpc_obj.wait_for_progression_status(status=constants.STATUS_COMPLETED)
    config.switch_ctx(restore_index)


def generate_kubeobject_capture_interval():
    """
    Generate KubeObject Capture Interval

    Returns:
        int: capture interval value to be used

    """
    capture_interval = int(get_all_drpolicy()[0]["spec"]["schedulingInterval"][:-1])

    if capture_interval <= 5 and capture_interval != 1:
        return capture_interval - 1
    elif capture_interval > 6:
        return 5
    else:
        return capture_interval


def disable_dr_rdr(discovered_apps=False):
    """
    Disable DR for the applications
    """
    config.switch_acm_ctx()

    # Delete the placement under openshift-dr-ops namespace
    if discovered_apps:
        run_cmd(f"oc delete drpc --all -n {constants.DR_OPS_NAMESAPCE}")
        run_cmd(f"oc delete placement --all -n {constants.DR_OPS_NAMESAPCE}")
    sample = TimeoutSampler(
        timeout=300,
        sleep=5,
        func=verify_drpc_placement_deletion,
        cmd=f"oc get placement -n '{constants.DR_OPS_NAMESAPCE}'",
        expected_output_lst="No resources found",
    )
    if not sample.wait_for_func_status(result=True):
        raise Exception("All placements are not deleted")

    # Edit drpc to add annotation
    drpc_obj = ocp.OCP(kind=constants.DRPC)
    drpcs = drpc_obj.get(all_namespaces=True).get("items")
    for drpc in drpcs:
        namespace = drpc["metadata"]["namespace"]
        name = drpc["metadata"]["name"]
        logger.info(f"Adding annotation to drpc - {name}")
        annotation_data = (
            '{"metadata": {"annotations": {'
            '"drplacementcontrol.ramendr.openshift.io/do-not-delete-pvc": "true"}}}'
        )
        cmd = f"oc patch {constants.DRPC} {name} -n {namespace} --type=merge -p '{annotation_data}'"
        run_cmd(cmd)

    # Delete all drpc
    logger.info("Deleting the drpc...")
    run_cmd("oc delete drpc --all -A")
    sample = TimeoutSampler(
        timeout=300,
        sleep=5,
        func=verify_drpc_placement_deletion,
        cmd="oc get drpc -A",
        expected_output_lst="No resources found",
    )
    if not sample.wait_for_func_status(result=True):
        raise Exception("All drpcs are not deleted")


@retry(CommandFailed, tries=10, delay=30, backoff=1)
def verify_drpc_placement_deletion(cmd, expected_output_lst):
    """
    Function to validate drpc deletion

    Args:
        cmd(str): cli command
        expected_output_lst(set): A set of strings that need to be included in the command output.

    Returns:
        bool: True, if all strings are included in the command output, False otherwise.

    """
    drpc_out = exec_cmd(cmd)
    for expected_output in expected_output_lst:
        if expected_output not in drpc_out.stderr.decode():
            return False
    return True


def verify_last_kubeobject_protection_time(drpc_obj, kubeobject_sync_interval):
    """
    Verifies that the lastKubeObjectProtectionTime for a given DRPC object is within the expected range.

    Args:
        drpc_obj (obj): DRPC object
        kubeobject_sync_interval (int): The KubeObject sync interval in minutes

    Returns:
        str: Current lastKubeObjectProtectionTime

    Raises:
        AssertionError: If the lastKubeObjectProtectionTime is outside the expected range
            (greater than or equal to two times the scheduling interval)

    """
    restore_index = config.cur_index
    config.switch_acm_ctx()
    last_kubeobject_protection_time = drpc_obj.get_last_kubeobject_protection_time()
    if not last_kubeobject_protection_time:
        assert last_kubeobject_protection_time, (
            "There is no lastKubeObjectProtectionTime. "
            "Verify that certificates are included correctly in the Ramen Hub configuration map."
        )
    # Verify lastGroupSyncTime
    time_format = "%Y-%m-%dT%H:%M:%SZ"
    last_kubeobject_protection_time_formatted = datetime.strptime(
        last_kubeobject_protection_time, time_format
    )
    current_time = datetime.strptime(
        datetime.utcnow().strftime(time_format), time_format
    )
    time_since_last_sync = (
        current_time - last_kubeobject_protection_time_formatted
    ).total_seconds() / 60
    logger.info(
        f"Time in minutes since the last Kube Object sync {time_since_last_sync}"
    )
    assert (
        time_since_last_sync < 2 * kubeobject_sync_interval
    ), "The syncing of Kube Resources is exceeding three times the Kube object sync interval"
    logger.info("Verified lastKubeObjectProtectionTime value within expected range")
    config.switch_ctx(restore_index)
    return last_kubeobject_protection_time


def configure_rdr_hub_recovery():
    """
    RDR helper function to create backup schedule on the active hub cluster needed for hub recovery
    using backup and restore.

    This function ensures all pre-reqs are verified before hub recovery is performed.

    """
    # Create backup-schedule on active hub
    config.switch_acm_ctx()
    logger.info("Create backup schedule on the active hub cluster")
    create_backup_schedule()
    wait_time = 420
    logger.info(f"Wait {wait_time} seconds until backup is taken ")
    time.sleep(wait_time)
    logger.info(
        "Check pre-reqs on both the hub clusters before performing hub recovery"
    )
    acm_indexes = get_all_acm_indexes()
    for _ in acm_indexes:
        config.switch_ctx(_)
        verify_backup_is_taken()
        # To avoid circular import
        from ocs_ci.deployment.deployment import (
            Deployment,
            get_multicluster_dr_deployment,
        )

        dr_conf = Deployment().get_rdr_conf()
        rdrclass_obj = get_multicluster_dr_deployment()(dr_conf)
        rdrclass_obj.validate_dpa()
        assert rdrclass_obj.validate_policy_compliance_status(
            resource_name="backup-restore-enabled",
            resource_namespace="open-cluster-management-backup",
            compliance_state="Compliant",
        )
    config.switch_ctx(get_passive_acm_index())
    logger.info(
        "Add label for cluster-monitoring needed to fire VolumeSyncronizationDelay alert on the Hub cluster"
    )
    exec_cmd(
        "oc label namespace openshift-operators openshift.io/cluster-monitoring='true'"
    )
    logger.info("All pre-reqs verified for performing hub recovery")
    return True


def get_cluster_set_name():
    """
    Get Cluster set name from managedcluster

    Returns:
        list: List of uniq cluster set name
    """
    cluster_set = []
    restore_index = config.cur_index
    config.switch_acm_ctx()
    managed_clusters = ocp.OCP(kind=constants.ACM_MANAGEDCLUSTER).get().get("items", [])
    current_managed_clusters_list = [
        cluster_name.ENV_DATA.get("cluster_name") for cluster_name in config.clusters
    ]
    # ignore local-cluster here
    for i in managed_clusters:
        if (
            i["metadata"]["name"] != constants.ACM_LOCAL_CLUSTER
            and i["metadata"]["name"] in current_managed_clusters_list
        ):
            cluster_set.append(i["metadata"]["labels"][constants.ACM_CLUSTERSET_LABEL])
    if all(x == cluster_set[0] for x in cluster_set):
        logger.info(f"Found the unique clusterset {cluster_set[0]}")
    else:
        raise UnexpectedDeploymentConfiguration(
            "There are more then one clusterset added to the managed clusters"
        )

    config.switch_ctx(restore_index)
    return cluster_set


def wait_for_vrg_state(
    vrg_state, vrg_namespace, resource_name, timeout=900, sleep_timeout=5
):
    """
    Wait for VRG state

    Args:
        vrg_state (str): VRG expected state
        vrg_namespace (str): VRG resource namespace
        resource_name (str): VRG resource name
        timeout (int): Timeout for wait
        sleep_timeout (int) Sleep between timeouts

    """
    logger.info(f"Waiting for VRG to reach {vrg_state} state")
    sample = TimeoutSampler(
        timeout=timeout,
        sleep=sleep_timeout,
        func=check_vrg_state,
        state=vrg_state,
        namespace=vrg_namespace,
        resource_name=resource_name,
    )
    if not sample.wait_for_func_status(result=True):
        error_msg = (
            f"VRG hasn't reached expected state {vrg_state} within the time limit."
        )
        logger.info(error_msg)
        raise TimeoutExpiredError(error_msg)


def validate_storage_cluster_peer_state():
    """
    Validate Storage cluster peer state

    Raises:
        TimeoutExpiredError: incase storage cluster peer state is not reached 'Peered' state.

    """
    restore_index = config.cur_index
    managed_clusters = get_non_acm_cluster_config()
    for cluster in managed_clusters:
        index = cluster.MULTICLUSTER["multicluster_index"]
        config.switch_ctx(index)
        logger.info("Validating Storage Cluster Peer status")
        sample = TimeoutSampler(
            timeout=300,
            sleep=5,
            func=check_storage_cluster_peer_state,
        )
        if not sample.wait_for_func_status(result=True):
            error_msg = (
                "Storage cluster peer status does not have expected values within the time "
                f"limit on cluster {cluster.ENV_DATA['cluster_name']}"
            )
            logger.error(error_msg)
            raise TimeoutExpiredError(error_msg)
    config.switch_ctx(restore_index)


def check_storage_cluster_peer_state():
    """
    Checks Storage cluster peer state

    Returns:
        bool: True if storage cluster peer state is 'Peered'. otherwise False

    """
    storage_cluster_peer = ocp.OCP(
        kind=constants.STORAGECLUSTERPEER,
        namespace=config.ENV_DATA["cluster_namespace"],
    )
    storage_cluster_peer_data = storage_cluster_peer.get()
    storage_cluster_peer_status = storage_cluster_peer_data["items"][0]["status"].get(
        "state"
    )
    if storage_cluster_peer_status == constants.STATUS_PEERED:
        return True
    else:
        logger.warning(f"storage cluster peer state is {storage_cluster_peer_status}")
        return False


def create_service_exporter():
    """
    Create Service exporter
    """
    restore_index = config.cur_index
    managed_clusters = get_non_acm_cluster_config()
    for cluster in managed_clusters:
        index = cluster.MULTICLUSTER["multicluster_index"]
        config.switch_ctx(index)
        logger.info("Creating Service exporter")
        run_cmd(f"oc create -f {constants.DR_SERVICE_EXPORTER}")
    config.switch_ctx(restore_index)


def verify_volsync():
    """
    Verify volsync pod is created in volsync-system namespace
    """
    restore_index = config.cur_index
    managed_clusters = get_non_acm_cluster_config()
    for cluster in managed_clusters:
        index = cluster.MULTICLUSTER["multicluster_index"]
        config.switch_ctx(index)
        logger.info(
            f"Verifying volsync pod in namespace {constants.VOLSYNC_SYSTEM_NAMESPACE}"
        )
        pod = ocp.OCP(kind=constants.POD, namespace=constants.VOLSYNC_SYSTEM_NAMESPACE)
        assert pod.wait_for_resource(
            condition="Running",
            selector=constants.VOLSYNC_LABEL,
            resource_count=1,
            timeout=600,
        )
    config.switch_ctx(restore_index)
