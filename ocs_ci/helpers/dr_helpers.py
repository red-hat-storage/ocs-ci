"""
Helper functions specific for DR
"""
import json
import logging
import tempfile

from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.resources.drpc import DRPC
from ocs_ci.ocs.resources.pod import get_all_pods
from ocs_ci.ocs.resources.pv import get_all_pvs
from ocs_ci.ocs.resources.pvc import get_all_pvc_objs
from ocs_ci.ocs.node import gracefully_reboot_nodes
from ocs_ci.ocs.utils import (
    get_non_acm_cluster_config,
    get_active_acm_index,
    get_primary_cluster_config,
    get_passive_acm_index,
)
from ocs_ci.utility import version, templating
from ocs_ci.utility.utils import TimeoutSampler, CommandFailed, run_cmd

logger = logging.getLogger(__name__)


def get_current_primary_cluster_name(namespace, workload_type=constants.SUBSCRIPTION):
    """
    Get current primary cluster name based on workload namespace

    Args:
        namespace (str): Name of the namespace
        workload_type (str): Type of workload, i.e., Subscription or ApplicationSet

    Returns:
        str: Current primary cluster name

    """
    restore_index = config.cur_index
    if workload_type == constants.APPLICATION_SET:
        namespace = constants.GITOPS_CLUSTER_NAMESPACE
    drpc_data = DRPC(namespace=namespace).get()
    if drpc_data.get("spec").get("action") == constants.ACTION_FAILOVER:
        cluster_name = drpc_data["spec"]["failoverCluster"]
    else:
        cluster_name = drpc_data["spec"]["preferredCluster"]
    config.switch_ctx(restore_index)
    return cluster_name


def get_current_secondary_cluster_name(namespace, workload_type=constants.SUBSCRIPTION):
    """
    Get current secondary cluster name based on workload namespace

    Args:
        namespace (str): Name of the namespace
        workload_type (str): Type of workload, i.e., Subscription or ApplicationSet

    Returns:
        str: Current secondary cluster name

    """
    restore_index = config.cur_index
    if workload_type == constants.APPLICATION_SET:
        namespace = constants.GITOPS_CLUSTER_NAMESPACE
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


def get_scheduling_interval(namespace, workload_type=constants.SUBSCRIPTION):
    """
    Get scheduling interval for the workload in the given namespace

    Args:
        namespace (str): Name of the namespace
        workload_type (str): Type of workload, i.e., Subscription or ApplicationSet

    Returns:
        int: scheduling interval value from DRPolicy

    """
    restore_index = config.cur_index
    if workload_type == constants.APPLICATION_SET:
        namespace = constants.GITOPS_CLUSTER_NAMESPACE
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
):
    """
    Initiates Failover action to the specified cluster

    Args:
        failover_cluster (str): Cluster name to which the workload should be failed over
        namespace (str): Namespace where workload is running
        workload_type (str): Type of workload, i.e., Subscription or ApplicationSet
        workload_placement_name (str): Placement name
        switch_ctx (int): The cluster index by the cluster name

    """
    restore_index = config.cur_index
    switch_ctx if switch_ctx else config.switch_acm_ctx()
    failover_params = f'{{"spec":{{"action":"{constants.ACTION_FAILOVER}","failoverCluster":"{failover_cluster}"}}}}'
    if workload_type == constants.APPLICATION_SET:
        namespace = constants.GITOPS_CLUSTER_NAMESPACE
        drpc_obj = DRPC(
            namespace=namespace, resource_name=f"{workload_placement_name}-drpc"
        )
    else:
        drpc_obj = DRPC(namespace=namespace)

    drpc_obj.wait_for_peer_ready_status()
    logger.info(f"Initiating Failover action with failoverCluster:{failover_cluster}")
    assert drpc_obj.patch(
        params=failover_params, format_type="merge"
    ), f"Failed to patch {constants.DRPC}: {drpc_obj.resource_name}"

    logger.info(
        f"Wait for {constants.DRPC}: {drpc_obj.resource_name} to reach {constants.STATUS_FAILEDOVER} phase"
    )
    drpc_obj.wait_for_phase(constants.STATUS_FAILEDOVER)
    config.switch_ctx(restore_index)


def relocate(
    preferred_cluster,
    namespace,
    workload_type=constants.SUBSCRIPTION,
    workload_placement_name=None,
    switch_ctx=None,
):
    """
    Initiates Relocate action to the specified cluster

    Args:
        preferred_cluster (str): Cluster name to which the workload should be relocated
        namespace (str): Namespace where workload is running
        workload_type (str): Type of workload, i.e., Subscription or ApplicationSet
        workload_placement_name (str): Placement name
        switch_ctx (int): The cluster index by the cluster name

    """
    restore_index = config.cur_index
    switch_ctx if switch_ctx else config.switch_acm_ctx()
    relocate_params = f'{{"spec":{{"action":"{constants.ACTION_RELOCATE}","preferredCluster":"{preferred_cluster}"}}}}'
    if workload_type == constants.APPLICATION_SET:
        namespace = constants.GITOPS_CLUSTER_NAMESPACE
        drpc_obj = DRPC(
            namespace=namespace, resource_name=f"{workload_placement_name}-drpc"
        )
    else:
        drpc_obj = DRPC(namespace=namespace)
    drpc_obj.wait_for_peer_ready_status()
    logger.info(f"Initiating Relocate action with preferredCluster:{preferred_cluster}")
    assert drpc_obj.patch(
        params=relocate_params, format_type="merge"
    ), f"Failed to patch {constants.DRPC}: {drpc_obj.resource_name}"

    logger.info(
        f"Wait for {constants.DRPC}: {drpc_obj.resource_name} to reach {constants.STATUS_RELOCATED} phase"
    )
    drpc_obj.wait_for_phase(constants.STATUS_RELOCATED)
    config.switch_ctx(restore_index)


def check_mirroring_status_ok(replaying_images=None):
    """
    Check if mirroring status has health OK and expected number of replaying images

    Args:
        replaying_images (int): Expected number of images in replaying state

    Returns:
        bool: True if status contains expected health and states values, False otherwise

    """
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
        ocs_version = version.get_semantic_ocs_version_from_config()
        if ocs_version >= version.VERSION_4_12:
            expected_value = [replaying_images]
        else:
            expected_value = range(replaying_images, replaying_images + 3)

        current_value = mirroring_status.get("states").get("replaying")

        if current_value not in expected_value:
            logger.warning(
                f"Unexpected states. Current replaying count is {current_value} but expected {expected_value}"
            )
            return False

    return True


def wait_for_mirroring_status_ok(replaying_images=None, timeout=300):
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
        if pv.get("spec").get("claimRef").get("namespace") == namespace
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


def check_vrg_existence(namespace):
    """
    Check if VRG resource exists in the given namespace

    Args:
        namespace (str): the namespace of the VRG resource

    """
    vrg_list = (
        ocp.OCP(kind=constants.VOLUME_REPLICATION_GROUP, namespace=namespace)
        .get()
        .get("items")
    )
    if len(vrg_list) > 0:
        return True
    else:
        return False


def check_vrg_state(state, namespace):
    """
    Check if VRG in the given namespace is in expected state

    Args:
        state (str): The VRG state to check for (e.g. 'primary', 'secondary')
        namespace (str): the namespace of the VRG resources

    Returns:
        bool: True if VRG is in expected state or was deleted, False otherwise

    """
    vrg_obj = ocp.OCP(kind=constants.VOLUME_REPLICATION_GROUP, namespace=namespace)
    vrg_list = vrg_obj.get().get("items")

    # Skip state check if resource was deleted
    if len(vrg_list) == 0 and state.lower() == "secondary":
        logger.info("VRG resource not found, skipping state check")
        return True

    vrg_name = vrg_list[0]["metadata"]["name"]
    desired_state = vrg_list[0]["spec"]["replicationState"]
    current_state = vrg_list[0]["status"]["state"]
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


def wait_for_replication_resources_creation(vr_count, namespace, timeout):
    """
    Wait for replication resources to be created

    Args:
        vr_count (int): Expected number of VR resources or ReplicationSource count
        namespace (str): the namespace of the VR or ReplicationSource resources
        timeout (int): time in seconds to wait for VR or ReplicationSource resources to be created
            or reach expected state
    Raises:
        TimeoutExpiredError: In case replication resources not created

    """
    logger.info("Waiting for VRG to be created")
    sample = TimeoutSampler(
        timeout=timeout, sleep=5, func=check_vrg_existence, namespace=namespace
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

    logger.info("Waiting for VRG to reach primary state")
    sample = TimeoutSampler(
        timeout=timeout,
        sleep=5,
        func=check_vrg_state,
        state="primary",
        namespace=namespace,
    )
    if not sample.wait_for_func_status(result=True):
        error_msg = "VRG hasn't reached expected state primary within the time limit."
        logger.error(error_msg)
        raise TimeoutExpiredError(error_msg)


def wait_for_replication_resources_deletion(namespace, timeout, check_state=True):
    """
    Wait for replication resources to be deleted

    Args:
        namespace (str): the namespace of the resources'
        timeout (int): time in seconds to wait for resources to reach expected
            state or deleted
        check_state (bool): True for checking resources state before deletion, False otherwise

    Raises:
        TimeoutExpiredError: In case replication resources not deleted

    """
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

        logger.info("Waiting for VRG to reach secondary state")
        sample = TimeoutSampler(
            timeout=timeout,
            sleep=5,
            func=check_vrg_state,
            state="secondary",
            namespace=namespace,
        )
        if not sample.wait_for_func_status(result=True):
            error_msg = (
                "VRG hasn't reached expected state secondary within the time limit."
            )
            logger.info(error_msg)
            raise TimeoutExpiredError(error_msg)

    if "cephfs" not in namespace:
        logger.info("Waiting for VRG to be deleted")
        sample = TimeoutSampler(
            timeout=timeout, sleep=5, func=check_vrg_existence, namespace=namespace
        )
        if not sample.wait_for_func_status(result=False):
            error_msg = "VRG resource not deleted"
            logger.info(error_msg)
            raise TimeoutExpiredError(error_msg)

    if config.MULTICLUSTER["multicluster_mode"] != "metro-dr":
        logger.info(f"Waiting for all {resource_kind} to be deleted")
        sample = TimeoutSampler(
            timeout=timeout,
            sleep=5,
            func=count_function,
            namespace=namespace,
        )
        sample.wait_for_func_value(0)


def wait_for_all_resources_creation(
    pvc_count, pod_count, namespace, timeout=900, skip_replication_resources=False
):
    """
    Wait for workload and replication resources to be created

    Args:
        pvc_count (int): Expected number of PVCs
        pod_count (int): Expected number of Pods
        namespace (str): the namespace of the workload
        timeout (int): time in seconds to wait for resource creation
        skip_replication_resources (bool): if true vr status wont't be check

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
        wait_for_replication_resources_creation(pvc_count, namespace, timeout)


def wait_for_all_resources_deletion(
    namespace, check_replication_resources_state=True, timeout=1000
):
    """
    Wait for workload and replication resources to be deleted

    Args:
        namespace (str): the namespace of the workload
        check_replication_resources_state (bool): True for checking replication resources state, False otherwise
        timeout (int): time in seconds to wait for resource deletion

    """
    logger.info("Waiting for all pods to be deleted")
    all_pods = get_all_pods(namespace=namespace)
    for pod_obj in all_pods:
        if "volsync-rsync-tls-dst" not in pod_obj.name:
            pod_obj.ocp.wait_for_delete(
                resource_name=pod_obj.name, timeout=timeout, sleep=5
            )

    wait_for_replication_resources_deletion(
        namespace, timeout, check_replication_resources_state
    )

    if not (
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
        if "cephfs" not in namespace:
            logger.info("Waiting for all PVs to be deleted")
            sample = TimeoutSampler(
                timeout=timeout,
                sleep=5,
                func=get_pv_count,
                namespace=namespace,
            )
            sample.wait_for_func_value(0)


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


def get_image_uuids(namespace):
    """
    Gets all image UUIDs associated with the PVCs in the given namespace

    Args:
        namespace (str): the namespace of the VR resources

    Returns:
        list: List of all image UUIDs

    """
    image_uuids = []
    for cluster in get_non_acm_cluster_config():
        config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
        logger.info(
            f"Fetching image UUIDs from cluster: {cluster.ENV_DATA['cluster_name']}"
        )
        all_pvcs = get_all_pvc_objs(namespace=namespace)
        for pvc_obj in all_pvcs:
            image_uuids.append(pvc_obj.image_uuid)
    image_uuids = list(set(image_uuids))
    logger.info(f"All image UUIDs from managed clusters: {image_uuids}")
    return image_uuids


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
    switch_ctx if switch_ctx else config.switch_acm_ctx()
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
    switch_ctx if switch_ctx else config.switch_acm_ctx()
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
    switch_ctx if switch_ctx else config.switch_acm_ctx()
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

    Returns:
        state (str): If drcluster are fenced: Fenced or Unfenced, else None if not defined
        switch_ctx (int): The cluster index by the cluster name

    """
    restore_index = config.cur_index
    switch_ctx if switch_ctx else config.switch_acm_ctx()
    drcluster_obj = ocp.OCP(resource_name=drcluster_name, kind=constants.DRCLUSTER)
    state = drcluster_obj.get().get("spec").get("clusterFence")
    config.switch_ctx(restore_index)
    return state


def create_backup_schedule():
    """
    Create backupschedule resource only on active hub

    """
    old_ctx = config.cur_index
    config.switch_ctx(get_active_acm_index())
    backup_schedule = templating.load_yaml(constants.MDR_BACKUP_SCHEDULE_YAML)
    backup_schedule_yaml = tempfile.NamedTemporaryFile(
        mode="w+", prefix="bkp", delete=False
    )
    templating.dump_data_to_temp_yaml(backup_schedule, backup_schedule_yaml.name)
    run_cmd(f"oc create -f {backup_schedule_yaml.name}")
    config.switch_ctx(old_ctx)


def gracefully_reboot_ocp_nodes(
    namespace, drcluster_name, workload_type=constants.SUBSCRIPTION
):
    """
    Gracefully reboot OpenShift Container Platform
    nodes which was fenced before

    Args:
        namespace (str): Name of the namespace
        drcluster_name (str): Name of the drcluster which needs to be rebooted
        workload_type (str): Type of workload. ie Subscription(Default) or ApplicationSet

    """

    primary_cluster_name = get_current_primary_cluster_name(
        namespace=namespace, workload_type=workload_type
    )
    if primary_cluster_name == drcluster_name:
        set_current_primary_cluster_context(namespace, workload_type)
    else:
        set_current_secondary_cluster_context(namespace, workload_type)
    gracefully_reboot_nodes()


def restore_backup():
    """
    Restores the backup in new hub and make it as active

    """

    restore_index = config.cur_index
    config.switch_ctx(get_passive_acm_index())
    backup_schedule = templating.load_yaml(constants.MDR_RESTORE_YAML)
    backup_schedule_yaml = tempfile.NamedTemporaryFile(
        mode="w+", prefix="restore", delete=False
    )
    templating.dump_data_to_temp_yaml(backup_schedule, backup_schedule_yaml.name)
    run_cmd(f"oc create -f {backup_schedule_yaml.name}")
    config.switch_ctx(restore_index)


def verify_drpolicy_cli(switch_ctx=None):
    """
    Function to verify DRPolicy status

    Returns:
        bool: True if the status is in succeed state, else False
        switch_ctx (int): The cluster index by the cluster name

    """

    restore_index = config.cur_index
    switch_ctx if switch_ctx else config.switch_acm_ctx()
    drpolicy_obj = ocp.OCP(kind=constants.DRPOLICY)
    status = drpolicy_obj.get().get("items")[0].get("status").get("conditions")[0]
    if status.get("reason") == "Succeeded":
        return True
    else:
        logger.warning(f"DRPolicy is not in succeeded or validated state: {status}")
        return False
    config.switch_ctx(restore_index)
