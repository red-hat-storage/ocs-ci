"""
Helper functions specific for DR
"""
import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.resources.drpc import DRPC
from ocs_ci.ocs.resources.pod import get_all_pods
from ocs_ci.ocs.resources.pv import get_all_pvs
from ocs_ci.ocs.resources.pvc import get_all_pvc_objs
from ocs_ci.ocs.node import gracefully_reboot_nodes
from ocs_ci.ocs.utils import get_non_acm_cluster_config
from ocs_ci.utility import version
from ocs_ci.utility.utils import TimeoutSampler, CommandFailed

logger = logging.getLogger(__name__)


def get_current_primary_cluster_name(namespace):
    """
    Get current primary cluster name based on workload namespace

    Args:
        namespace (str): Name of the namespace

    Returns:
        str: Current primary cluster name

    """
    restore_index = config.cur_index
    drpc_data = DRPC(namespace=namespace).get()
    if drpc_data.get("spec").get("action") == constants.ACTION_FAILOVER:
        cluster_name = drpc_data["spec"]["failoverCluster"]
    else:
        cluster_name = drpc_data["spec"]["preferredCluster"]
    config.switch_ctx(restore_index)
    return cluster_name


def get_current_secondary_cluster_name(namespace):
    """
    Get current secondary cluster name based on workload namespace

    Args:
        namespace (str): Name of the namespace

    Returns:
        str: Current secondary cluster name

    """
    restore_index = config.cur_index
    primary_cluster_name = get_current_primary_cluster_name(namespace)
    drpolicy_data = DRPC(namespace=namespace).drpolicy_obj.get()
    config.switch_ctx(restore_index)
    for cluster_name in drpolicy_data["spec"]["drClusters"]:
        if not cluster_name == primary_cluster_name:
            return cluster_name


def set_current_primary_cluster_context(namespace):
    """
    Set current primary cluster context based on workload namespace

    Args:
        namespace (str): Name of the namespace

    """
    cluster_name = get_current_primary_cluster_name(namespace)
    config.switch_to_cluster_by_name(cluster_name)


def set_current_secondary_cluster_context(namespace):
    """
    Set secondary cluster context based on workload namespace

    Args:
        namespace (str): Name of the namespace

    """
    cluster_name = get_current_secondary_cluster_name(namespace)
    config.switch_to_cluster_by_name(cluster_name)


def get_scheduling_interval(namespace):
    """
    Get scheduling interval for the workload in the given namespace

    Args:
        namespace (str): Name of the namespace

    Returns:
        int: scheduling interval value from DRPolicy

    """
    restore_index = config.cur_index
    drpolicy_obj = DRPC(namespace=namespace).drpolicy_obj
    interval_value = int(drpolicy_obj.get()["spec"]["schedulingInterval"][:-1])
    config.switch_ctx(restore_index)
    return interval_value


def failover(failover_cluster, namespace):
    """
    Initiates Failover action to the specified cluster
    Args:
        failover_cluster (str): Cluster name to which the workload should be failed over
        namespace (str): Namespace where workload is running
    """
    restore_index = config.cur_index
    config.switch_acm_ctx()
    failover_params = f'{{"spec":{{"action":"{constants.ACTION_FAILOVER}","failoverCluster":"{failover_cluster}"}}}}'
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


def relocate(preferred_cluster, namespace):
    """
    Initiates Relocate action to the specified cluster

    Args:
        preferred_cluster (str): Cluster name to which the workload should be relocated
        namespace (str): Namespace where workload is running

    """
    restore_index = config.cur_index
    config.switch_acm_ctx()
    relocate_params = f'{{"spec":{{"action":"{constants.ACTION_RELOCATE}","preferredCluster":"{preferred_cluster}"}}}}'
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
        vr_count (int): Expected number of VR resources
        namespace (str): the namespace of the VR resources
        timeout (int): time in seconds to wait for VR resources to be created
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

    if config.ENV_DATA["multicluster_mode"] != "metro-dr":
        logger.info(f"Waiting for {vr_count} VRs to be created")
        sample = TimeoutSampler(
            timeout=timeout,
            sleep=5,
            func=get_vr_count,
            namespace=namespace,
        )
        sample.wait_for_func_value(vr_count)

        logger.info(f"Waiting for {vr_count} VRs to reach primary state")
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
    if check_state:
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

    logger.info("Waiting for VRG to be deleted")
    sample = TimeoutSampler(
        timeout=timeout, sleep=5, func=check_vrg_existence, namespace=namespace
    )
    if not sample.wait_for_func_status(result=False):
        error_msg = "VRG resource not deleted"
        logger.info(error_msg)
        raise TimeoutExpiredError(error_msg)

    if config.ENV_DATA["multicluster_mode"] != "metro-dr":
        logger.info("Waiting for all VRs to be deleted")
        sample = TimeoutSampler(
            timeout=timeout,
            sleep=5,
            func=get_vr_count,
            namespace=namespace,
        )
        sample.wait_for_func_value(0)


def wait_for_all_resources_creation(pvc_count, pod_count, namespace, timeout=900):
    """
    Wait for workload and replication resources to be created

    Args:
        pvc_count (int): Expected number of PVCs
        pod_count (int): Expected number of Pods
        namespace (str): the namespace of the workload
        timeout (int): time in seconds to wait for resource creation

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
        pod_obj.ocp.wait_for_delete(
            resource_name=pod_obj.name, timeout=timeout, sleep=5
        )

    wait_for_replication_resources_deletion(
        namespace, timeout, check_replication_resources_state
    )

    logger.info("Waiting for all PVCs to be deleted")
    all_pvcs = get_all_pvc_objs(namespace=namespace)
    for pvc_obj in all_pvcs:
        pvc_obj.ocp.wait_for_delete(
            resource_name=pvc_obj.name, timeout=timeout, sleep=5
        )

    if config.ENV_DATA["multicluster_mode"] != "metro-dr":
        logger.info("Waiting for all PVs to be deleted")
        sample = TimeoutSampler(
            timeout=timeout,
            sleep=5,
            func=get_pv_count,
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


def enable_fence(drcluster_name):
    """
    Once the managed cluster is fenced, all communication
    from applications to the ODF external storage cluster will fail

    Args:
        drcluster_name (str): Name of the DRcluster which needs to be fenced

    """

    logger.info(
        f"Edit the DRCluster resource for {drcluster_name} cluster on the Hub cluster"
    )
    restore_index = config.cur_index
    config.switch_acm_ctx()
    fence_params = f'{{"spec":{{"clusterFence":"{constants.ACTION_FENCE}"}}}}'
    drcluster_obj = ocp.OCP(resource_name=drcluster_name, kind=constants.DRCLUSTER)
    if not drcluster_obj.patch(params=fence_params, format_type="merge"):
        raise CommandFailed(f"Failed to patch {constants.DRCLUSTER}: {drcluster_name}")
    logger.info(f"Successfully fenced {constants.DRCLUSTER}: {drcluster_name}")
    config.switch_ctx(restore_index)


def enable_unfence(drcluster_name):
    """
    The OpenShift cluster to be Unfenced is the one where applications
    are not currently running and the cluster that was Fenced earlier.

    Args:
        drcluster_name (str): Name of the DRcluster which needs to be fenced

    """

    logger.info(
        f"Edit the DRCluster resource for {drcluster_name} cluster on the Hub cluster"
    )
    restore_index = config.cur_index
    config.switch_acm_ctx()
    unfence_params = f'{{"spec":{{"clusterFence":"{constants.ACTION_UNFENCE}"}}}}'
    drcluster_obj = ocp.OCP(resource_name=drcluster_name, kind=constants.DRCLUSTER)
    if not drcluster_obj.patch(params=unfence_params, format_type="merge"):
        raise CommandFailed(f"Failed to patch {constants.DRCLUSTER}: {drcluster_name}")
    logger.info(f"Successfully unfenced {constants.DRCLUSTER}: {drcluster_name}")
    config.switch_ctx(restore_index)


def fence_state(drcluster_name, fence_state):
    """
    Sets the specified clusterFence state
    Args:
       drcluster_name (str): Name of the DRcluster which needs to be fenced
       fence_state (str): Specify the clusterfence state either constants.ACTION_UNFENCE and ACTION_FENCE
    """

    logger.info(
        f"Edit the DRCluster {drcluster_name} cluster clusterfence state {fence_state}  "
    )
    restore_index = config.cur_index
    config.switch_acm_ctx()
    params = f'{{"spec":{{"clusterFence":"{fence_state}"}}}}'
    drcluster_obj = ocp.OCP(resource_name=drcluster_name, kind=constants.DRCLUSTER)
    if not drcluster_obj.patch(params=params, format_type="merge"):
        raise CommandFailed(f"Failed to patch {constants.DRCLUSTER}: {drcluster_name}")
    logger.info(
        f"Successfully changed clusterfence state to {fence_state} {constants.DRCLUSTER}: {drcluster_name}"
    )
    config.switch_ctx(restore_index)


def get_fence_state(drcluster_name):
    """
    Returns the clusterfence state of given drcluster
    Args:
        drcluster_name (str): Name of the DRcluster
    Returns:
        state (str): If drcluster are fenced: Fenced or Unfenced, else None if not defined
    """
    restore_index = config.cur_index
    config.switch_acm_ctx()
    drcluster_obj = ocp.OCP(resource_name=drcluster_name, kind=constants.DRCLUSTER)
    state = drcluster_obj.get().get("spec").get("clusterFence")
    config.switch_ctx(restore_index)
    return state


def gracefully_reboot_ocp_nodes(namespace, drcluster_name):
    """
    Gracefully reboot OpenShift Container Platform
    nodes which was fenced before
    Args:
        namespace (str): Name of the namespace
        drcluster_name (str): Name of the drcluster which need to be reboot
    """

    primary_cluster_name = get_current_primary_cluster_name(namespace=namespace)
    if primary_cluster_name == drcluster_name:
        set_current_primary_cluster_context(namespace)
    else:
        set_current_secondary_cluster_context(namespace)
    gracefully_reboot_nodes()
