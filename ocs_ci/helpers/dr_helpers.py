"""
Helper functions specific for DR
"""
import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.resources.drpc import DRPC
from ocs_ci.ocs.resources.pod import get_all_pods
from ocs_ci.ocs.resources.pvc import get_all_pvcs_in_storageclass, get_all_pvc_objs
from ocs_ci.ocs.utils import get_non_acm_cluster_config
from ocs_ci.utility.utils import TimeoutSampler

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
        namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
    )
    mirroring_status = cbp_obj.get().get("status").get("mirroringStatus").get("summary")
    logger.info(f"Mirroring status: {mirroring_status}")
    keys_to_check = ["health", "daemon_health", "image_health", "states"]
    for key in keys_to_check:
        if key != "states":
            expected_value = "OK"
            current_value = mirroring_status.get(key)
        elif key == "states" and replaying_images:
            # Replaying images count can be higher due to presence of dummy images
            # There can be upto 2 dummy images in each ODF cluster
            expected_value = range(replaying_images, replaying_images + 3)
            current_value = mirroring_status.get("states").get("replaying")
        else:
            continue

        if current_value not in expected_value:
            logger.warning(
                f"Unexpected {key} status. Current status is {current_value} but expected {expected_value}"
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
    if not replaying_images:
        replaying_images = 0
        for cluster in get_non_acm_cluster_config():
            config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
            replaying_images += len(
                get_all_pvcs_in_storageclass(constants.CEPHBLOCKPOOL_SC)
            )
        replaying_images -= 2  # Ignore db-noobaa-db-pg-0 PVCs

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
    namespace, check_replication_resources_state=True, timeout=900
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
