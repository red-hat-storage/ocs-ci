"""
Helper functions specific for DR
"""
import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.utils import get_non_acm_cluster_config
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


def failover(failover_cluster, drpc_name, namespace):
    """
    Initiates Failover action to the specified cluster

    Args:
        failover_cluster (str): Cluster name to which the workload should be failed over
        drpc_name (str): Name of the DRPC resource to apply the patch
        namespace (str): Name of the namespace to use

    """
    prev_index = config.cur_index
    config.switch_acm_ctx()
    failover_params = (
        f'{{"spec":{{"action":"Failover","failoverCluster":"{failover_cluster}"}}}}'
    )
    drpc_obj = ocp.OCP(
        kind=constants.DRPC, namespace=namespace, resource_name=drpc_name
    )
    drpc_obj._has_phase = True

    logger.info(f"Initiating failover action to {failover_cluster}")
    assert drpc_obj.patch(
        params=failover_params, format_type="merge"
    ), f"Failed to patch {constants.DRPC}: {drpc_name}"

    logger.info(
        f"Wait for {constants.DRPC}: {drpc_name} to reach {constants.STATUS_FAILEDOVER} phase"
    )
    drpc_obj.wait_for_phase(constants.STATUS_FAILEDOVER)

    config.switch_ctx(prev_index)


def relocate(preferred_cluster, drpc_name, namespace):
    """
    Initiates Relocate action to the specified cluster

    Args:
        preferred_cluster (str): Cluster name to which the workload should be relocated
        drpc_name (str): Name of the DRPC resource to apply the patch
        namespace (str): Name of the namespace to use

    """
    prev_index = config.cur_index
    config.switch_acm_ctx()
    relocate_params = (
        f'{{"spec":{{"action":"Relocate","preferredCluster":"{preferred_cluster}"}}}}'
    )
    drpc_obj = ocp.OCP(
        kind=constants.DRPC, namespace=namespace, resource_name=drpc_name
    )
    drpc_obj._has_phase = True

    logger.info(f"Initiating relocate action to {preferred_cluster}")
    assert drpc_obj.patch(
        params=relocate_params, format_type="merge"
    ), f"Failed to patch {constants.DRPC}: {drpc_name}"

    logger.info(
        f"Wait for {constants.DRPC}: {drpc_name} to reach {constants.STATUS_RELOCATED} phase"
    )
    drpc_obj.wait_for_phase(constants.STATUS_RELOCATED)

    config.switch_ctx(prev_index)


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
            value = mirroring_status.get(key)
        elif key == "states" and replaying_images:
            # Replaying images count can be higher due to presence of dummy images
            expected_value = range(replaying_images, replaying_images + 3)
            value = mirroring_status.get(key).get("replaying")
        else:
            continue

        if value not in expected_value:
            logger.warning(
                f"Unexpected {key} status. Current status is {value} but expected {expected_value}"
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
        AssertionError: In case of unexpected mirroring status

    """
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
        assert sample.wait_for_func_status(result=True), (
            "The mirroring status does not have expected values within the time"
            f" limit on cluster {cluster.ENV_DATA['cluster_name']}"
        )


def get_all_vrs(namespace=None):
    """
    Gets all VRs in given namespace

    Args:
        namespace (str): the namespace of the VR resources

    Returns:
         list: list of all VR in namespace

    """
    vr_obj = ocp.OCP(kind=constants.VOLUME_REPLICATION, namespace=namespace)
    vr_items = vr_obj.get().get("items")
    vr_list = [vr.get("metadata").get("name") for vr in vr_items]
    return vr_list


def check_vr_status(state, namespace):
    """
    Check if all VR in the given namespace are in expected state

    Args:
        state (str): The VR state to check for (e.g. 'primary', 'secondary')
        namespace (str): the namespace of the VR resources

    Returns:
        bool: True if all VR are in expected state, False otherwise

    """
    vr_obj = ocp.OCP(kind=constants.VOLUME_REPLICATION, namespace=namespace)
    vr_list = get_all_vrs(namespace)

    vr_state_mismatch = []
    for vr in vr_list:
        desired_state = vr_obj.get(vr).get("spec").get("replicationState")
        current_state = vr_obj.get(vr).get("status").get("state")
        logger.info(
            f"VR: {vr} desired state is {desired_state}, current state is {current_state}"
        )

        if not (
            state.lower() == desired_state.lower()
            and state.lower() == current_state.lower()
        ):
            vr_state_mismatch.append(vr)

    if not vr_state_mismatch:
        logger.info(f"All {len(vr_list)} VR are in expected state {state}")
        return True
    else:
        logger.warning(
            f"Following {len(vr_state_mismatch)} VR are not in expected state: {vr_state_mismatch}"
        )
        return False


def wait_for_vr(count, namespace, state="primary", timeout=300):
    """
    Wait for all VR resources to exist in expected state in the given namespace

    Args:
        count (int): Expected number of VR resources
        namespace (str): the namespace of the VR resources
        state (str): The VR state to check for (e.g. 'primary', 'secondary')
        timeout (int): time in seconds to wait for VR resources to be created
            or reach expected state

    Returns:
        bool: True if all VR are in expected state

    Raises:
        Exception: In case of unexpected VR resource count or status

    """
    try:
        for sample in TimeoutSampler(
            timeout=timeout,
            sleep=5,
            func=get_all_vrs,
            namespace=namespace,
        ):
            current_num = len(sample)
            logger.info(
                f"Expected VR resources: {count}, "
                f"Current VR resources: {current_num}"
            )
            if current_num == count:
                break
    except TimeoutExpiredError:
        logger.exception(f"Current VR resources did not reach expected count {count}")
        raise

    sample = TimeoutSampler(
        timeout=timeout, sleep=5, func=check_vr_status, state=state, namespace=namespace
    )
    assert sample.wait_for_func_status(
        result=True
    ), f"One or more VR haven't reached expected state {state} within the time limit."

    return True
