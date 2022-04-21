"""
Helper functions specific for DR
"""
import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp

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


def check_mirroring_status(replaying_images=None):
    """
    Check if mirroring status have expected health and states values

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
        value = mirroring_status.get(key)
        if key == "states":
            if replaying_images:
                value = value.get("replaying")
                expected_value = replaying_images
            else:
                continue
        else:
            expected_value = "OK"

        if value != expected_value:
            logger.error(
                f"Unexpected {key} status. Current status is {value} but expected {expected_value}"
            )
            return False

    return True


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
    vr_items = vr_obj.get().get("items")
    vr_list = [vr.get("metadata").get("name") for vr in vr_items]

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
        logger.info(f"All VR reached desired state {desired_state}")
        return True
    else:
        logger.error(f"Following VR haven't reached desired state: {vr_state_mismatch}")
        return False
