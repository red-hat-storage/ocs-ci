"""
Helper functions specific for DR
"""
import logging

from ocs_ci.helpers.helpers import get_all_pvs
from ocs_ci.ocs.resources import pod

from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.resources.pod import get_all_pods
from ocs_ci.ocs.resources.pvc import get_all_pvc_objs
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
            # There can be upto 2 dummy images in each ODF cluster
            expected_value = range(replaying_images, replaying_images + 3)
            value = mirroring_status.get("states").get("replaying")
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


def get_all_vrs(namespace):
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


def get_vr_count(namespace):
    """
    Gets VR resource count in given namespace

    Args:
        namespace (str): the namespace of the VR resources

    Returns:
         int: VR resource count

    """
    return len(get_all_vrs(namespace))


def check_vr_state(state, namespace):
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
            f"Following {len(vr_state_mismatch)} VR are not in expected {state} state: {vr_state_mismatch}"
        )
        return False


def wait_for_vr_count(count, namespace, timeout=300):
    """
    Wait for all VR resources to reach expected count in the given namespace

    Args:
        count (int): Expected number of VR resources
        namespace (str): the namespace of the VR resources
        timeout (int): time in seconds to wait for VR resources to be created
            or reach expected state

    Returns:
        bool: True if all VR are in expected state

    """
    sample = TimeoutSampler(
        timeout=timeout,
        sleep=5,
        func=get_vr_count,
        namespace=namespace,
    )
    sample.wait_for_func_value(count)

    return True


def wait_for_vr_state(state, namespace, timeout=300):
    """
    Wait for all VR resources to reach expected state in the given namespace

    Args:
        state (str): The VR state to check for (e.g. 'primary', 'secondary')
        namespace (str): the namespace of the VR resources
        timeout (int): time in seconds to wait for VR resources to be created
            or reach expected state

    Returns:
        bool: True if all VR are in expected state

    Raises:
        AssertionError: If VR resources are not in expected state

    """
    sample = TimeoutSampler(
        timeout=timeout, sleep=3, func=check_vr_state, state=state, namespace=namespace
    )
    assert sample.wait_for_func_status(
        result=True
    ), f"One or more VR haven't reached expected state {state} within the time limit."

    return True


def wait_for_vr_creation(count, namespace, timeout=300):
    """
    Wait for all VR resources to be deleted in the given namespace

    Args:
        count (int): Expected number of VR resources
        namespace (str): the namespace of the VR resources
        timeout (int): time in seconds to wait for VR resources to be created
            or reach expected state

    """
    wait_for_vr_count(count, namespace, timeout)
    wait_for_vr_state("primary", namespace, timeout)


def wait_for_vr_deletion(namespace, timeout=300):
    """
    Wait for all VR resources to be deleted in the given namespace

    Args:
        namespace (str): the namespace of the VR resources
        timeout (int): time in seconds to wait for VR resources to be created
            or reach expected state

    """
    wait_for_vr_state("secondary", namespace, timeout)
    wait_for_vr_count(0, namespace, timeout)


def wait_for_workload_resource_creation(pvc_count, pod_count, namespace, timeout=120):
    """
    Wait for workload resources such as PVCs and Pods to be created

    Args:
        pvc_count (int): Expected number of PVCs
        pod_count (int): Expected number of Pods
        namespace (str): the namespace of the workload
        timeout (int): time in seconds to wait for resource creation

    """
    logger.info(f"Waiting for {pvc_count} PVCs to reach {constants.STATUS_BOUND} state")
    ocp.OCP(kind=constants.PVC, namespace=namespace).wait_for_resource(
        condition=constants.STATUS_BOUND, resource_count=pvc_count, timeout=timeout
    )
    logger.info(
        f"Waiting for {pod_count} pods to reach {constants.STATUS_RUNNING} state"
    )
    ocp.OCP(kind=constants.POD, namespace=namespace).wait_for_resource(
        condition=constants.STATUS_RUNNING, resource_count=pod_count, timeout=timeout
    )


def wait_for_workload_resource_deletion(namespace, timeout=120):
    """
    Wait for workload resources to be deleted

    Args:
        namespace (str): the namespace of the workload
        timeout (int): time in seconds to wait for resource deletion

    """
    logger.info("Waiting for all pods to be deleted")
    all_pods = get_all_pods(namespace=namespace)
    for pod_obj in all_pods:
        pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name, timeout=timeout)

    logger.info("Waiting for all PVCs to be deleted")
    all_pvcs = get_all_pvc_objs(namespace=namespace)
    for pvc_obj in all_pvcs:
        pvc_obj.ocp.wait_for_delete(resource_name=pvc_obj.name, timeout=timeout)


def check_rbd_mirrored_image_status(namespace, image_state):
    """
    Check RBD mirror image status for mirrored images

    Args:
        namespace (str): Name of namespace
        image_state (str): Image state based on primary and secomdary

    Returns:
        bool: True if all images are in expected state or else False

    """
    # TODO: Handle code if user looking for image state in secondary cluster
    ct_pod = pod.get_ceph_tools_pod()
    cmd = f"rbd mirror pool status {constants.DEFAULT_BLOCKPOOL} --verbose --debug-rbd 0"
    rbd_mirror_image_status_output = ct_pod.exec_ceph_cmd(ceph_cmd=cmd, format="json")
    image_name_list = list()
    pv_dict = get_all_pvs()['items']
    failed_count = 0
    for pv_name in pv_dict:
        if pv_name['spec']['claimRef']['namespace'] == namespace:
            pv_data_dict = {
                "pvc_name": pv_name['spec']['claimRef']['name'] ,
                "rbd_image_name": pv_name["spec"]["csi"]["volumeAttributes"]["imageName"]
            }
            image_name_list.append(pv_data_dict)
    for ceph_image_name in image_name_list:
        for rbd_images in rbd_mirror_image_status_output['images']:
            if ceph_image_name['rbd_image_name'] == rbd_images['name']:
                if rbd_images['state'] == image_state:
                    logger.info(
                        f"Rbd mirror image status check for "
                        f"{ceph_image_name['rbd_image_name']}/{ceph_image_name['pvc_name']} Passed"
                    )
                else:
                    logger.error(
                        f"Rbd mirror image status check for "
                        f"{ceph_image_name['rbd_image_name']}/{ceph_image_name['pvc_name']} Failed "
                        f"\n Image status:= {rbd_images['state']}"
                        f" Description:= {rbd_images['description']}"
                    )
                    failed_count += 1

    if failed_count:
        return False
    return True
