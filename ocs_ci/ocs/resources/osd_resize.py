import logging

from ocs_ci.ocs.exceptions import (
    StorageSizeNotReflectedException,
    ResourceWrongStatusException,
    CephHealthException,
)
from ocs_ci.ocs.resources.pod import (
    get_osd_pods,
    delete_pods,
    wait_for_pods_to_be_in_statuses,
    get_ceph_tools_pod,
)
from ocs_ci.ocs.resources.pvc import get_deviceset_pvcs, get_deviceset_pvs
from ocs_ci.ocs.resources.pv import get_pv_size
from ocs_ci.ocs.resources.storage_cluster import (
    get_storage_size,
    get_device_class,
    verify_storage_device_class,
    verify_device_class_in_osd_tree,
)
from ocs_ci.ocs.cluster import check_ceph_osd_tree, CephCluster
from ocs_ci.utility.utils import ceph_health_check, TimeoutSampler, convert_device_size
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework import config


logger = logging.getLogger(__name__)


def check_resources_state_post_resize_osd(old_osd_pods, old_osd_pvcs, old_osd_pvs):
    """
    Check that the pods, PVCs, and PVs are in the expected state post resizing the osd.
    It will perform the following steps:
    1. Check that the old osd pods are in a terminating state or deleted
    2. Check that the new osd pods running, and we have exactly the same number of osd pods as the old ones.
    3. Check that the PVCs are in a Bound state
    4. Check that the old PVC and PV names are equal to the current PVC and PV names

    Args:
        old_osd_pods (list): The old osd pod objects before resizing the osd
        old_osd_pvcs (list): The old osd PVC objects before resizing the osd
        old_osd_pvs (list): The old osd PV objects before resizing the osd

    Raises:
        ResourceWrongStatusException: If the following occurs:
            1. The OSD pods failed to reach the status Terminated or to be deleted
            2. The old PVC and PV names are not equal to the current PVC and PV names

    """
    old_osd_pods_count = len(old_osd_pods)
    logger.info("Wait for the OSD pods to reach the status Terminated or to be deleted")
    old_osd_pod_names = [p.name for p in old_osd_pods]
    res = wait_for_pods_to_be_in_statuses(
        expected_statuses=[constants.STATUS_TERMINATING],
        pod_names=old_osd_pod_names,
        timeout=300,
        sleep=20,
    )
    if not res:
        raise ResourceWrongStatusException(
            "The OSD pods failed to reach the status Terminated or to be deleted"
        )

    logger.info("Check that the new OSD pods are running")
    ocp_pod = OCP(kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"])
    ocp_pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.OSD_APP_LABEL,
        resource_count=old_osd_pods_count,
        timeout=300,
        sleep=20,
    )

    logger.info(
        f"Check that the number of the new OSD pods are exactly {old_osd_pods_count}"
    )
    for osd_pods in TimeoutSampler(timeout=180, sleep=10, func=get_osd_pods):
        osd_pods_count = len(osd_pods)
        logger.info(f"number of osd pods = {osd_pods_count}")
        if old_osd_pods_count == osd_pods_count:
            break

    logger.info("Check that the PVCs are in a Bound state")
    ocp_pvc = OCP(kind=constants.PVC, namespace=config.ENV_DATA["cluster_namespace"])
    ocp_pvc.wait_for_resource(
        timeout=30,
        sleep=5,
        condition=constants.STATUS_BOUND,
        selector=constants.OSD_PVC_GENERIC_LABEL,
        resource_count=len(old_osd_pvcs),
    )

    current_osd_pvcs = get_deviceset_pvcs()
    old_pvc_names = [p.name for p in old_osd_pvcs]
    current_pvc_names = [p.name for p in current_osd_pvcs]
    logger.info(f"Old PVC names = {old_pvc_names}")
    logger.info(f"Current PVC names = {current_pvc_names}")

    current_osd_pvs = get_deviceset_pvs()
    old_pv_names = [p.name for p in old_osd_pvs]
    current_pv_names = [p.name for p in current_osd_pvs]
    logger.info(f"Old PV names = {old_pv_names}")
    logger.info(f"Current PV names = {current_pv_names}")

    logger.info(
        "Check that the old PVC and PV names are equal to the current PVC and PV names"
    )
    if not old_pvc_names == current_pvc_names:
        raise ResourceWrongStatusException(
            f"The old PVC names {old_pvc_names} are not equal to the "
            f"current PVC names {current_pvc_names}"
        )
    if not old_pv_names == current_pv_names:
        raise ResourceWrongStatusException(
            f"The old PV names {old_pv_names} are not equal to the "
            f"current PV names {current_pv_names}"
        )


def check_storage_size_is_reflected(expected_storage_size):
    """
    Check that the expected storage size is reflected in the current storage size, PVCs, PVs,
    and ceph capacity.

    Args:
        expected_storage_size (str): The expected storage size

    Raises:
        StorageSizeNotReflectedException: If the current storage size, PVCs, PVs, and ceph capacity
            are not in the expected size

    """
    logger.info(f"The expected storage size is {expected_storage_size}")

    current_storage_size = get_storage_size()
    logger.info(f"The current storage size is {current_storage_size}")
    logger.info(
        "Check that the current storage size equal to the expected storage size"
    )
    if get_storage_size() != expected_storage_size:
        raise StorageSizeNotReflectedException(
            f"The current storage size {current_storage_size} is not equal "
            f"to the expected size {expected_storage_size}"
        )

    logger.info(
        "Check that the PVC and PV sizes are equal to the expected storage size"
    )
    current_osd_pvcs = get_deviceset_pvcs()
    expected_storage_size_in_gb = convert_device_size(expected_storage_size, "GB")
    pvc_sizes = [pvc.size for pvc in current_osd_pvcs]
    logger.info(f"PVC sizes = {pvc_sizes}")
    if not all([p_size == expected_storage_size_in_gb for p_size in pvc_sizes]):
        raise StorageSizeNotReflectedException(
            f"The PVC sizes are not equal to the expected storage size {expected_storage_size_in_gb}"
        )

    current_osd_pvs = get_deviceset_pvs()
    pv_sizes = [get_pv_size(pv.get()) for pv in current_osd_pvs]
    logger.info(f"PV sizes {pv_sizes}")
    if not all([p_size == expected_storage_size_in_gb for p_size in pv_sizes]):
        raise StorageSizeNotReflectedException(
            f"The PV sizes are not equal to the expected storage size {expected_storage_size_in_gb}"
        )

    ceph_cluster = CephCluster()
    ceph_capacity = ceph_cluster.get_ceph_capacity()
    expected_storage_size_in_gb = convert_device_size(expected_storage_size, "GB")
    logger.info(
        f"Check that the Ceph capacity {ceph_capacity} is equal "
        f"to the expected storage size {expected_storage_size_in_gb}"
    )
    if not int(ceph_capacity) == expected_storage_size_in_gb:
        raise StorageSizeNotReflectedException(
            f"The Ceph capacity {ceph_capacity} is not equal to the "
            f"expected storage size {expected_storage_size_in_gb}"
        )


def check_ceph_state_post_resize_osd():
    """
    Check the Ceph state post resize osd.
    The function checks the Ceph device classes and osd tree.

    Raises:
        CephHealthException: In case the Ceph device classes and osd tree checks
            didn't finish successfully

    """
    logger.info("Check the Ceph device classes and osd tree")
    device_class = get_device_class()
    ct_pod = get_ceph_tools_pod()
    try:
        verify_storage_device_class(device_class)
        verify_device_class_in_osd_tree(ct_pod, device_class)
    except AssertionError as ex:
        raise CephHealthException(ex)
    if not check_ceph_osd_tree():
        raise CephHealthException("The ceph osd tree checks didn't finish successfully")


def base_ceph_verification_steps_post_resize_osd(
    old_osd_pods, old_osd_pvcs, old_osd_pvs, expected_storage_size
):
    """
    Check the Ceph verification steps post resize OSD.
    It will perform the following steps:
    1. Check the resources state post resize OSD
    2. Check the resources size post resize OSD
    3. Check the Ceph state post resize OSD

    Args:
        old_osd_pods (list): The old osd pod objects before resizing the osd
        old_osd_pvcs (list): The old osd PVC objects before resizing the osd
        old_osd_pvs (list): The old osd PV objects before resizing the osd
        expected_storage_size (str): The expected storage size after resizing the osd

    Raises:
        StorageSizeNotReflectedException: If the current storage size, PVCs, PVs, and ceph capacity
            are not in the expected size

    """
    logger.info("Check the resources state post resize OSD")
    check_resources_state_post_resize_osd(old_osd_pods, old_osd_pvcs, old_osd_pvs)
    logger.info("Check the resources size post resize OSD")
    check_storage_size_is_reflected(expected_storage_size)
    logger.info("Check the Ceph state post resize OSD")
    check_ceph_state_post_resize_osd()
    logger.info("All the Ceph verification steps post resize osd finished successfully")


def ceph_verification_steps_post_resize_osd(
    old_osd_pods, old_osd_pvcs, old_osd_pvs, expected_storage_size, num_of_tries=6
):
    """
    Try to execute the function 'base_ceph_verification_steps_post_resize_osd' a number of tries
    until success, ignoring the exception 'StorageSizeNotReflectedException'.
    In every iteration, if we get the exception 'StorageSizeNotReflectedException', it will restart
    the osd pods and try again until it reaches the maximum tries.

    Args:
        old_osd_pods (list): The old osd pod objects before resizing the osd
        old_osd_pvcs (list): The old osd PVC objects before resizing the osd
        old_osd_pvs (list): The old osd PV objects before resizing the osd
        expected_storage_size (str): The expected storage size after resizing the osd
        num_of_tries (int): The number of tries to try executing the
            function 'base_ceph_verification_steps_post_resize_osd'.

    Raises:
        StorageSizeNotReflectedException: If the current storage size, PVCs, PVs, and ceph capacity
            are not in the expected size

    """
    ex = StorageSizeNotReflectedException()
    for i in range(1, num_of_tries + 1):
        try:
            base_ceph_verification_steps_post_resize_osd(
                old_osd_pods, old_osd_pvcs, old_osd_pvs, expected_storage_size
            )
            return
        except StorageSizeNotReflectedException as local_ex:
            ex = local_ex
            logger.warning(
                f"The Ceph verification steps failed due to the error: {str(local_ex)}. "
                f"Try to restart the OSD pods before the next iteration"
            )
            old_osd_pods = get_osd_pods()
            delete_pods(old_osd_pods, wait=False)

    logger.warning(
        f"Failed to complete the Ceph verification steps post resize osd after {num_of_tries} tries"
    )
    raise ex


def check_ceph_health_after_resize_osd(
    ceph_health_tries=40, ceph_rebalance_timeout=900
):
    """
    Check Ceph health after resize osd

    Args:
        ceph_health_tries (int): The number of tries to wait for the Ceph health to be OK.
        ceph_rebalance_timeout (int): The time to wait for the Ceph cluster rebalanced.

    """
    if config.RUN.get("io_in_bg"):
        logger.info(
            "Increase the time to wait for Ceph health to be health OK, "
            "because we run IO in the background"
        )
        additional_ceph_health_tries = int(config.RUN.get("io_load") * 1.3)
        ceph_health_tries += additional_ceph_health_tries

        additional_ceph_rebalance_timeout = int(config.RUN.get("io_load") * 100)
        ceph_rebalance_timeout += additional_ceph_rebalance_timeout

    ceph_health_check(
        namespace=config.ENV_DATA["cluster_namespace"], tries=ceph_health_tries
    )
    ceph_cluster_obj = CephCluster()
    assert ceph_cluster_obj.wait_for_rebalance(
        timeout=ceph_rebalance_timeout
    ), "Data re-balance failed to complete"
