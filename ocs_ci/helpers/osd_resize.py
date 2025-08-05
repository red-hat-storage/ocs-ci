import logging
import pytest

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
from ocs_ci.ocs.resources.pvc import get_deviceset_pvcs, get_deviceset_pvs, get_pvc_size
from ocs_ci.ocs.resources.pv import get_pv_size
from ocs_ci.ocs.resources.storage_cluster import (
    get_storage_size,
    get_device_class,
    verify_storage_device_class,
    verify_device_class_in_osd_tree,
    resize_osd,
)
from ocs_ci.ocs.cluster import check_ceph_osd_tree, CephCluster, check_ceph_osd_df_tree
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.utility.utils import (
    ceph_health_check,
    TimeoutSampler,
    convert_device_size,
    human_to_bytes_ui,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework import config
from ocs_ci.ocs.constants import (
    MAX_RESIZE_OSD,
    AWS_MAX_RESIZE_OSD_COUNT,
    AWS_PLATFORM,
    MAX_TOTAL_CLUSTER_CAPACITY,
    MAX_IBMCLOUD_TOTAL_CLUSTER_CAPACITY,
    ROSA_HCP_PLATFORM,
)


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
        StorageSizeNotReflectedException: If the OSD pods failed to restart
        ResourceWrongStatusException: The old PVC and PV names are not equal to the current PVC and PV names

    """
    old_osd_pods_count = len(old_osd_pods)
    logger.info("Wait for the OSD pods to reach the status Terminated or to be deleted")
    old_osd_pod_names = [p.name for p in old_osd_pods]
    res = wait_for_pods_to_be_in_statuses(
        expected_statuses=[constants.STATUS_TERMINATING],
        pod_names=old_osd_pod_names,
        timeout=900,
        sleep=20,
    )
    if not res:
        raise StorageSizeNotReflectedException(
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

    logger.info("Verify that the new osd pod names are different than the old ones")
    osd_pods = get_osd_pods()
    new_name_set = {p.name for p in osd_pods}
    old_name_set = {p.name for p in old_osd_pods}
    if new_name_set.intersection(old_name_set):
        raise ResourceWrongStatusException(
            f"There are shared values between the new osd pod names and the old osd pod names. "
            f"old osd pod names = {old_name_set}, new osd pod names = {new_name_set}"
        )

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


def check_storage_size_is_reflected(expected_storage_size, expected_ceph_capacity=None):
    """
    Check that the expected storage size is reflected in the current storage size, PVCs, PVs,
    and ceph capacity.

    Args:
        expected_storage_size (str): The expected storage size
        expected_ceph_capacity (int): Expected Ceph raw capacity in GiB

    Raises:
        StorageSizeNotReflectedException: If the current storage size, PVCs, PVs, and ceph capacity
            are not in the expected size

    """
    logger.info(f"The expected storage size is {expected_storage_size}")
    current_storage_size = get_storage_size()
    logger.info(f"The current storage size is {current_storage_size}")

    expected_storage_size_in_gb = convert_device_size(expected_storage_size, "GB", 1024)
    current_storage_size_in_gb = convert_device_size(current_storage_size, "GB", 1024)
    logger.info(
        "Check that the current storage size equal to the expected storage size"
    )
    if current_storage_size_in_gb != expected_storage_size_in_gb:
        raise StorageSizeNotReflectedException(
            f"The current storage size {current_storage_size} is not equal "
            f"to the expected size {expected_storage_size}"
        )

    logger.info(
        "Check that the PVC and PV sizes are equal to the expected storage size"
    )
    current_osd_pvcs = get_deviceset_pvcs()
    pvc_sizes = [get_pvc_size(pvc) for pvc in current_osd_pvcs]
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

    if expected_ceph_capacity:
        check_ceph_capacity_increased(expected_ceph_capacity)


def check_ceph_capacity_increased(
    expected_ceph_capacity,
    tolerance_percent=1.0,
):
    """
    Validates that the current Ceph raw capacity is within the expected range.

    Args:
        expected_ceph_capacity (int): Expected Ceph raw capacity in GiB
        tolerance_percent (float): Acceptable deviation in percentage (default: 1%)

    Raises:
        StorageSizeNotReflectedException: If current capacity is outside the tolerated range

    """
    ceph_cluster = CephCluster()
    current_capacity = ceph_cluster.get_ceph_capacity(replica_divide=False)
    tolerance = expected_ceph_capacity * (tolerance_percent / 100)

    logger.info(f"Expected Ceph capacity: {expected_ceph_capacity}GiB ± {tolerance}GiB")
    logger.info(f"Actual Ceph capacity: {current_capacity}GiB")

    if abs(expected_ceph_capacity - current_capacity) > tolerance:
        raise StorageSizeNotReflectedException(
            f"Ceph capacity {current_capacity}GiB is outside the expected range "
            f"{expected_ceph_capacity}GiB ± {tolerance}GiB"
        )

    logger.info("Ceph capacity is within the expected range.")


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
    if not check_ceph_osd_df_tree():
        raise CephHealthException(
            "The ceph osd df tree output is not formatted correctly"
        )


def base_ceph_verification_steps_post_resize_osd(
    old_osd_pods,
    old_osd_pvcs,
    old_osd_pvs,
    expected_storage_size,
    expected_ceph_capacity=None,
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
        expected_ceph_capacity (int): Expected Ceph raw capacity in GiB after OSD resize

    Raises:
        StorageSizeNotReflectedException: If the current storage size, PVCs, PVs, and ceph capacity
            are not in the expected size

    """
    logger.info("Check the resources state post resize OSD")
    check_resources_state_post_resize_osd(old_osd_pods, old_osd_pvcs, old_osd_pvs)
    logger.info("Check the resources size post resize OSD")
    check_storage_size_is_reflected(expected_storage_size, expected_ceph_capacity)
    logger.info("Check the Ceph state post resize OSD")
    check_ceph_state_post_resize_osd()
    logger.info("All the Ceph verification steps post resize osd finished successfully")


def ceph_verification_steps_post_resize_osd(
    old_osd_pods,
    old_osd_pvcs,
    old_osd_pvs,
    expected_storage_size,
    expected_ceph_capacity=None,
    num_of_tries=6,
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
        expected_ceph_capacity (int): Expected Ceph raw capacity in GiB after OSD resize
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
                old_osd_pods,
                old_osd_pvcs,
                old_osd_pvs,
                expected_storage_size,
                expected_ceph_capacity,
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


def check_resize_osd_pre_conditions(expected_storage_size):
    """
    Check the resize osd pre-conditions:
    1. Check that the current storage size is less than the osd max size
    2. If we use AWS or ROSA HCP platforms, check that the osd resize count is no more
    than the AWS max resize count.

    If the conditions are not met, the test will be skipped.

    Args:
        expected_storage_size (str): The expected storage size for the storage cluster

    """
    expected_storage_size_in_gb = convert_device_size(expected_storage_size, "GB", 1024)
    max_storage_size_in_gb = convert_device_size(MAX_RESIZE_OSD, "GB", 1024)
    if expected_storage_size_in_gb > max_storage_size_in_gb:
        pytest.skip(
            f"The expected storage size {expected_storage_size} is greater than the "
            f"max resize osd {MAX_RESIZE_OSD}"
        )

    if config.ENV_DATA["platform"] == constants.IBMCLOUD_PLATFORM:
        max_cluster_capacity = MAX_IBMCLOUD_TOTAL_CLUSTER_CAPACITY
    else:
        max_cluster_capacity = MAX_TOTAL_CLUSTER_CAPACITY
    max_cluster_capacity_in_gb = convert_device_size(max_cluster_capacity, "GB", 1024)
    expected_cluster_capacity_in_gb = expected_storage_size_in_gb * len(get_osd_pods())
    if expected_cluster_capacity_in_gb > max_cluster_capacity_in_gb:
        pytest.skip(
            f"The expected cluster capacity {expected_cluster_capacity_in_gb}Gi is greater than the "
            f"max cluster capacity {max_cluster_capacity}"
        )

    config.RUN["resize_osd_count"] = config.RUN.get("resize_osd_count", 0)
    logger.info(f"resize osd count = {config.RUN['resize_osd_count']}")
    platforms_to_skip = [AWS_PLATFORM, ROSA_HCP_PLATFORM]
    if (
        config.ENV_DATA["platform"].lower() in platforms_to_skip
        and config.RUN["resize_osd_count"] >= AWS_MAX_RESIZE_OSD_COUNT
    ):
        pytest.skip(
            f"We can resize the osd no more than {AWS_MAX_RESIZE_OSD_COUNT} times when using aws platform"
        )


def update_resize_osd_count(old_storage_size):
    """
    Update the resize osd count

    Args:
        old_storage_size (str): The old storage size before the osd resizing

    """
    old_storage_size_in_gb = convert_device_size(old_storage_size, "GB", 1024)
    new_storage_size_in_gb = convert_device_size(get_storage_size(), "GB", 1024)
    logger.info(
        f"old storage size in GB = {old_storage_size_in_gb}, "
        f"new storage size in GB = {new_storage_size_in_gb}"
    )
    if new_storage_size_in_gb > old_storage_size_in_gb:
        logger.info(
            "The osd size has increased successfully. Increasing the resize osd count by 1"
        )
        config.RUN["resize_osd_count"] = config.RUN.get("resize_osd_count", 0) + 1
    else:
        logger.warning("The osd size has not increased")


def basic_resize_osd(old_storage_size):
    """
    The function perform the basic resize osd scenario. It increases the osd size by multiply 2

    Args:
        old_storage_size (str): The old storagecluster storage size(which represent the old osd size)

    Returns:
        str: The new storage size after increasing the osd size

    """
    logger.info(f"The current osd size is {old_storage_size}")
    size = int(old_storage_size[0:-2])
    size_type = old_storage_size[-2:]
    new_storage_size = f"{size * 2}{size_type}"
    logger.info(f"Increase the osd size to {new_storage_size}")
    resize_osd(new_storage_size)
    return new_storage_size


def check_storage_size_is_reflected_in_ui():
    """
    Check that the current total storage size is reflected in the
    UI 'ocs-storagecluster-storagesystem' page.

    """
    block_and_file = (
        PageNavigator()
        .nav_odf_default_page()
        .nav_storage_systems_tab()
        .nav_storagecluster_storagesystem_details()
        .nav_block_and_file()
    )
    used, available = block_and_file.get_raw_capacity_card_values()
    block_and_file.take_screenshot("raw_capacity_card_values")
    # Get the used, available and total size in bytes
    used_size_bytes = human_to_bytes_ui(used)
    available_size_bytes = human_to_bytes_ui(available)
    total_size_bytes = used_size_bytes + available_size_bytes

    # Convert the used, available and total size to GB
    bytes_to_gb = 1024**3
    used_size_gb = used_size_bytes / bytes_to_gb
    available_size_gb = available_size_bytes / bytes_to_gb
    total_size_gb = round(total_size_bytes / bytes_to_gb)
    logger.info(f"Used size = {used_size_gb}Gi")
    logger.info(f"Available size = {available_size_gb}Gi")
    logger.info(f"Total size = {total_size_gb}Gi")

    ceph_cluster = CephCluster()
    ceph_capacity = int(ceph_cluster.get_ceph_capacity(replica_divide=False))

    # There could be a small gap between the total size in the UI and the actual Ceph total size.
    # So, instead of checking the accurate size, we check that the total size is within the expected range.
    max_gap = 6 if ceph_capacity < 1500 else 12
    expected_total_size_range_gb = range(
        ceph_capacity - max_gap, ceph_capacity + max_gap
    )
    logger.info(
        f"Check that the total UI size {total_size_gb}Gi is in the "
        f"expected total size range {expected_total_size_range_gb}Gi"
    )
    assert total_size_gb in expected_total_size_range_gb, (
        f"The total UI size {total_size_gb}Gi is not in the "
        f"expected total size range {expected_total_size_range_gb}Gi"
    )
