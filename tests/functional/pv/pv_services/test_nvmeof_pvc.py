import logging
import subprocess
from time import sleep

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    ignore_leftovers,
    skipif_no_nvmeof,
    tier1,
    tier4a,
    polarion_id,
)
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.framework import config
from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants, node
from ocs_ci.ocs.exceptions import CommandFailed, TimeoutExpiredError
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources import pod, storage_cluster
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)

# Device path exposed inside the pod for the raw block (volumeMode: Block) PVC
NVMEOF_RAW_BLOCK_DEVICE = "/dev/xvda"
# Amount of raw data written to and read back from the block device
RAW_BLOCK_IO_SIZE_MIB = 100
# In-pod path the raw data is generated into before writing it to the device
RAW_BLOCK_SOURCE_PATH = "/tmp/source_data"
# In-pod path the raw data is read back to
RAW_BLOCK_READBACK_PATH = "/tmp/readback"

# Size of the data files written before and after the gateway outage
SCALE_DATA_SIZE_MIB = 50
# Runtime of the continuous fio, long enough to span the gateway outage
SCALE_FIO_RUNTIME = 600
# Time the new PVC is observed to confirm it is not provisioned while the
# gateways are scaled to 0
PENDING_PVC_OBSERVATION_TIME = 60

# Event messages reported when a RWO volume is already attached to a pod on
# another node. kubernetes/kubernetes#138837 updated the FailedAttachVolume
# event message, hence both the old and the new wording are accepted.
MULTI_ATTACH_FAILURE_STRS = (
    "Multi-Attach error for volume",
    "Waiting for detach for volume",
)
# Time the second pod of the RWO test is observed to confirm it does not start
# while the first pod still holds the volume
RWO_ENFORCEMENT_OBSERVATION_TIME = 60

# Resource requirements set on the NVMe-oF gateway to make the ocs-operator
# roll out new gateway pods
UPDATE_GATEWAY_RESOURCES = {"requests": {"cpu": "1", "memory": "4Gi"}}
# Time to wait for all the gateway pods to be replaced by the rollout
GATEWAY_ROLLOUT_TIMEOUT = 900
# Interval between two observations of the consumer IO during the rollout
GATEWAY_ROLLOUT_OBSERVATION_INTERVAL = 30


def verify_pv_reclaim(pv_obj, reclaim_policy):
    """
    Verify that a PV is reclaimed according to its reclaim policy.

    For the Delete policy the PV is expected to be removed. For the Retain
    policy the PV is expected to become Released, and is then cleaned up by
    switching its reclaim policy to Delete so that no leftovers remain.

    Args:
        pv_obj (OCS): The PV object backing the deleted PVC
        reclaim_policy (str): The reclaim policy of the PV

    """
    pv_name = pv_obj.name
    if reclaim_policy == constants.RECLAIM_POLICY_DELETE:
        pv_obj.ocp.wait_for_delete(resource_name=pv_name, timeout=180)
        logger.assertion("PV %s deleted as per Delete reclaim policy", pv_name)
    elif reclaim_policy == constants.RECLAIM_POLICY_RETAIN:
        helpers.wait_for_resource_state(pv_obj, constants.STATUS_RELEASED, timeout=180)
        logger.assertion("PV %s is Released as per Retain reclaim policy", pv_name)
        # Cleanup the retained PV so no leftovers remain. Switch the reclaim
        # policy to Delete so the controller removes the backing volume too,
        # then wait for controller-driven deletion (pvc_factory_fixture pattern).
        patch_param = '{"spec":{"persistentVolumeReclaimPolicy":"Delete"}}'
        pv_obj.ocp.patch(resource_name=pv_name, params=patch_param)
        pv_obj.ocp.wait_for_delete(resource_name=pv_name, timeout=180)
    else:
        pytest.fail(f"Unexpected reclaim policy {reclaim_policy} for PV {pv_name}")


def md5sum_of_device_head(pod_obj, device_path, size_mib):
    """
    Calculate the md5sum of the first size_mib MiB of a raw block device.

    The whole device cannot be compared against the data written by the test
    because only its beginning holds the written data, hence the read is
    limited to the written range. Direct IO is used so that the data is read
    from the device and not from the page cache.

    Args:
        pod_obj (Pod): The pod the block device is attached to
        device_path (str): The devicePath of the block device in the pod
        size_mib (int): Size of the range to read, in MiB

    Returns:
        str: The md5sum of the read range

    """
    md5sum_out = pod_obj.exec_sh_cmd_on_pod(
        command=(
            f"dd if={device_path} bs=1M count={size_mib} iflag=direct 2>/dev/null "
            "| md5sum"
        )
    )
    md5sum = md5sum_out.split()[0]
    logger.info("md5sum of the first %s MiB of %s: %s", size_mib, device_path, md5sum)
    return md5sum


def log_pod_events(pod_obj):
    """
    Log the events of a pod. Used to record the observed behaviour of a pod
    during a disruption, no assertion is made on the events.

    Args:
        pod_obj (Pod): The pod to log the events of

    """
    event_ocp = OCP(kind="Event", namespace=pod_obj.namespace)
    events = event_ocp.get(
        field_selector=f"involvedObject.name={pod_obj.name}",
    )["items"]
    if not events:
        logger.info("No events recorded for pod %s", pod_obj.name)
        return
    for event in events:
        logger.info(
            "Event on pod %s: type=%s reason=%s message=%s",
            pod_obj.name,
            event.get("type"),
            event.get("reason"),
            event.get("message"),
        )


def is_pod_io_responsive(pod_obj, timeout=120):
    """
    Check whether the volume mounted in the pod still serves IO.

    A pod whose NVMe-oF volume lost its gateway does not necessarily fail, its
    IO can also block, hence the probe is bounded by a timeout and both a
    failing and a blocking command mean that IO is not served.

    Args:
        pod_obj (Pod): The pod to probe
        timeout (int): Time in seconds to wait for the probe to complete

    Returns:
        bool: True if the probe wrote to the volume, False otherwise

    """
    probe_path = pod.get_file_path(pod_obj, "io_probe")
    try:
        pod_obj.exec_sh_cmd_on_pod(
            command=(
                f"dd if=/dev/urandom of={probe_path} bs=1M count=1 oflag=direct "
                f"&& sync && rm -f {probe_path}"
            ),
            timeout=timeout,
        )
    except (CommandFailed, subprocess.TimeoutExpired) as ex:
        logger.warning("IO is not served by pod %s: %s", pod_obj.name, ex)
        return False
    return True


def verify_nvmeof_consumer_healthy(pod_obj, file_name, md5sum):
    """
    Verify that a pod consuming an NVMe-oF volume still serves IO and that the
    data written to the volume earlier is intact.

    Args:
        pod_obj (Pod): The pod consuming the NVMe-oF volume
        file_name (str): The name of the file to verify
        md5sum (str): The md5sum the file is expected to have

    """
    assert is_pod_io_responsive(
        pod_obj
    ), f"Pod {pod_obj.name} does not serve IO on its NVMe-oF volume"
    assert pod.verify_data_integrity(
        pod_obj, file_name, md5sum
    ), f"Data of file {file_name} is corrupted on pod {pod_obj.name}"


def get_nvmeof_gateway_pods(statuses=None):
    """
    Get the NVMe-oF gateway pods.

    Args:
        statuses (list): If given, only pods in one of these statuses are
            returned

    Returns:
        list: Info of the NVMe-oF gateway pods

    """
    return pod.get_pods_having_label(
        label=constants.NVMEOF_APP_LABEL,
        namespace=config.ENV_DATA["cluster_namespace"],
        statuses=statuses,
    )


def monitor_nvmeof_gateway_rollout(pod_obj, initial_uids, expected_count):
    """
    Wait for every NVMe-oF gateway pod to be replaced by a new one while
    observing the IO of a consumer pod.

    The IO of a consumer can pause while the gateway it is connected to is
    being replaced, so the observations are only logged. The health of the
    consumer is verified once the rollout is complete.

    Args:
        pod_obj (Pod): The consumer pod to observe during the rollout
        initial_uids (set): UIDs of the gateway pods before the rollout
        expected_count (int): The number of gateway pods expected at the end
            of the rollout

    Raises:
        TimeoutExpiredError: If the gateway pods were not all replaced within
            GATEWAY_ROLLOUT_TIMEOUT

    """
    try:
        for gateway_pods in TimeoutSampler(
            timeout=GATEWAY_ROLLOUT_TIMEOUT,
            sleep=GATEWAY_ROLLOUT_OBSERVATION_INTERVAL,
            func=get_nvmeof_gateway_pods,
            statuses=[constants.STATUS_RUNNING],
        ):
            current_uids = {
                gateway_pod["metadata"]["uid"] for gateway_pod in gateway_pods
            }
            io_served = is_pod_io_responsive(pod_obj, timeout=60)
            logger.info(
                "Gateway rollout in progress: %s running pods, %s of them "
                "new, IO served by pod %s: %s",
                len(gateway_pods),
                len(current_uids - initial_uids),
                pod_obj.name,
                io_served,
            )
            if len(gateway_pods) == expected_count and not current_uids & initial_uids:
                logger.info("All %s NVMe-oF gateway pods were replaced", expected_count)
                return
    except TimeoutExpiredError:
        raise TimeoutExpiredError(
            GATEWAY_ROLLOUT_TIMEOUT,
            custom_message=(
                "The NVMe-oF gateway pods were not all replaced within "
                f"{GATEWAY_ROLLOUT_TIMEOUT}s. Either the rollout is stuck or "
                "the change of the gateway resource requirements on the "
                "StorageCluster is not propagated to the gateway by this "
                "version of the operator."
            ),
        )


@pytest.fixture(autouse=True)
def nvmeof_prerequisites():
    """
    Verify NVMe-oF prerequisites before running the test:
        - NVMe-oF Gateway pods are deployed and healthy (Running).
        - NVMe-oF StorageClass exists.

    """
    namespace = config.ENV_DATA["cluster_namespace"]

    # NVMe-oF StorageClass must exist
    sc_ocp_obj = OCP(kind=constants.STORAGECLASS, namespace=namespace)
    assert sc_ocp_obj.is_exist(resource_name=constants.CEPH_NVMEOF_SC), (
        f"NVMe-oF StorageClass {constants.CEPH_NVMEOF_SC} does not exist. "
        "Ensure the StorageCluster was deployed with nvmeof enabled."
    )
    logger.assertion("NVMe-oF StorageClass %s exists", constants.CEPH_NVMEOF_SC)

    # NVMe-oF Gateway pods must be deployed and healthy
    gateway_pods = pod.get_pods_having_label(
        label=constants.NVMEOF_APP_LABEL, namespace=namespace
    )
    assert gateway_pods, (
        "No NVMe-oF Gateway pods found with label "
        f"{constants.NVMEOF_APP_LABEL} in namespace {namespace}"
    )
    gateway_pod_names = [pod_data["metadata"]["name"] for pod_data in gateway_pods]
    logger.info("Found NVMe-oF Gateway pods: %s", gateway_pod_names)
    assert pod.wait_for_pods_to_be_running(
        namespace=namespace, pod_names=gateway_pod_names, timeout=300
    ), "NVMe-oF Gateway pods are not in Running state"
    logger.assertion("All NVMe-oF Gateway pods are healthy (Running)")


@pytest.fixture()
def nvmeof_storageclass():
    """
    Return the existing NVMe-oF StorageClass as an OCS object so that it can
    be consumed by the pvc_factory fixture.

    Returns:
        OCS: OCS instance of the NVMe-oF StorageClass

    """
    sc_ocp_obj = OCP(
        kind=constants.STORAGECLASS,
        namespace=config.ENV_DATA["cluster_namespace"],
        resource_name=constants.CEPH_NVMEOF_SC,
    )
    return OCS(**sc_ocp_obj.get())


@green_squad
@tier1
@skipif_no_nvmeof
class TestNvmeofPvc(ManageTest):
    """
    Tests for basic PVC lifecycle and data integrity using the NVMe-oF
    (NVMe over Fabrics) StorageClass.
    """

    @polarion_id("OCS-8237")
    def test_nvmeof_pvc_data_integrity_and_reclaim(
        self, nvmeof_storageclass, pvc_factory, pod_factory
    ):
        """
        Verify PVC lifecycle and data integrity on the NVMe-oF StorageClass.

        Steps:
            1. Create a RWO PVC using the NVMe-oF StorageClass and verify it
               reaches the Bound state.
            2. Create a pod that mounts the PVC, write data with fio and verify
               data integrity by comparing md5sum after re-reading.
            3. Delete the pod and the PVC.
            4. Verify the PV is reclaimed according to its reclaim policy.

        """
        # Step 1: Create a RWO PVC using the NVMe-oF StorageClass
        logger.test_step(
            "Create a RWO PVC using StorageClass %s", constants.CEPH_NVMEOF_SC
        )
        pvc_obj = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            storageclass=nvmeof_storageclass,
            size=5,
            access_mode=constants.ACCESS_MODE_RWO,
            status=constants.STATUS_BOUND,
        )
        logger.assertion("PVC %s reached Bound state", pvc_obj.name)

        # Capture PV details and reclaim policy before deletion
        pv_obj = pvc_obj.backed_pv_obj
        pv_name = pv_obj.name
        reclaim_policy = pvc_obj.reclaim_policy
        logger.info(
            "PVC %s is backed by PV %s with reclaim policy %s",
            pvc_obj.name,
            pv_name,
            reclaim_policy,
        )

        # Step 2: Create a pod mounting the PVC, run IO and verify data integrity
        logger.test_step(
            "Create a pod mounting PVC %s, run IO and verify data integrity",
            pvc_obj.name,
        )
        pod_obj = pod_factory(
            interface=constants.CEPHBLOCKPOOL,
            pvc=pvc_obj,
            status=constants.STATUS_RUNNING,
        )
        logger.info("Pod %s is running and mounts PVC %s", pod_obj.name, pvc_obj.name)

        file_name = pod_obj.name
        logger.info("Running fio on pod %s to write file %s", pod_obj.name, file_name)
        pod_obj.run_io(
            storage_type="fs",
            size="1G",
            io_direction="write",
            fio_filename=file_name,
            end_fsync=1,
        )
        fio_result = pod_obj.get_fio_results()
        err_count = fio_result.get("jobs")[0].get("error")
        assert (
            err_count == 0
        ), f"IO error on pod {pod_obj.name}. FIO result: {fio_result}"
        logger.info("fio completed successfully on pod %s", pod_obj.name)

        # Calculate md5sum of the written file and verify data integrity on re-read
        original_md5sum = pod.cal_md5sum(pod_obj, file_name)
        assert pod.verify_data_integrity(
            pod_obj, file_name, original_md5sum
        ), f"Data integrity check failed for file {file_name} on pod {pod_obj.name}"
        logger.assertion("Data integrity verified on pod %s", pod_obj.name)

        # Step 3: Delete the pod, then the PVC
        logger.test_step("Delete pod %s and PVC %s", pod_obj.name, pvc_obj.name)
        pod_obj.delete()
        pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)

        logger.info("Deleting PVC %s", pvc_obj.name)
        pvc_obj.delete()
        pvc_obj.ocp.wait_for_delete(resource_name=pvc_obj.name)

        # Step 4: Verify the PV is reclaimed according to its reclaim policy
        logger.test_step(
            "Verify PV %s is reclaimed according to its %s reclaim policy",
            pv_name,
            reclaim_policy,
        )
        verify_pv_reclaim(pv_obj, reclaim_policy)

    def test_nvmeof_raw_block_pvc(self, nvmeof_storageclass, pvc_factory, pod_factory):
        """
        Verify that an NVMe-oF PVC can be consumed as a raw block device and
        that the raw data written to it survives a pod restart.

        Steps:
            1. Prerequisites (NVMe-oF Gateway deployed, StorageClass exists)
               are verified by the nvmeof_prerequisites fixture.
            2. Create a PVC with volumeMode Block and accessMode RWO using the
               NVMe-oF StorageClass.
            3. Create a pod with a volumeDevices entry (not volumeMounts)
               referencing the PVC, and verify the pod is Running and the
               device exists.
            4. Write raw data to the device, read it back and compare the
               checksums.
            5. Delete the pod, create a new pod consuming the same PVC as a
               block device and verify the data persisted.
            6. Delete the pod and the PVC and verify the PV is reclaimed.

        """
        # Step 2: Create a RWO raw block PVC using the NVMe-oF StorageClass
        logger.test_step(
            "Create a RWO PVC with volumeMode %s using StorageClass %s",
            constants.VOLUME_MODE_BLOCK,
            constants.CEPH_NVMEOF_SC,
        )
        pvc_obj = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            storageclass=nvmeof_storageclass,
            size=5,
            access_mode=constants.ACCESS_MODE_RWO,
            volume_mode=constants.VOLUME_MODE_BLOCK,
            status=constants.STATUS_BOUND,
        )
        assert (
            pvc_obj.get()["spec"]["volumeMode"] == constants.VOLUME_MODE_BLOCK
        ), f"PVC {pvc_obj.name} was not created with volumeMode Block"
        logger.assertion("Raw block PVC %s reached Bound state", pvc_obj.name)

        # Capture PV details and reclaim policy before deletion
        pv_obj = pvc_obj.backed_pv_obj
        pv_name = pv_obj.name
        reclaim_policy = pvc_obj.reclaim_policy
        logger.info(
            "PVC %s is backed by PV %s with reclaim policy %s",
            pvc_obj.name,
            pv_name,
            reclaim_policy,
        )

        # Step 3: Create a pod consuming the PVC as a raw block device
        logger.test_step(
            "Create a pod consuming PVC %s as a block device at %s",
            pvc_obj.name,
            NVMEOF_RAW_BLOCK_DEVICE,
        )
        pod_obj = pod_factory(
            interface=constants.CEPHBLOCKPOOL,
            pvc=pvc_obj,
            pod_dict_path=constants.CSI_RBD_RAW_BLOCK_POD_YAML,
            raw_block_pv=True,
            raw_block_device=NVMEOF_RAW_BLOCK_DEVICE,
            status=constants.STATUS_RUNNING,
        )
        logger.info("Pod %s is running", pod_obj.name)

        # The PVC has to be consumed via volumeDevices, not volumeMounts.
        # Only the volume backing the PVC is checked because the container also
        # carries the automatically injected service account token mount.
        pod_spec = pod_obj.get()["spec"]
        pvc_volume_name = next(
            volume["name"]
            for volume in pod_spec["volumes"]
            if volume.get("persistentVolumeClaim", {}).get("claimName") == pvc_obj.name
        )
        container = pod_spec["containers"][0]
        mounted_volume_names = [
            volume_mount["name"] for volume_mount in container.get("volumeMounts", [])
        ]
        device_volume_names = [
            volume_device["name"]
            for volume_device in container.get("volumeDevices", [])
        ]
        assert pvc_volume_name in device_volume_names, (
            f"Pod {pod_obj.name} does not consume PVC {pvc_obj.name} via "
            "volumeDevices"
        )
        assert pvc_volume_name not in mounted_volume_names, (
            f"Pod {pod_obj.name} consumes PVC {pvc_obj.name} via volumeMounts "
            "instead of volumeDevices"
        )
        device_path = pod.get_device_path(pod_obj)
        assert device_path == NVMEOF_RAW_BLOCK_DEVICE, (
            f"Pod {pod_obj.name} exposes the device at {device_path} instead "
            f"of {NVMEOF_RAW_BLOCK_DEVICE}"
        )
        logger.assertion(
            "Pod %s consumes PVC %s via volumeDevices at %s",
            pod_obj.name,
            pvc_obj.name,
            device_path,
        )

        # The device has to be present in the pod as a block special file
        device_check = pod_obj.exec_sh_cmd_on_pod(
            command=f"test -b {device_path} && echo present || echo missing"
        )
        assert (
            "present" in device_check
        ), f"Block device {device_path} does not exist in pod {pod_obj.name}"
        logger.assertion("Block device %s exists in pod %s", device_path, pod_obj.name)

        # Step 4: Write raw data to the device, read it back and compare
        logger.test_step(
            "Write %s MiB of raw data to %s and verify the readback",
            RAW_BLOCK_IO_SIZE_MIB,
            device_path,
        )
        # The random data is generated into a file first so that the device
        # and the readback can both be compared against the data that was
        # actually written. Comparing them with each other would pass even if
        # the device returned wrong data consistently.
        pod_obj.exec_sh_cmd_on_pod(
            command=(
                f"dd if=/dev/urandom of={RAW_BLOCK_SOURCE_PATH} bs=1M "
                f"count={RAW_BLOCK_IO_SIZE_MIB} && sync"
            )
        )
        source_md5sum = pod.cal_md5sum(pod_obj, RAW_BLOCK_SOURCE_PATH, raw_path=True)
        pod_obj.exec_sh_cmd_on_pod(
            command=(
                f"dd if={RAW_BLOCK_SOURCE_PATH} of={device_path} bs=1M "
                f"count={RAW_BLOCK_IO_SIZE_MIB} oflag=direct"
            )
        )
        pod_obj.exec_sh_cmd_on_pod(
            command=(
                f"dd if={device_path} of={RAW_BLOCK_READBACK_PATH} bs=1M "
                f"count={RAW_BLOCK_IO_SIZE_MIB} iflag=direct"
            )
        )
        readback_md5sum = pod.cal_md5sum(
            pod_obj, RAW_BLOCK_READBACK_PATH, raw_path=True
        )
        assert readback_md5sum == source_md5sum, (
            f"Data read back from {device_path} on pod {pod_obj.name} does not "
            f"match the data written to it. Written md5sum: {source_md5sum}, "
            f"readback md5sum: {readback_md5sum}"
        )
        device_md5sum = md5sum_of_device_head(
            pod_obj, device_path, RAW_BLOCK_IO_SIZE_MIB
        )
        assert device_md5sum == source_md5sum, (
            f"The first {RAW_BLOCK_IO_SIZE_MIB} MiB of {device_path} on pod "
            f"{pod_obj.name} do not match the data written to it. Written "
            f"md5sum: {source_md5sum}, device md5sum: {device_md5sum}"
        )
        logger.assertion(
            "Raw data read back from %s matches the data written to it", device_path
        )

        # Step 5: Delete the pod and verify the data persists on a new pod
        logger.test_step(
            "Delete pod %s and verify the data persists on a new pod",
            pod_obj.name,
        )
        pod_obj.delete()
        pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)

        new_pod_obj = pod_factory(
            interface=constants.CEPHBLOCKPOOL,
            pvc=pvc_obj,
            pod_dict_path=constants.CSI_RBD_RAW_BLOCK_POD_YAML,
            raw_block_pv=True,
            raw_block_device=NVMEOF_RAW_BLOCK_DEVICE,
            status=constants.STATUS_RUNNING,
        )
        logger.info(
            "Pod %s is running and consumes PVC %s as a block device",
            new_pod_obj.name,
            pvc_obj.name,
        )

        new_pod_obj.exec_sh_cmd_on_pod(
            command=(
                f"dd if={device_path} of={RAW_BLOCK_READBACK_PATH} bs=1M "
                f"count={RAW_BLOCK_IO_SIZE_MIB} iflag=direct"
            )
        )
        persisted_md5sum = pod.cal_md5sum(
            new_pod_obj, RAW_BLOCK_READBACK_PATH, raw_path=True
        )
        assert persisted_md5sum == source_md5sum, (
            f"Data on {device_path} did not persist across pod recreation. "
            f"Expected md5sum: {source_md5sum}, actual: {persisted_md5sum}"
        )
        logger.assertion(
            "Raw data on PVC %s persisted after pod recreation", pvc_obj.name
        )

        # Step 6: Delete the pod and the PVC, then verify the PV is reclaimed
        logger.test_step("Delete pod %s and PVC %s", new_pod_obj.name, pvc_obj.name)
        new_pod_obj.delete()
        new_pod_obj.ocp.wait_for_delete(resource_name=new_pod_obj.name)

        logger.info("Deleting PVC %s", pvc_obj.name)
        pvc_obj.delete()
        pvc_obj.ocp.wait_for_delete(resource_name=pvc_obj.name)

        logger.test_step(
            "Verify PV %s is reclaimed according to its %s reclaim policy",
            pv_name,
            reclaim_policy,
        )
        verify_pv_reclaim(pv_obj, reclaim_policy)

    def test_nvmeof_rwo_access_mode_enforcement(
        self, nvmeof_storageclass, pvc_factory, pod_factory
    ):
        """
        Verify that a RWO PVC on the NVMe-oF StorageClass is attached to a
        single pod at a time.

        Steps:
            1. Prerequisites (NVMe-oF Gateway deployed, StorageClass exists)
               are verified by the nvmeof_prerequisites fixture.
            2. Create a RWO PVC and a pod-A mounting it on the first worker
               node, and verify pod-A is Running.
            3. Create a pod-B mounting the same PVC on a second worker node
               and verify it cannot start.
            4. Delete pod-A and verify pod-B starts and mounts the PVC.

        """
        worker_nodes = node.get_worker_nodes()
        if len(worker_nodes) < 2:
            pytest.skip(
                "Enforcement of the RWO access mode can only be verified with "
                f"pods on two different nodes, the cluster has "
                f"{len(worker_nodes)} worker node(s)"
            )

        # Step 2: RWO PVC mounted by pod-A
        logger.test_step(
            "Create a RWO PVC using StorageClass %s and a pod mounting it on "
            "node %s",
            constants.CEPH_NVMEOF_SC,
            worker_nodes[0],
        )
        pvc_obj = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            storageclass=nvmeof_storageclass,
            size=5,
            access_mode=constants.ACCESS_MODE_RWO,
            status=constants.STATUS_BOUND,
        )
        pod_a_obj = pod_factory(
            interface=constants.CEPHBLOCKPOOL,
            pvc=pvc_obj,
            status=constants.STATUS_RUNNING,
            node_name=worker_nodes[0],
        )
        logger.assertion(
            "Pod %s is Running and mounts PVC %s", pod_a_obj.name, pvc_obj.name
        )

        # Data written by pod-A, used to verify the mount of pod-B later on
        file_name = pod_a_obj.name
        pod_a_obj.run_io(
            storage_type="fs",
            size="1G",
            io_direction="write",
            fio_filename=file_name,
            end_fsync=1,
        )
        fio_result = pod_a_obj.get_fio_results()
        err_count = fio_result.get("jobs")[0].get("error")
        assert (
            err_count == 0
        ), f"IO error on pod {pod_a_obj.name}. FIO result: {fio_result}"
        md5sum_pod_a = pod.cal_md5sum(pod_a_obj, file_name)

        # Step 3: pod-B on another node must not get the volume attached
        logger.test_step(
            "Create a second pod mounting PVC %s on node %s and verify it "
            "cannot start",
            pvc_obj.name,
            worker_nodes[1],
        )
        pod_b_obj = pod_factory(
            interface=constants.CEPHBLOCKPOOL,
            pvc=pvc_obj,
            status=None,
            node_name=worker_nodes[1],
        )
        node_pod_a = pod_a_obj.get()["spec"]["nodeName"]
        node_pod_b = pod_b_obj.get()["spec"]["nodeName"]
        assert node_pod_a != node_pod_b, (
            f"Pods {pod_a_obj.name} and {pod_b_obj.name} are both scheduled on "
            f"node {node_pod_a}, the RWO access mode is only enforced across "
            "nodes"
        )

        helpers.wait_for_resource_state(
            resource=pod_b_obj,
            state=constants.STATUS_CONTAINER_CREATING,
            timeout=120,
        )
        helpers.verify_expected_failure_event(
            ocs_obj=pod_b_obj, failure_strs=MULTI_ATTACH_FAILURE_STRS
        )

        # The failure event alone only proves that the attach was rejected at
        # some point. Observe pod B for a bounded period, with pod A still
        # running and holding the volume, to assert that it really does not
        # start.
        sleep(RWO_ENFORCEMENT_OBSERVATION_TIME)
        pod_a_status = pod_a_obj.ocp.get_resource_status(pod_a_obj.name)
        assert pod_a_status == constants.STATUS_RUNNING, (
            f"Pod {pod_a_obj.name} is in {pod_a_status} state, it has to stay "
            f"Running and hold PVC {pvc_obj.name} for the enforcement of the "
            "RWO access mode to be verified"
        )
        pod_b_status = pod_b_obj.ocp.get_resource_status(pod_b_obj.name)
        assert pod_b_status != constants.STATUS_RUNNING, (
            f"Pod {pod_b_obj.name} reached the Running state on node "
            f"{node_pod_b} after {RWO_ENFORCEMENT_OBSERVATION_TIME}s while pod "
            f"{pod_a_obj.name} still holds PVC {pvc_obj.name} on node "
            f"{node_pod_a}. The RWO access mode is not enforced across nodes"
        )
        logger.assertion(
            "Pod %s stays in %s state and cannot mount PVC %s while it is "
            "attached to pod %s",
            pod_b_obj.name,
            pod_b_status,
            pvc_obj.name,
            pod_a_obj.name,
        )

        # Step 4: pod-B takes the volume over once pod-A is gone
        logger.test_step(
            "Delete pod %s and verify pod %s mounts PVC %s",
            pod_a_obj.name,
            pod_b_obj.name,
            pvc_obj.name,
        )
        pod_a_obj.delete()
        pod_a_obj.ocp.wait_for_delete(resource_name=pod_a_obj.name)

        helpers.wait_for_resource_state(
            resource=pod_b_obj, state=constants.STATUS_RUNNING, timeout=300
        )
        assert pod.verify_data_integrity(pod_b_obj, file_name, md5sum_pod_a), (
            f"Data written by pod {pod_a_obj.name} is not readable by pod "
            f"{pod_b_obj.name}"
        )
        logger.assertion(
            "Pod %s is Running and reads the data of PVC %s",
            pod_b_obj.name,
            pvc_obj.name,
        )


@green_squad
@tier4a
@ignore_leftovers
@skipif_no_nvmeof
class TestNvmeofGatewayScaling(ManageTest):
    """
    Tests for changing the replica count of the NVMe-oF Gateway, covering the
    behaviour of NVMe-oF consumers while the gateway is scaled down and their
    recovery once it is scaled back up.
    """

    @pytest.fixture()
    def nvmeof_gateway_instances(
        self, request, project_factory, pvc_factory, pod_factory, teardown_factory
    ):
        """
        Return the configured number of NVMe-oF gateway instances and restore
        it after the test, so that a failure in the middle of the test does not
        leave the cluster without gateways.

        The resource factories are requested, although they are not used here,
        so that they are set up before this fixture and therefore finalized
        after it. The gateways have to be restored before the pods and the PVCs
        are deleted, their volumes cannot be detached while the gateways are
        scaled down.

        Returns:
            int: The number of gateway instances configured before the test

        """
        original_instances = storage_cluster.get_nvmeof_gateway_instances()

        def finalizer():
            if storage_cluster.get_nvmeof_gateway_instances() != original_instances:
                logger.info(
                    "Restoring the NVMe-oF gateway to %s instances",
                    original_instances,
                )
                storage_cluster.scale_nvmeof_gateway(original_instances)

        request.addfinalizer(finalizer)
        return original_instances

    def test_nvmeof_gateway_scale_to_zero(
        self,
        nvmeof_gateway_instances,
        nvmeof_storageclass,
        project_factory,
        pvc_factory,
        pod_factory,
        teardown_factory,
    ):
        """
        Verify how NVMe-oF consumers behave while the gateway is scaled to 0
        and that they recover once it is scaled back up.

        Steps:
            1. Verify the gateway is deployed with at least 2 instances and
               start a pod with continuous fio on an NVMe-oF PVC.
            2. Scale the gateway to 0 and wait for all gateway pods to
               terminate.
            3. Observe the IO behaviour of the running pod, its events and the
               state of fio.
            4. Create a new PVC on the NVMe-oF StorageClass and verify it stays
               Pending while the gateways are down.
            5. Scale the gateway back to its original number of instances and
               wait for the gateway pods to be Running.
            6. Verify the pod serves IO again, restarting it if needed, write
               new data and read back the old and the new data.
            7. Verify the PVC from step 4 transitions to Bound.

        """
        # Step 1: Gateway with 2 instances and a pod running continuous IO
        logger.test_step(
            "Verify the NVMe-oF gateway is deployed with at least 2 instances "
            "and start continuous IO on an NVMe-oF PVC"
        )
        assert nvmeof_gateway_instances >= 2, (
            "The test requires an NVMe-oF gateway with at least 2 instances, "
            f"found {nvmeof_gateway_instances}"
        )
        logger.info(
            "NVMe-oF gateway is configured with %s instances",
            nvmeof_gateway_instances,
        )

        project_obj = project_factory()
        pvc_obj = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            project=project_obj,
            storageclass=nvmeof_storageclass,
            size=5,
            access_mode=constants.ACCESS_MODE_RWO,
            status=constants.STATUS_BOUND,
        )
        pod_obj = pod_factory(
            interface=constants.CEPHBLOCKPOOL,
            pvc=pvc_obj,
            status=constants.STATUS_RUNNING,
        )

        # Data written before the outage, verified again after the recovery
        pre_scale_file = "pre_scale_data"
        pre_scale_path = pod.get_file_path(pod_obj, pre_scale_file)
        pod_obj.exec_sh_cmd_on_pod(
            command=(
                f"dd if=/dev/urandom of={pre_scale_path} bs=1M "
                f"count={SCALE_DATA_SIZE_MIB} oflag=direct && sync"
            )
        )
        pre_scale_md5sum = pod.cal_md5sum(pod_obj, pre_scale_file)

        # Continuous IO spanning the outage. fio runs in a background thread,
        # its results are collected after the gateways are back.
        pod_obj.run_io(
            storage_type="fs",
            size="1G",
            io_direction="rw",
            runtime=SCALE_FIO_RUNTIME,
            fio_filename="fio_continuous",
        )
        logger.assertion(
            "Pod %s runs continuous IO on PVC %s", pod_obj.name, pvc_obj.name
        )

        # Step 2: Scale the gateway to 0
        logger.test_step("Scale the NVMe-oF gateway to 0 instances")
        storage_cluster.scale_nvmeof_gateway(0)
        assert not pod.get_pods_having_label(
            label=constants.NVMEOF_APP_LABEL,
            namespace=config.ENV_DATA["cluster_namespace"],
        ), "NVMe-oF gateway pods are still present after scaling to 0"
        logger.assertion("All NVMe-oF gateway pods are terminated")

        # Step 3: Observe the IO behaviour of the running pod
        logger.test_step(
            "Observe the IO behaviour of pod %s while the gateways are down",
            pod_obj.name,
        )
        pod_obj.reload()
        logger.info(
            "Pod %s is in %s state while the gateways are scaled to 0",
            pod_obj.name,
            pod_obj.ocp.get_resource_status(pod_obj.name),
        )
        logger.info(
            "fio on pod %s finished: %s", pod_obj.name, pod_obj.fio_thread.done()
        )
        log_pod_events(pod_obj)
        # Losing the gateways must not take the consumer pod away, its IO is
        # expected to either fail or block, both of which are recorded above.
        assert pod_obj.ocp.is_exist(
            resource_name=pod_obj.name
        ), f"Pod {pod_obj.name} disappeared while the gateways were scaled to 0"
        logger.assertion(
            "Pod %s is still present while the gateways are down", pod_obj.name
        )

        # Step 4: A new PVC must not get provisioned while the gateways are down
        logger.test_step(
            "Create a PVC on StorageClass %s while the gateways are scaled to 0",
            constants.CEPH_NVMEOF_SC,
        )
        pending_pvc_obj = helpers.create_pvc(
            sc_name=constants.CEPH_NVMEOF_SC,
            namespace=project_obj.namespace,
            size="1Gi",
            do_reload=False,
            access_mode=constants.ACCESS_MODE_RWO,
        )
        teardown_factory(pending_pvc_obj)
        sleep(PENDING_PVC_OBSERVATION_TIME)
        pending_pvc_obj.reload()
        assert pending_pvc_obj.status == constants.STATUS_PENDING, (
            f"PVC {pending_pvc_obj.name} is in {pending_pvc_obj.status} state "
            f"after {PENDING_PVC_OBSERVATION_TIME}s, it is expected to stay "
            "Pending while the NVMe-oF gateways are scaled to 0"
        )
        logger.assertion(
            "PVC %s stays Pending while the gateways are down",
            pending_pvc_obj.name,
        )

        # Step 5: Scale the gateway back up
        logger.test_step(
            "Scale the NVMe-oF gateway back to %s instances",
            nvmeof_gateway_instances,
        )
        storage_cluster.scale_nvmeof_gateway(nvmeof_gateway_instances)
        logger.assertion(
            "NVMe-oF gateway pods are Running again (%s instances)",
            nvmeof_gateway_instances,
        )

        # Step 6: Verify the pod serves IO again and the data is intact
        logger.test_step(
            "Verify pod %s serves IO again and verify the data", pod_obj.name
        )
        try:
            fio_result = pod_obj.get_fio_results(timeout=SCALE_FIO_RUNTIME)
            logger.info(
                "fio on pod %s completed with error count %s",
                pod_obj.name,
                fio_result.get("jobs")[0].get("error"),
            )
        except Exception as ex:
            # fio is expected to be disrupted by the outage, its failure is
            # recorded but the recovery is verified by the IO probe below.
            logger.warning(
                "fio on pod %s did not complete cleanly: %s", pod_obj.name, ex
            )

        io_pod_obj = pod_obj
        if not is_pod_io_responsive(pod_obj):
            logger.info(
                "IO on pod %s did not recover, restarting the pod", pod_obj.name
            )
            pod_obj.delete()
            pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)
            io_pod_obj = pod_factory(
                interface=constants.CEPHBLOCKPOOL,
                pvc=pvc_obj,
                status=constants.STATUS_RUNNING,
            )
            assert is_pod_io_responsive(io_pod_obj), (
                f"Pod {io_pod_obj.name} does not serve IO on PVC "
                f"{pvc_obj.name} after the gateways were scaled back up"
            )
        logger.assertion("IO is served again by pod %s", io_pod_obj.name)

        # Write new data, then read back the old and the new data
        post_scale_file = "post_scale_data"
        post_scale_path = pod.get_file_path(io_pod_obj, post_scale_file)
        io_pod_obj.exec_sh_cmd_on_pod(
            command=(
                f"dd if=/dev/urandom of={post_scale_path} bs=1M "
                f"count={SCALE_DATA_SIZE_MIB} oflag=direct && sync"
            )
        )
        post_scale_md5sum = pod.cal_md5sum(io_pod_obj, post_scale_file)
        assert pod.verify_data_integrity(
            io_pod_obj, pre_scale_file, pre_scale_md5sum
        ), f"Data written before the outage is corrupted on PVC {pvc_obj.name}"
        assert pod.verify_data_integrity(
            io_pod_obj, post_scale_file, post_scale_md5sum
        ), f"Data written after the recovery is corrupted on PVC {pvc_obj.name}"
        logger.assertion(
            "Data written before and after the outage is intact on PVC %s",
            pvc_obj.name,
        )

        # Step 7: The PVC created during the outage must get provisioned
        logger.test_step("Verify PVC %s transitions to Bound", pending_pvc_obj.name)
        helpers.wait_for_resource_state(
            pending_pvc_obj, constants.STATUS_BOUND, timeout=300
        )
        logger.assertion(
            "PVC %s is Bound after the gateways were scaled back up",
            pending_pvc_obj.name,
        )

    def test_nvmeof_gateway_replica_scaling(
        self,
        nvmeof_gateway_instances,
        nvmeof_storageclass,
        project_factory,
        pvc_factory,
        pod_factory,
    ):
        """
        Verify that the NVMe-oF gateway can be scaled up and down and that the
        consumers keep working at every replica count.

        Steps:
            1. Verify the gateway is deployed with at least 2 instances and
               provision a PVC with a pod consuming it.
            2. Scale the gateway up by one instance.
            3. Scale the gateway back down to its original number of instances.
            4. Scale the gateway down to a single instance.
            5. Scale the gateway back to its original number of instances.

        After every scaling operation the number of gateway pods, the IO of the
        existing consumer, the integrity of its data and the provisioning of a
        new PVC are verified.

        """
        namespace = config.ENV_DATA["cluster_namespace"]

        # Step 1: Gateway with the default number of instances and a consumer
        logger.test_step(
            "Verify the NVMe-oF gateway is deployed with at least 2 instances "
            "and provision a PVC with a pod consuming it"
        )
        assert nvmeof_gateway_instances >= 2, (
            "The test requires an NVMe-oF gateway with at least 2 instances, "
            f"found {nvmeof_gateway_instances}"
        )
        logger.info(
            "NVMe-oF gateway is configured with %s instances",
            nvmeof_gateway_instances,
        )

        project_obj = project_factory()
        pvc_obj = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            project=project_obj,
            storageclass=nvmeof_storageclass,
            size=5,
            access_mode=constants.ACCESS_MODE_RWO,
            status=constants.STATUS_BOUND,
        )
        pod_obj = pod_factory(
            interface=constants.CEPHBLOCKPOOL,
            pvc=pvc_obj,
            status=constants.STATUS_RUNNING,
        )

        data_file = "scaling_data"
        data_path = pod.get_file_path(pod_obj, data_file)
        pod_obj.exec_sh_cmd_on_pod(
            command=(
                f"dd if=/dev/urandom of={data_path} bs=1M "
                f"count={SCALE_DATA_SIZE_MIB} oflag=direct && sync"
            )
        )
        data_md5sum = pod.cal_md5sum(pod_obj, data_file)
        logger.assertion(
            "PVC %s is Bound and consumed by pod %s", pvc_obj.name, pod_obj.name
        )

        # Steps 2 to 5: scale up, back down, to the minimum and back to the
        # original number of instances. The last step also leaves the cluster
        # in its original state.
        scaling_steps = [
            ("up", nvmeof_gateway_instances + 1),
            ("down", nvmeof_gateway_instances),
            ("down to the minimum", 1),
            ("back to the original", nvmeof_gateway_instances),
        ]
        for description, target_instances in scaling_steps:
            logger.test_step(
                "Scale the NVMe-oF gateway %s, to %s instances",
                description,
                target_instances,
            )
            storage_cluster.scale_nvmeof_gateway(target_instances)

            configured_instances = storage_cluster.get_nvmeof_gateway_instances()
            assert configured_instances == target_instances, (
                f"StorageCluster requests {configured_instances} NVMe-oF "
                f"gateway instances instead of {target_instances}"
            )
            gateway_pods = pod.get_pods_having_label(
                label=constants.NVMEOF_APP_LABEL,
                namespace=namespace,
                statuses=[constants.STATUS_RUNNING],
            )
            assert len(gateway_pods) == target_instances, (
                f"Found {len(gateway_pods)} running NVMe-oF gateway pods "
                f"instead of {target_instances}"
            )
            logger.assertion(
                "NVMe-oF gateway is running with %s instances", target_instances
            )

            # The existing consumer must keep serving IO across the change
            verify_nvmeof_consumer_healthy(pod_obj, data_file, data_md5sum)
            logger.assertion(
                "Pod %s serves IO and its data is intact with %s gateway " "instances",
                pod_obj.name,
                target_instances,
            )

            # New volumes must still be provisioned at this replica count
            new_pvc_obj = pvc_factory(
                interface=constants.CEPHBLOCKPOOL,
                project=project_obj,
                storageclass=nvmeof_storageclass,
                size=1,
                access_mode=constants.ACCESS_MODE_RWO,
                status=constants.STATUS_BOUND,
            )
            logger.assertion(
                "PVC %s is provisioned with %s gateway instances",
                new_pvc_obj.name,
                target_instances,
            )


@green_squad
@tier4a
@ignore_leftovers
@skipif_no_nvmeof
class TestNvmeofGatewayUpdateStrategy(ManageTest):
    """
    Tests for the rolling update of the NVMe-oF Gateway pods.
    """

    @pytest.fixture()
    def nvmeof_gateway_resources(self, request):
        """
        Restore the resource requirements of the NVMe-oF gateway after the
        test, which rolls the gateway pods back to their original spec.

        Returns:
            dict: The gateway resource requirements configured before the
                test, None if none were configured

        """
        original_resources = storage_cluster.get_nvmeof_gateway_resources()

        def finalizer():
            if storage_cluster.get_nvmeof_gateway_resources() == original_resources:
                return
            logger.info(
                "Restoring the NVMe-oF gateway resources to %s", original_resources
            )
            storage_cluster.set_nvmeof_gateway_resources(original_resources)
            gateway_instances = storage_cluster.get_nvmeof_gateway_instances()
            assert pod.wait_for_pods_by_label_count(
                label=constants.NVMEOF_APP_LABEL,
                expected_count=gateway_instances,
                namespace=config.ENV_DATA["cluster_namespace"],
                timeout=GATEWAY_ROLLOUT_TIMEOUT,
            ), "NVMe-oF gateway pods were not restored after the test"

        request.addfinalizer(finalizer)
        return original_resources

    def test_nvmeof_gateway_update_strategy(
        self,
        nvmeof_gateway_resources,
        nvmeof_storageclass,
        project_factory,
        pvc_factory,
        pod_factory,
    ):
        """
        Verify that the NVMe-oF gateway pods can be rolled out and that the
        consumers are functional once the rollout is complete.

        Steps:
            1. Provision a PVC with a pod running continuous IO on it.
            2. Trigger a rollout of the gateway pods by changing the resource
               requirements of the gateway.
            3. Observe the IO of the consumer while the gateway pods are
               replaced.
            4. Verify all gateway pods are healthy and that the existing and
               the newly created PVCs and pods are functional.

        """
        assert nvmeof_gateway_resources != UPDATE_GATEWAY_RESOURCES, (
            "The NVMe-oF gateway already requests "
            f"{UPDATE_GATEWAY_RESOURCES}, which would not trigger a rollout"
        )

        # Step 1: A PVC with a pod running continuous IO on it
        logger.test_step("Provision a PVC with a pod running continuous IO on it")
        gateway_instances = storage_cluster.get_nvmeof_gateway_instances()
        project_obj = project_factory()
        pvc_obj = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            project=project_obj,
            storageclass=nvmeof_storageclass,
            size=5,
            access_mode=constants.ACCESS_MODE_RWO,
            status=constants.STATUS_BOUND,
        )
        pod_obj = pod_factory(
            interface=constants.CEPHBLOCKPOOL,
            pvc=pvc_obj,
            status=constants.STATUS_RUNNING,
        )

        data_file = "pre_update_data"
        data_path = pod.get_file_path(pod_obj, data_file)
        pod_obj.exec_sh_cmd_on_pod(
            command=(
                f"dd if=/dev/urandom of={data_path} bs=1M "
                f"count={SCALE_DATA_SIZE_MIB} oflag=direct && sync"
            )
        )
        data_md5sum = pod.cal_md5sum(pod_obj, data_file)

        pod_obj.run_io(
            storage_type="fs",
            size="1G",
            io_direction="rw",
            runtime=SCALE_FIO_RUNTIME,
            fio_filename="fio_continuous",
        )
        logger.assertion(
            "Pod %s runs continuous IO on PVC %s", pod_obj.name, pvc_obj.name
        )

        initial_uids = {
            gateway_pod["metadata"]["uid"]
            for gateway_pod in get_nvmeof_gateway_pods(
                statuses=[constants.STATUS_RUNNING]
            )
        }
        logger.info(
            "NVMe-oF gateway is running %s pods before the rollout",
            len(initial_uids),
        )

        # Step 2: Trigger the rollout by changing the gateway resources
        logger.test_step(
            "Trigger a rollout of the gateway pods by setting their resource "
            "requirements to %s",
            UPDATE_GATEWAY_RESOURCES,
        )
        storage_cluster.set_nvmeof_gateway_resources(UPDATE_GATEWAY_RESOURCES)

        # Step 3: Observe the IO of the consumer during the rollout
        logger.test_step(
            "Observe the IO of pod %s while the gateway pods are replaced",
            pod_obj.name,
        )
        monitor_nvmeof_gateway_rollout(pod_obj, initial_uids, gateway_instances)
        log_pod_events(pod_obj)
        logger.assertion(
            "All %s NVMe-oF gateway pods were replaced by the rollout",
            gateway_instances,
        )

        # Step 4: The gateways and the consumers have to be healthy again
        logger.test_step(
            "Verify the gateway pods are healthy and the consumers are " "functional"
        )
        gateway_pods = get_nvmeof_gateway_pods(statuses=[constants.STATUS_RUNNING])
        gateway_pod_names = [
            gateway_pod["metadata"]["name"] for gateway_pod in gateway_pods
        ]
        assert len(gateway_pods) == gateway_instances, (
            f"Found {len(gateway_pods)} NVMe-oF gateway pods instead of "
            f"{gateway_instances} after the rollout"
        )
        assert pod.wait_for_pods_to_be_running(
            namespace=config.ENV_DATA["cluster_namespace"],
            pod_names=gateway_pod_names,
            timeout=300,
        ), f"NVMe-oF gateway pods {gateway_pod_names} are not Running"
        assert storage_cluster.get_nvmeof_gateway_resources() == (
            UPDATE_GATEWAY_RESOURCES
        ), "The NVMe-oF gateway does not request the updated resources"
        logger.assertion(
            "All %s NVMe-oF gateway pods are healthy after the rollout",
            gateway_instances,
        )

        # fio is expected to be disrupted by the rollout, its outcome is only
        # recorded, the consumer is verified by the checks below.
        try:
            fio_result = pod_obj.get_fio_results(timeout=SCALE_FIO_RUNTIME)
            logger.info(
                "fio on pod %s completed with error count %s",
                pod_obj.name,
                fio_result.get("jobs")[0].get("error"),
            )
        except Exception as ex:
            logger.warning(
                "fio on pod %s did not complete cleanly: %s", pod_obj.name, ex
            )

        verify_nvmeof_consumer_healthy(pod_obj, data_file, data_md5sum)
        logger.assertion(
            "Pod %s serves IO and the data of PVC %s is intact after the " "rollout",
            pod_obj.name,
            pvc_obj.name,
        )

        # A new PVC and pod have to be functional as well
        new_pvc_obj = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            project=project_obj,
            storageclass=nvmeof_storageclass,
            size=1,
            access_mode=constants.ACCESS_MODE_RWO,
            status=constants.STATUS_BOUND,
        )
        new_pod_obj = pod_factory(
            interface=constants.CEPHBLOCKPOOL,
            pvc=new_pvc_obj,
            status=constants.STATUS_RUNNING,
        )
        assert is_pod_io_responsive(
            new_pod_obj
        ), f"Pod {new_pod_obj.name} does not serve IO on PVC {new_pvc_obj.name}"
        logger.assertion(
            "PVC %s is provisioned and pod %s serves IO on it after the " "rollout",
            new_pvc_obj.name,
            new_pod_obj.name,
        )
