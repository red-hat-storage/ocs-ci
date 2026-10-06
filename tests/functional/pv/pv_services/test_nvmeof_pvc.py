import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    skipif_no_nvmeof,
    tier1,
    polarion_id,
)
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.framework import config
from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.ocs import OCS

logger = logging.getLogger(__name__)

# Device path exposed inside the pod for the raw block (volumeMode: Block) PVC
NVMEOF_RAW_BLOCK_DEVICE = "/dev/xvda"
# Amount of raw data written to and read back from the block device
RAW_BLOCK_IO_SIZE_MIB = 100
# In-pod path the raw data is read back to
RAW_BLOCK_READBACK_PATH = "/tmp/readback"


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


@green_squad
@tier1
@skipif_no_nvmeof
class TestNvmeofPvc(ManageTest):
    """
    Tests for basic PVC lifecycle and data integrity using the NVMe-oF
    (NVMe over Fabrics) StorageClass.
    """

    @pytest.fixture(autouse=True)
    def nvmeof_prerequisites(self):
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
    def nvmeof_storageclass(self):
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
        pod_obj.exec_sh_cmd_on_pod(
            command=(
                f"dd if=/dev/urandom of={device_path} bs=1M "
                f"count={RAW_BLOCK_IO_SIZE_MIB} oflag=direct"
            )
        )
        pod_obj.exec_sh_cmd_on_pod(
            command=(
                f"dd if={device_path} of={RAW_BLOCK_READBACK_PATH} bs=1M "
                f"count={RAW_BLOCK_IO_SIZE_MIB} iflag=direct"
            )
        )
        device_md5sum = md5sum_of_device_head(
            pod_obj, device_path, RAW_BLOCK_IO_SIZE_MIB
        )
        readback_md5sum = pod.cal_md5sum(
            pod_obj, RAW_BLOCK_READBACK_PATH, raw_path=True
        )
        assert device_md5sum == readback_md5sum, (
            f"Data read back from {device_path} on pod {pod_obj.name} does not "
            f"match the device content. Device md5sum: {device_md5sum}, "
            f"readback md5sum: {readback_md5sum}"
        )
        logger.assertion(
            "Raw data read back from %s matches the device content", device_path
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
        assert persisted_md5sum == device_md5sum, (
            f"Data on {device_path} did not persist across pod recreation. "
            f"Expected md5sum: {device_md5sum}, actual: {persisted_md5sum}"
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
