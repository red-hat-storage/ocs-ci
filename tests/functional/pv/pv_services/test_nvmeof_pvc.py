import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    skipif_no_nvmeof,
    tier1,
    polarion_id,
)
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.nvmeof import (
    get_nvmeof_storageclass,
    wait_for_nvmeof_gateway_pods_running,
)
from ocs_ci.ocs.resources import pod

logger = logging.getLogger(__name__)


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
        wait_for_nvmeof_gateway_pods_running()
        logger.assertion("All NVMe-oF Gateway pods are healthy (Running)")
        get_nvmeof_storageclass()
        logger.assertion(f"NVMe-oF StorageClass {constants.CEPH_NVMEOF_SC} exists")

    @pytest.fixture()
    def nvmeof_storageclass(self):
        """
        Return the existing NVMe-oF StorageClass as an OCS object so that it can
        be consumed by the pvc_factory fixture.

        Returns:
            OCS: OCS instance of the NVMe-oF StorageClass

        """
        return get_nvmeof_storageclass()

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
        if reclaim_policy == constants.RECLAIM_POLICY_DELETE:
            pv_obj.ocp.wait_for_delete(resource_name=pv_name, timeout=180)
            logger.assertion("PV %s deleted as per Delete reclaim policy", pv_name)
        elif reclaim_policy == constants.RECLAIM_POLICY_RETAIN:
            helpers.wait_for_resource_state(
                pv_obj, constants.STATUS_RELEASED, timeout=180
            )
            logger.assertion("PV %s is Released as per Retain reclaim policy", pv_name)
            # Cleanup the retained PV so no leftovers remain. Switch the reclaim
            # policy to Delete so the controller removes the backing volume too,
            # then wait for controller-driven deletion (pvc_factory_fixture pattern).
            patch_param = '{"spec":{"persistentVolumeReclaimPolicy":"Delete"}}'
            pv_obj.ocp.patch(resource_name=pv_name, params=patch_param)
            pv_obj.ocp.wait_for_delete(resource_name=pv_name, timeout=180)
        else:
            pytest.fail(f"Unexpected reclaim policy {reclaim_policy} for PV {pv_name}")
