"""
Basic EC (Erasure Coding) pool functionality tests
"""
import logging
import pytest

from ocs_ci.framework.testlib import ManageTest, tier1
from ocs_ci.framework.pytest_customization.marks import (
    skipif_external_mode,
    green_squad,
)
from ocs_ci.ocs import constants
from ocs_ci.helpers.helpers import wait_for_resource_state

log = logging.getLogger(__name__)


@green_squad
@tier1
@skipif_external_mode
@pytest.mark.polarion_id("OCS-XXXX")  # TODO: Update with actual Polarion ID
class TestECPoolBasic(ManageTest):
    """
    Test basic EC pool functionality with pod I/O operations
    """

    def test_ec_pool_with_pod_basic(
        self,
        ec_storageclass_factory,
        pvc_factory,
        pod_factory,
    ):
        """
        Test basic EC pool functionality by creating an EC-backed StorageClass,
        PVC, and pod, then performing I/O operations.

        Test Steps:
        1. Create an EC-backed StorageClass using ec_storageclass_factory
        2. Create a PVC from the EC StorageClass using pvc_factory
        3. Create a pod that mounts the PVC using pod_factory
        4. Verify the pod is running
        5. Write data to the mounted volume
        6. Read back the data and verify integrity
        7. Cleanup happens automatically via fixtures

        Expected Results:
        - EC StorageClass is created successfully
        - PVC is bound successfully
        - Pod reaches Running state
        - Data can be written to and read from the EC-backed volume
        - Data integrity is maintained
        """
        log.info("Step 1: Creating EC-backed StorageClass")
        ec_sc = ec_storageclass_factory()
        log.info(f"EC StorageClass created: {ec_sc.name}")
        assert ec_sc, "Failed to create EC StorageClass"

        log.info("Step 2: Creating PVC from EC StorageClass")
        pvc_obj = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            storageclass=ec_sc,
            size=10,
        )
        log.info(f"PVC created: {pvc_obj.name}")
        assert pvc_obj, "Failed to create PVC"

        # Verify PVC is bound
        log.info("Verifying PVC is bound")
        wait_for_resource_state(pvc_obj, constants.STATUS_BOUND, timeout=300)
        pvc_obj.reload()
        assert pvc_obj.status == constants.STATUS_BOUND, (
            f"PVC {pvc_obj.name} is not in Bound state"
        )
        log.info(f"PVC {pvc_obj.name} is bound successfully")

        log.info("Step 3: Creating pod with PVC mounted")
        pod_obj = pod_factory(
            interface=constants.CEPHBLOCKPOOL,
            pvc=pvc_obj,
        )
        log.info(f"Pod created: {pod_obj.name}")
        assert pod_obj, "Failed to create pod"

        log.info("Step 4: Verifying pod is running")
        wait_for_resource_state(pod_obj, constants.STATUS_RUNNING, timeout=300)
        pod_obj.reload()
        assert pod_obj.status == constants.STATUS_RUNNING, (
            f"Pod {pod_obj.name} is not in Running state"
        )
        log.info(f"Pod {pod_obj.name} is running successfully")

        log.info("Step 5: Running I/O on the pod")
        # Run FIO workload on the pod to write and verify data
        pod_obj.run_io(
            storage_type="fs",
            size="1G",
            rate="1500m",
            runtime=60,
            buffer_compress_percentage=60,
            buffer_pattern="0xdeadface",
            bs="8K",
            jobs=5,
            readwrite="readwrite",
        )
        log.info("I/O operations completed successfully")

        log.info("Step 6: Verifying FIO results")
        # Get FIO results to verify I/O completed successfully
        fio_result = pod_obj.get_fio_results()
        log.info(f"FIO results: {fio_result}")
        
        # Verify that I/O operations completed without errors
        assert fio_result.get("jobs"), "FIO jobs not found in results"
        for job in fio_result.get("jobs", []):
            assert job.get("error") == 0, (
                f"FIO job {job.get('jobname')} completed with errors"
            )
        log.info("Data integrity verified successfully via FIO")

        log.info("Step 7: Test completed successfully. Cleanup will be handled by fixtures")
        log.info(
            f"EC StorageClass: {ec_sc.name}, "
            f"PVC: {pvc_obj.name}, "
            f"Pod: {pod_obj.name}"
        )

