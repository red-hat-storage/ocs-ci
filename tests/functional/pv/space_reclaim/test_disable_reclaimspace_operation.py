import logging
import pytest
import math
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import tier2
from ocs_ci.helpers.helpers import (
    change_reclaimspacecronjob_state_for_pvc,
    get_rbd_image_info,
    create_pods,
    verify_reclaimspacecronjob_suspend_state_for_pvc,
)
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.resources.pod import delete_pods

log = logging.getLogger(__name__)


@green_squad
class TestDisableReclaimSpaceOperation:
    @pytest.fixture(autouse=True)
    def setup(self, storageclass_factory, multi_pvc_factory):
        """Setup the test environment by creating StorageClass and PVCs."""
        reclaimspace_annotations = {
            constants.RECLAIMSPACE_SCHEDULE_ANNOTATION: "* * * * *"
        }
        self.sc_obj = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL, annotations=reclaimspace_annotations
        )
        self.pvc_objs = multi_pvc_factory(
            size=5,
            num_of_pvc=3,
            storageclass=self.sc_obj,
            access_modes=[
                f"{constants.ACCESS_MODE_RWX}-Block",
                f"{constants.ACCESS_MODE_RWO}-Block",
            ],
            wait_each=True,
        )

        yield
        reset_reclaimspace_annotations = {
            constants.RECLAIMSPACE_SCHEDULE_ANNOTATION: "@weekly"
        }

        self.sc_obj = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL,
            annotations=reset_reclaimspace_annotations,
        )

    @retry(UnexpectedBehaviour, tries=3, delay=10)
    def wait_till_expected_image_size(self, pvc_obj, expected_size, tolerance=0.3):
        """Wait until the RBD image size matches the expected size."""
        rbd_image_name = pvc_obj.get_rbd_image_name
        image_info = get_rbd_image_info(constants.DEFAULT_CEPHBLOCKPOOL, rbd_image_name)
        image_size = image_info.get("used_size_gib")
        if not math.isclose(image_size, expected_size, abs_tol=tolerance):
            raise UnexpectedBehaviour(
                f"RBD image {rbd_image_name} size mismatch: {image_size}GiB, "
                f"expected {expected_size}GiB (tolerance: ±{tolerance}GiB)"
            )
        log.info(
            f"RBD Image {rbd_image_name} is size of {image_size}GiB (within tolerance ±{tolerance}GiB)"
        )
        return True

    def execute_reclaimspace_test(self, pod_factory, suspend_state):
        """Test reclaim space operation for PVCs with pods."""
        # Create and attach pods to PVCs
        pod_objs = create_pods(
            self.pvc_objs,
            pod_factory,
            constants.CEPHBLOCKPOOL,
            pods_for_rwx=1,
            status=constants.STATUS_RUNNING,
        )

        # Write data to block devices
        actual_data_written = 1.0  # 1 GiB
        for pod_obj in pod_objs:
            storage_path = pod_obj.get_storage_path("block")
            log.info(f"Writing {actual_data_written}GiB of data to the block device")
            pod_obj.exec_cmd_on_pod(
                f"dd if=/dev/zero of={storage_path} bs=1M count=1024 oflag=direct > /dev/null 2>&1 &",
                shell=True,
            )

        # Validate RBD image sizes after data writes
        for pvc_obj in self.pvc_objs:
            self.wait_till_expected_image_size(pvc_obj, actual_data_written)

        # Delete pods
        delete_pods(pod_objs, wait=True)

        # Verify RBD image sizes after pods are deleted
        expected_volume_size = actual_data_written if suspend_state else 0.0
        for pvc_obj in self.pvc_objs:
            self.wait_till_expected_image_size(pvc_obj, expected_volume_size)

    @pytest.mark.polarion_id("OCS-6279")
    @tier2
    def test_disable_reclaimspace_operation(self, pod_factory):
        """Test to verify disabling and enabling reclaim space operation.

        Steps:
            1. Create a RBD PVC with diferent access modes (RWO, RWX)
            2. Run a pod and attach a PVC to the pod.
            3. Disable reclaimspace for all PVC by editing reclaimspacecronjob.
            4. Verify ReclaimSpace Operation is disabled for the PVC.
            5. re-enable reclaimspace operation for the PVC.
            6. Verify reclaimspace Operation is enabled for the PVC.
        """

        log.info("Disabling reclaim space operation for all PVCs.")
        change_reclaimspacecronjob_state_for_pvc(self.pvc_objs, suspend=True)

        log.info("Verifying ReclaimSpaceCronJob suspend state (suspend=true).")
        for pvc_obj in self.pvc_objs:
            assert verify_reclaimspacecronjob_suspend_state_for_pvc(
                pvc_obj
            ), f"Reclaimspace cronjob is not suspended for PVC: {pvc_obj.name}"

        log.info("Validating ReclaimSpace operation is disabled.")
        self.execute_reclaimspace_test(pod_factory, suspend_state=True)

        log.info("Re-enabling reclaim space cronjob for all PVCs.")
        change_reclaimspacecronjob_state_for_pvc(self.pvc_objs, suspend=False)

        log.info("Verifying ReclaimSpaceCronJob suspend state (suspend=false).")
        for pvc_obj in self.pvc_objs:
            assert not verify_reclaimspacecronjob_suspend_state_for_pvc(
                pvc_obj
            ), f"Reclaimspace cronjob is still suspended for PVC: {pvc_obj.name}"

        log.info("Validating ReclaimSpace operation is enabled.")
        self.execute_reclaimspace_test(pod_factory, suspend_state=False)
