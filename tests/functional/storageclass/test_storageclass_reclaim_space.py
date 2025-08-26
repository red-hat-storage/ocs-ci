import logging

from ocs_ci.ocs import constants
from ocs_ci.helpers.helpers import get_rbd_image_info
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.utility.retry import retry
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    polarion_id,
    tier2,
)

log = logging.getLogger(__name__)


@green_squad
@polarion_id("OCS-6197")
class TestStorageClassReclamespace:
    @retry(UnexpectedBehaviour, tries=5, delay=10)
    def wait_till_expected_image_size(self, pvc_obj, expected_size):
        """
        Waiting till rbd image size is became expected size.
        """
        rbd_image_name = pvc_obj.get_rbd_image_name
        image_size = get_rbd_image_info(self.pool_name, rbd_image_name).get(
            "used_size_gib"
        )

        if image_size != expected_size:
            raise UnexpectedBehaviour(
                f"RBD image {rbd_image_name} size is not expected as {expected_size}GiB"
            )
        log.info(f" RBD Image { rbd_image_name} is size of {image_size}GiB")
        return True

    @tier2
    def test_storageclass_reclaimspace(
        self, storageclass_factory, multi_pvc_factory, pod_factory
    ):
        """
        Test Space Reclaim Operation with Storageclass annotation.

        Steps:

        1. Create a RBD storageclass with  reclaimspace annotations.
        "reclaimspace.csiaddons.openshift.io/schedule: */3 * * * *".
        2. Create a 3 PVC
        4. Create a pod w.r.t each PVC
        5. Write a 2.0GiB on block device mounted in the pod.
        6. destroy all pods
        7. wait for reclaimspace operation to be complete
        6. verify reclaimspace Job ran successfully for the storageclass.
        """

        # Storegeclass ReclaimSpace annotations.
        reclaimspace_annotations = {
            constants.RECLAIMSPACE_SCHEDULE_ANNOTATION: "*/3 * * * *"
        }

        # Creating StorageClass with reclaimspace annotations.
        self.sc_obj = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL, annotations=reclaimspace_annotations
        )
        self.pool_name = self.sc_obj.data["parameters"]["pool"]

        # Create a PVC's with volume block mode
        pvc_objs = multi_pvc_factory(
            size=5,
            storageclass=self.sc_obj,
            num_of_pvc=3,
            access_modes=[f"{constants.ACCESS_MODE_RWO}-Block"],
            wait_each=True,
        )

        # Create POds
        self.pod_objs = []

        # Create pods
        for pvc in pvc_objs:
            pod_obj = pod_factory(
                pvc=pvc,
                status=constants.STATUS_RUNNING,
                raw_block_pv=True,
            )
            self.pod_objs.append(pod_obj)

        # Writing data to the block device
        for pod_obj, pvc_obj in zip(self.pod_objs, pvc_objs):
            storage_path = pod_obj.get_storage_path("block")
            log.info("Writing 2.0GiB of data to the block device")
            pod_obj.exec_cmd_on_pod(
                f"dd if=/dev/zero of={storage_path} bs=1M count=2048 oflag=direct > /dev/null 2>&1 &",
                shell=True,
            )

        # Wait until all writes are complete and the RBD image shows the expected size.
        for pvc_obj in pvc_objs:
            assert self.wait_till_expected_image_size(
                pvc_obj, 2.0
            ), f"RBD Image '{pvc_obj.get_rbd_image_name}' expected size of '2.0 GiB' does not match the actual size."

        # Delete all pods
        for pod_obj in self.pod_objs:
            pod_obj.delete()
            pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)

        # Wait till reclaim space operations is complete
        for pvc_obj in pvc_objs:
            assert self.wait_till_expected_image_size(
                pvc_obj, 0.0
            ), f"RBD Image '{pvc_obj.get_rbd_image_name}' expected size of '0.0 GiB' does not match the actual size."

        log.info("ReclaimSpace JOB has ran successfully. ")
