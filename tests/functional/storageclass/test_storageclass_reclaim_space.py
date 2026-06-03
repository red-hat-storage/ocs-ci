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

logger = logging.getLogger(__name__)


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
        logger.info(f"RBD Image {rbd_image_name} is size of {image_size}GiB")
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

        logger.test_step("Create StorageClass with reclaimspace annotations")
        reclaimspace_annotations = {
            constants.RECLAIMSPACE_SCHEDULE_ANNOTATION: "*/3 * * * *"
        }
        self.sc_obj = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL, annotations=reclaimspace_annotations
        )
        self.pool_name = self.sc_obj.data["parameters"]["pool"]
        logger.info(
            f"Created StorageClass {self.sc_obj.name} with reclaimspace schedule"
        )

        logger.test_step("Create PVCs with volume block mode")
        pvc_objs = multi_pvc_factory(
            size=5,
            storageclass=self.sc_obj,
            num_of_pvc=3,
            access_modes=[f"{constants.ACCESS_MODE_RWO}-Block"],
            wait_each=True,
        )
        logger.info(f"Created {len(pvc_objs)} PVCs with block volume mode")

        logger.test_step("Create pods for each PVC")
        self.pod_objs = []
        for pvc in pvc_objs:
            pod_obj = pod_factory(
                pvc=pvc,
                status=constants.STATUS_RUNNING,
                raw_block_pv=True,
            )
            self.pod_objs.append(pod_obj)
        logger.info(f"Created {len(self.pod_objs)} pods")

        logger.test_step("Write 2.0GiB of data to block devices on each pod")
        for pod_obj, pvc_obj in zip(self.pod_objs, pvc_objs):
            storage_path = pod_obj.get_storage_path("block")
            logger.debug(
                f"Writing 2.0GiB of data to block device on pod {pod_obj.name}"
            )
            pod_obj.exec_cmd_on_pod(
                f"dd if=/dev/zero of={storage_path} bs=1M count=2048 oflag=direct > /dev/null 2>&1 &",
                shell=True,
            )

        logger.test_step("Verify RBD images show expected size of 2.0GiB after write")
        for pvc_obj in pvc_objs:
            assert self.wait_till_expected_image_size(
                pvc_obj, 2.0
            ), f"RBD Image '{pvc_obj.get_rbd_image_name}' expected size of '2.0 GiB' does not match the actual size."

        logger.test_step("Delete all pods and verify reclaimspace reclaims storage")
        for pod_obj in self.pod_objs:
            pod_obj.delete()
            pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)

        for pvc_obj in pvc_objs:
            assert self.wait_till_expected_image_size(
                pvc_obj, 0.0
            ), f"RBD Image '{pvc_obj.get_rbd_image_name}' expected size of '0.0 GiB' does not match the actual size."

        logger.info("ReclaimSpace job has run successfully")
