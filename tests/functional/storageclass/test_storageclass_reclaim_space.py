import logging

from ocs_ci.ocs import constants
from ocs_ci.helpers.helpers import get_rbd_image_info
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.utility.retry import retry
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    polarion_id,
    tier1,
)

log = logging.getLogger(__name__)


@green_squad
@polarion_id("OCS-6197")
class TestStorageClassReclamespace:
    @retry(UnexpectedBehaviour, tries=5, delay=10)
    def wait_till_expected_image_size(self, expected_size):
        """
        Waiting till rbd image size is became expected size.
        """
        image_size = get_rbd_image_info(self.pool_name, self.rbd_image_name).get(
            "used_size_gib"
        )

        if image_size != expected_size:
            raise UnexpectedBehaviour(
                f"RBD image {self.rbd_image_name} size is not expected as {expected_size}GiB"
            )
        log.info(f" RBD Image { self.rbd_image_name} is size of {image_size}GiB")
        return True

    @tier1
    def test_storageclass_reclaimspace(
        self, storageclass_factory, pvc_factory, pod_factory
    ):
        """
        Test Space Reclaim Operation with Storageclass annotation.

        Steps:

        1. Create a RBD storageclass with  reclaimspace annotations.
        "reclaimspace.csiaddons.openshift.io/schedule: */3 * * * *".
        2. Create a pvc
        4. Create a pods
        5. Write a 2.0GiB data on rnd block
        6. destroy pod
        7. wait for reclaimspace operation to be complete
        6. verify reclaimspace Job ran successfully for the storageclass.
        """

        # Storegeclass ReclaimSpace annotations.
        reclaimspace_annotations = {
            "reclaimspace.csiaddons.openshift.io/schedule": "*/3 * * * *"
        }

        # Creating StorageClass with reclaimspace annotations.
        self.sc_obj = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL, annotations=reclaimspace_annotations
        )

        # Create a PVC with volume block mode
        pvc_obj = pvc_factory(
            size=5, storageclass=self.sc_obj, volume_mode=constants.VOLUME_MODE_BLOCK
        )

        pod_obj = pod_factory(
            pvc=pvc_obj,
            status=constants.STATUS_RUNNING,
            raw_block_pv=True,
        )

        storage_path = pod_obj.get_storage_path("block")

        log.info("Writing 2.0GiB of data to the block device")
        pod_obj.exec_cmd_on_pod(
            f"dd if=/dev/zero of={storage_path} bs=1M count=2048 oflag=direct"
        )

        self.pool_name = self.sc_obj.data["parameters"]["pool"]
        self.rbd_image_name = pvc_obj.get_rbd_image_name

        assert self.wait_till_expected_image_size(
            2.0
        ), f"RBD Image '{self.rbd_image_name}' expected size of '2.0 GiB' does not match the actual size."

        pod_obj.delete()
        pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)

        assert self.wait_till_expected_image_size(
            0.0
        ), f"RBD Image '{self.rbd_image_name}' expected size of '0.0 GiB' does not match the actual size."

        log.info("ReclaimSpace JOB has ran successfully. ")
