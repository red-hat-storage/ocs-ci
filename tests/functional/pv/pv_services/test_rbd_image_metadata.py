import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.helpers.helpers import create_unique_resource_name, get_snapshot_content_obj
from ocs_ci.ocs.defaults import RBD_NAME
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod
from ocs_ci.framework.pytest_customization.marks import (
    tier2,
    polarion_id,
    green_squad,
)

logger = logging.getLogger(__name__)


@green_squad
class TestRbdImageMetadata:
    @tier2
    @polarion_id("OCS-4465")
    @polarion_id("OCS-4675")
    def test_rbd_image_metadata(
        self, pvc_factory, pvc_clone_factory, snapshot_restore_factory
    ):
        """
        Test by default the rbd images doesnot have metdata details for,
        1. a newly created RBD PVC
        2. PVC clone and check its status
        3. volume snapshot
        4. Restore volume from snapshot

        """

        rbd_images = []
        logger.test_step("Create a PVC with ceph-rbd storage class")
        pvc_obj = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            status=constants.STATUS_BOUND,
            volume_mode="Block",
        )
        logger.info(f"PVC {pvc_obj.name} created!")
        rbd_images.append(pvc_obj.get_rbd_image_name)

        logger.test_step("Create a snapshot of the PVC")
        snap_obj = pvc_obj.create_snapshot(
            snapshot_name=create_unique_resource_name("test", "snapshot"),
            wait=True,
        )
        logger.info(f"Snapshot of PVC {pvc_obj.name} created!")
        snapshot_content = get_snapshot_content_obj(snap_obj=snap_obj)
        snap_handle = snapshot_content.get().get("status").get("snapshotHandle")
        snap_image_name = f'csi-snap-{snap_handle.split("-", 5)[5]}'
        rbd_images.append(snap_image_name)

        logger.test_step("Restore the snapshot")
        restored_pvc = snapshot_restore_factory(
            snapshot_obj=snap_obj, volume_mode=pvc_obj.get_pvc_vol_mode, timeout=600
        )
        logger.info(f"restored the snapshot {restored_pvc.name} created!")

        logger.test_step("Create a clone of the PVC")
        clone_obj = pvc_clone_factory(pvc_obj)
        logger.info(f"Clone of PVC {pvc_obj.name} created!")
        rbd_images.append(clone_obj.get_rbd_image_name)

        logger.test_step("Verify metadata is not set on each RBD image")
        rbd_pool_name = (
            (config.ENV_DATA.get("rbd_name") or RBD_NAME)
            if config.DEPLOYMENT["external_mode"]
            else constants.DEFAULT_CEPHBLOCKPOOL
        )
        ceph_tool_pod = get_ceph_tools_pod()
        for image in rbd_images:
            cmd = f"rbd image-meta list {rbd_pool_name}/{image}"
            metadata = ceph_tool_pod.exec_cmd_on_pod(command=cmd, out_yaml_format=False)
            logger.debug(f"Metadata for {image}: {metadata}")
            logger.assertion(
                f"RBD image {image} metadata: expected='There are 0 metadata on this image', actual='{metadata}'"
            )
            assert (
                "There are 0 metadata on this image" in metadata
            ), f"Not expected, Metadata is being set for the rbd image - {image}!"
        logger.info("Verified: Metadata is not being set for any of the RBD images")
