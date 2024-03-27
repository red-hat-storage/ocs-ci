import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.helpers.helpers import (
    create_unique_resource_name,
    get_snapshot_content_obj,
    default_ceph_block_pool,
)
from ocs_ci.ocs.defaults import RBD_NAME
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod
from ocs_ci.framework.pytest_customization.marks import (
    tier2,
    polarion_id,
    bugzilla,
    green_squad,
)

log = logging.getLogger(__name__)


@green_squad
class TestRbdImageMetadata:
    @tier2
    @polarion_id("OCS-4465")
    @polarion_id("OCS-4675")
    @bugzilla("2099965")
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
        # create a pvc with ceph-rbd sc
        pvc_obj = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            status=constants.STATUS_BOUND,
            volume_mode="Block",
        )
        log.info(f"PVC {pvc_obj.name} created!")
        rbd_images.append(pvc_obj.get_rbd_image_name)

        # create a snapshot of the PVC
        snap_obj = pvc_obj.create_snapshot(
            snapshot_name=create_unique_resource_name("test", "snapshot"),
            wait=True,
        )
        log.info(f"Snapshot of PVC {pvc_obj.name} created!")
        snapshot_content = get_snapshot_content_obj(snap_obj=snap_obj)
        snap_handle = snapshot_content.get().get("status").get("snapshotHandle")
        snap_image_name = f'csi-snap-{snap_handle.split("-", 5)[5]}'
        rbd_images.append(snap_image_name)

        # restore the snapshot
        restored_pvc = snapshot_restore_factory(snapshot_obj=snap_obj, timeout=600)
        log.info(f"restored the snapshot {restored_pvc.name} created!")

        # create a clone of the PVC
        clone_obj = pvc_clone_factory(pvc_obj)
        log.info(f"Clone of PVC {pvc_obj.name} created!")
        rbd_images.append(clone_obj.get_rbd_image_name)

        # check the metadata on each images
        rbd_pool_name = (
            (config.ENV_DATA.get("rbd_name") or RBD_NAME)
            if config.DEPLOYMENT["external_mode"]
            else default_ceph_block_pool()
        )
        ceph_tool_pod = get_ceph_tools_pod()
        for image in rbd_images:
            cmd = f"rbd image-meta list {rbd_pool_name}/{image}"
            metadata = ceph_tool_pod.exec_cmd_on_pod(command=cmd, out_yaml_format=False)
            log.info(f"Metdata for {image}\n{metadata}")
            assert (
                "There are 0 metadata on this image" in metadata
            ), f"Not expected, Metadata is being set for the rbd image - {image}!"
        log.info("Metadata is not being set for the rbd images as expected!")
