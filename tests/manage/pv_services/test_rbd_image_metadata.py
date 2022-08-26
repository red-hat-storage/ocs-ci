import logging

from ocs_ci.ocs import constants
from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod
from ocs_ci.framework.pytest_customization.marks import tier2, polarion_id, bugzilla

log = logging.getLogger(__name__)


@tier2
@polarion_id("OCS-4465")
@bugzilla("2099965")
class TestRbdImageMetadata:
    @tier2
    @polarion_id("OCS-4465")
    @bugzilla("2099965")
    def test_rbd_image_metadata(self, pvc_factory, pvc_clone_factory):
        """
        Test if the rbd images have metdata being set
        """

        # create a pvc with ceph-rbd
        pvc_obj = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            status=constants.STATUS_BOUND,
            volume_mode="Block",
        )
        log.info(f"PVC {pvc_obj.name} created!")

        # create a snapshot of the PVC
        pvc_obj.create_snapshot(
            snapshot_name=create_unique_resource_name("test", "snapshot"),
            wait=True,
        )
        log.info(f"Snapshot of PVC {pvc_obj.name} created!")

        # create a clone of the PVC
        pvc_clone_factory(pvc_obj)
        log.info(f"Clone of PVC {pvc_obj.name} created!")

        # get all the images
        ceph_tool_pod = get_ceph_tools_pod()
        rbd_images = ceph_tool_pod.exec_cmd_on_pod(
            command=f"rbd ls {constants.DEFAULT_CEPHBLOCKPOOL}"
        ).split(" ")

        # check the metadata on each images
        for image in rbd_images:
            cmd = f"rbd image-meta list {constants.DEFAULT_CEPHBLOCKPOOL}/{image}"
            metadata = ceph_tool_pod.exec_cmd_on_pod(command=cmd)
            log.info(f"Metdata for {image}\n{metadata}")
            assert (
                "There are 0 metadata on this image" in metadata
            ), f"Not expected, Metadata is being set for the rbd image - {image}!"
        log.info("Metadata is not being set for the rbd images as expected!")
