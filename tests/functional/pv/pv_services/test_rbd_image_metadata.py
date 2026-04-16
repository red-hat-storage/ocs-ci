import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.helpers.helpers import create_unique_resource_name, get_snapshot_content_obj
from ocs_ci.ocs.defaults import RBD_NAME
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod
from ocs_ci.utility.metadata_utils import validate_metadata
from ocs_ci.framework.pytest_customization.marks import (
    tier2,
    polarion_id,
    green_squad,
)

log = logging.getLogger(__name__)


@green_squad
class TestRbdImageMetadata:
    @tier2
    @polarion_id("OCS-4465")
    @polarion_id("OCS-4675")
    def test_rbd_image_metadata(
        self, pvc_factory, pvc_clone_factory, snapshot_restore_factory
    ):
        """
        Test that rbd images have correct metadata details for,
        1. a newly created RBD PVC
        2. PVC clone
        3. volume snapshot
        4. Restore volume from snapshot

        """

        rbd_pool_name = (
            (config.ENV_DATA.get("rbd_name") or RBD_NAME)
            if config.DEPLOYMENT["external_mode"]
            else constants.DEFAULT_CEPHBLOCKPOOL
        )
        ceph_tool_pod = get_ceph_tools_pod()

        # create a pvc with ceph-rbd sc
        pvc_obj = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            status=constants.STATUS_BOUND,
            volume_mode="Block",
        )
        log.info(f"PVC {pvc_obj.name} created!")

        # Get cluster name from PV's CSI volumeAttributes
        pv_obj = pvc_obj.backed_pv_obj
        cluster_name = pv_obj.get()["spec"]["csi"]["volumeAttributes"]["clusterID"]
        log.info(f"Cluster name: {cluster_name}")

        # Validate metadata for the original PVC
        image_name = pvc_obj.get_rbd_image_name
        metadata = self.get_rbd_image_metadata(ceph_tool_pod, rbd_pool_name, image_name)
        validate_metadata(
            metadata=metadata,
            clustername=cluster_name,
            pv_name=pvc_obj.backed_pv,
            pvc_name=pvc_obj.name,
            namespace=pvc_obj.namespace,
        )
        log.info(f"Metadata validation passed for PVC image {image_name}")

        # create a snapshot of the PVC
        snap_obj = pvc_obj.create_snapshot(
            snapshot_name=create_unique_resource_name("test", "snapshot"),
            wait=True,
        )
        log.info(f"Snapshot of PVC {pvc_obj.name} created!")
        snapshot_content = get_snapshot_content_obj(snap_obj=snap_obj)
        snap_content_name = snapshot_content.get().get("metadata").get("name")
        snap_handle = snapshot_content.get().get("status").get("snapshotHandle")
        snap_image_name = f'csi-snap-{snap_handle.split("-", 5)[5]}'

        # Validate metadata for the snapshot image
        metadata = self.get_rbd_image_metadata(
            ceph_tool_pod, rbd_pool_name, snap_image_name
        )
        validate_metadata(
            metadata=metadata,
            clustername=cluster_name,
            volumesnapshot_name=snap_obj.name,
            volumesnapshot_content=snap_content_name,
            namespace=pvc_obj.namespace,
        )
        log.info(f"Metadata validation passed for snapshot image {snap_image_name}")

        # restore the snapshot
        restored_pvc = snapshot_restore_factory(
            snapshot_obj=snap_obj, volume_mode=pvc_obj.get_pvc_vol_mode, timeout=600
        )
        log.info(f"Restored snapshot PVC {restored_pvc.name} created!")

        # Validate metadata for the restored PVC
        restored_image = restored_pvc.get_rbd_image_name
        metadata = self.get_rbd_image_metadata(
            ceph_tool_pod, rbd_pool_name, restored_image
        )
        validate_metadata(
            metadata=metadata,
            clustername=cluster_name,
            pv_name=restored_pvc.backed_pv,
            pvc_name=restored_pvc.name,
            namespace=restored_pvc.namespace,
        )
        log.info(f"Metadata validation passed for restored PVC image {restored_image}")

        # create a clone of the PVC
        clone_obj = pvc_clone_factory(pvc_obj)
        log.info(f"Clone of PVC {pvc_obj.name} created!")

        # Validate metadata for the clone
        clone_image = clone_obj.get_rbd_image_name
        metadata = self.get_rbd_image_metadata(
            ceph_tool_pod, rbd_pool_name, clone_image
        )
        validate_metadata(
            metadata=metadata,
            clustername=cluster_name,
            pv_name=clone_obj.backed_pv,
            pvc_name=clone_obj.name,
            namespace=clone_obj.namespace,
        )
        log.info(f"Metadata validation passed for clone image {clone_image}")

        log.info("Metadata validation passed for all rbd images!")

    @staticmethod
    def get_rbd_image_metadata(ceph_tool_pod, rbd_pool_name, image_name):
        """
        Get metadata for an RBD image as a dictionary.

        Args:
            ceph_tool_pod: Ceph tools pod object
            rbd_pool_name (str): RBD pool name
            image_name (str): RBD image name

        Returns:
            dict: Metadata key-value pairs
        """
        cmd = f"rbd image-meta list {rbd_pool_name}/{image_name} --format=json"
        metadata = ceph_tool_pod.exec_cmd_on_pod(command=cmd)
        log.info(f"Metadata for {image_name}: {metadata}")
        return metadata
