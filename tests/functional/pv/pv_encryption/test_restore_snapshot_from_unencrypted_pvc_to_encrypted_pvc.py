import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework import config
from time import sleep
from ocs_ci.helpers import helpers

from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier1,
    skipif_ocp_version,
    kms_config_required,
    skipif_managed_service,
    skipif_disconnected_cluster,
    skipif_proxy_cluster,
)
from ocs_ci.ocs.resources.pod import cal_md5sum
from ocs_ci.helpers.helpers import (
    create_pods,
    get_snapshot_content_obj,
)
from ocs_ci.framework.pytest_customization.marks import green_squad

log = logging.getLogger(__name__)

# Set the arg values based on KMS provider.
if config.ENV_DATA["KMS_PROVIDER"].lower() == constants.HPCS_KMS_PROVIDER:
    kmsprovider = constants.HPCS_KMS_PROVIDER
    argnames = ["kv_version", "kms_provider"]
    argvalues = [
        pytest.param("v1", kmsprovider),
    ]
else:
    kmsprovider = constants.VAULT_KMS_PROVIDER
    argnames = ["kv_version", "kms_provider", "use_vault_namespace"]
    argvalues = [
        pytest.param(
            "v1", kmsprovider, False, marks=pytest.mark.polarion_id("OCS-2612")
        ),
        pytest.param(
            "v2", kmsprovider, False, marks=pytest.mark.polarion_id("OCS-2613")
        ),
    ]


@green_squad
@pytest.mark.parametrize(
    argnames=argnames,
    argvalues=argvalues,
)
@tier1
@skipif_ocs_version("<4.8")
@skipif_ocp_version("<4.8")
@kms_config_required
@skipif_managed_service
@skipif_disconnected_cluster
@skipif_proxy_cluster
class TestUnencryptedPvcSnapshotRestore(ManageTest):
    """
    Test we can not restore an unencrypted snapshot to an encrypted PVC

    Steps:
        1:- Create project
        2:- Create an encryption enabled storageclass for RBD
        3:- Create a storageclass for RBD without encryption
        4:- Create ceph-csi-kms-token in the tenant namespace
        5:- Create PVC with unencrypted storageclass and Pods
        6:- Find initial md5sum
        7:- Run IO
        8:- Wait for IO completion
        9:- Verify md5sum has changed after IO
        10:- Create snapshot for the PVC
        11:- Verify snapshots are ready
        12:- Restoring snapshots to create new PVCs using encrypted storageclass
        13:- Confirm that the restored PVCs are in Pending status and validate error log
        14:- Deletion of Pods, PVCs and Snapshots

    """

    @pytest.fixture(autouse=True)
    def setup(
        self,
        kv_version,
        kms_provider,
        use_vault_namespace,
        pv_encryption_kms_setup_factory,
        project_factory,
        pod_factory,
        storageclass_factory,
        multi_pvc_factory,
    ):
        """
        Setup csi-kms-connection-details configmap

        """

        log.info("Setting up csi-kms-connection-details configmap")
        self.kms = pv_encryption_kms_setup_factory(kv_version, use_vault_namespace)
        log.info("csi-kms-connection-details setup successful")

        # Create a project
        self.proj_obj = project_factory()

        # Create an encryption enabled storageclass for RBD
        self.sc_obj_with_encryption = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL,
            encrypted=True,
            encryption_kms_id=self.kms.kmsid,
        )

        # Create a storageclass for RBD without encryption
        self.sc_obj_without_encryption = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL,
        )

        if kms_provider == constants.VAULT_KMS_PROVIDER:
            # Create ceph-csi-kms-token in the tenant namespace
            self.kms.vault_path_token = self.kms.generate_vault_token()
            self.kms.create_vault_csi_kms_token(namespace=self.proj_obj.namespace)

        # Create PVC and Pods
        self.pvc_size = 2
        self.pvc_objs = multi_pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            project=self.proj_obj,
            storageclass=self.sc_obj_without_encryption,
            size=self.pvc_size,
            access_modes=[
                f"{constants.ACCESS_MODE_RWX}-Block",
                f"{constants.ACCESS_MODE_RWO}-Block",
            ],
            status=constants.STATUS_BOUND,
            num_of_pvc=1,
            wait_each=False,
        )

        self.pod_objs = create_pods(
            self.pvc_objs,
            pod_factory,
            constants.CEPHBLOCKPOOL,
            pods_for_rwx=1,
            status=constants.STATUS_RUNNING,
        )

    def test_unencrypted_pvc_snapshot_restore(
        self,
        snapshot_factory,
        snapshot_restore_factory,
    ):
        """
        Test to restore snapshots of unencrypted RBD Block VolumeMode PVCs to an encrypted PVC

        """

        log.info(
            "Check for unencrypted device, find initial md5sum value and run IO on all pods"
        )
        for pod_obj in self.pod_objs:
            # Find initial md5sum
            pod_obj.md5sum_before_io = cal_md5sum(
                pod_obj=pod_obj,
                file_name=pod_obj.get_storage_path(storage_type="block"),
                block=True,
            )

            # Run IO
            pod_obj.run_io(
                storage_type="block",
                size=f"{self.pvc_size - 1}G",
                io_direction="write",
                runtime=60,
            )
            log.info("IO started on all pods")

            # Wait for IO completion
            pod_obj.get_fio_results()
            log.info("IO completed on all pods")

        snap_objs, snap_handles = ([] for i in range(2))

        # Verify md5sum has changed after IO.
        log.info("Verify md5sum has changed after IO and create snapshot from all PVCs")
        for pod_obj in self.pod_objs:
            md5sum_after_io = cal_md5sum(
                pod_obj=pod_obj,
                file_name=pod_obj.get_storage_path(storage_type="block"),
                block=True,
            )
            assert (
                pod_obj.md5sum_before_io != md5sum_after_io
            ), f"md5sum has not changed after IO on pod {pod_obj.name}"

            # Create snapshot
            log.info(f"Creating snapshot of PVC {pod_obj.pvc.name}")
            snap_obj = snapshot_factory(pod_obj.pvc, wait=False)
            snap_obj.md5sum = md5sum_after_io
            snap_objs.append(snap_obj)
            log.info("Snapshots created")

        # Verify snapshots are ready
        log.info("Verify snapshots are ready")
        for snap_obj in snap_objs:
            snap_obj.ocp.wait_for_resource(
                condition="true",
                resource_name=snap_obj.name,
                column=constants.STATUS_READYTOUSE,
                timeout=180,
            )
            snapshot_content = get_snapshot_content_obj(snap_obj=snap_obj)
            snap_handle = snapshot_content.get().get("status").get("snapshotHandle")
            snap_handles.append(snap_handle)
        sleep(60)

        # Restoring snapshots to create new PVCs
        log.info("Creating new PVCs from snapshots")
        restore_pvcs = []
        for snap_obj in snap_objs:
            # for storageclass in sc_objs:
            log.info(f"Creating a PVC from snapshot {snap_obj.name}")
            restore_pvc_obj = snapshot_restore_factory(
                snapshot_obj=snap_obj,
                storageclass=self.sc_obj_with_encryption.name,
                size=f"{self.pvc_size}Gi",
                volume_mode=snap_obj.parent_volume_mode,
                access_mode=snap_obj.parent_access_mode,
                status="",
            )
            restore_pvcs.append(restore_pvc_obj)

            log.info(
                f"Created PVC {restore_pvc_obj.name} from snapshot {snap_obj.name}."
                f"Used the storage class {self.sc_obj_with_encryption}"
            )
            restore_pvc_obj.md5sum = snap_obj.md5sum
            failure_str = "cannot create encrypted volume from unencrypted volume"
            # Confirm that the restored PVCs are in Pending status
            for restore_pvc_obj in restore_pvcs:
                helpers.wait_for_resource_state(
                    resource=restore_pvc_obj,
                    state=constants.STATUS_PENDING,
                    timeout=200,
                )
                restore_pvc_obj.reload()
                if failure_str in restore_pvc_obj.describe():
                    log.info(
                        f"cannot create encrypted volume from unencrypted volume {snap_obj.name}"
                    )
                else:
                    log.warning(
                        f"Able to create encrypted volume from unencrypted volume {snap_obj.name}"
                    )
        log.info("Verified: Restored PVCs are in Pending state.")

        # Deletion of Pods, PVCs and Snapshots
        log.info("Deleting pod")
        for pod_obj in self.pod_objs:
            pod_obj.delete()
            pod_obj.ocp.wait_for_delete(
                pod_obj.name, 180
            ), f"Pod {pod_obj.name} is not deleted"

        log.info("Deleting PVC")
        for pvc_obj in self.pvc_objs:
            pvc_obj.delete()
            pvc_obj.ocp.wait_for_delete(
                resource_name=pvc_obj.name
            ), f"PVC {pvc_obj.name} is not deleted"
            log.info(f"Verified: PVC {pvc_obj.name} is deleted.")

        log.info("Deleting restored PVCs")
        for restore_pvc_obj in restore_pvcs:
            pv_obj = restore_pvc_obj.backed_pv_obj
            restore_pvc_obj.delete()
            pv_obj.ocp.wait_for_delete(resource_name=pv_obj.name)

        log.info("Deleting the snapshots")
        for snap_obj in snap_objs:
            snapcontent_obj = get_snapshot_content_obj(snap_obj=snap_obj)
            snap_obj.delete()
            snapcontent_obj.ocp.wait_for_delete(resource_name=snapcontent_obj.name)
