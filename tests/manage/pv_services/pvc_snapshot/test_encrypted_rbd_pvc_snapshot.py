import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.ocs.defaults import VAULT_CSI_CONNECTION_CONF
from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier1,
    skipif_ocp_version,
)
from ocs_ci.ocs.resources.pod import cal_md5sum, verify_data_integrity
from ocs_ci.helpers.helpers import (
    wait_for_resource_state,
    create_pods,
    create_unique_resource_name,
    get_snapshot_content_obj,
)
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    KMSResourceCleaneupError,
    ResourceNotFoundError,
)
from ocs_ci.utility import kms
from ocs_ci.ocs.ocp import OCP

log = logging.getLogger(__name__)


@pytest.mark.parametrize(
    argnames=["kv_version"],
    argvalues=[
        pytest.param("v1", marks=pytest.mark.polarion_id("OCS-2585")),
        pytest.param("v2", marks=pytest.mark.polarion_id("OCS-2592")),
    ],
)
@tier1
@skipif_ocs_version("<4.8")
@skipif_ocp_version("<4.8")
class TestEncryptedRbdBlockPvcSnapshot(ManageTest):
    """
    Tests to take snapshots of encrypted RBD Block VolumeMode PVCs

    """

    @pytest.fixture(autouse=True)
    def setup(self, kv_version, request):
        """
        Setup csi-kms-connection-details configmap

        """
        # Initialize Vault
        self.vault = kms.Vault()
        self.vault.gather_init_vault_conf()
        self.vault.update_vault_env_vars()

        # Check if cert secrets already exist, if not create cert resources
        ocp_obj = OCP(kind="secret", namespace=constants.OPENSHIFT_STORAGE_NAMESPACE)
        try:
            ocp_obj.get_resource(resource_name="ocs-kms-ca-secret", column="NAME")
        except CommandFailed as cfe:
            if "not found" not in str(cfe):
                raise
            else:
                self.vault.create_ocs_vault_cert_resources()

        # Create vault namespace, backend path and policy in vault
        self.vault_resource_name = create_unique_resource_name("test", "vault")
        self.vault.vault_create_namespace(namespace=self.vault_resource_name)
        self.vault.vault_create_backend_path(
            backend_path=self.vault_resource_name, kv_version=kv_version
        )
        self.vault.vault_create_policy(policy_name=self.vault_resource_name)

        ocp_obj = OCP(kind="configmap", namespace=constants.OPENSHIFT_STORAGE_NAMESPACE)

        # If csi-kms-connection-details exists, edit the configmap to add new vault config
        try:
            ocp_obj.get_resource(
                resource_name="csi-kms-connection-details", column="NAME"
            )
            self.new_kmsid = self.vault_resource_name
            vdict = VAULT_CSI_CONNECTION_CONF
            for key in vdict.keys():
                old_key = key
            vdict[self.new_kmsid] = vdict.pop(old_key)
            vdict[self.new_kmsid]["VAULT_BACKEND_PATH"] = self.vault_resource_name
            vdict[self.new_kmsid]["VAULT_NAMESPACE"] = self.vault_resource_name
            kms.update_csi_kms_vault_connection_details(vdict)

        except CommandFailed as cfe:
            if "not found" not in str(cfe):
                raise
            else:
                self.new_kmsid = "1-vault"
                self.vault.create_vault_csi_kms_connection_details()

        def finalizer():
            # Remove the vault config from csi-kms-connection-details configMap
            if len(kms.get_encryption_kmsid()) > 1:
                kms.remove_kmsid(self.new_kmsid)

            # Delete the resources in vault
            self.vault.remove_vault_backend_path()
            self.vault.remove_vault_policy()
            self.vault.remove_vault_namespace()

        request.addfinalizer(finalizer)

    def test_encrypted_rbd_block_pvc_snapshot(
        self,
        snapshot_factory,
        snapshot_restore_factory,
        pod_factory,
        project_factory,
        storageclass_factory,
        multi_pvc_factory,
        kv_version,
    ):
        """
        Test to take snapshots of encrypted RBD Block VolumeMode PVCs

        """
        # Create a project
        proj_obj = project_factory()

        # Create an encryption enabled storageclass for RBD
        sc_obj = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL,
            encrypted=True,
            encryption_kms_id=self.new_kmsid,
        )

        # Create ceph-csi-kms-token in the tenant namespace
        self.vault.vault_path_token = self.vault.generate_vault_token()
        self.vault.create_vault_csi_kms_token(namespace=proj_obj.namespace)

        pvc_size = 5

        pvc_objs = multi_pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            project=proj_obj,
            storageclass=sc_obj,
            size=pvc_size,
            access_modes=[
                f"{constants.ACCESS_MODE_RWX}-Block",
                f"{constants.ACCESS_MODE_RWO}-Block",
            ],
            status=constants.STATUS_BOUND,
            num_of_pvc=2,
            wait_each=False,
        )

        pod_objs = create_pods(
            pvc_objs,
            pod_factory,
            constants.CEPHBLOCKPOOL,
            pods_for_rwx=1,
            status=constants.STATUS_RUNNING,
        )
        # Verify if the key is created in Vault
        vol_handles = []
        for pvc_obj in pvc_objs:
            pv_obj = pvc_obj.backed_pv_obj
            vol_handle = pv_obj.get().get("spec").get("csi").get("volumeHandle")
            vol_handles.append(vol_handle)

            # Check if encryption key is created in Vault
            if kms.is_key_present_in_path(
                key=vol_handle, path=self.vault.vault_backend_path
            ):
                log.info(f"Vault: Found key for {pvc_obj.name}")
            else:
                raise ResourceNotFoundError(f"Vault: Key not found for {pvc_obj.name}")

        log.info(
            "Check for encrypted device, find initial md5sum value and run IO on all pods"
        )
        for vol_handle, pod_obj in zip(vol_handles, pod_objs):
            # Verify whether encrypted device is present inside the pod
            if pod_obj.exec_sh_cmd_on_pod(
                command=f"lsblk | grep {vol_handle} | grep crypt"
            ):
                log.info(f"Encrypted device found in {pod_obj.name}")
            else:
                log.error(f"Encrypted device not found in {pod_obj.name}")

            # Find initial md5sum
            pod_obj.md5sum_before_io = cal_md5sum(
                pod_obj=pod_obj,
                file_name=pod_obj.get_storage_path(storage_type="block"),
                block=True,
            )
            pod_obj.run_io(
                storage_type="block",
                size=f"{pvc_size - 1}G",
                io_direction="write",
                runtime=60,
            )
        log.info("IO started on all pods")

        # Wait for IO completion
        for pod_obj in pod_objs:
            pod_obj.get_fio_results()
        log.info("IO completed on all pods")

        snap_objs, snap_handles = ([] for i in range(2))

        # Verify md5sum has changed after IO. Create snapshot
        log.info("Verify md5sum has changed after IO and create snapshot from all PVCs")
        for pod_obj in pod_objs:
            md5sum_after_io = cal_md5sum(
                pod_obj=pod_obj,
                file_name=pod_obj.get_storage_path(storage_type="block"),
                block=True,
            )
            assert (
                pod_obj.md5sum_before_io != md5sum_after_io
            ), f"md5sum has not changed after IO on pod {pod_obj.name}"
            log.info(f"Creating snapshot of PVC {pod_obj.pvc.name}")
            snap_obj = snapshot_factory(pod_obj.pvc, wait=False)
            snap_obj.md5sum = md5sum_after_io
            snap_objs.append(snap_obj)
        log.info("Snapshots created")

        # Verify snapshots are ready and verify if encryption key is created in vault
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
            if kms.is_key_present_in_path(
                key=snap_handle, path=self.vault.vault_backend_path
            ):
                log.info(f"Vault: Found key for snapshot {snap_obj.name}")
            else:
                raise ResourceNotFoundError(
                    f"Vault: Key not found for snapshot {snap_obj.name}"
                )
            snap_handles.append(snap_handle)

        # Delete pods
        log.info("Deleting the pods")
        for pod_obj in pod_objs:
            pod_obj.delete()
            pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)
        log.info("Deleted all the pods")

        # Delete parent PVCs to verify snapshot is independent
        log.info("Deleting parent PVCs")
        for pvc_obj in pvc_objs:
            pv_obj = pvc_obj.backed_pv_obj
            pvc_obj.delete()
            pvc_obj.ocp.wait_for_delete(resource_name=pvc_obj.name)
            log.info(
                f"Deleted PVC {pvc_obj.name}. Verifying whether PV "
                f"{pv_obj.name} is deleted."
            )
            pv_obj.ocp.wait_for_delete(resource_name=pv_obj.name)
        log.info(
            "Deleted parent PVCs before restoring snapshot. " "PVs are also deleted."
        )

        restore_pvc_objs, restore_vol_handles = ([] for i in range(2))

        # Create PVCs out of the snapshots
        log.info("Creating new PVCs from snapshots")
        for snap_obj in snap_objs:
            log.info(f"Creating a PVC from snapshot {snap_obj.name}")
            restore_pvc_obj = snapshot_restore_factory(
                snapshot_obj=snap_obj,
                storageclass=sc_obj.name,
                size=f"{pvc_size}Gi",
                volume_mode=snap_obj.parent_volume_mode,
                access_mode=snap_obj.parent_access_mode,
                status="",
            )
            log.info(
                f"Created PVC {restore_pvc_obj.name} from snapshot " f"{snap_obj.name}"
            )
            restore_pvc_obj.md5sum = snap_obj.md5sum
            restore_pvc_objs.append(restore_pvc_obj)
        log.info("Created new PVCs from all the snapshots")

        # Confirm that the restored PVCs are Bound
        log.info("Verify the restored PVCs are Bound")
        for pvc_obj in restore_pvc_objs:
            wait_for_resource_state(
                resource=pvc_obj, state=constants.STATUS_BOUND, timeout=180
            )
            pvc_obj.reload()
        log.info("Verified: Restored PVCs are Bound.")

        # Attach the restored PVCs to pods. Attach RWX PVC on two pods
        log.info("Attach the restored PVCs to pods")
        restore_pod_objs = create_pods(
            restore_pvc_objs,
            pod_factory,
            constants.CEPHBLOCKPOOL,
            pods_for_rwx=1,
            status="",
        )

        # Verify the new pods are running
        log.info("Verify the new pods are running")
        for pod_obj in restore_pod_objs:
            timeout = (
                300
                if config.ENV_DATA["platform"] == constants.IBMCLOUD_PLATFORM
                else 60
            )
            wait_for_resource_state(pod_obj, constants.STATUS_RUNNING, timeout)
        log.info("Verified: New pods are running")

        # Verify encryption keys are created for restored PVCs in Vault
        for pvc_obj in restore_pvc_objs:
            pv_obj = pvc_obj.backed_pv_obj
            vol_handle = pv_obj.get().get("spec").get("csi").get("volumeHandle")
            restore_vol_handles.append(vol_handle)
            if kms.is_key_present_in_path(
                key=vol_handle, path=self.vault.vault_backend_path
            ):
                log.info(f"Vault: Found key for restore PVC {pvc_obj.name}")
            else:
                raise ResourceNotFoundError(
                    f"Vault: Key not found for restored PVC {pvc_obj.name}"
                )

        # Verify encrypted device is present and md5sum on all pods
        for vol_handle, pod_obj in zip(restore_vol_handles, restore_pod_objs):
            if pod_obj.exec_sh_cmd_on_pod(
                command=f"lsblk | grep {vol_handle} | grep crypt"
            ):
                log.info(f"Encrypted device found in {pod_obj.name}")
            else:
                log.error(f"Encrypted device not found in {pod_obj.name}")

            log.info(f"Verifying md5sum on pod {pod_obj.name}")
            verify_data_integrity(
                pod_obj=pod_obj,
                file_name=pod_obj.get_storage_path(storage_type="block"),
                original_md5sum=pod_obj.pvc.md5sum,
                block=True,
            )
            log.info(f"Verified md5sum on pod {pod_obj.name}")

        # Run IO on new pods
        log.info("Starting IO on new pods")
        for pod_obj in restore_pod_objs:
            pod_obj.run_io(storage_type="block", size="500M", runtime=15)

        # Wait for IO completion on new pods
        log.info("Waiting for IO completion on new pods")
        for pod_obj in restore_pod_objs:
            pod_obj.get_fio_results()
        log.info("IO completed on new pods.")

        # Delete the restored pods, PVC and snapshots
        log.info("Deleting pods using restored PVCs")
        for pod_obj in restore_pod_objs:
            pod_obj.delete()
            pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)

        log.info("Deleting restored PVCs")
        for pvc_obj in restore_pvc_objs:
            pv_obj = pvc_obj.backed_pv_obj
            pvc_obj.delete()
            pv_obj.ocp.wait_for_delete(resource_name=pv_obj.name)

        log.info("Deleting the snapshots")
        for snap_obj in snap_objs:
            snapcontent_obj = get_snapshot_content_obj(snap_obj=snap_obj)
            snap_obj.delete()
            snapcontent_obj.ocp.wait_for_delete(resource_name=snapcontent_obj.name)

        # Verify if keys for PVCs and snapshots are deleted from  Vault
        if kv_version == "v1":
            log.info(
                "Verify whether the keys for PVCs and snapshots are deleted in vault"
            )
            for key in vol_handles + snap_handles + restore_vol_handles:
                if not kms.is_key_present_in_path(
                    key=key, path=self.vault.vault_backend_path
                ):
                    log.info(f"Vault: Key deleted for {key}")
                else:
                    raise KMSResourceCleaneupError(
                        f"Vault: Key deletion failed for {key}"
                    )
            log.info("All keys from vault were deleted")
