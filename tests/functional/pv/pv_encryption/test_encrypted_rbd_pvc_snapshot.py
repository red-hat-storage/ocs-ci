import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier1,
    skipif_ocp_version,
    kms_config_required,
    skipif_managed_service,
    skipif_hci_provider_and_client,
    skipif_disconnected_cluster,
    skipif_proxy_cluster,
)
from ocs_ci.ocs.resources.pod import cal_md5sum, verify_data_integrity
from ocs_ci.helpers.helpers import (
    wait_for_resource_state,
    create_pods,
    get_snapshot_content_obj,
)
from ocs_ci.ocs.exceptions import (
    KMSResourceCleaneupError,
    ResourceNotFoundError,
)
from ocs_ci.utility import kms
from semantic_version import Version
from ocs_ci.ocs.node import list_encrypted_rbd_devices_onnode

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
    if config.ENV_DATA.get("vault_hcp"):
        argvalues = [
            pytest.param(
                "v1", kmsprovider, True, marks=pytest.mark.polarion_id("OCS-3969")
            ),
            pytest.param(
                "v2", kmsprovider, True, marks=pytest.mark.polarion_id("OCS-3970")
            ),
        ]
    else:
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
@skipif_hci_provider_and_client
@skipif_disconnected_cluster
@skipif_proxy_cluster
class TestEncryptedRbdBlockPvcSnapshot(ManageTest):
    """
    Tests to take snapshots of encrypted RBD Block VolumeMode PVCs

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
        self.sc_obj = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL,
            encrypted=True,
            encryption_kms_id=self.kms.kmsid,
        )

        if kms_provider == constants.VAULT_KMS_PROVIDER:
            # Create ceph-csi-kms-token in the tenant namespace
            self.kms.vault_path_token = self.kms.generate_vault_token()
            self.kms.create_vault_csi_kms_token(namespace=self.proj_obj.namespace)

        # Create PVC and Pods
        self.pvc_size = 5
        self.pvc_objs = multi_pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            project=self.proj_obj,
            storageclass=self.sc_obj,
            size=self.pvc_size,
            access_modes=[
                f"{constants.ACCESS_MODE_RWX}-Block",
                f"{constants.ACCESS_MODE_RWO}-Block",
            ],
            status=constants.STATUS_BOUND,
            num_of_pvc=2,
            wait_each=False,
        )

        self.pod_objs = create_pods(
            self.pvc_objs,
            pod_factory,
            constants.CEPHBLOCKPOOL,
            pods_for_rwx=1,
            status=constants.STATUS_RUNNING,
        )

        # Verify if the key is created in Vault
        self.vol_handles = []
        for pvc_obj in self.pvc_objs:
            pv_obj = pvc_obj.backed_pv_obj
            vol_handle = pv_obj.get().get("spec").get("csi").get("volumeHandle")
            self.vol_handles.append(vol_handle)
            if kms_provider == constants.VAULT_KMS_PROVIDER:
                if kms.is_key_present_in_path(
                    key=vol_handle, path=self.kms.vault_backend_path
                ):
                    log.info(f"Vault: Found key for {pvc_obj.name}")
                else:
                    raise ResourceNotFoundError(
                        f"Vault: Key not found for {pvc_obj.name}"
                    )

    def test_encrypted_rbd_block_pvc_snapshot(
        self,
        kms_provider,
        snapshot_factory,
        snapshot_restore_factory,
        pod_factory,
        kv_version,
    ):
        """
        Test to take snapshots of encrypted RBD Block VolumeMode PVCs

        """

        log.info(
            "Check for encrypted device, find initial md5sum value and run IO on all pods"
        )
        for vol_handle, pod_obj in zip(self.vol_handles, self.pod_objs):

            rbd_devices = list_encrypted_rbd_devices_onnode(pod_obj.get_node())
            crypt_device = [device for device in rbd_devices if vol_handle in device]
            if not crypt_device:
                raise ResourceNotFoundError(
                    f"Encrypted device not found in {pod_obj.name}"
                )

            # Find initial md5sum
            pod_obj.md5sum_before_io = cal_md5sum(
                pod_obj=pod_obj,
                file_name=pod_obj.get_storage_path(storage_type="block"),
                block=True,
            )
            pod_obj.run_io(
                storage_type="block",
                size=f"{self.pvc_size - 1}G",
                io_direction="write",
                runtime=60,
                end_fsync=1,
                direct=1,
            )
        log.info("IO started on all pods")

        # Wait for IO completion
        for pod_obj in self.pod_objs:
            pod_obj.get_fio_results()
        log.info("IO completed on all pods")

        snap_objs, snap_handles = ([] for i in range(2))

        # Verify md5sum has changed after IO. Create snapshot
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
            if kms_provider == constants.VAULT_KMS_PROVIDER:
                if kms.is_key_present_in_path(
                    key=snap_handle, path=self.kms.vault_backend_path
                ):
                    log.info(f"Vault: Found key for snapshot {snap_obj.name}")
                else:
                    raise ResourceNotFoundError(
                        f"Vault: Key not found for snapshot {snap_obj.name}"
                    )
            snap_handles.append(snap_handle)

        # Delete pods
        log.info("Deleting the pods")
        for pod_obj in self.pod_objs:
            pod_obj.delete()
            pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)
        log.info("Deleted all the pods")

        # Delete parent PVCs to verify snapshot is independent
        log.info("Deleting parent PVCs")
        for pvc_obj in self.pvc_objs:
            pv_obj = pvc_obj.backed_pv_obj
            pvc_obj.delete()
            pvc_obj.ocp.wait_for_delete(resource_name=pvc_obj.name)
            log.info(
                f"Deleted PVC {pvc_obj.name}. Verifying whether PV "
                f"{pv_obj.name} is deleted."
            )
            pv_obj.ocp.wait_for_delete(resource_name=pv_obj.name)
        log.info("All parent PVCs and PVs are deleted before restoring snapshot.")

        restore_pvc_objs, restore_vol_handles = ([] for i in range(2))

        # Create PVCs out of the snapshots
        log.info("Creating new PVCs from snapshots")
        for snap_obj in snap_objs:
            log.info(f"Creating a PVC from snapshot {snap_obj.name}")
            restore_pvc_obj = snapshot_restore_factory(
                snapshot_obj=snap_obj,
                storageclass=self.sc_obj.name,
                size=f"{self.pvc_size}Gi",
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
            if kms_provider == constants.VAULT_KMS_PROVIDER:
                if kms.is_key_present_in_path(
                    key=vol_handle, path=self.kms.vault_backend_path
                ):
                    log.info(f"Vault: Found key for restore PVC {pvc_obj.name}")
                else:
                    raise ResourceNotFoundError(
                        f"Vault: Key not found for restored PVC {pvc_obj.name}"
                    )

        # Verify encrypted device is present and md5sum on all pods
        for vol_handle, pod_obj in zip(restore_vol_handles, restore_pod_objs):
            rbd_devices = list_encrypted_rbd_devices_onnode(pod_obj.get_node())
            crypt_device = [device for device in rbd_devices if vol_handle in device]
            if not crypt_device:
                raise ResourceNotFoundError(
                    f"Encrypted device not found in {pod_obj.name}"
                )

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

        if kms_provider == constants.VAULT_KMS_PROVIDER:
            # Verify if keys for PVCs and snapshots are deleted from  Vault
            if kv_version == "v1" or Version.coerce(
                config.ENV_DATA["ocs_version"]
            ) >= Version.coerce("4.9"):
                log.info(
                    "Verify whether the keys for PVCs and snapshots are deleted in vault"
                )
                for key in self.vol_handles + snap_handles + restore_vol_handles:
                    if not kms.is_key_present_in_path(
                        key=key, path=self.kms.vault_backend_path
                    ):
                        log.info(f"Vault: Key deleted for {key}")
                    else:
                        raise KMSResourceCleaneupError(
                            f"Vault: Key deletion failed for {key}"
                        )
                log.info("All keys from vault were deleted")
