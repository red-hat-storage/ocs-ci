import logging
import pytest

from ocs_ci.ocs import constants
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
    config,
)
from ocs_ci.ocs.resources import pvc
from ocs_ci.ocs.resources import pod
from ocs_ci.helpers import helpers

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
                "v1", kmsprovider, True, marks=pytest.mark.polarion_id("OCS-3971")
            ),
            pytest.param(
                "v2", kmsprovider, True, marks=pytest.mark.polarion_id("OCS-3972")
            ),
        ]
    else:
        argvalues = [
            pytest.param(
                "v1", kmsprovider, False, marks=pytest.mark.polarion_id("OCS-2650")
            ),
            pytest.param(
                "v2", kmsprovider, False, marks=pytest.mark.polarion_id("OCS-2651")
            ),
        ]


@green_squad
@tier1
@skipif_ocs_version("<4.8")
@skipif_ocp_version("<4.8")
@kms_config_required
@skipif_managed_service
@skipif_hci_provider_and_client
@skipif_disconnected_cluster
@skipif_proxy_cluster
@pytest.mark.parametrize(
    argnames=argnames,
    argvalues=argvalues,
)
class TestEncryptedRbdClone(ManageTest):
    """
    Tests to verify PVC to PVC clone feature for encrypted RBD Block VolumeMode PVCs

    """

    @pytest.fixture(autouse=True)
    def setup(
        self,
        kv_version,
        kms_provider,
        use_vault_namespace,
        pv_encryption_kms_setup_factory,
        project_factory,
        multi_pvc_factory,
        pod_factory,
        storageclass_factory,
    ):
        """
        Setup csi-kms-connection-details configmap and create resources for the test

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
        self.pvc_size = 1
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

        self.pod_objs = helpers.create_pods(
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

    def test_pvc_to_pvc_clone(self, kv_version, kms_provider, pod_factory):
        """
        Test to create a clone from an existing encrypted RBD PVC.
        Verify that the cloned PVC is encrypted and all the data is preserved.

        """

        log.info("Checking for encrypted device and running IO on all pods")
        for vol_handle, pod_obj in zip(self.vol_handles, self.pod_objs):
            rbd_devices = list_encrypted_rbd_devices_onnode(pod_obj.get_node())
            crypt_device = [device for device in rbd_devices if vol_handle in device]
            if not crypt_device:
                raise ResourceNotFoundError(
                    f"Encrypted device not found in {pod_obj.name}"
                )

            log.info(f"File created during IO {pod_obj.name}")
            pod_obj.run_io(
                storage_type="block",
                size="500M",
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

        cloned_pvc_objs, cloned_vol_handles = ([] for i in range(2))

        # Calculate the md5sum value and create clones of exisiting PVCs
        log.info("Calculate the md5sum after IO and create clone of all PVCs")
        for pod_obj in self.pod_objs:
            pod_obj.md5sum_after_io = pod.cal_md5sum(
                pod_obj=pod_obj,
                file_name=pod_obj.get_storage_path(storage_type="block"),
                block=True,
            )

            cloned_pvc_obj = pvc.create_pvc_clone(
                self.sc_obj.name,
                pod_obj.pvc.name,
                constants.CSI_RBD_PVC_CLONE_YAML,
                self.proj_obj.namespace,
                volume_mode=constants.VOLUME_MODE_BLOCK,
                access_mode=pod_obj.pvc.access_mode,
            )
            helpers.wait_for_resource_state(cloned_pvc_obj, constants.STATUS_BOUND)
            cloned_pvc_obj.reload()
            cloned_pvc_obj.md5sum = pod_obj.md5sum_after_io
            cloned_pvc_objs.append(cloned_pvc_obj)
        log.info("Clone of all PVCs created")

        # Create and attach pod to the pvc
        cloned_pod_objs = helpers.create_pods(
            cloned_pvc_objs,
            pod_factory,
            constants.CEPHBLOCKPOOL,
            pods_for_rwx=1,
            status="",
        )

        # Verify the new pods are running
        log.info("Verify the new pods are running")
        for pod_obj in cloned_pod_objs:
            helpers.wait_for_resource_state(pod_obj, constants.STATUS_RUNNING)
            pod_obj.reload()
        log.info("Verified: New pods are running")

        # Verify encryption keys are created for cloned PVCs in Vault
        for pvc_obj in cloned_pvc_objs:
            pv_obj = pvc_obj.backed_pv_obj
            vol_handle = pv_obj.get().get("spec").get("csi").get("volumeHandle")
            cloned_vol_handles.append(vol_handle)

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
        for vol_handle, pod_obj in zip(cloned_vol_handles, cloned_pod_objs):
            rbd_devices = list_encrypted_rbd_devices_onnode(pod_obj.get_node())
            crypt_device = [device for device in rbd_devices if vol_handle in device]
            if not crypt_device:
                raise ResourceNotFoundError(
                    f"Encrypted device not found in {pod_obj.name}"
                )

            log.info(f"Verifying md5sum on pod {pod_obj.name}")
            pod.verify_data_integrity(
                pod_obj=pod_obj,
                file_name=pod_obj.get_storage_path(storage_type="block"),
                original_md5sum=pod_obj.pvc.md5sum,
                block=True,
            )
            log.info(f"Verified md5sum on pod {pod_obj.name}")

        # Run IO on new pods
        log.info("Starting IO on new pods")
        for pod_obj in cloned_pod_objs:
            pod_obj.run_io(storage_type="block", size="100M", runtime=10)

        # Wait for IO completion on new pods
        log.info("Waiting for IO completion on new pods")
        for pod_obj in cloned_pod_objs:
            pod_obj.get_fio_results()
        log.info("IO completed on new pods.")

        # Delete the restored pods, PVC and snapshots
        log.info("Deleting all pods")
        for pod_obj in cloned_pod_objs + self.pod_objs:
            pod_obj.delete()
            pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)

        log.info("Deleting all PVCs")
        for pvc_obj in cloned_pvc_objs + self.pvc_objs:
            pv_obj = pvc_obj.backed_pv_obj
            pvc_obj.delete()
            pv_obj.ocp.wait_for_delete(resource_name=pv_obj.name)

        if kms_provider == constants.VAULT_KMS_PROVIDER:
            # Verify if the keys for parent and cloned PVCs are deleted from Vault
            if kv_version == "v1" or Version.coerce(
                config.ENV_DATA["ocs_version"]
            ) >= Version.coerce("4.9"):
                log.info(
                    "Verify whether the keys for cloned PVCs are deleted from vault"
                )
                for key in cloned_vol_handles + self.vol_handles:
                    if not kms.is_key_present_in_path(
                        key=key, path=self.kms.vault_backend_path
                    ):
                        log.info(f"Vault: Key deleted for {key}")
                    else:
                        raise KMSResourceCleaneupError(
                            f"Vault: Key deletion failed for {key}"
                        )
                log.info("All keys from vault were deleted")
