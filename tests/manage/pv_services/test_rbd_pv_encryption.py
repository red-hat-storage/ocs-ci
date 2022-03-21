import logging
import pytest

from ocs_ci.framework.testlib import (
    ManageTest,
    tier1,
    skipif_ocs_version,
    kms_config_required,
    skipif_managed_service,
    config,
)
from ocs_ci.helpers.helpers import (
    create_pods,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import (
    KMSResourceCleaneupError,
    ResourceNotFoundError,
)
from ocs_ci.utility import kms

log = logging.getLogger(__name__)

# Set the arg values based on KMS provider.
if config.ENV_DATA["KMS_PROVIDER"].lower() == constants.HPCS_KMS_PROVIDER:
    kmsprovider = constants.HPCS_KMS_PROVIDER
    argvalues = [
        pytest.param("v1", kmsprovider),
    ]
else:
    kmsprovider = constants.VAULT_KMS_PROVIDER
    argvalues = [
        pytest.param("v1", kmsprovider, marks=pytest.mark.polarion_id("OCS-2585")),
        pytest.param("v2", kmsprovider, marks=pytest.mark.polarion_id("OCS-2592")),
    ]


@pytest.mark.parametrize(
    argnames=["kv_version", "kms_provider"],
    argvalues=argvalues,
)
@skipif_ocs_version("<4.7")
@kms_config_required
@skipif_managed_service
class TestRbdPvEncryption(ManageTest):
    """
    Test to verify RBD PV encryption

    """

    @pytest.fixture(autouse=True)
    def setup(
        self,
        kv_version,
        pv_encryption_kms_setup_factory,
    ):
        """
        Setup csi-kms-connection-details configmap

        """
        log.info("Setting up csi-kms-connection-details configmap")
        self.kms = pv_encryption_kms_setup_factory(kv_version)
        log.info("csi-kms-connection-details setup successful")

    @tier1
    def test_rbd_pv_encryption(
        self,
        kms_provider,
        project_factory,
        storageclass_factory,
        multi_pvc_factory,
        pod_factory,
        kv_version,
    ):
        """
        Test to verify creation and deletion of encrypted RBD PVC

        """
        # Create a project
        proj_obj = project_factory()

        # Create an encryption enabled storageclass for RBD
        sc_obj = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL,
            encrypted=True,
            encryption_kms_id=self.kms.kmsid,
        )

        if kms_provider == constants.VAULT_KMS_PROVIDER:
            # Create ceph-csi-kms-token in the tenant namespace
            self.kms.vault_path_token = self.kms.generate_vault_token()
            self.kms.create_vault_csi_kms_token(namespace=proj_obj.namespace)

        # Create RBD PVCs with volume mode Block
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
            num_of_pvc=3,
            wait_each=False,
        )

        # Create pods
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

            if kms_provider == constants.VAULT_KMS_PROVIDER:
                # Check if encryption key is created in Vault
                if kms.is_key_present_in_path(
                    key=vol_handle, path=self.kms.vault_backend_path
                ):
                    log.info(f"Vault: Found key for {pvc_obj.name}")
                else:
                    raise ResourceNotFoundError(
                        f"Vault: Key not found for {pvc_obj.name}"
                    )

        # Verify whether encrypted device is present inside the pod and run IO
        for vol_handle, pod_obj in zip(vol_handles, pod_objs):
            if pod_obj.exec_sh_cmd_on_pod(
                command=f"lsblk | grep {vol_handle} | grep crypt"
            ):
                log.info(f"Encrypted device found in {pod_obj.name}")
            else:
                log.error(f"Encrypted device not found in {pod_obj.name}")

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

        # Delete the pod
        for pod_obj in pod_objs:
            pod_obj.delete()
            pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)

        # Delete the PVC
        for pvc_obj in pvc_objs:
            pv_obj = pvc_obj.backed_pv_obj
            pvc_obj.delete()
            pv_obj.ocp.wait_for_delete(resource_name=pv_obj.name)

        if kms_provider == constants.VAULT_KMS_PROVIDER:
            # Verify whether the key is deleted in Vault. Skip check for kv-v2 due to BZ#1979244
            if kv_version == "v1":
                for vol_handle in vol_handles:
                    if not kms.is_key_present_in_path(
                        key=vol_handle, path=self.kms.vault_backend_path
                    ):
                        log.info(f"Vault: Key deleted for {vol_handle}")
                    else:
                        raise KMSResourceCleaneupError(
                            f"Vault: Key deletion failed for {vol_handle}"
                        )
