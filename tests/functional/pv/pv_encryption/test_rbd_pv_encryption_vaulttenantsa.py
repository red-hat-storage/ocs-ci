import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    aws_platform_required,
    ManageTest,
    tier1,
    skipif_ocs_version,
    skipif_disconnected_cluster,
    skipif_proxy_cluster,
    kms_config_required,
    skipif_managed_service,
    skipif_hci_provider_and_client,
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
from ocs_ci.ocs.node import list_encrypted_rbd_devices_onnode

log = logging.getLogger(__name__)

# Set the arg values based on whether HCP Vault is being used
if config.ENV_DATA.get("vault_hcp"):
    argvalues = [
        pytest.param("v1", True, True, marks=pytest.mark.polarion_id("OCS-3839")),
        pytest.param("v2", True, True, marks=pytest.mark.polarion_id("OCS-3968")),
        pytest.param("v1", False, True, marks=pytest.mark.polarion_id("OCS-3840")),
        pytest.param("v2", False, True, marks=pytest.mark.polarion_id("OCS-3967")),
    ]
else:
    argvalues = [
        pytest.param("v1", True, False, marks=pytest.mark.polarion_id("OCS-2799")),
        pytest.param("v2", True, False, marks=pytest.mark.polarion_id("OCS-2800")),
    ]


@green_squad
@aws_platform_required
@skipif_ocs_version("<4.9")
@kms_config_required
@skipif_managed_service
@skipif_hci_provider_and_client
@skipif_disconnected_cluster
@skipif_proxy_cluster
@pytest.mark.parametrize(
    argnames=["kv_version", "use_auth_path", "use_vault_namespace"],
    argvalues=argvalues,
)
class TestRbdPvEncryptionVaultTenantSA(ManageTest):
    """
    Test to verify RBD PV encryption using vaulttenantsa method

    """

    @pytest.fixture(autouse=True)
    def setup(
        self,
        kv_version,
        use_auth_path,
        use_vault_namespace,
        vault_tenant_sa_setup_factory,
    ):
        """
        Configure kubernetes authentication method and setup csi-kms-connection-details configmap

        """
        log.info(
            "Configuring kube auth method and csi-kms-connection-details configmap"
        )
        self.kms = vault_tenant_sa_setup_factory(
            kv_version,
            use_auth_path=use_auth_path,
            use_vault_namespace=use_vault_namespace,
        )
        log.info("Test setup complete")

    @tier1
    def test_rbd_pv_encryption_vaulttenantsa(
        self,
        project_factory,
        storageclass_factory,
        multi_pvc_factory,
        pod_factory,
        kv_version,
    ):
        """
        Test to verify creation and deletion of encrypted RBD PVC using vaulttenantsa method

        """
        # Create a project
        proj_obj = project_factory()

        # Create an encryption enabled storageclass for RBD
        sc_obj = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL,
            encrypted=True,
            encryption_kms_id=self.kms.kmsid,
        )

        # Create serviceaccount in the tenant namespace
        self.kms.create_tenant_sa(namespace=proj_obj.namespace)

        # Create role in Vault
        self.kms.create_vault_kube_auth_role(namespace=proj_obj.namespace)

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

        # Check if encryption key is created in Vault
        if kms.is_key_present_in_path(key=vol_handle, path=self.kms.vault_backend_path):
            log.info(f"Vault: Found key for {pvc_obj.name}")
        else:
            raise ResourceNotFoundError(f"Vault: Key not found for {pvc_obj.name}")

        # Verify whether encrypted device is present inside the pod and run IO
        for vol_handle, pod_obj in zip(vol_handles, pod_objs):
            rbd_devices = list_encrypted_rbd_devices_onnode(pod_obj.get_node())
            crypt_device = [device for device in rbd_devices if vol_handle in device]
            if not crypt_device:
                raise ResourceNotFoundError(
                    f"Encrypted device not found in {pod_obj.name}"
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

        # Delete the pod
        for pod_obj in pod_objs:
            pod_obj.delete()
            pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)

        # Delete the PVC
        for pvc_obj in pvc_objs:
            pv_obj = pvc_obj.backed_pv_obj
            pvc_obj.delete()
            pv_obj.ocp.wait_for_delete(resource_name=pv_obj.name)

        # Verify whether the key is deleted in Vault
        for vol_handle in vol_handles:
            if not kms.is_key_present_in_path(
                key=vol_handle, path=self.kms.vault_backend_path
            ):
                log.info(f"Vault: Key deleted for {vol_handle}")
            else:
                raise KMSResourceCleaneupError(
                    f"Vault: Key deletion failed for {vol_handle}"
                )
