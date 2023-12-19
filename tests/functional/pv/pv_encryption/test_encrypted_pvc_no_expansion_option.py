import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    ManageTest,
    tier1,
    skipif_ocs_version,
    kms_config_required,
    skipif_managed_service,
    skipif_hci_provider_and_client,
    skipif_disconnected_cluster,
    skipif_proxy_cluster,
    config,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed


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
                "v1", kmsprovider, True, marks=pytest.mark.polarion_id("OCS-5396")
            ),
            pytest.param(
                "v2", kmsprovider, True, marks=pytest.mark.polarion_id("OCS-5397")
            ),
        ]
    else:
        argvalues = [
            pytest.param(
                "v1", kmsprovider, False, marks=pytest.mark.polarion_id("OCS-5394")
            ),
            pytest.param(
                "v2", kmsprovider, False, marks=pytest.mark.polarion_id("OCS-5395")
            ),
        ]


@green_squad
@skipif_ocs_version("<4.7")
@kms_config_required
@skipif_managed_service
@skipif_hci_provider_and_client
@skipif_disconnected_cluster
@skipif_proxy_cluster
class TestEncryptedPVCWithAllowVolumeExpansionFalse(ManageTest):
    """
    Test Encrypted Volume Expansion

    """

    @pytest.fixture()
    def setup(
        self,
        kv_version,
        use_vault_namespace,
        pv_encryption_kms_setup_factory,
    ):
        """
        Setup csi-kms-connection-details configmap

        """
        log.info("Setting up csi-kms-connection-details configmap")
        self.kms = pv_encryption_kms_setup_factory(kv_version, use_vault_namespace)
        log.info("csi-kms-connection-details setup successful")

    @tier1
    @pytest.mark.parametrize(
        argnames=argnames,
        argvalues=argvalues,
    )
    def test_encrypted_pvc_expansion_with_allow_volume_expansion_false(
        self,
        setup,
        kms_provider,
        project_factory,
        storageclass_factory,
        pvc_factory,
        pod_factory,
        kv_version,
    ):
        """Test Encrypted PVC expansion with 'allowVolumeExpansion: false' option

        Steps:
        1. Create a encrypted RBD storage class with 'allowVolumeExpansion: false' option.
        2. Configure the Persistent Volume (PV) encryption settings with Valut service.
        3. Request the creation of a PVC with a specified size of 5GB.
        4. Deploy a Pod that utilizes the previously created PVC for storage.
        5. Resize the PVC to increase its storage capacity by 5GB.
        6. Check the Error message that appear on resize operation.

        """
        # Create a project
        proj_obj = project_factory()

        # Create an encryption enabled storageclass for RBD
        sc_obj = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL,
            encrypted=True,
            encryption_kms_id=self.kms.kmsid,
            allow_volume_expansion=False,
        )

        if kms_provider == constants.VAULT_KMS_PROVIDER:
            # Create ceph-csi-kms-token in the tenant namespace
            self.kms.vault_path_token = self.kms.generate_vault_token()
            self.kms.create_vault_csi_kms_token(namespace=proj_obj.namespace)

        # Create RBD PVCs with volume mode Block
        pvc_size = 5
        pvc_obj = pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            project=proj_obj,
            storageclass=sc_obj,
            size=pvc_size,
            status=constants.STATUS_BOUND,
        )

        pod_obj = pod_factory(pvc=pvc_obj)
        # Verify the pod status
        log.info("Verifying the pod status.")
        assert (
            pod_obj.data["status"]["phase"] == constants.STATUS_RUNNING
        ), f"Pod {pod_obj.name} is not in {constants.STATUS_RUNNING} state."

        log.info("Resizing PVC")
        new_size = pvc_size + 5
        log.info(f"Expanding size of PVC {pvc_obj.name} to {new_size}G")

        with pytest.raises(CommandFailed):
            pvc_obj.resize_pvc(new_size, True)
