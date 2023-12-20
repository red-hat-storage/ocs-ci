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
from ocs_ci.helpers.helpers import verify_pvc_size


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
                "v1", kmsprovider, True, marks=pytest.mark.polarion_id("OCS-5389")
            ),
            pytest.param(
                "v2", kmsprovider, True, marks=pytest.mark.polarion_id("OCS-5390")
            ),
        ]
    else:
        argvalues = [
            pytest.param(
                "v1", kmsprovider, False, marks=pytest.mark.polarion_id("OCS-5387")
            ),
            pytest.param(
                "v2", kmsprovider, False, marks=pytest.mark.polarion_id("OCS-5388")
            ),
        ]


@green_squad
@skipif_ocs_version("<4.7")
@kms_config_required
@skipif_managed_service
@skipif_hci_provider_and_client
@skipif_disconnected_cluster
@skipif_proxy_cluster
class TestEncryptedVolumeExpansion(ManageTest):
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
    def test_encrypted_volume_expansion_with_vault_kms(
        self,
        setup,
        kms_provider,
        project_factory,
        storageclass_factory,
        pvc_factory,
        pod_factory,
        kv_version,
    ):
        """Test to verify encrypted PVC expansion with vault KMS service.

        Steps:
        1. Configure the Persistent Volume (PV) encryption settings with Valut service.
        2. Define and deploy a storage class with encryption enabled.
        3. Request the creation of a PVC with a specified size of 5GB.
        4. Deploy a Pod that utilizes the previously created PVC for storage.
        5. Dynamically resize the PVC to increase its storage capacity by 5GB.
        6. Check and confirm that the PVC has successfully expanded to a total size of 10GB.

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
        pvc_obj.resize_pvc(new_size, True)

        assert verify_pvc_size(
            pod_obj, new_size
        ), f"Expected pvc size {new_size}G is not matched with the attached PVC on pod {pod_obj.name}"

    @tier1
    @pytest.mark.polarion_id("OCS-5391")
    def test_encrypted_volume_expansion_with_kmip(
        self,
        pv_encryption_kmip_setup_factory,
        project_factory,
        storageclass_factory,
        pvc_factory,
        pod_factory,
    ):
        """Test to verify encrypted PVC expansion with KMIP service.

        Steps:
        1. Configure the Persistent Volume (PV) encryption settings with KMIP.
        2. Define and deploy a storage class with encryption enabled.
        3. Request the creation of a PVC with a specified size of 5GB.
        4. Deploy a Pod that utilizes the previously created PVC for storage.
        5. Dynamically resize the PVC to increase its storage capacity by 5GB.
        6. Check and confirm that the PVC has successfully expanded to a total size of 10GB.

        """

        log.info("Setting up csi-kms-connection-details configmap")
        kms = pv_encryption_kmip_setup_factory()
        log.info("csi-kms-connection-details setup successful")

        # Create a project
        proj_obj = project_factory()

        # Create an encryption enabled storageclass for RBD
        sc_obj = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL,
            encrypted=True,
            encryption_kms_id=kms.kmsid,
        )

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
        pvc_obj.resize_pvc(new_size, True)
        assert verify_pvc_size(
            pod_obj, new_size
        ), f"Expected pvc size {new_size}G is not matched with the attached PVC on pod {pod_obj.name}"
