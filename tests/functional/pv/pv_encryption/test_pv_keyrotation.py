import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    ManageTest,
    tier1,
    tier2,
    skipif_ocs_version,
    kms_config_required,
    skipif_managed_service,
    skipif_hci_provider_and_client,
    skipif_disconnected_cluster,
    skipif_proxy_cluster,
    config,
    vault_kms_deployment_required,
)
from ocs_ci.ocs import constants
from ocs_ci.helpers.keyrotation_helper import PVKeyRotation


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
                "v1",
                kmsprovider,
                True,
                marks=[tier2, pytest.mark.polarion_id("OCS-6179")],
            ),
            pytest.param(
                "v2",
                kmsprovider,
                True,
                marks=[tier1, pytest.mark.polarion_id("OCS-6180")],
            ),
        ]
    else:
        argvalues = [
            pytest.param(
                "v1",
                kmsprovider,
                False,
                marks=[tier2, pytest.mark.polarion_id("OCS-6181")],
            ),
            pytest.param(
                "v2",
                kmsprovider,
                False,
                marks=[tier1, pytest.mark.polarion_id("OCS-6182")],
            ),
        ]


@green_squad
@skipif_ocs_version("<4.17")
@kms_config_required
@skipif_managed_service
@skipif_hci_provider_and_client
@skipif_disconnected_cluster
@skipif_proxy_cluster
@vault_kms_deployment_required
class TestPVKeyRotationWithVaultKMS(ManageTest):
    """
    Test Key Rotation for encrypted PV.

    """

    @pytest.fixture(autouse=True)
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

    @pytest.mark.parametrize(
        argnames=argnames,
        argvalues=argvalues,
    )
    def test_encrypted_pvc_key_rotation(
        self,
        kms_provider,
        project_factory,
        storageclass_factory,
        pvc_factory,
        pod_factory,
    ):
        """
        Test Encrypted PVC keyrotation.

        Steps:

        1.	Create an encrypted RBD storage class.
            2.	Add an annotation to the encrypted storage class.
            "keyrotation.csiaddons.openshift.io/schedule='*/3 * * * *'"
            3.	Configure the Persistent Volume (PV) encryption settings with the Vault service.
            4.	Create a PVC using the encrypted storage class.
            5.	Deploy a Pod that utilizes the previously created PVC for storage.
            6.	Start an IO workload with verify=True option.
            7.	Wait for the key rotation to occur for the PV.
            8.	Check for any error messages that appear during the IO operation.

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

        # Annotate Storageclass for keyrotation.
        pvk_obj = PVKeyRotation(sc_obj)
        pvk_obj.annotate_storageclass_key_rotation(schedule="*/3 * * * *")

        # Create RBD PVCs with volume mode Block
        pvc_obj = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            project=proj_obj,
            storageclass=sc_obj,
            size=10,
            status=constants.STATUS_BOUND,
        )

        pod_obj = pod_factory(pvc=pvc_obj)

        # Verify the pod status
        log.info("Verifying the pod status.")
        assert (
            pod_obj.data["status"]["phase"] == constants.STATUS_RUNNING
        ), f"Pod {pod_obj.name} is not in {constants.STATUS_RUNNING} state."

        pod_obj.run_io("fs", size="5G", verify=True, runtime=180)

        # Verify PV Keyrotation.
        assert pvk_obj.wait_till_keyrotation(
            pvc_obj.get_pv_volume_handle_name
        ), f"Failed to rotate Key for the PVC {pvc_obj.name}"

        log.info("Getting FIO results")
        result = pod_obj.get_fio_results(timeout=180)

        assert (
            "Error" not in result
        ), f" IO Failed when Keyrotation operation happen for the PVC: {pvc_obj.name}"
