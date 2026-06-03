import pytest
import logging

from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    green_squad,
    azure_platform_required,
    azure_kv_config_required,
    polarion_id,
    skipif_ocs_version,
    skipif_managed_service,
    skipif_hci_provider_and_client,
    skipif_disconnected_cluster,
    skipif_proxy_cluster,
)

from ocs_ci.ocs import constants
from ocs_ci.helpers.helpers import create_pods
from ocs_ci.ocs.node import verify_crypt_device_present_onnode

logger = logging.getLogger(__name__)


@tier1
@green_squad
@azure_platform_required
@azure_kv_config_required
@skipif_ocs_version("<4.16")
@skipif_managed_service
@skipif_hci_provider_and_client
@skipif_disconnected_cluster
@skipif_proxy_cluster
class TestAzureKMSPVEncryption:
    @pytest.fixture(autouse=True)
    def setup(
        self,
        pv_encryption_kms_setup_factory,
    ):
        """
        Setup csi-kms-connection-details configmap

        """
        logger.test_step("Set up csi-kms-connection-details configmap for Azure KV")
        self.kms = pv_encryption_kms_setup_factory()
        logger.info("csi-kms-connection-details setup successful")

    @polarion_id("OCS-5795")
    def test_azure_kms_pv_encryption(
        self, project_factory, storageclass_factory, multi_pvc_factory, pod_factory
    ):
        """
        Verify Azure KV encryption operation with PV encryption.

        Steps:
            1. Set up Azure KV with the cluster.
            2. Create multiple PVCs and attach them to pods.
            3. Verify in Azure secrets that each PV has its corresponding secrets stored.
            4. Start IO from the pods on the encrypted PVCs.
            5. Verify that each PV's encrypted device is present on the node.
            6. Wait until IO is complete.
            7. Delete all the pods and PVCs that were created.
            8. Verify that after deleting the PVCs, the respective secrets are also removed from the Azure Vault.

        """
        logger.test_step("Create project and encryption-enabled RBD storage class")
        proj_obj = project_factory()

        sc_obj = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL,
            encrypted=True,
            encryption_kms_id=self.kms.azure_kms_connection_name,
        )

        logger.test_step("Create RBD PVCs with volume mode Block")
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

        logger.test_step("Verify PV secrets are present in Azure KV")
        vol_handles = []
        for pvc_obj in pvc_objs:
            pv_obj = pvc_obj.backed_pv_obj
            vol_handle = pv_obj.get().get("spec").get("csi").get("volumeHandle")
            vol_handles.append(vol_handle)

            logger.assertion(
                f"PV secret present in Azure KV for vol_handle={vol_handle}: "
                f"expected=True, actual={self.kms.verify_pv_secrets_present_in_azure_kv(vol_handle)}"
            )
            assert self.kms.verify_pv_secrets_present_in_azure_kv(
                vol_handle
            ), f"PV secret for vol_handle : {vol_handle} not found in the Azure KV"

        logger.test_step("Verify encrypted devices and start IO on all pods")
        for vol_handle, pod_obj in zip(vol_handles, pod_objs):
            node = pod_obj.get_node()
            logger.assertion(
                f"Crypt device present on node {node} for vol_handle={vol_handle}: "
                f"expected=True"
            )
            assert verify_crypt_device_present_onnode(
                node, vol_handle
            ), f"Crypt devicve {vol_handle} not found on node:{node}"

            pod_obj.run_io(
                storage_type="block",
                size=f"{pvc_size - 1}G",
                io_direction="write",
                runtime=60,
            )
        logger.info("IO started on all pods")

        logger.test_step("Wait for IO completion on all pods")
        for pod_obj in pod_objs:
            pod_obj.get_fio_results()
        logger.info("IO completed on all pods")

        logger.test_step("Delete pods and PVCs")
        for pod_obj in pod_objs:
            pod_obj.delete()
            pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)

        for pvc_obj in pvc_objs:
            pv_obj = pvc_obj.backed_pv_obj
            pvc_obj.delete()
            pv_obj.ocp.wait_for_delete(resource_name=pv_obj.name)

        logger.test_step(
            "Verify PV secrets are removed from Azure KV after PVC deletion"
        )
        for vol_handle in vol_handles:
            logger.assertion(
                f"PV secret removed from Azure KV for vol_handle={vol_handle}: "
                f"expected=True"
            )
            assert not self.kms.verify_pv_secrets_present_in_azure_kv(
                vol_handle
            ), f"PV secret for vol_handle : {vol_handle} not removed the Azure KV"
