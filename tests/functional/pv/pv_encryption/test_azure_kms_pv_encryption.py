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

log = logging.getLogger(__name__)


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
        log.info("Setting up csi-kms-connection-details configmap")
        self.kms = pv_encryption_kms_setup_factory()
        log.info("csi-kms-connection-details setup successful")

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
        # Create a project
        proj_obj = project_factory()

        # Create an encryption enabled storageclass for RBD
        sc_obj = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL,
            encrypted=True,
            encryption_kms_id=self.kms.azure_kms_connection_name,
        )

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

        # # Verify Keys are create on the Azure KV
        vol_handles = []
        for pvc_obj in pvc_objs:
            pv_obj = pvc_obj.backed_pv_obj
            vol_handle = pv_obj.get().get("spec").get("csi").get("volumeHandle")
            vol_handles.append(vol_handle)

            assert self.kms.verify_pv_secrets_present_in_azure_kv(
                vol_handle
            ), f"PV secret for vol_handle : {vol_handle} not found in the Azure KV"

        # pass

        # Verify whether encrypted device is present inside the pod and run IO
        for vol_handle, pod_obj in zip(vol_handles, pod_objs):
            node = pod_obj.get_node()
            assert verify_crypt_device_present_onnode(
                node, vol_handle
            ), f"Crypt devicve {vol_handle} not found on node:{node}"

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

        for vol_handle in vol_handles:
            assert not self.kms.verify_pv_secrets_present_in_azure_kv(
                vol_handle
            ), f"PV secret for vol_handle : {vol_handle} not removed the Azure KV"
