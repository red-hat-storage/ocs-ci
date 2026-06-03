import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    ManageTest,
    tier1,
    skipif_ocs_version,
    aws_platform_required,
    kms_config_required,
    skipif_managed_service,
    skipif_hci_provider_and_client,
    skipif_disconnected_cluster,
    skipif_proxy_cluster,
    polarion_id,
)
from ocs_ci.helpers.helpers import (
    create_pods,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import verify_crypt_device_present_onnode

logger = logging.getLogger(__name__)


@green_squad
@aws_platform_required
@skipif_ocs_version("<4.12")
@kms_config_required
@skipif_managed_service
@skipif_hci_provider_and_client
@skipif_disconnected_cluster
@skipif_proxy_cluster
@polarion_id("OCS-4665")
class TestKmipRbdPvEncryptionKMIP(ManageTest):
    """
    Test to verify RBD PV encryption using KMIP

    """

    @pytest.fixture(autouse=True)
    def setup(
        self,
        pv_encryption_kmip_setup_factory,
    ):
        """
        Setup csi-kms-connection-details configmap

        """
        logger.test_step("Set up csi-kms-connection-details configmap for KMIP")
        self.kms = pv_encryption_kmip_setup_factory()
        logger.info("csi-kms-connection-details setup successful")

    @tier1
    def test_rbd_pv_encryption_kmip(
        self,
        project_factory,
        storageclass_factory,
        multi_pvc_factory,
        pod_factory,
    ):
        """
        Test to verify creation and deletion of encrypted RBD PVC

        """
        logger.test_step("Create project and encryption-enabled RBD storage class")
        proj_obj = project_factory()

        sc_obj = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL,
            encrypted=True,
            encryption_kms_id=self.kms.kmsid,
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

        logger.test_step("Verify encrypted devices and start IO on all pods")
        vol_handles = []
        for pvc_obj in pvc_objs:
            pv_obj = pvc_obj.backed_pv_obj
            vol_handles.append(pv_obj.get().get("spec").get("csi").get("volumeHandle"))

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
        logger.info("IO started on all pods")

        logger.test_step("Wait for IO completion on all pods")
        for pod_obj in pod_objs:
            pod_obj.get_fio_results()
        logger.info("IO completed on all pods")
