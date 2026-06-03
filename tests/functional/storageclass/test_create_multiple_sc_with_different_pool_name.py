import pytest
import logging
from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import get_fio_rw_iops
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    ManageTest,
    tier2,
    skipif_external_mode,
    skipif_hci_provider_and_client,
)
from tests.fixtures import create_rbd_secret, create_project

logger = logging.getLogger(__name__)


@green_squad
@skipif_external_mode
@tier2
@skipif_hci_provider_and_client
@pytest.mark.usefixtures(
    create_project.__name__,
    create_rbd_secret.__name__,
)
@pytest.mark.polarion_id("OCS-624")
class TestCreateMultipleScWithDifferentPoolName(ManageTest):
    """
    Create Multiple Storage Class with different pool name
    """

    def test_create_multiple_sc_with_different_pool_name(self, teardown_factory):
        """
        This test function does below,
        *. Creates multiple Storage Classes with different pool name
        *. Creates PVCs using each Storage Class
        *. Mount each PVC to an app pod
        *. Run IO on each app pod
        """

        logger.test_step("Create 2 storage classes, each with a different pool name")
        cbp_list = []
        sc_list = []
        logger.info("Creating 2 CephBlockPools and corresponding RBD storage classes")
        for i in range(2):
            logger.debug(f"Creating cephblockpool iteration {i}")
            cbp_obj = helpers.create_ceph_block_pool()
            logger.debug(f"CephBlockPool {cbp_obj.name} created successfully")
            cbp_list.append(cbp_obj)
            sc_obj = helpers.create_storage_class(
                interface_type=constants.CEPHBLOCKPOOL,
                interface_name=cbp_obj.name,
                secret_name=self.rbd_secret_obj.name,
            )

            logger.debug(
                f"StorageClass: {sc_obj.name} "
                f"created successfully using {cbp_obj.name}"
            )
            sc_list.append(sc_obj)
            teardown_factory(cbp_obj)
            teardown_factory(sc_obj)
        logger.info(
            f"Created 2 CephBlockPools and storage classes: "
            f"{[sc.name for sc in sc_list]}"
        )

        logger.test_step("Create PVCs using each storage class")
        pvc_list = []
        for i in range(2):
            logger.debug(f"Creating a PVC using {sc_list[i].name}")
            pvc_obj = helpers.create_pvc(sc_list[i].name)
            logger.debug(
                f"PVC: {pvc_obj.name} created successfully using {sc_list[i].name}"
            )
            pvc_list.append(pvc_obj)
            teardown_factory(pvc_obj)
            helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND)
            pvc_obj.reload()
        logger.info(f"Created 2 PVCs: {[pvc.name for pvc in pvc_list]}")

        logger.test_step("Create app pods and mount each PVC")
        pod_list = []
        for i in range(2):
            logger.debug(f"Creating an app pod and mounting {pvc_list[i].name}")
            pod_obj = helpers.create_pod(
                interface_type=constants.CEPHBLOCKPOOL,
                pvc_name=pvc_list[i].name,
            )
            logger.debug(
                f"{pod_obj.name} created successfully and "
                f"mounted {pvc_list[i].name}"
            )
            pod_list.append(pod_obj)
            teardown_factory(pod_obj)
            helpers.wait_for_resource_state(pod_obj, constants.STATUS_RUNNING)
            pod_obj.reload()
        logger.info(f"Created 2 app pods: {[pod.name for pod in pod_list]}")

        logger.test_step("Run IO on each app pod")
        for pod in pod_list:
            logger.debug(f"Running FIO on {pod.name}")
            pod.run_io("fs", size="2G")

        for pod in pod_list:
            get_fio_rw_iops(pod)
        logger.info("FIO completed on all pods")
