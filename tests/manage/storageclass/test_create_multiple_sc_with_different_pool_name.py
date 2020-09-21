import pytest
import logging
from tests import helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import get_fio_rw_iops
from ocs_ci.framework.testlib import ManageTest, tier2
from tests.fixtures import (
    create_rbd_secret, create_project
)

log = logging.getLogger(__name__)


@tier2
@pytest.mark.usefixtures(
    create_project.__name__,
    create_rbd_secret.__name__,
)
@pytest.mark.polarion_id("OCS-624")
class TestCreateMultipleScWithDifferentPoolName(ManageTest):
    """
    Create Multiple Storage Class with different pool name
    """

    def test_create_multiple_sc_with_different_pool_name(
        self, teardown_factory
    ):
        """
        This test function does below,
        *. Creates multiple Storage Classes with different pool name
        *. Creates PVCs using each Storage Class
        *. Mount each PVC to an app pod
        *. Run IO on each app pod
        """

        # Create 2 storageclasses, each with different pool name
        cbp_list = []
        sc_list = []
        for i in range(2):
            log.info("Creating cephblockpool")
            cbp_obj = helpers.create_ceph_block_pool()
            log.info(
                f"{cbp_obj.name} created successfully"
            )
            log.info(
                f"Creating a RBD storage class using {cbp_obj.name}"
            )
            cbp_list.append(cbp_obj)
            sc_obj = helpers.create_storage_class(
                interface_type=constants.CEPHBLOCKPOOL,
                interface_name=cbp_obj.name,
                secret_name=self.rbd_secret_obj.name
            )

            log.info(
                f"StorageClass: {sc_obj.name} "
                f"created successfully using {cbp_obj.name}"
            )
            sc_list.append(sc_obj)
            teardown_factory(cbp_obj)
            teardown_factory(sc_obj)

        # Create PVCs using each SC
        pvc_list = []
        for i in range(2):
            log.info(f"Creating a PVC using {sc_list[i].name}")
            pvc_obj = helpers.create_pvc(sc_list[i].name)
            log.info(
                f"PVC: {pvc_obj.name} created successfully using "
                f"{sc_list[i].name}"
            )
            pvc_list.append(pvc_obj)
            teardown_factory(pvc_obj)
            helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND)
            pvc_obj.reload()

        # Create app pod and mount each PVC
        pod_list = []
        for i in range(2):
            log.info(f"Creating an app pod and mount {pvc_list[i].name}")
            pod_obj = helpers.create_pod(
                interface_type=constants.CEPHBLOCKPOOL,
                pvc_name=pvc_list[i].name,
            )
            log.info(
                f"{pod_obj.name} created successfully and "
                f"mounted {pvc_list[i].name}"
            )
            pod_list.append(pod_obj)
            teardown_factory(pod_obj)
            helpers.wait_for_resource_state(pod_obj, constants.STATUS_RUNNING)
            pod_obj.reload()

        # Run IO on each app pod for sometime
        for pod in pod_list:
            log.info(f"Running FIO on {pod.name}")
            pod.run_io('fs', size='2G')

        for pod in pod_list:
            get_fio_rw_iops(pod)
