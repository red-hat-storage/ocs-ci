import logging
import pytest
from os import path

from ocs_ci.framework.testlib import (
    ManageTest,
    tier2,
    polarion_id,
)
from ocs_ci.helpers.helpers import (
    default_thick_storage_class,
    fetch_used_size,
    default_ceph_block_pool,
)
from ocs_ci.ocs import constants


log = logging.getLogger(__name__)


@pytest.mark.skip(reason="Depricated")
class TestVerifyRbdThickPvcUtilization(ManageTest):
    """
    Tests to verify storage utilization of RBD thick provisioned PVC

    """

    @pytest.fixture(autouse=True)
    def setup(self, project_factory):
        """
        Create project for the test

        """
        self.proj_obj = project_factory()

    @tier2
    @polarion_id("OCS-2537")
    def test_verify_rbd_thick_pvc_utilization(
        self,
        pvc_factory,
        pod_factory,
    ):
        """
        Test to verify the storage utilization of RBD thick provisioned PVC

        """
        pvc_size = 15
        replica_size = 3
        file1 = "fio_file1"
        file2 = "fio_file2"
        rbd_pool = default_ceph_block_pool()

        size_before_pvc = fetch_used_size(rbd_pool)
        log.info(f"Storage pool used size before creating the PVC is {size_before_pvc}")

        # Create RBD thick PVC
        pvc_obj = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            project=self.proj_obj,
            storageclass=default_thick_storage_class(),
            size=pvc_size,
            access_mode=constants.ACCESS_MODE_RWO,
            status=constants.STATUS_BOUND,
        )

        size_after_pvc = fetch_used_size(
            rbd_pool, size_before_pvc + (pvc_size * replica_size)
        )
        log.info(
            f"Verified: Storage pool used size after creating the PVC is {size_after_pvc}"
        )

        pod_obj = pod_factory(
            interface=constants.CEPHBLOCKPOOL,
            pvc=pvc_obj,
            status=constants.STATUS_RUNNING,
        )

        # Create 5GB file
        pod_obj.run_io(
            storage_type="fs",
            size="5G",
            runtime=60,
            fio_filename=file1,
            end_fsync=1,
        )
        pod_obj.get_fio_results()

        # Verify the used size after IO
        fetch_used_size(rbd_pool, size_before_pvc + (pvc_size * replica_size))

        # Create another 5GB file
        pod_obj.run_io(
            storage_type="fs",
            size="5G",
            runtime=60,
            fio_filename=file2,
            end_fsync=1,
        )
        pod_obj.get_fio_results()

        # Verify the used size after IO
        fetch_used_size(rbd_pool, size_before_pvc + (pvc_size * replica_size))

        # Delete the files created by fio
        mount_point = pod_obj.get_storage_path()
        rm_cmd = f"rm {path.join(mount_point, file1)} {path.join(mount_point, file2)}"
        pod_obj.exec_cmd_on_pod(command=rm_cmd, out_yaml_format=False)

        # Verify the used size after deleting the files
        fetch_used_size(rbd_pool, size_before_pvc + (pvc_size * replica_size))

        # Delete the pod
        pod_obj.delete()
        pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)

        # Delete the PVC
        pvc_obj.delete()
        pvc_obj.ocp.wait_for_delete(resource_name=pvc_obj.name)

        # Verify used size after deleting the PVC
        size_after_pvc_delete = fetch_used_size(rbd_pool, size_before_pvc)
        log.info(
            f"Verified: Storage pool used size after deleting the PVC is {size_after_pvc_delete}"
        )
