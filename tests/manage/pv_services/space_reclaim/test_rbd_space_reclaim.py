import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier1,
    polarion_id,
    acceptance,
)
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.resources.pod import get_file_path, check_file_existence
from ocs_ci.helpers.helpers import fetch_used_size

log = logging.getLogger(__name__)


@tier1
@acceptance
@skipif_ocs_version("<4.10")
@polarion_id("")
class TestRbdSpaceReclaim(ManageTest):
    """
    Tests to verify RBD space reclamation

    """

    @pytest.fixture(autouse=True)
    def setup(self, project_factory, storageclass_factory, create_pvcs_and_pods):
        """
        Create PVCs and pods

        """
        self.pool_replica = 3
        pvc_size_gi = 25
        self.sc_obj = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL,
            replica=self.pool_replica,
            new_rbd_pool=True,
        )
        self.pvc, self.pod = create_pvcs_and_pods(
            pvc_size=pvc_size_gi,
            access_modes_rbd=[constants.ACCESS_MODE_RWO],
            num_of_rbd_pvc=1,
            num_of_cephfs_pvc=0,
            sc_rbd=self.sc_obj,
        )

    def test_rbd_space_reclaim(self, pvc_clone_factory, pod_factory):
        """
        Test to verify RBD space reclamation

        Steps:
        1. Create and attach RBD PVC of size 25GiB to an app pod.
        2. Verify the used size and objects using 'ceph df' command
        3. Create two files of size 10GiB
        4. Verify the increased used size and objects using 'ceph df' command
        5. Delete the file
        6. Create ReclaimSpaceJob
        7. Verify the decreased used size and objects using 'ceph df' command.

        """
        pvc_obj = self.pvc[0]
        pod_obj = self.pod[0]

        fio_filename1 = "fio_file1"
        fio_filename2 = "fio_file2"

        # Fetch the used size of pool
        cbp_name = self.sc_obj.get().get("parameters").get("pool")

        used_size_before_io = fetch_used_size(cbp_name)
        log.info(f"Used size before IO is {used_size_before_io}")

        # Create two 10 GB file
        for filename in fio_filename1, fio_filename2:
            pod_obj.run_io(
                storage_type="fs",
                size="10G",
                runtime=120,
                fio_filename=filename,
                end_fsync=1,
            )
            pod_obj.get_fio_results()

        # Verify used size after IO
        exp_used_size_after_io = used_size_before_io + (20 * self.pool_replica)
        used_size_after_io = fetch_used_size(cbp_name, exp_used_size_after_io)
        log.info(f"Used size after IO is {used_size_after_io}")

        # Delete one file
        file_path = get_file_path(pod_obj, fio_filename2)
        pod_obj.exec_cmd_on_pod(command=f"rm -f {file_path}", out_yaml_format=False)

        # Verify file is deleted
        try:
            check_file_existence(pod_obj=pod_obj, file_path=file_path)
        except CommandFailed as cmdfail:
            if "No such file or directory" not in str(cmdfail):
                raise
            log.info(f"Verified: File {file_path} deleted.")

        # Create ReclaimSpaceJob
        reclaim_space_job = pvc_obj.create_reclaim_space_job()

        # Wait for the Succeeded result of ReclaimSpaceJob
        try:
            reclaim_space_job.ocp.wait_for_resource(
                condition="Succeeded",
                resource_name=reclaim_space_job.name,
                column="RESULT",
                timeout=30,
                sleep=3,
            )
        except Exception as ex:
            log.error(str(ex))
            raise

        # Verify space is reclaimed by checking the used size
        used_after_reclaiming_space = fetch_used_size(
            cbp_name, used_size_after_io - (10 * self.pool_replica)
        )
        log.info(
            f"Space reclamation verified. Used size after reclaiming space is {used_after_reclaiming_space}."
        )
