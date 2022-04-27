import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier1,
    polarion_id,
)
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    TimeoutExpiredError,
    UnexpectedBehaviour,
)
from ocs_ci.ocs.resources.pod import get_file_path, check_file_existence
from ocs_ci.helpers.helpers import fetch_used_size
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


@skipif_ocs_version("<4.10")
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
            num_of_rbd_pvc=3,
            num_of_cephfs_pvc=0,
            sc_rbd=self.sc_obj,
        )

    @polarion_id("OCS-2804")
    @tier1
    def test_space_reclaim_multiple_pvc_without_io(self):
        """
        Test to verify RBD space reclamation with multiple PVC

        Steps:
        1. Create and attach RBD PVC of size 25 GiB to three app pods.
        2. Get the used size of the RBD pool
        3. Create a file of size 10GiB
        4. Verify the increased used size of the RBD pool
        5. Delete the files from all the pods
        6. Create ReclaimSpaceJob
        7. Verify the decreased used size of the RBD pool

        """

        pvc_obj1, pvc_obj2, pvc_obj3 = self.pvc[0], self.pvc[1], self.pvc[2]
        pod_obj1, pod_obj2, pod_obj3 = self.pod[0], self.pod[1], self.pod[2]

        fio_filename1 = "fio_file1"

        # Fetch the used size of pool
        cbp_name = self.sc_obj.get().get("parameters").get("pool")
        used_size_before_io = fetch_used_size(cbp_name)
        log.info(f"Used size before IO is {used_size_before_io}")

        # Create a 10 GB file in all the three pods
        for pod_obj in [pod_obj1, pod_obj2, pod_obj3]:
            pod_obj.run_io(
                storage_type="fs",
                size="10G",
                runtime=120,
                fio_filename=fio_filename1,
                end_fsync=1,
            )
            pod_obj.get_fio_results()

        # Verify used size after IO
        exp_used_size_after_io = used_size_before_io + (30 * self.pool_replica)
        used_size_after_io = fetch_used_size(cbp_name, exp_used_size_after_io)
        log.info(f"Used size after IO is {used_size_after_io}")

        # Delete the file from all the pods
        self.delete_file_from_the_pods([pod_obj1, pod_obj2, pod_obj3], fio_filename1)

        # Create ReclaimSpaceJob
        for pvc_obj in [pvc_obj1, pvc_obj2, pvc_obj3]:
            self.verify_reclaim_space_job = pvc_obj.create_reclaim_space_job()

        # Verify whether space is reclaimed by checking the used size of the RBD pool
        used_after_reclaiming_space = fetch_used_size(cbp_name, used_size_before_io)
        log.info(
            f"Space reclamation verified. Used size after reclaiming space is {used_after_reclaiming_space}."
        )

    @polarion_id("OCS-2805")
    @tier1
    def test_space_reclaim_multiple_pvc_with_io(self):
        """
        Test to verify RBD space reclamation with multiple PVCs
        with IO

        Steps:
        1. Create and attach RBD PVC of size 25 GiB to three app pods.
        2. Get the used size of the RBD pool
        3. Create a file of size 10GiB
        4. Verify the increased used size of the RBD pool
        5. Delete the file from all the pods
        6. Run IO to create 2GiB file in all the pods
        6. Create ReclaimSpaceJob
        7. Verify the used size of the RBD pool

        """

        pvc_obj1, pvc_obj2, pvc_obj3 = self.pvc[0], self.pvc[1], self.pvc[2]
        pod_obj1, pod_obj2, pod_obj3 = self.pod[0], self.pod[1], self.pod[2]

        fio_filename1 = "fio_file1"

        # Fetch the used size of pool
        cbp_name = self.sc_obj.get().get("parameters").get("pool")
        used_size_before_io = fetch_used_size(cbp_name)
        log.info(f"Used size before IO is {used_size_before_io}")

        # Create a 10 GB file in all the three pods
        for pod_obj in [pod_obj1, pod_obj2, pod_obj3]:
            pod_obj.run_io(
                storage_type="fs",
                size="10G",
                runtime=120,
                fio_filename=fio_filename1,
                end_fsync=1,
            )
            pod_obj.get_fio_results()

        # Verify the used size after IO
        exp_used_size_after_io = used_size_before_io + (30 * self.pool_replica)
        used_size_after_io = fetch_used_size(cbp_name, exp_used_size_after_io)
        log.info(f"Used size after IO is {used_size_after_io}")

        # Delete the file from all the pods
        self.delete_file_from_the_pods([pod_obj1, pod_obj2, pod_obj3], fio_filename1)

        # Create a 5 GB file in all the three pods
        log.info("Running IO")
        for pod_obj in [pod_obj1, pod_obj2, pod_obj3]:
            pod_obj.run_io(
                storage_type="fs",
                size="5G",
                runtime=100,
                fio_filename=fio_filename1,
                end_fsync=1,
            )

        # Create the ReclaimSpaceJob
        for pvc_obj in [pvc_obj1, pvc_obj2, pvc_obj3]:
            self.verify_reclaim_space_job = pvc_obj.create_reclaim_space_job()

        # Verify whether space is reclaimed by checking the used size of the RBD pool
        used_size_after_io += 15 * self.pool_replica
        exp_used_size_after_io = used_size_after_io - (30 * self.pool_replica)
        used_after_reclaiming_space = fetch_used_size(cbp_name, exp_used_size_after_io)
        log.info(
            f"Space reclamation verified. Used size after reclaiming space is {used_after_reclaiming_space}."
        )

    def verify_reclaim_space_job(self, reclaim_space_job):
        """
        Function to verify the result of reclaim space job

        Args:
            reclaim_space_job(object): reclaim space job object

        Returns: None

        """
        log.info("Verifying the reclaim space job")

        # Wait for the Succeeded result of ReclaimSpaceJob
        try:
            for reclaim_space_job_yaml in TimeoutSampler(
                timeout=120, sleep=5, func=reclaim_space_job.get
            ):
                result = reclaim_space_job_yaml.get("status", {}).get("result")
                if result == "Succeeded":
                    log.info(f"ReclaimSpaceJob {reclaim_space_job.name} succeeded")
                    break
                else:
                    log.info(
                        f"Waiting for the Succeeded result of the ReclaimSpaceJob {reclaim_space_job.name}. "
                        f"Present value of result is {result}"
                    )
        except TimeoutExpiredError:
            raise UnexpectedBehaviour(
                f"ReclaimSpaceJob {reclaim_space_job.name} is not successful. Yaml output:{reclaim_space_job.get()}"
            )

    def delete_file_from_the_pods(self, pod_obj_list, fio_filename1):
        """
        Args:
            pod_obj_list(object): list of pod objects
            fio_filename1(str): file to be deleted in the respective pod

        Return: None

        """
        for pod_obj in pod_obj_list:
            file_path = get_file_path(pod_obj, fio_filename1)
            pod_obj.exec_cmd_on_pod(command=f"rm -f {file_path}", out_yaml_format=False)

            # Verify whether the file is deleted
            try:
                check_file_existence(pod_obj=pod_obj, file_path=file_path)
            except CommandFailed as cmdfail:
                if "No such file or directory" not in str(cmdfail):
                    raise
                log.info(f"Verified: File {file_path} deleted.")
