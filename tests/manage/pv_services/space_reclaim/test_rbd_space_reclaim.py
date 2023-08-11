import logging
import time

import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier1,
    tier2,
    polarion_id,
    skipif_managed_service,
    skipif_external_mode,
)
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    TimeoutExpiredError,
    UnexpectedBehaviour,
)
from ocs_ci.ocs.resources.pod import get_file_path, check_file_existence, delete_pods
from ocs_ci.helpers.helpers import fetch_used_size
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


@green_squad
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
            num_of_rbd_pvc=1,
            num_of_cephfs_pvc=0,
            sc_rbd=self.sc_obj,
        )

    @polarion_id("OCS-2741")
    @tier1
    @skipif_external_mode
    @skipif_managed_service
    def test_rbd_space_reclaim(self):
        """
        Test to verify RBD space reclamation

        Steps:
        1. Create and attach RBD PVC of size 25 GiB to an app pod.
        2. Get the used size of the RBD pool
        3. Create two files of size 10GiB
        4. Verify the increased used size of the RBD pool
        5. Delete one file
        6. Create ReclaimSpaceJob
        7. Verify the decreased used size of the RBD pool

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
        file_path = get_file_path(pod_obj, fio_filename1)
        pod_obj.exec_cmd_on_pod(command=f"rm -f {file_path}", out_yaml_format=False)

        # Verify whether file is deleted
        try:
            check_file_existence(pod_obj=pod_obj, file_path=file_path)
        except CommandFailed as cmdfail:
            if "No such file or directory" not in str(cmdfail):
                raise
            log.info(f"Verified: File {file_path} deleted.")

        # Wait for 15 seconds after deleting the file
        time.sleep(15)

        # Create ReclaimSpaceJob
        reclaim_space_job = pvc_obj.create_reclaim_space_job()

        # Verify Succeeded result of ReclaimSpaceJob
        self.reclaim_space_job(reclaim_space_job)

        time.sleep(120)

        # Verify space is reclaimed by checking the used size of the RBD pool
        used_after_reclaiming_space = fetch_used_size(
            cbp_name, used_size_after_io - (10 * self.pool_replica)
        )
        log.info(
            f"Space has been reclaimed. Used size after io is {used_after_reclaiming_space}."
        )

        # Verify the presence of another file in the directory
        log.info("Verifying the existence of remaining file in the pod")
        file_path = get_file_path(pod_obj, fio_filename2)
        log.info(check_file_existence(pod_obj=pod_obj, file_path=file_path))
        if check_file_existence(pod_obj=pod_obj, file_path=file_path):
            log.info(f"{fio_filename2} is intact")

    @polarion_id("OCS-2774")
    @tier1
    @skipif_managed_service
    @skipif_external_mode
    def test_rbd_space_reclaim_no_space(self):
        """
        Test to verify RBD space reclamation

        Steps:
        1. Create and attach RBD PVC of size 25 GiB to an app pod.
        2. Get the used size of the RBD pool
        3. Create a file of size 10GiB
        4. Verify the used size of the RBD pool
        5. Create ReclaimSpaceJob
        6. Verify the size of the RBD pool, no changes should be seen.

        """
        pvc_obj = self.pvc[0]
        pod_obj = self.pod[0]

        fio_filename1 = "fio_file1"
        fio_filename2 = "fio_file2"

        # Fetch the used size of pool
        cbp_name = self.sc_obj.get().get("parameters").get("pool")
        used_size_before_io = fetch_used_size(cbp_name)
        log.info(f"Used size before IO is {used_size_before_io}")

        # Create a 10 GB file
        for filename in [fio_filename1, fio_filename2]:
            pod_obj.run_io(
                storage_type="fs",
                size="10G",
                runtime=100,
                fio_filename=filename,
                end_fsync=1,
            )
            pod_obj.get_fio_results()

        # Verify used size after IO
        exp_used_size_after_io = used_size_before_io + (20 * self.pool_replica)
        used_size_after_io = fetch_used_size(cbp_name, exp_used_size_after_io)
        log.info(f"Used size after IO is {used_size_after_io}")

        # Create ReclaimSpaceJob
        reclaim_space_job = pvc_obj.create_reclaim_space_job()

        # Verify Succeeded result of ReclaimSpaceJob
        self.reclaim_space_job(reclaim_space_job)

        # Verify space is reclaimed by checking the used size of the RBD pool
        used_after_reclaiming_space = fetch_used_size(cbp_name, used_size_after_io)
        log.info(
            f"Memory remains intact. Used size after io is {used_after_reclaiming_space}."
        )

    @polarion_id("OCS-3733")
    @tier2
    @skipif_external_mode
    def test_no_volume_mounted(self):
        """
        Test reclaimspace job with no volume mounted

        Steps:
        1. Create and attach RBD PVC of size 25 GiB to an app pod.
        2. Get the used size of the RBD pool
        3. Create a file of size 10GiB
        4. Delete the file
        5. Delete the pod
        6. Create ReclaimSpaceJob
        7. No errors should be seen in reclaim space job

        """
        pvc_obj = self.pvc[0]
        pod_obj = self.pod[0]

        fio_filename1 = "fio_file1"

        # Fetch the used size of pool
        cbp_name = self.sc_obj.get().get("parameters").get("pool")
        used_size_before_io = fetch_used_size(cbp_name)
        log.info(f"Used size before IO is {used_size_before_io}")

        # Create a 10 GB file
        pod_obj.run_io(
            storage_type="fs",
            size="10G",
            runtime=120,
            fio_filename=fio_filename1,
            end_fsync=1,
        )
        pod_obj.get_fio_results()

        # Verify used size after IO
        exp_used_size_after_io = used_size_before_io + (10 * self.pool_replica)
        used_size_after_io = fetch_used_size(cbp_name, exp_used_size_after_io)
        log.info(f"Used size after IO is {used_size_after_io}")

        # Delete the file
        file_path = get_file_path(pod_obj, fio_filename1)
        pod_obj.exec_cmd_on_pod(command=f"rm -f {file_path}", out_yaml_format=False)

        # Verify whether file is deleted
        try:
            check_file_existence(pod_obj=pod_obj, file_path=file_path)
        except CommandFailed as cmdfail:
            if "No such file or directory" not in str(cmdfail):
                raise
            log.info(f"Verified: File {file_path} deleted.")

        # Delete the pod
        log.info(f"Deleting the pod {pod_obj}")
        delete_pods([pod_obj])

        # Create ReclaimSpaceJob
        reclaim_space_job = pvc_obj.create_reclaim_space_job()

        # Verify Succeeded result of ReclaimSpaceJob
        self.reclaim_space_job(reclaim_space_job)

    def reclaim_space_job(self, reclaim_space_job):
        """
        Verify the result of the reclaim space job
        Args:
            reclaim_space_job(object): reclaim space job object
        Returns:
            None
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
