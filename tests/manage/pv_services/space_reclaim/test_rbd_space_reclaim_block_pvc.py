import logging
import pytest
import time

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier1,
    polarion_id,
)
from ocs_ci.ocs.exceptions import (
    TimeoutExpiredError,
    UnexpectedBehaviour,
)
from ocs_ci.helpers.helpers import check_rbd_image_used_size
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
            access_modes_rbd=[
                f"{constants.ACCESS_MODE_RWX}-Block",
                f"{constants.ACCESS_MODE_RWO}-Block",
            ],
            num_of_rbd_pvc=1,
            num_of_cephfs_pvc=0,
            sc_rbd=self.sc_obj,
        )

    @polarion_id("OCS-3913")
    @tier1
    def test_rbd_space_reclaim(self):
        """
        Test to verify RBD space reclamation

        Steps:
        1. Create and attach RBD Block PVC of size 25GiB to an app pod.
        2. Get the used size of the RBD pool
        3. Write the data of 20GiB
        4. Verify the increased used size of the RBD pool
        5. Delete the data of 5GiB
        6. Create ReclaimSpaceJob
        7. Verify the decreased used size of the RBD pool

        """

        pvc_obj = self.pvc[0]
        pod_obj = self.pod[0]

        # Fetch the used size of pool
        rbd_pool = self.sc_obj.get().get("parameters").get("pool")
        used_size = 20
        # Run IO from each pod and verify md5sum on all pods
        log.info("Run IO from one pod")
        pod_obj.run_io(
            storage_type="block",
            size="20G",
            runtime=300,
            end_fsync=1,
        )
        pod_obj.get_fio_results()

        # verify used size of the rbd image
        memory_usage = check_rbd_image_used_size(
            [pvc_obj], f"{used_size}GiB", rbd_pool=rbd_pool, expect_match=True
        )

        if memory_usage:
            pass
        else:
            raise UnexpectedBehaviour("Write operation on the block device failed")

        command = "blkdiscard -o 0 -l 5G /dev/rbdblock"
        pod_obj.exec_cmd_on_pod(command)

        time.sleep(100)

        # Create ReclaimSpaceJob
        reclaim_space_job = pvc_obj.create_reclaim_space_job()

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

        # verify used size of the rbd image
        expected_size_after_reclaim = used_size - 5
        memory_usage = check_rbd_image_used_size(
            [pvc_obj],
            f"{expected_size_after_reclaim}GiB",
            rbd_pool=rbd_pool,
            expect_match=True,
        )

        if memory_usage:
            pass
        else:
            raise UnexpectedBehaviour("memory usage is not as expected")
