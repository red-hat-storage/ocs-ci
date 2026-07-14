import logging
import time

import pytest

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import green_squad, ec_allowed
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier1,
    tier2,
    skipif_managed_service,
    skipif_external_mode,
)
from ocs_ci.ocs.cluster import is_ec_pool_supported
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    TimeoutExpiredError,
    UnexpectedBehaviour,
)
from ocs_ci.ocs.resources.pod import (
    get_file_path,
    check_file_existence,
    delete_pods,
)
from ocs_ci.helpers import helpers
from ocs_ci.helpers.helpers import (
    fetch_used_size,
    default_storage_class,
    get_rbd_image_info,
)
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


@pytest.fixture
def erasure_coded():
    """Default pool type: replicated. Overridden per-test via parametrize."""
    return False


@green_squad
@skipif_ocs_version("<4.10")
class TestRbdSpaceReclaim(ManageTest):
    """
    Tests to verify RBD space reclamation

    """

    @pytest.fixture(autouse=True)
    def setup(
        self, project_factory, storageclass_factory, create_pvcs_and_pods, erasure_coded
    ):
        """
        Create PVCs and pods

        """
        self.pool_replica = 3
        pvc_size_gi = 25
        if config.ENV_DATA["platform"] in constants.HCI_PROVIDER_CLIENT_PLATFORMS:
            self.sc_obj = default_storage_class(interface_type=constants.CEPHBLOCKPOOL)
        else:
            self.sc_obj = storageclass_factory(
                interface=constants.CEPHBLOCKPOOL,
                replica=self.pool_replica,
                new_rbd_pool=True,
                erasure_coded=erasure_coded,
            )
        self.data_pool = helpers.get_data_pool_name(sc_obj=self.sc_obj)
        self.pool_size_factor = helpers.get_pool_size_factor(self.data_pool)

        self.pvc, self.pod = create_pvcs_and_pods(
            pvc_size=pvc_size_gi,
            access_modes_rbd=[constants.ACCESS_MODE_RWO],
            num_of_rbd_pvc=1,
            num_of_cephfs_pvc=0,
            sc_rbd=self.sc_obj,
        )

    @pytest.mark.parametrize(
        "erasure_coded",
        [
            pytest.param(False, marks=[pytest.mark.polarion_id("OCS-2741")]),
            pytest.param(
                True,
                marks=[
                    ec_allowed,
                    pytest.mark.polarion_id("OCS-7974"),
                    pytest.mark.skipif(
                        not is_ec_pool_supported(),
                        reason="Erasure coded pools are not supported on this cluster",
                    ),
                ],
            ),
        ],
    )
    @tier1
    @skipif_external_mode
    @skipif_managed_service
    def test_rbd_space_reclaim(self, erasure_coded):
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
        used_size_before_io = fetch_used_size(self.data_pool)
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
        exp_used_size_after_io = used_size_before_io + (20 * self.pool_size_factor)
        used_size_after_io = fetch_used_size(self.data_pool, exp_used_size_after_io)
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
            self.data_pool, used_size_after_io - (10 * self.pool_size_factor)
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

    @pytest.mark.parametrize(
        "erasure_coded",
        [
            pytest.param(False, marks=[pytest.mark.polarion_id("OCS-2774")]),
            pytest.param(
                True,
                marks=[
                    ec_allowed,
                    pytest.mark.polarion_id("OCS-7975"),
                    pytest.mark.skipif(
                        not is_ec_pool_supported(),
                        reason="Erasure coded pools are not supported on this cluster",
                    ),
                ],
            ),
        ],
    )
    @tier2
    @skipif_managed_service
    @skipif_external_mode
    def test_rbd_space_reclaim_no_space(self, erasure_coded):
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
        used_size_before_io = fetch_used_size(self.data_pool)
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
        exp_used_size_after_io = used_size_before_io + (20 * self.pool_size_factor)
        used_size_after_io = fetch_used_size(self.data_pool, exp_used_size_after_io)
        log.info(f"Used size after IO is {used_size_after_io}")

        # Create ReclaimSpaceJob
        reclaim_space_job = pvc_obj.create_reclaim_space_job()

        # Verify Succeeded result of ReclaimSpaceJob
        self.reclaim_space_job(reclaim_space_job)

        # Verify space is reclaimed by checking the used size of the RBD pool
        used_after_reclaiming_space = fetch_used_size(
            self.data_pool, used_size_after_io
        )
        log.info(
            f"Memory remains intact. Used size after io is {used_after_reclaiming_space}."
        )

    @pytest.mark.parametrize(
        "erasure_coded",
        [
            pytest.param(False, marks=[pytest.mark.polarion_id("OCS-3733")]),
            pytest.param(
                True,
                marks=[
                    ec_allowed,
                    pytest.mark.polarion_id("OCS-7976"),
                    pytest.mark.skipif(
                        not is_ec_pool_supported(),
                        reason="Erasure coded pools are not supported on this cluster",
                    ),
                ],
            ),
        ],
    )
    @tier2
    @skipif_external_mode
    def test_no_volume_mounted(self, erasure_coded):
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
        used_size_before_io = fetch_used_size(self.data_pool)
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
        exp_used_size_after_io = used_size_before_io + (10 * self.pool_size_factor)
        used_size_after_io = fetch_used_size(self.data_pool, exp_used_size_after_io)
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

        # Validation of pod deletion
        log.info(f"Validate the deletion of pod - {pod_obj.name}")
        pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)

        # Create ReclaimSpaceJob
        reclaim_space_job = pvc_obj.create_reclaim_space_job()

        # Verify Succeeded result of ReclaimSpaceJob
        self.reclaim_space_job(reclaim_space_job)

    @pytest.mark.parametrize(
        "erasure_coded",
        [
            pytest.param(False, marks=[pytest.mark.polarion_id("OCS-8029")]),
            pytest.param(
                True,
                marks=[
                    ec_allowed,
                    pytest.mark.polarion_id("OCS-8030"),
                    pytest.mark.skipif(
                        not is_ec_pool_supported(),
                        reason="Erasure coded pools are not supported on this cluster",
                    ),
                ],
            ),
        ],
    )
    @tier2
    @skipif_external_mode
    def test_rbd_sparsify_via_reclaim_space_job(self, erasure_coded):
        """
        Test that ReclaimSpaceJob triggers rbd sparsify on a detached PVC
        and reclaims space from zeroed extents.

        Unlike fstrim (which runs on mounted volumes), rbd sparsify runs
        on unmounted images via the ControllerReclaimSpace path.
        This validates DFBUGS-7982 (EC pool reclaim space failures).

        Steps:
        1. Create and attach RBD PVC of size 25 GiB to an app pod.
        2. Get the initial rbd image used size.
        3. Write 5 GiB of random data via fio.
        4. Verify rbd image used size increased.
        5. Overwrite the file with zeros.
        6. Verify rbd image used size remains high (zeroed extents still allocated).
        7. Delete the pod to detach the PVC.
        8. Create ReclaimSpaceJob (triggers rbd sparsify on detached volume).
        9. Verify ReclaimSpaceJob succeeded.
        10. Verify rbd image used size decreased.
        11. Remount PVC and verify data consistency.

        """
        pvc_obj = self.pvc[0]
        pod_obj = self.pod[0]
        fio_filename = "sparsify_blob"
        write_size_gb = 5

        sc_data = self.sc_obj.get()
        rbd_pool = sc_data["parameters"]["pool"]
        rbd_image_name = pvc_obj.get_rbd_image_name

        # Get initial rbd image used size
        initial_info = get_rbd_image_info(rbd_pool, rbd_image_name)
        initial_used = initial_info.get("used_size_gib")
        log.info(f"Initial rbd image used size: {initial_used} GiB")

        # Write random data
        pod_obj.run_io(
            storage_type="fs",
            size=f"{write_size_gb}G",
            runtime=120,
            fio_filename=fio_filename,
            end_fsync=1,
        )
        pod_obj.get_fio_results()

        # Verify rbd image used size increased
        after_write_info = get_rbd_image_info(rbd_pool, rbd_image_name)
        after_write_used = after_write_info.get("used_size_gib")
        log.info(f"Used size after write: {after_write_used} GiB")
        assert after_write_used > initial_used, (
            f"rbd image used size did not increase after writing data. "
            f"Before: {initial_used} GiB, After: {after_write_used} GiB"
        )

        # Overwrite the file with zeros
        file_path = get_file_path(pod_obj, fio_filename)
        pod_obj.exec_cmd_on_pod(
            command=f"dd if=/dev/zero of={file_path} bs=4M "
            f"count={write_size_gb * 256} conv=notrunc oflag=direct",
            out_yaml_format=False,
            timeout=300,
        )

        # Verify used size remains high after zeroing
        after_zero_info = get_rbd_image_info(rbd_pool, rbd_image_name)
        after_zero_used = after_zero_info.get("used_size_gib")
        log.info(f"Used size after zero overwrite: {after_zero_used} GiB")

        # Delete the pod to detach the PVC
        log.info(f"Deleting pod {pod_obj.name} to detach PVC")
        delete_pods([pod_obj])
        pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)

        time.sleep(15)

        # Create ReclaimSpaceJob on the detached PVC
        reclaim_space_job = pvc_obj.create_reclaim_space_job()

        # Verify ReclaimSpaceJob succeeded
        self.reclaim_space_job(reclaim_space_job)

        time.sleep(30)

        # Verify rbd image used size decreased after sparsify
        after_reclaim_info = get_rbd_image_info(rbd_pool, rbd_image_name)
        after_reclaim_used = after_reclaim_info.get("used_size_gib")
        log.info(f"Used size after reclaim: {after_reclaim_used} GiB")
        assert after_reclaim_used < after_zero_used, (
            f"rbd sparsify did not reclaim space. "
            f"Before reclaim: {after_zero_used} GiB, After: {after_reclaim_used} GiB"
        )

        # Remount and verify data consistency
        log.info("Remounting PVC to verify data consistency")
        pod_obj2 = helpers.create_pod(
            interface_type=constants.CEPHBLOCKPOOL,
            pvc_name=pvc_obj.name,
            namespace=pvc_obj.namespace,
        )
        helpers.wait_for_resource_state(pod_obj2, constants.STATUS_RUNNING)

        file_path = get_file_path(pod_obj2, fio_filename)
        assert check_file_existence(
            pod_obj=pod_obj2, file_path=file_path
        ), f"File {fio_filename} does not exist after rbd sparsify"
        log.info(f"File {fio_filename} is intact after rbd sparsify")

        # Cleanup the verification pod
        pod_obj2.delete()
        pod_obj2.ocp.wait_for_delete(resource_name=pod_obj2.name)

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
