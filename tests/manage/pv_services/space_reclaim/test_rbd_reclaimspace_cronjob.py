import logging
import random

import pytest

from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier1,
    polarion_id,
    skipif_external_mode,
)
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    TimeoutExpiredError,
    UnexpectedBehaviour,
)
from ocs_ci.ocs.resources.pod import get_file_path, check_file_existence
from ocs_ci.helpers.helpers import fetch_used_size, create_unique_resource_name
from ocs_ci.utility.utils import TimeoutSampler, exec_cmd

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
            new_rbd_pool=False,
        )
        self.pvc, self.pod = create_pvcs_and_pods(
            pvc_size=pvc_size_gi,
            access_modes_rbd=[constants.ACCESS_MODE_RWO],
            num_of_rbd_pvc=1,
            num_of_cephfs_pvc=0,
            sc_rbd=self.sc_obj,
        )

    @polarion_id("OCS-2759")
    @tier1
    def test_rbd_space_reclaim_cronjob(self, pause_and_resume_cluster_load):
        """
        Test to verify RBD space reclamation
        Steps:
        1. Create and attach RBD PVC of size 25 GiB to an app pod.
        2. Get the used size of the RBD pool
        3. Create four files of size 4GiB
        4. Verify the increased used size of the RBD pool
        5. Delete three file
        6. Create ReclaimSpaceJob
        7. Verify the decreased used size of the RBD pool
        8. Verify the presence of other files in the folder
        """

        pvc_obj = self.pvc[0]
        pod_obj = self.pod[0]

        fio_filename1 = "fio_file1"
        fio_filename2 = "fio_file2"
        fio_filename3 = "fio_file3"
        fio_filename4 = "fio_file4"

        schedule = ["hourly", "midnight", "weekly"]
        # Fetch the used size of pool
        cbp_name = self.sc_obj.get().get("parameters").get("pool")
        log.info(f"Cephblock pool name {cbp_name}")
        used_size_before_io = fetch_used_size(cbp_name)
        log.info(f"Used size before IO is {used_size_before_io}")

        # Create four 4 GB file
        for filename in [fio_filename1, fio_filename2, fio_filename3, fio_filename4]:
            pod_obj.run_io(
                storage_type="fs",
                size="4G",
                runtime=100,
                fio_filename=filename,
                end_fsync=1,
            )
            pod_obj.get_fio_results()

        # Verify used size after IO
        exp_used_size_after_io = used_size_before_io + (16 * self.pool_replica)
        used_size_after_io = fetch_used_size(cbp_name, exp_used_size_after_io)
        log.info(f"Used size after IO is {used_size_after_io}")

        # Delete the file and validate the reclaimspace cronjob
        for filename in [fio_filename1, fio_filename2, fio_filename3]:
            file_path = get_file_path(pod_obj, filename)
            pod_obj.exec_cmd_on_pod(
                command=f"rm -f {file_path}", out_yaml_format=False, timeout=100
            )

            # Verify file is deleted
            try:
                check_file_existence(pod_obj=pod_obj, file_path=file_path)
            except CommandFailed as cmdfail:
                if "No such file or directory" not in str(cmdfail):
                    raise
                log.info(f"Verified: File {file_path} deleted.")

        # Create ReclaimSpaceCronJob
        for type in schedule:
            reclaim_space_job = pvc_obj.create_reclaim_space_cronjob(schedule=type)

            # Wait for the Succeeded result of ReclaimSpaceJob
            try:
                for reclaim_space_job_yaml in TimeoutSampler(
                    timeout=120, sleep=5, func=reclaim_space_job.get
                ):
                    result = reclaim_space_job_yaml["spec"]["schedule"]
                    if result == "@" + type:
                        log.info(f"ReclaimSpaceJob {reclaim_space_job.name} succeeded")
                        break
                    else:
                        log.info(
                            f"Waiting for the Succeeded result of the ReclaimSpaceCronJob {reclaim_space_job.name}. "
                            f"Present value of result is {result}"
                        )
            except TimeoutExpiredError:
                raise UnexpectedBehaviour(
                    f"ReclaimSpaceJob {reclaim_space_job.name} is not successful. Yaml output:{reclaim_space_job.get()}"
                )

        # Verify the presence of another file in the directory
        log.info("Verifying the existence of remaining file in the directory")
        file_path = get_file_path(pod_obj, fio_filename4)
        log.info(check_file_existence(pod_obj=pod_obj, file_path=file_path))
        if check_file_existence(pod_obj=pod_obj, file_path=file_path):
            log.info(f"{fio_filename4} is intact")

    @tier1
    @skipif_external_mode
    @pytest.mark.parametrize(
        argnames=["replica", "compression", "volume_binding_mode", "pvc_status"],
        argvalues=[
            pytest.param(
                *[
                    2,
                    "aggressive",
                    constants.IMMEDIATE_VOLUMEBINDINGMODE,
                    constants.STATUS_PENDING,
                ],
                marks=pytest.mark.polarion_id("OCS-8888"),
            ),
            pytest.param(
                *[
                    3,
                    "aggressive",
                    constants.IMMEDIATE_VOLUMEBINDINGMODE,
                    constants.STATUS_BOUND,
                ],
                marks=pytest.mark.polarion_id("OCS-8888"),
            ),
            pytest.param(
                *[
                    2,
                    "none",
                    constants.WFFC_VOLUMEBINDINGMODE,
                    constants.STATUS_PENDING,
                ],
                marks=pytest.mark.polarion_id("OCS-8888"),
            ),
            pytest.param(
                *[
                    3,
                    "none",
                    constants.IMMEDIATE_VOLUMEBINDINGMODE,
                    constants.STATUS_BOUND,
                ],
                marks=pytest.mark.polarion_id("OCS-8888"),
            ),
        ],
    )
    def test_reclaim_space_cronjob_with_annotation(
        self,
        replica,
        compression,
        volume_binding_mode,
        pvc_status,
        project_factory,
        storageclass_factory_class,
        pvc_factory,
        pod_factory,
    ):
        """
        Test case to check that reclaim space job is created for rbd pvc
        in the openshift-* namespace with reclaim space annotation

        Steps:
        1. Create a project
        2. Create a storage class with reclaim policy as delete
        3. Create a pvc with above storage class
        4. Create a pod using above pvc
        5. Run IO on the pod
        6. Add reclaim space annotation to the pvc
        7. Validate the reclaim space cronjob
        """

        self.namespace = create_unique_resource_name(
            "reclaim-space-cronjob", "namespace"
        )
        project = project_factory(project_name=self.namespace)

        interface_type = constants.CEPHBLOCKPOOL
        sc_obj = storageclass_factory_class(
            interface=interface_type,
            new_rbd_pool=True,
            replica=replica,
            compression=compression,
            volume_binding_mode=volume_binding_mode,
            pool_name="test-pool",
        )

        pvc_obj = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            project=self.namespace,
            storageclass=sc_obj.name,
            size="1Gi",
            access_mode=constants.ACCESS_MODE_RWO,
            status=pvc_status,
            volume_mode=volume_binding_mode,
        )

        helpers.wait_for_resource_state(pvc_obj, pvc_status)

        schedule = ["hourly", "midnight", "weekly"]
        schedule = random.choice(schedule)

        log.info("add reclaimspace.csiaddons.openshift.io/schedule label to PVC ")
        pvc_obj.add_label(
            "reclaimspace.csiaddons.openshift.io/schedule", f"@{schedule}"
        )

        chron_job_list = self.wait_for_cronjobs(True, 60)

        assert chron_job_list, "Reclaim space cron job does not exist"

    def wait_for_cronjobs(self, cronjobs_exist, timeout=60):
        """
        Runs 'oc get reclaimspacecronjob' with the TimeoutSampler

        Args:
            cronjobs_exist (bool): Condition to be tested, True if cronjobs should exist, False otherwise
            timeout (int): Timeout
        Returns:

            list : Result of 'oc get reclaimspacecronjob' command

        """
        try:
            for sample in TimeoutSampler(
                timeout=timeout,
                sleep=5,
                func=exec_cmd,
                cmd="get reclaimspacecronjob",
                namespace=self.namespace,
            ):
                if (len(sample) > 1 and cronjobs_exist) or (
                    len(sample) == 1 and not cronjobs_exist
                ):
                    return sample
        except TimeoutExpiredError:
            return False
