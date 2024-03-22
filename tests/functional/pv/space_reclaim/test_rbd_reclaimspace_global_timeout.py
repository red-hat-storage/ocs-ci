import logging
import time

import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier1,
    polarion_id,
    skipif_managed_service,
    skipif_hci_provider_and_client,
    skipif_external_mode,
)
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    TimeoutExpiredError,
    UnexpectedBehaviour,
)
from ocs_ci.ocs.resources.pod import get_file_path, check_file_existence
from ocs_ci.helpers.helpers import (
    fetch_used_size,
    create_csi_addons_global_timeout_configmap,
)
from ocs_ci.utility.utils import TimeoutSampler, run_cmd
from ocs_ci.ocs.utils import get_pod_name_by_pattern

log = logging.getLogger(__name__)


@green_squad
@skipif_ocs_version("<4.10")
class TestRbdSpaceReclaim(ManageTest):
    """
    Tests to verify RBD space reclamation

    """

    @pytest.fixture(autouse=True)
    def setup(self, project_factory, storageclass_factory, pvc_factory, pod_factory):
        """
        Create PVCs and pods

        """
        self.pool_replica = 3
        self.sc_obj = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL,
            replica=self.pool_replica,
            new_rbd_pool=True,
        )
        self.pvc_obj = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            access_mode=constants.ACCESS_MODE_RWO,
            size=200,
        )
        self.pod_obj = pod_factory(
            pvc=self.pvc_obj,
        )

    @polarion_id("OCS-5441")
    @tier1
    @skipif_external_mode
    @skipif_managed_service
    @skipif_hci_provider_and_client
    def test_rbd_space_reclaim_global_timeout(self, teardown_factory):
        """
        Test to verify RBD space reclamation

        Steps:
        1. Create and attach RBD PVC of size 200 GiB to an app pod.
        2. Get the used size of the RBD pool
        3. Create two files of size 50GiB each
        4. Delete one file
        5. Create global config map for reclaim space job
        6. Restart the csi-manager-controller pod
        6. Create ReclaimSpaceJob
        7. Validate the timeout and used size of the pool

        """

        fio_filename1 = "fio_file1"
        fio_filename2 = "fio_file2"

        # Fetch the used size of pool
        cbp_name = self.sc_obj.get().get("parameters").get("pool")
        used_size_before_io = fetch_used_size(cbp_name)
        log.info(f"Used size before IO is {used_size_before_io}")

        # Create two 50 GB file
        for filename in [fio_filename1, fio_filename2]:
            self.pod_obj.run_io(
                storage_type="fs",
                size="50G",
                runtime=120,
                fio_filename=filename,
                end_fsync=1,
            )
            self.pod_obj.get_fio_results()

        # Verify used size after IO
        exp_used_size_after_io = used_size_before_io + (100 * self.pool_replica)
        used_size_after_io = fetch_used_size(cbp_name, exp_used_size_after_io)
        log.info(f"Used size after IO is {used_size_after_io}")

        # Delete a file
        file_path = get_file_path(self.pod_obj, filename)
        self.pod_obj.exec_cmd_on_pod(
            command=f"rm -f {file_path}", out_yaml_format=False
        )

        # Verify whether file is deleted
        try:
            check_file_existence(pod_obj=self.pod_obj, file_path=file_path)
        except CommandFailed as cmdfail:
            if "No such file or directory" not in str(cmdfail):
                raise
            log.info(f"Verified: File {file_path} deleted.")

        # Wait for 15 seconds after deleting the file
        time.sleep(15)

        # Create Global config map with required timeout set in openshift-storage namespace
        log.info("Creating global config timeout")
        global_timeout, self.cm_obj = create_csi_addons_global_timeout_configmap()

        if global_timeout:
            pattern = ' "Timeout": "' + str(global_timeout) + '0s"'
        else:
            raise UnexpectedBehaviour("Global timeout has not been configured")

        time.sleep(15)

        # Get csi control manager pod
        csi_controller_manager_pod = get_pod_name_by_pattern(
            "csi-addons-controller-manager", namespace="openshift-storage"
        )

        log.info(
            f"Restarting csi_controller_manager_pod {csi_controller_manager_pod[0]}"
        )
        run_cmd(
            "oc delete pod "
            + str(csi_controller_manager_pod[0])
            + " -n openshift-storage",
            timeout=60,
        )

        # wait for 60 secs after pod restart
        time.sleep(60)

        # Create ReclaimSpaceJob
        reclaim_space_job = self.pvc_obj.create_reclaim_space_job(global_timeout=True)

        # Verify Succeeded result of ReclaimSpaceJob
        self.reclaim_space_job(reclaim_space_job)

        # Verify space is reclaimed by checking the used size of the RBD pool
        used_after_reclaiming_space = fetch_used_size(
            cbp_name, used_size_after_io - (50 * self.pool_replica)
        )
        log.info(
            f"Space has been reclaimed. Used size after io is {used_after_reclaiming_space}."
        )

        # collecting logs from restarted csi-addons-controller-manager pod to validate the global timeout
        new_csi_controller_manager_pod = get_pod_name_by_pattern(
            "csi-addons-controller-manager", namespace="openshift-storage"
        )

        controller_manager_pod_logs = run_cmd(
            "oc logs "
            + str(new_csi_controller_manager_pod[0])
            + " -n openshift-storage",
            timeout=60,
        )

        # validation of global timeout
        if pattern in controller_manager_pod_logs:
            log.info("Global Timeout has been configured successfully")
        else:
            raise UnexpectedBehaviour(
                "Global Timeout not found in csi-addon-controller-manager pod"
            )

        log.info("Deleting global config map")
        teardown_factory(self.cm_obj)

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
