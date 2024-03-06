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
    skipif_hci_provider_and_client,
    skipif_external_mode,
)
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    TimeoutExpiredError,
    UnexpectedBehaviour,
)
from ocs_ci.ocs.resources.pod import get_file_path, check_file_existence, delete_pods
from ocs_ci.helpers.helpers import fetch_used_size, create_csi_addons_global_timeout_configmap
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
    @skipif_hci_provider_and_client
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
