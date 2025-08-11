import logging
import pytest
import time

from ocs_ci.ocs.resources.storage_cluster import (
    in_transit_encryption_verification,
    set_in_transit_encryption,
    get_in_transit_encryption_config_state,
)
from ocs_ci.framework.pytest_customization.marks import (
    tier4a,
    skipif_ocs_version,
    green_squad,
    runs_on_provider,
)
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import CommandFailed

from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.helpers.helpers import modify_deployment_replica_count

log = logging.getLogger(__name__)


@tier4a
@skipif_ocs_version("<4.13")
@pytest.mark.polarion_id("OCS-4919")
@green_squad
@runs_on_provider
class TestMonFailuresWithIntransitEncryption:
    @pytest.fixture(autouse=True)
    def teardown_fixture(self, request):
        """
        Teardown operations.
        """

        def scale_up_mons_at_teardown():
            """
            Restoring the mon replicasets
            """
            for mon in self.mons:
                modify_deployment_replica_count(mon, 1)

        request.addfinalizer(scale_up_mons_at_teardown)

    def test_mon_failures_with_intransit_encryption(self):
        """
        The test case ensures the proper functioning of in-transit encryption
        after mon and mgr failures case.

        Steps:
            1. Ensure that in-transit encryption is enabled on the setup.
            2. Scale down two mons.
            3. Restart Mgr pod.
            4. Sleep for 5 seconds.
            5. Scale up mons.
            6. Sleep for 10 seconds.
            7. Wait for mgr pod to move to the running state.
            8. Verify the in-transit encryption configuration after
            scaling up mons and restarting mgr pod.
        """
        self.mons = []
        if not get_in_transit_encryption_config_state():
            if config.ENV_DATA.get("in_transit_encryption"):
                pytest.fail(
                    "In-transit encryption is not enabled on the setup while it was supposed to be."
                )
            else:
                set_in_transit_encryption()

        ceph_obj = CephCluster()

        log.info("Verifying the in-transit encryption is enable on setup.")
        assert (
            in_transit_encryption_verification()
        ), "In transit encryption verification failed"

        # Select Two mons
        self.mons = ceph_obj.get_mons_from_cluster()[:2]
        # self.mons = get_mon_deployments()[:2]

        # Scale Down Mon Count to replica=0
        for mon in self.mons:
            modify_deployment_replica_count(mon, 0)

        # Sleeping for 10 seconds to emulate a condition where the 2 mons is inaccessibe  for 10 seconds.
        time.sleep(10)

        def restart_mgr_pod():
            ceph_obj.scan_cluster()
            mgr_pod = ceph_obj.mgrs[0]
            mgr_pod.delete(wait=True)

        # Restart Mgr pod
        retry(
            (CommandFailed),
            tries=5,
            delay=10,
        )(restart_mgr_pod)()

        # Sleeping for 5 seconds to rejoin the manager's pod.
        time.sleep(5)

        log.info(f"Scaling up mons {','.join(self.mons)}")
        for mon in self.mons:
            modify_deployment_replica_count(mon, 1)

        log.info("Waiting for mgr pod move to Running state")
        ceph_obj.scan_cluster()

        assert ceph_obj.POD.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=constants.MGR_APP_LABEL,
            resource_count=1,
            timeout=100,
        ), "Mgr pod did'nt move to Running state after 100 seconds"

        log.info(
            "Verifying the in-transit encryption config "
            "After scaling up mon and restarting mgr pod"
        )

        assert (
            in_transit_encryption_verification()
        ), "In transit encryption verification failed"
