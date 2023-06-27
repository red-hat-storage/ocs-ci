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
)
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.utility.utils import run_cmd

from ocs_ci.ocs.cluster import CephCluster

log = logging.getLogger(__name__)


@tier4a
@skipif_ocs_version("<4.13")
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
                self.scale_ceph_mon(1, mon, ignore_error=True)

        request.addfinalizer(scale_up_mons_at_teardown)

    def scale_ceph_mon(self, replica_count, mon_name, ignore_error=False):
        """
        Scaling up/down mon deployment.

        Args:
            replica_count (int): number of replicas to scale
            mon_name (str): mon deployment name
            ignore_errors (bool, optional): Ignore errors. Defaults to False.

        Returns:
            None
        """
        log.info(f"Scaling mon {mon_name} to replica count {replica_count}")
        run_cmd(
            f"oc scale --replicas={replica_count} deploy/{mon_name} -n {constants.OPENSHIFT_STORAGE_NAMESPACE}",
            ignore_error=ignore_error,
        )

    def test_mon_failures_with_intransit_encryption(self):
        """
        Test case to ensure the proper functioning of mon failures with in-transit encryption.

        Steps:
            1. Ensure that in-transit encryption is enabled on the setup.
            2. Scale down two mons.
            3. Restart Mgr pod.
            4. Sleep for 5 seconds.
            5. Scale up mons.
            6. Sleep for 10 seconds.
            7. Wait for mgr pod to move to the running state.
            8. Verify the in-transit encryption configuration after scaling up mons and restarting mgr pod.
        """

        if not get_in_transit_encryption_config_state():
            if config.ENV_DATA.get("in_transit_encryption"):
                pytest.fail("In-transit encryption is not enabled on the setup")
            else:
                set_in_transit_encryption()

        ceph_obj = CephCluster()

        log.info("Verifying the in-transit encryption is enable on setup.")
        assert in_transit_encryption_verification()

        # Select Two mons
        self.mons = ceph_obj.get_mons_from_cluster()[:2]

        # Scale Down Mon Count to replica=0
        for mon in self.mons:
            self.scale_ceph_mon(0, mon)

        time.sleep(10)

        # Restart Mgr pod
        mgr_pod = ceph_obj.mgrs[0]
        mgr_pod.delete(wait=True)

        time.sleep(5)

        log.info(f"Scaling up mons {','.join(self.mons)}")
        for mon in self.mons:
            self.scale_ceph_mon(1, mon)

        log.info("Waiting for mgr pod move to Running state")
        ceph_obj.scan_cluster()
        mgr_pod = ceph_obj.mgrs[0]

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

        assert in_transit_encryption_verification()
