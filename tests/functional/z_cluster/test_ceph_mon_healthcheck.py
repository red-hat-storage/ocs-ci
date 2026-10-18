import logging
import time

import pytest

from ocs_ci.framework.testlib import (
    ManageTest,
    ignore_leftovers,
    tier4a,
    brown_squad,
)
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.helpers.ceph_helpers import (
    wait_for_mon_status,
    wait_for_mons_in_quorum,
    get_mon_quorum_count,
)
from ocs_ci.helpers.ceph_mon_healthcheck import (
    patch_storagecluster_mon_healthcheck,
    wait_for_mon_healthcheck_consistency,
    verify_mon_healthcheck_timeout_value_in_logs,
    delete_storagecluster_mon_healthcheck,
    get_storagecluster_mon_healthcheck,
    wait_for_cephcluster_to_settle,
    get_mon_ids,
    delete_mon_pod,
    wait_for_mon_failover,
    select_mon_id_and_node,
)
from ocs_ci.ocs.node import (
    unschedule_nodes,
    schedule_nodes,
    get_node_objs,
    recover_node_to_ready_state,
)
from ocs_ci.ocs import constants

log = logging.getLogger(__name__)


@brown_squad
@ignore_leftovers
class TestCephMonHealthCheck(ManageTest):
    """
    Test Ceph Mon HealthCheck functionality class

    """

    @pytest.fixture(autouse=True)
    def init_sanity(self, pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory):
        """
        Initialize Sanity instance

        """
        log.info("Initializing Sanity instance")
        self.sanity_helpers = Sanity()
        self.pvc_factory = pvc_factory
        self.pod_factory = pod_factory
        self.bucket_factory = bucket_factory
        self.rgw_bucket_factory = rgw_bucket_factory

        self.mon_count = get_mon_quorum_count()
        log.info(f"Mon count in the cluster: {self.mon_count}")

    def check_cluster_health(self):
        log.info("Checking the cluster and Ceph health")
        self.sanity_helpers.health_check(cluster_check=True, tries=40)
        log.info("Check basic cluster functionality by creating some resources")
        self.sanity_helpers.create_resources(
            self.pvc_factory,
            self.pod_factory,
            self.bucket_factory,
            self.rgw_bucket_factory,
        )

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Check that the new osd size has increased and increase the resize osd count

        """

        def finalizer():
            mon_healthcheck = get_storagecluster_mon_healthcheck()
            log.info(f"Mon healthcheck present in StorageCluster: {mon_healthcheck}")
            if mon_healthcheck:
                log.info("Removing mon healthcheck from StorageCluster")
                delete_storagecluster_mon_healthcheck()

            ocp_nodes = get_node_objs()
            for n in ocp_nodes:
                recover_node_to_ready_state(n)

        request.addfinalizer(finalizer)

    @tier4a
    @pytest.mark.parametrize(
        "mon_timeout, mon_interval",
        [
            pytest.param(
                "3m",
                "20s",
                marks=pytest.mark.polarion_id("OCS-7428"),
            ),
            pytest.param(
                "12m",
                "30s",
                marks=pytest.mark.polarion_id("OCS-7429"),
            ),
        ],
    )
    def test_patch_and_verify_mon_healthcheck(self, mon_timeout, mon_interval):
        """
        Test patch and verify ceph mon healthcheck and verify the new values are respected.

        Steps:
        1. Patch mon healthcheck values in StorageCluster
        2. Wait until the mon healthcheck in StorageCluster and CephCluster are consistent
        3. Cordon a mon node and wait for the cephcluster to settle
        4. Delete the mon pod on that node and wait for the mon to go down
        5. Verify mon healthcheck timeout value in logs
        6. Wait for the mon failover and verify it took at least the mon timeout
        7. Schedule the mon node back
        8. Wait for mons to be in quorum
        9. Check cluster health

        """
        mon_timeout_seconds = int(mon_timeout[:-1]) * 60
        patch_storagecluster_mon_healthcheck(mon_timeout, mon_interval)
        wait_for_mon_healthcheck_consistency()

        mon_id, node_name = select_mon_id_and_node()
        mon_ids_before = get_mon_ids()
        # Cordon the node before taking the mon down, and let the resulting
        # orchestration finish. The operator stops the mon health check while a
        # cephcluster orchestration runs, and an orchestration that starts when
        # a mon is already down cannot update the remaining mons ("not enough
        # monitors would be available"), so the health check never resumes.
        unschedule_nodes([node_name])
        wait_for_cephcluster_to_settle()

        delete_mon_pod(mon_id)
        wait_for_mon_status(
            mon_id=mon_id, status=constants.MON_STATUS_DOWN, timeout=180
        )
        mon_down_time = time.time()
        res = verify_mon_healthcheck_timeout_value_in_logs(
            mon_id, mon_timeout_seconds, timeout=720
        )
        assert res, "Mon healthcheck timeout value not found in logs"
        # Add a small gap to the timeout to account for any delays
        timeout_gap = 180
        # Calculate the expected mon failover timeout (in seconds). In the rook-ceph-operator,
        # the mon failover may happen after 2 times the mon timeout value.
        mon_failover_timeout = mon_timeout_seconds * 2 + timeout_gap
        new_mon_id = wait_for_mon_failover(mon_ids_before, timeout=mon_failover_timeout)
        failover_seconds = time.time() - mon_down_time
        log.info(
            f"The mon '{mon_id}' failed over to the new mon '{new_mon_id}' "
            f"after {failover_seconds:.0f} seconds"
        )

        # Schedule the node back as soon as the failover is seen, so the new mon
        # can start and the cluster is not left degraded during the checks below
        schedule_nodes([node_name])

        # The mon is detected as down by polling, so the measured time can be
        # slightly shorter than the configured mon healthcheck timeout
        min_failover_seconds = mon_timeout_seconds * 0.8
        assert failover_seconds >= min_failover_seconds, (
            f"The mon '{mon_id}' failed over after {failover_seconds:.0f} seconds, which is less "
            f"than the minimum of {min_failover_seconds:.0f} seconds expected for the mon "
            f"healthcheck timeout {mon_timeout}"
        )

        wait_for_mons_in_quorum(expected_mon_count=self.mon_count)
        self.check_cluster_health()
