import logging
import pytest

from ocs_ci.framework.testlib import (
    ManageTest,
    ignore_leftovers,
    tier4a,
    brown_squad,
)
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.helpers.ceph_helpers import (
    wait_for_mon_down,
    wait_for_mons_in_quorum,
    get_mon_quorum_count,
)
from ocs_ci.helpers.ceph_mon_healthcheck import (
    patch_storagecluster_mon_healthcheck,
    wait_for_mon_healthcheck_consistency,
    verify_mon_healthcheck_timeout_value_in_logs,
    delete_storagecluster_mon_healthcheck,
    get_storagecluster_mon_healthcheck,
    wait_for_mon_pod_restart,
    select_mon_id_and_node,
)
from ocs_ci.ocs.node import (
    drain_nodes,
    schedule_nodes,
    get_node_objs,
    recover_node_to_ready_state,
)

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
        3. Drain a mon node and wait for mon to go down
        4. Verify mon healthcheck timeout value in logs
        5. Wait for mon pod restart in the expected time
        6. Schedule the mon node back
        7. Wait for mons to be in quorum
        8. Check cluster health

        """
        mon_timeout_seconds = int(mon_timeout[:-1]) * 60
        patch_storagecluster_mon_healthcheck(mon_timeout, mon_interval)
        wait_for_mon_healthcheck_consistency()

        mon_id, node_name = select_mon_id_and_node()

        drain_nodes([node_name])
        wait_for_mon_down(mon_id=mon_id, timeout=300)
        res = verify_mon_healthcheck_timeout_value_in_logs(mon_id, mon_timeout_seconds)
        assert res, "Mon healthcheck timeout value not found in logs"
        # Add a small gap to the timeout to account for any delays
        timeout_gap = 180
        # Calculate the expected mon pod restart timeout (in seconds). In the rook-ceph-operator,
        # the mon pod restart may happen after 2 times the mon timeout value.
        mon_pod_timeout = mon_timeout_seconds * 2 + timeout_gap
        wait_for_mon_pod_restart(mon_id, mon_pod_timeout)

        schedule_nodes([node_name])
        wait_for_mons_in_quorum(expected_mon_count=self.mon_count)
        self.check_cluster_health()
