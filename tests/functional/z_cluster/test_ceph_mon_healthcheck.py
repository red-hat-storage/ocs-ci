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
    wait_for_mon_failover,
    select_mon_id_and_node,
)
from ocs_ci.ocs.resources.deployment import get_mon_deployments
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
    def test_patch_and_verify_mon_healthcheck(self, mon_timeout, mon_interval, request):
        """
        Test patch and verify ceph mon healthcheck and verify the new values are respected.

        Steps:
        1. Patch mon healthcheck values in StorageCluster
        2. Wait until the mon healthcheck in StorageCluster and CephCluster are consistent
        3. Wait for the cephcluster to settle
        4. Scale down a mon deployment and wait for the mon to go down
        5. Wait for the mon failover and check its timing
        6. Verify the mon healthcheck timeout value in the operator logs
        7. Wait for mons to be in quorum
        8. Check cluster health

        """
        mon_timeout_seconds = int(mon_timeout[:-1]) * 60
        patch_storagecluster_mon_healthcheck(mon_timeout, mon_interval)
        wait_for_mon_healthcheck_consistency()

        # The operator pauses the mon health check during orchestration.
        wait_for_cephcluster_to_settle()
        mon_id, _ = select_mon_id_and_node()
        deployments = get_mon_deployments()
        matching = [
            deployment
            for deployment in deployments
            if deployment.data.get("metadata", {}).get("labels", {}).get("mon")
            == mon_id
        ]
        assert len(matching) == 1, f"Expected one deployment for mon '{mon_id}'"
        mon_deployment = matching[0]
        original_uid = mon_deployment.data["metadata"]["uid"]
        original_replicas = mon_deployment.replicas
        assert original_replicas == 1, f"Mon '{mon_id}' is not scaled to one replica"
        failover_observed = False

        def restore_mon_deployment():
            if failover_observed or not mon_deployment.ocp.is_exist(
                resource_name=mon_deployment.name
            ):
                return
            current = mon_deployment.ocp.get(resource_name=mon_deployment.name)
            if current["metadata"]["uid"] == original_uid:
                mon_deployment.scale(original_replicas)

        request.addfinalizer(restore_mon_deployment)
        mon_ids_before = get_mon_ids()
        assert mon_id in mon_ids_before, f"Selected mon '{mon_id}' is no longer present"

        disruption_start = time.monotonic()
        mon_deployment.scale(0)
        wait_for_mon_status(
            mon_id=mon_id, status=constants.MON_STATUS_DOWN, timeout=180
        )
        mon_down_time = time.monotonic()
        # Add a small gap to the timeout to account for any delays
        timeout_gap = 180
        # Calculate the expected mon failover timeout (in seconds). In the rook-ceph-operator,
        # the mon failover may happen after 2 times the mon timeout value.
        mon_failover_timeout = mon_timeout_seconds * 2 + timeout_gap
        min_failover_seconds = mon_timeout_seconds * 0.8
        new_mon_id = wait_for_mon_failover(
            mon_ids_before, timeout=mon_failover_timeout, min_wait=min_failover_seconds
        )
        failover_observed = True
        failover_seconds = time.monotonic() - mon_down_time
        log.info(
            f"The mon '{mon_id}' failed over to the new mon '{new_mon_id}' "
            f"after {failover_seconds:.0f} seconds"
        )

        # Keep the disruption in every log polling window.
        res = verify_mon_healthcheck_timeout_value_in_logs(
            mon_id, mon_timeout_seconds, timeout=720, since_start=disruption_start
        )
        assert res, "Mon healthcheck timeout value not found in logs"

        wait_for_mons_in_quorum(expected_mon_count=self.mon_count)
        self.check_cluster_health()
