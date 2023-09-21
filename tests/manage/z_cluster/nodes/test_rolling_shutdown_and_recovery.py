import logging
import time
import pytest


from ocs_ci.framework.pytest_customization.marks import brown_squad
from ocs_ci.framework.testlib import (
    tier4b,
    ignore_leftovers,
    ManageTest,
    bugzilla,
    skipif_external_mode,
    skipif_ibm_cloud,
    skipif_managed_service,
)
from ocs_ci.ocs.node import get_ocs_nodes
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.helpers.sanity_helpers import Sanity


log = logging.getLogger(__name__)


@brown_squad
@tier4b
@pytest.mark.polarion_id("OCS-2633")
@bugzilla("1895819")
@skipif_ibm_cloud
@skipif_external_mode
@skipif_managed_service
@ignore_leftovers
class TestRollingWorkerNodeShutdownAndRecovery(ManageTest):
    """
    Test rolling shutdown and recovery of OCS pods running worker nodes
    """

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    @pytest.fixture(autouse=True)
    def teardown(self, request, nodes):
        """
        Make sure all nodes are up again

        """

        def finalizer():
            nodes.restart_nodes_by_stop_and_start_teardown()

        request.addfinalizer(finalizer)

    def test_rolling_shutdown_and_recovery(
        self, nodes, pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
    ):
        SECONDS_TO_WAIT = 180
        """
        Test rolling shutdown and recovery of OCS worker nodes

        """
        # Get OCS worker node objects
        ocs_node_objs = get_ocs_nodes()

        # Start rolling shutdown and recovery of OCS worker nodes
        log.info("ShutDown OCS worker")
        for node_obj in ocs_node_objs:
            nodes.stop_nodes(nodes=[node_obj])
            log.info(f"Keeping node in stopped state for {SECONDS_TO_WAIT} mins")
            time.sleep(SECONDS_TO_WAIT)
            nodes.start_nodes(nodes=[node_obj])
            self.sanity_helpers.health_check(cluster_check=False, tries=60)
            log.info("Checking storage pods status")
            # Validate storage pods are running
            wait_for_pods_to_be_running(timeout=600)

        # Check basic cluster functionality by creating some resources
        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
        )
