import logging
import pytest


from ocs_ci.framework.testlib import (
    tier4b,
    ignore_leftovers,
    ManageTest,
    skipif_external_mode,
    skipif_ibm_cloud,
    managed_service_required,
)
from ocs_ci.ocs.node import (
    get_ocs_nodes,
    wait_for_node_count_to_reach_status,
    get_node_objs,
    recover_node_to_ready_state,
    consumers_verification_steps_after_provider_node_replacement,
)
from ocs_ci.ocs.resources.pod import check_pods_after_node_replacement
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.framework import config
from ocs_ci.ocs.cluster import is_ms_consumer_cluster, is_ms_provider_cluster

log = logging.getLogger(__name__)


@tier4b
@skipif_ibm_cloud
@skipif_external_mode
@ignore_leftovers
@managed_service_required
class TestRollingWorkerNodeShutdownAndRecoveryMS(ManageTest):
    """
    Test rolling shutdown and recovery of the OCS worker nodes when using the Managed Service
    """

    @pytest.fixture(autouse=True)
    def setup(self, create_scale_pods_and_pvcs_using_kube_job_on_ms_consumers):
        """
        Initialize Sanity instance, and create pods and PVCs factory

        """
        self.orig_index = config.cur_index
        self.sanity_helpers = Sanity()
        self.create_pods_and_pvcs_factory = (
            create_scale_pods_and_pvcs_using_kube_job_on_ms_consumers
        )

    def create_resources(self):
        """
        Create resources on the consumers and run IO

        """
        if is_ms_consumer_cluster():
            consumer_indexes = [config.cur_index]
        else:
            consumer_indexes = config.get_consumer_indexes_list()

        self.create_pods_and_pvcs_factory(consumer_indexes=consumer_indexes)

    @pytest.fixture(autouse=True)
    def teardown(self, request, nodes):
        """
        Make sure all nodes are up again
        """

        def finalizer():
            ocp_nodes = get_node_objs()
            for n in ocp_nodes:
                recover_node_to_ready_state(n)

            config.switch_ctx(self.orig_index)
            # If the cluster is an MS provider cluster, and we also have MS consumer clusters in the run
            if is_ms_provider_cluster() and config.is_consumer_exist():
                log.info(
                    "Execute the the consumers verification steps before starting the next test"
                )
                consumers_verification_steps_after_provider_node_replacement()

        request.addfinalizer(finalizer)

    @pytest.mark.polarion_id("OCS-4637")
    def test_rolling_shutdown_and_recovery_in_controlled_fashion(self, nodes):
        """
        Test rolling shutdown and recovery of the OCS worker nodes, when waiting for the pods to
        be running and Ceph Health OK between the iterations. This test is for the Managed Service

        """
        # Get OCS worker node objects
        ocs_node_objs = get_ocs_nodes()

        # Start rolling shutdown and recovery of OCS worker nodes
        for node_obj in ocs_node_objs:
            nodes.stop_nodes(nodes=[node_obj])
            # When we use the managed service, the worker node should recover automatically
            # by starting the node, or removing it and creating a new one
            log.info("Waiting for all the worker nodes to be ready...")
            wait_for_node_count_to_reach_status(
                node_count=len(ocs_node_objs), timeout=900
            )
            log.info("Waiting for all the pods to be running")
            assert check_pods_after_node_replacement(), "Not all the pods are running"

            # If the cluster is an MS provider cluster, and we also have MS consumer clusters in the run
            if is_ms_provider_cluster() and config.is_consumer_exist():
                assert consumers_verification_steps_after_provider_node_replacement()
            self.sanity_helpers.health_check(cluster_check=False, tries=40)

        # Check basic cluster functionality by creating some resources
        self.create_resources()
