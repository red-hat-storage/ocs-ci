import logging
import pytest


from ocs_ci.framework.pytest_customization.marks import brown_squad
from ocs_ci.framework.testlib import (
    tier4b,
    ignore_leftovers,
    ManageTest,
    skipif_external_mode,
    skipif_ibm_cloud,
    provider_client_ms_platform_required,
)
from ocs_ci.ocs.node import (
    get_ocs_nodes,
    wait_for_node_count_to_reach_status,
    get_node_objs,
    recover_node_to_ready_state,
    consumers_verification_steps_after_provider_node_replacement,
    generate_nodes_for_provider_worker_node_tests,
    get_worker_nodes,
)
from ocs_ci.ocs.resources.pod import check_pods_after_node_replacement
from ocs_ci.helpers.sanity_helpers import SanityManagedService
from ocs_ci.framework import config
from ocs_ci.ocs.cluster import is_ms_consumer_cluster, is_ms_provider_cluster
from ocs_ci.ocs.constants import MS_PROVIDER_TYPE, MS_CONSUMER_TYPE
from ocs_ci.utility.utils import switch_to_correct_cluster_at_setup, ceph_health_check

log = logging.getLogger(__name__)


@brown_squad
@tier4b
@skipif_ibm_cloud
@skipif_external_mode
@ignore_leftovers
@provider_client_ms_platform_required
class TestRollingWorkerNodeShutdownAndRecoveryMS(ManageTest):
    """
    Test rolling shutdown and recovery of the OCS worker nodes when using the Managed Service
    """

    @pytest.fixture(autouse=True)
    def setup(self, request, create_scale_pods_and_pvcs_using_kube_job_on_ms_consumers):
        """
        Save the current index and initialize the Sanity instance

        """
        self.orig_index = config.cur_index
        switch_to_correct_cluster_at_setup(request)
        self.sanity_helpers = SanityManagedService(
            create_scale_pods_and_pvcs_using_kube_job_on_ms_consumers
        )

    @pytest.fixture(autouse=True)
    def teardown(self, request, nodes):
        """
        Make sure all nodes are up again
        """

        def finalizer():
            ocp_nodes = get_node_objs()
            for n in ocp_nodes:
                recover_node_to_ready_state(n)

            # If the cluster is an MS provider cluster, and we also have MS consumer clusters in the run
            if is_ms_provider_cluster() and config.is_consumer_exist():
                log.info(
                    "Execute the the consumers verification steps before starting the next test"
                )
                consumers_verification_steps_after_provider_node_replacement()

            log.info("Switch to the original cluster index")
            config.switch_ctx(self.orig_index)
            ceph_health_check()

        request.addfinalizer(finalizer)

    @pytest.mark.polarion_id("OCS-4637")
    @pytest.mark.parametrize(
        "cluster_type",
        [MS_PROVIDER_TYPE, MS_CONSUMER_TYPE],
    )
    def test_rolling_shutdown_and_recovery_in_controlled_fashion(
        self, cluster_type, nodes
    ):
        """
        Test rolling shutdown and recovery of the OCS worker nodes, when waiting for the pods to
        be running and Ceph Health OK between the iterations. This test is for the Managed Service

        """
        wnode_count = len(get_worker_nodes())
        # Get OCS worker node objects
        if is_ms_provider_cluster():
            ocs_node_objs = generate_nodes_for_provider_worker_node_tests()
        else:
            ocs_node_objs = get_ocs_nodes()

        # Start rolling shutdown and recovery of OCS worker nodes
        for node_obj in ocs_node_objs:
            nodes.stop_nodes(nodes=[node_obj], wait=False)
            nodes.wait_for_nodes_to_stop_or_terminate(nodes=[node_obj])
            # When we use the managed service, the worker node should recover automatically
            # by starting the node, or removing it and creating a new one
            log.info("Waiting for all the worker nodes to be ready...")
            wait_for_node_count_to_reach_status(node_count=wnode_count, timeout=900)
            log.info("Waiting for all the pods to be running")
            assert check_pods_after_node_replacement(), "Not all the pods are running"

            # If the cluster is an MS provider cluster, and we also have MS consumer clusters in the run
            if is_ms_provider_cluster() and config.is_consumer_exist():
                assert consumers_verification_steps_after_provider_node_replacement()
            self.sanity_helpers.health_check(cluster_check=False, tries=40)

        # When we use the MS consumer cluster, we sometimes need to wait a little more time before
        # start creating resources
        assert check_pods_after_node_replacement()
        tries = 3 if is_ms_consumer_cluster() else 1
        # Check basic cluster functionality by creating some resources
        self.sanity_helpers.create_resources_on_ms_consumers(tries=tries)
