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
)
from ocs_ci.ocs.resources.pod import check_pods_after_node_replacement
from ocs_ci.helpers.sanity_helpers import SanityManagedService


log = logging.getLogger(__name__)


@tier4b
@skipif_ibm_cloud
@skipif_external_mode
@ignore_leftovers
@managed_service_required
class TestRollingWorkerNodeTerminateAndRecoveryMS(ManageTest):
    """
    Test rolling terminate and recovery of the OCS worker nodes when using the Managed Service
    """

    @pytest.fixture(autouse=True)
    def setup(self, create_scale_pods_and_pvcs_using_kube_job_on_ms_consumers):
        """
        Initialize the Sanity instance for the Managed Service

        """
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

        request.addfinalizer(finalizer)

    @pytest.mark.polarion_id("OCS-4661")
    def test_rolling_terminate_and_recovery_in_controlled_fashion(self, nodes):
        """
        Test rolling terminate and recovery of the OCS worker nodes, when waiting for the pods to
        be running and Ceph Health OK between the iterations. This test is for the Managed Service

        """
        # Get OCS worker node objects
        ocs_node_objs = get_ocs_nodes()

        # Start rolling shutdown and recovery of OCS worker nodes
        for node_obj in ocs_node_objs:
            nodes.terminate_nodes(nodes=[node_obj], wait=True)
            log.info(f"Successfully terminated the node: {node_obj.name}")

            log.info("Waiting for all the worker nodes to be ready...")
            wait_for_node_count_to_reach_status(
                node_count=len(ocs_node_objs), timeout=900
            )
            log.info("Waiting for all the pods to be running")
            assert check_pods_after_node_replacement(), "Not all the pods are running"
            self.sanity_helpers.health_check_ms(cluster_check=False, tries=40)

        # Check basic cluster functionality by creating some resources
        self.sanity_helpers.create_resources_on_ms_consumers()
