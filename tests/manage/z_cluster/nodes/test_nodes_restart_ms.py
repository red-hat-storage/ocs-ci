import logging
import pytest
import random


from ocs_ci.framework.testlib import (
    tier4a,
    tier4b,
    ignore_leftovers,
    ManageTest,
    bugzilla,
    managed_service_required,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import (
    get_node_objs,
    recover_node_to_ready_state,
    get_osd_running_nodes,
    get_node_osd_ids,
    wait_for_osd_ids_come_up_on_node,
    wait_for_nodes_status,
    verify_worker_nodes_security_groups,
)
from ocs_ci.ocs.resources import pod
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs.cluster import (
    ceph_health_check,
    is_ms_provider_cluster,
    is_ms_consumer_cluster,
)


logger = logging.getLogger(__name__)


@ignore_leftovers
@managed_service_required
class TestNodesRestartMS(ManageTest):
    """
    Test nodes restart scenarios when using managed service
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
            ocp_nodes = get_node_objs()
            for n in ocp_nodes:
                recover_node_to_ready_state(n)

            if is_ms_provider_cluster():
                logger.info(
                    "Verify the worker nodes security groups on the provider..."
                )
                assert verify_worker_nodes_security_groups()

            ceph_health_check()

        request.addfinalizer(finalizer)

    @tier4a
    def test_osd_node_restart_and_check_osd_pods_status(self, nodes):
        """
        1) Restart one of the osd nodes.
        2) Check that the osd pods associated with the node should change to a Terminating state.
        3) Wait for the node to reach Ready state.
        3) Check that the new osd pods with the same ids start on the same node.
        """
        # This is a workaround due to the issue https://github.com/red-hat-storage/ocs-ci/issues/6162
        if is_ms_consumer_cluster():
            pytest.skip("The test will not run on a consumer cluster")

        osd_node_name = random.choice(get_osd_running_nodes())
        osd_node = get_node_objs([osd_node_name])[0]

        old_osd_pod_ids = get_node_osd_ids(osd_node_name)
        logger.info(f"osd pod ids: {old_osd_pod_ids}")
        node_osd_pods = pod.get_osd_pods_having_ids(old_osd_pod_ids)
        node_osd_pod_names = [p.name for p in node_osd_pods]

        logger.info(f"Going to restart the node {osd_node_name}")
        nodes.restart_nodes(nodes=[osd_node], wait=False)

        logger.info("Verify the node osd pods go into a Terminating state")
        res = pod.wait_for_pods_to_be_in_statuses(
            [constants.STATUS_TERMINATING], node_osd_pod_names
        )
        assert res, "Not all the node osd pods are in a Terminating state"

        wait_for_nodes_status(node_names=[osd_node_name])
        assert wait_for_osd_ids_come_up_on_node(
            osd_node_name, old_osd_pod_ids, timeout=300
        )
        logger.info(
            f"the osd ids {old_osd_pod_ids} Successfully come up on the node {osd_node_name}"
        )

    @tier4a
    def test_nodes_restart(self, nodes):
        """
        Test nodes restart (from the platform layer, i.e, EC2 instances, VMWare VMs)

        """
        ocp_nodes = get_node_objs()
        nodes.restart_nodes(nodes=ocp_nodes, wait=True)
        self.sanity_helpers.health_check()

    @tier4b
    @bugzilla("1754287")
    @pytest.mark.polarion_id("OCS-2015")
    def test_rolling_nodes_restart(self, nodes):
        """
        Test restart nodes one after the other and check health status in between

        """
        ocp_nodes = get_node_objs()
        for node in ocp_nodes:
            nodes.restart_nodes(nodes=[node], wait=False)
            self.sanity_helpers.health_check(cluster_check=False, tries=60)
