import logging
import pytest

from subprocess import TimeoutExpired
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import (
    drain_nodes, schedule_nodes, get_typed_nodes, wait_for_nodes_status, get_node_objs
)
from ocs_ci.framework.testlib import (
    tier1, tier2, tier3, tier4, ManageTest, aws_platform_required, ignore_leftovers, bugzilla
)

from tests.sanity_helpers import Sanity

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def schedule_nodes_teardown(request):
    """
    Make sure that all cluster's nodes are in 'Ready' state and if not,
    change them back to 'Ready' state by marking them as scheduble

    """
    def finalizer():
        scheduling_disabled_nodes = [
            n.name for n in get_node_objs() if n.ocp.get_resource_status(
                n.name
            ) == constants.NODE_READY_SCHEDULING_DISABLED
        ]
        if scheduling_disabled_nodes:
            schedule_nodes(scheduling_disabled_nodes)
    request.addfinalizer(finalizer)


@ignore_leftovers
class TestNodesMaintenance(ManageTest):
    """
    Test basic flows of maintenance (unschedule and drain) and
    activate operations, followed by cluster functionality and health checks

    """
    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    @tier1
    @pytest.mark.parametrize(
        argnames=["node_type"],
        argvalues=[
            pytest.param(*['worker'], marks=pytest.mark.polarion_id("OCS-1269")),
            pytest.param(*['master'], marks=pytest.mark.polarion_id("OCS-1272"))
        ]
    )
    def test_node_maintenance(self, node_type, pvc_factory, pod_factory):
        """
        OCS-1269/OCS-1272:
        - Maintenance (mark as unscheduable and drain) 1 worker/master node
        - Check cluster functionality by creating resources
          (pools, storageclasses, PVCs, pods - both CephFS and RBD)
        - Mark the node as scheduable
        - Check cluster and Ceph health

        """
        # Get a list of 2 nodes. Pick one of them after checking
        # which one does't have the rook operator running on
        typed_nodes = get_typed_nodes(node_type=node_type, num_of_nodes=2)
        typed_node_name = typed_nodes[0].name
        # Workaround for BZ 1778488 - https://github.com/red-hat-storage/ocs-ci/issues/1222
        rook_operator_pod = pod.get_operator_pods()[0]
        operator_node = pod.get_pod_node(rook_operator_pod)
        if operator_node.get().get('metadata').get('name') == typed_node_name:
            typed_node_name = typed_nodes[1].name
        # End of workaround for BZ 1778488

        # Maintenance the node (unschedule and drain)
        drain_nodes([typed_node_name])

        # Check basic cluster functionality by creating resources
        # (pools, storageclasses, PVCs, pods - both CephFS and RBD),
        # run IO and delete the resources
        self.sanity_helpers.create_resources(pvc_factory, pod_factory)
        self.sanity_helpers.delete_resources()

        # Mark the node back to schedulable
        schedule_nodes([typed_node_name])

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check()

    @bugzilla('1778488')
    @tier4
    @aws_platform_required
    @pytest.mark.parametrize(
        argnames=["node_type"],
        argvalues=[
            pytest.param(*['worker'], marks=pytest.mark.polarion_id("OCS-1292")),
            pytest.param(*['master'], marks=pytest.mark.polarion_id("OCS-1293"))
        ]
    )
    def test_node_maintenance_restart_activate(
        self, nodes, pvc_factory, pod_factory, node_type
    ):
        """
        OCS-1292/OCS-1293:
        - Maintenance (mark as unscheduable and drain) 1 worker/master node
        - Restart the node
        - Mark the node as scheduable
        - Check cluster and Ceph health
        - Check cluster functionality by creating and deleting resources
          (pools, storageclasses, PVCs, pods - both CephFS and RBD)

        """
        # Get a list of 2 nodes. Pick one of them after checking
        # which one does't have the rook operator running on
        typed_nodes = get_typed_nodes(node_type=node_type, num_of_nodes=2)
        assert typed_nodes, f"Failed to find a {node_type} node for the test"
        typed_node_name = typed_nodes[0].name

        # Workaround for BZ 1778488 - https://github.com/red-hat-storage/ocs-ci/issues/1222
        rook_operator_pod = pod.get_operator_pods()[0]
        operator_node = pod.get_pod_node(rook_operator_pod)
        if operator_node.get().get('metadata').get('name') == typed_node_name:
            typed_node_name = typed_nodes[1].name
        # End of workaround for BZ 1778488

        # Maintenance the node (unschedule and drain). The function contains logging
        drain_nodes([typed_node_name])

        # Restarting the node
        nodes.restart_nodes(nodes=typed_nodes, wait=True)

        wait_for_nodes_status(
            node_names=[typed_node_name], status=constants.NODE_READY_SCHEDULING_DISABLED
        )
        # Mark the node back to schedulable
        schedule_nodes([typed_node_name])

        # Check cluster and Ceph health and checking basic cluster
        # functionality by creating resources (pools, storageclasses,
        # PVCs, pods - both CephFS and RBD), run IO and delete the resources
        self.sanity_helpers.health_check()
        self.sanity_helpers.create_resources(pvc_factory, pod_factory)
        self.sanity_helpers.delete_resources()

    @tier3
    @pytest.mark.parametrize(
        argnames=["nodes_type"],
        argvalues=[
            pytest.param(*['worker'], marks=pytest.mark.polarion_id("OCS-1273")),
            pytest.param(*['master'], marks=pytest.mark.polarion_id("OCS-1271"))
        ]
    )
    def test_2_nodes_maintenance_same_type(self, nodes_type):
        """
        OCS-1273/OCs-1271:
        - Try draining 2 nodes from the same type - should fail
        - Check cluster and Ceph health

        """
        # Get 2 nodes
        typed_nodes = get_typed_nodes(node_type=nodes_type, num_of_nodes=2)
        assert typed_nodes, f"Failed to find a {nodes_type} node for the test"

        typed_node_names = [typed_node.name for typed_node in typed_nodes]

        # Try draining 2 nodes - should fail
        try:
            drain_nodes(typed_node_names)
        except TimeoutExpired:
            logger.info(f"Draining of nodes {typed_node_names} failed as expected")

        schedule_nodes(typed_node_names)

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check()

    @tier2
    @pytest.mark.polarion_id("OCS-1274")
    def test_2_nodes_different_types(self, pvc_factory, pod_factory):
        """
        OCS-1274:
        - Maintenance (mark as unscheduable and drain) 1 worker node and 1
          master node
        - Check cluster functionality by creating resources
          (pools, storageclasses, PVCs, pods - both CephFS and RBD)
        - Mark the nodes as scheduable
        - Check cluster and Ceph health

        """
        # Get 1 node from each type
        nodes = [
            get_typed_nodes(
                node_type=node_type, num_of_nodes=1
            )[0] for node_type in ['worker', 'master']
        ]
        assert nodes, f"Failed to find a nodes for the test"

        node_names = [typed_node.name for typed_node in nodes]

        # Maintenance the nodes (unschedule and drain)
        drain_nodes(node_names)

        # Check basic cluster functionality by creating resources
        # (pools, storageclasses, PVCs, pods - both CephFS and RBD),
        # run IO and delete the resources
        self.sanity_helpers.create_resources(pvc_factory, pod_factory)
        self.sanity_helpers.delete_resources()

        # Mark the nodes back to schedulable
        schedule_nodes(node_names)

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check()
