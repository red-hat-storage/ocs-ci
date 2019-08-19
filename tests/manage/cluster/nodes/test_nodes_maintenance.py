import logging
import pytest

from ocs_ci.ocs import constants, node
from ocs_ci.framework.testlib import tier1, tier2, ManageTest, bugzilla

from tests import sanity_helpers

logger = logging.getLogger(__name__)


@pytest.fixture()
def schedule_nodes(request):
    """
    Make sure that all cluster's nodes are in 'Ready' state and if not,
    change them back to 'Ready' state by marking them as scheduble

    """
    def finalizer():
        scheduling_disabled_nodes = [
            n.name for n in node.get_node_objs() if n.ocp.get_resource_status(
                n.name
            ) == constants.NODE_READY_SCHEDULING_DISABLED
        ]
        if scheduling_disabled_nodes:
            node.schedule_nodes(scheduling_disabled_nodes)
    request.addfinalizer(finalizer)


@bugzilla('1734162')
class TestNodesMaintenance(ManageTest):
    """
    Test basic flows of maintenance (unschedule and drain) and
    activate operations, followed by cluster functionality and health checks

    """
    @tier1
    @pytest.mark.parametrize(
        argnames=["node_type"],
        argvalues=[
            pytest.param(*['worker'], marks=pytest.mark.polarion_id("OCS-1269")),
            pytest.param(*['master'], marks=pytest.mark.polarion_id("OCS-1272"))
        ]
    )
    def test_node_maintenance(self, resources, schedule_nodes, node_type):
        """
        OCS-1269/OCS-1272:
        - Maintenance (mark as unscheduable and drain) 1 worker/master node
        - Check cluster functionality by creating resources
          (pools, storageclasses, PVCs, pods - both CephFS and RBD)
        - Mark the node as scheduable
        - Check cluster and Ceph health

        """
        # Get 1 node
        typed_nodes = node.get_typed_nodes(node_type=node_type, num_of_nodes=1)
        typed_node_name = typed_nodes[0].name

        # Maintenance the node (unschedule and drain)
        node.maintenance_nodes([typed_node_name])

        # Check basic cluster functionality by creating resources
        # (pools, storageclasses, PVCs, pods - both CephFS and RBD),
        # run IO and delete the resources
        sanity_helpers.create_resources(resources)
        sanity_helpers.delete_resources(resources)

        # Mark the node back to schedulable
        node.schedule_nodes([typed_node_name])

        # Perform cluster and Ceph health checks
        sanity_helpers.health_check([typed_node_name])

    @tier2
    @pytest.mark.parametrize(
        argnames=["nodes_type"],
        argvalues=[
            pytest.param(*['worker'], marks=pytest.mark.polarion_id("OCS-1273")),
            pytest.param(*['master'], marks=pytest.mark.polarion_id("OCS-1271"))
        ]
    )
    def test_2_nodes_maintenance_same_type(self, resources, schedule_nodes, nodes_type):
        """
        OCS-1273/OCs-1271:
        - Maintenance (mark as unscheduable and drain) 2 worker/master nodes
        - Mark the nodes as scheduable
        - Check cluster and Ceph health
        - Check cluster functionality by creating resources
          (pools, storageclasses, PVCs, pods - both CephFS and RBD)

        """
        # Get 2 nodes
        typed_nodes = node.get_typed_nodes(node_type=nodes_type, num_of_nodes=2)
        typed_node_names = [typed_node.name for typed_node in typed_nodes]

        # Maintenance the nodes (unschedule and drain)
        node.maintenance_nodes(typed_node_names)

        # Mark the nodes back to schedulable
        node.schedule_nodes(typed_node_names)

        # Perform cluster and Ceph health checks
        sanity_helpers.health_check(typed_node_names)

        # Check basic cluster functionality by creating resources
        # (pools, storageclasses, PVCs, pods - both CephFS and RBD),
        # run IO and delete the resources
        sanity_helpers.create_resources(resources)
        sanity_helpers.delete_resources(resources)

    @tier2
    @pytest.mark.polarion_id("OCS-1274")
    def test_2_nodes_different_types(self, resources, schedule_nodes):
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
            node.get_typed_nodes(
                node_type=node_type, num_of_nodes=1
            )[0] for node_type in ['worker', 'master']
        ]

        node_names = [typed_node.name for typed_node in nodes]

        # Maintenance the nodes (unschedule and drain)
        node.maintenance_nodes(node_names)

        # Check basic cluster functionality by creating resources
        # (pools, storageclasses, PVCs, pods - both CephFS and RBD),
        # run IO and delete the resources
        sanity_helpers.create_resources(resources)
        sanity_helpers.delete_resources(resources)

        # Mark the nodes back to schedulable
        node.schedule_nodes(node_names)

        # Perform cluster and Ceph health checks
        sanity_helpers.health_check(node_names)
