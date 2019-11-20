import logging
import pytest

from ocs_ci.ocs import constants, node
from ocs_ci.utility import aws
from ocs_ci.framework.testlib import tier1, tier2, ManageTest, bugzilla, aws_platform_required

from tests.sanity_helpers import Sanity

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
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


@bugzilla('1769667')
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
        # Get 1 node
        typed_nodes = node.get_typed_nodes(node_type=node_type, num_of_nodes=1)
        typed_node_name = typed_nodes[0].name

        # Maintenance the node (unschedule and drain)
        node.drain_nodes([typed_node_name])

        # Check basic cluster functionality by creating resources
        # (pools, storageclasses, PVCs, pods - both CephFS and RBD),
        # run IO and delete the resources
        self.sanity_helpers.create_resources(pvc_factory, pod_factory)
        self.sanity_helpers.delete_resources()

        # Mark the node back to schedulable
        node.schedule_nodes([typed_node_name])

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check()

    @tier2
    @aws_platform_required
    @pytest.mark.parametrize(
        argnames=["node_type"],
        argvalues=[
            pytest.param(*['worker'], marks=pytest.mark.polarion_id("OCS-1292")),
            pytest.param(*['master'], marks=pytest.mark.polarion_id("OCS-1293"))
        ]
    )
    def test_node_maintenance_restart_activate(
        self, ec2_instances, aws_obj, pvc_factory, pod_factory, node_type
    ):
        """
        OCS-1292/OCS-1293:
        - Maintenance (mark as unscheduable and drain) 1 worker/master node
        - Restart the node's ec2 instance
        - Mark the node as scheduable
        - Check cluster and Ceph health
        - Check cluster functionality by creating and deleting resources
          (pools, storageclasses, PVCs, pods - both CephFS and RBD)

        """
        # Get 1 node
        typed_node = node.get_typed_nodes(node_type=node_type, num_of_nodes=1)
        assert typed_node, f"Failed to find a {node_type} node for the test"
        typed_node_name = typed_node[0].name

        # Maintenance the node (unschedule and drain). The function contains logging
        node.drain_nodes([typed_node_name])

        instance = aws.get_instances_ids_and_names(typed_node)
        assert instance, f"Failed to get ec2 instances for node {typed_node_name}"

        # Restarting ec2 instance
        aws_obj.restart_ec2_instances(instances=instance, wait=True)

        node.wait_for_nodes_status(
            node_names=[typed_node_name], status=constants.NODE_READY_SCHEDULING_DISABLED
        )
        # Mark the node back to schedulable
        node.schedule_nodes([typed_node_name])

        # Check cluster and Ceph health and checking basic cluster
        # functionality by creating resources (pools, storageclasses,
        # PVCs, pods - both CephFS and RBD), run IO and delete the resources
        self.sanity_helpers.health_check()
        self.sanity_helpers.create_resources(pvc_factory, pod_factory)
        self.sanity_helpers.delete_resources()

    @tier2
    @pytest.mark.parametrize(
        argnames=["nodes_type"],
        argvalues=[
            pytest.param(*['worker'], marks=pytest.mark.polarion_id("OCS-1273")),
            pytest.param(*['master'], marks=pytest.mark.polarion_id("OCS-1271"))
        ]
    )
    def test_2_nodes_maintenance_same_type(
        self, pvc_factory, pod_factory, nodes_type
    ):
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
        assert typed_nodes, f"Failed to find a {nodes_type} node for the test"

        typed_node_names = [typed_node.name for typed_node in typed_nodes]

        # Maintenance the nodes (unschedule and drain)
        node.drain_nodes(typed_node_names)

        # Mark the nodes back to schedulable
        node.schedule_nodes(typed_node_names)

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check()

        # Check basic cluster functionality by creating resources
        # (pools, storageclasses, PVCs, pods - both CephFS and RBD),
        # run IO and delete the resources
        self.sanity_helpers.create_resources(pvc_factory, pod_factory)
        self.sanity_helpers.delete_resources()

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
            node.get_typed_nodes(
                node_type=node_type, num_of_nodes=1
            )[0] for node_type in ['worker', 'master']
        ]
        assert nodes, f"Failed to find a nodes for the test"

        node_names = [typed_node.name for typed_node in nodes]

        # Maintenance the nodes (unschedule and drain)
        node.drain_nodes(node_names)

        # Check basic cluster functionality by creating resources
        # (pools, storageclasses, PVCs, pods - both CephFS and RBD),
        # run IO and delete the resources
        self.sanity_helpers.create_resources(pvc_factory, pod_factory)
        self.sanity_helpers.delete_resources()

        # Mark the nodes back to schedulable
        node.schedule_nodes(node_names)

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check()
