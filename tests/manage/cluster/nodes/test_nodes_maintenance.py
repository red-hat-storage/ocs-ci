import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.ocs import node
from ocs_ci.framework.testlib import tier1, ManageTest, bugzilla

from tests import sanity_helpers

logger = logging.getLogger(__name__)


@pytest.fixture()
def activate_nodes(request):
    """
    Activate all cluster nodes in case any of them are in
    SchedulingDisabled status

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


@tier1
@bugzilla('1734162')
class TestNodesMaintenance(ManageTest):
    """
    Test basic flow of maintenance and activate operations,
    followed by cluster functionality and health checks

    """
    @pytest.mark.polarion_id("OCS-1269")
    def test_worker_maintenance(self, resources, activate_nodes):
        """
        Maintenance and activate 1 worker node and check
        cluster functionality and health

        """
        # Get 1 worker node
        worker_nodes = node.get_typed_nodes(node_type='worker', num_of_nodes=1)
        worker_node_name = worker_nodes[0].name

        # Maintenance the worker node (unschedule and drain)
        node.maintenance_nodes([worker_node_name])

        sanity_helpers.create_resources(resources)
        sanity_helpers.delete_resources(resources)

        # Mark the worker node back to schedulable
        node.schedule_nodes([worker_node_name])

        # Perform cluster and Ceph health checks
        sanity_helpers.health_check([worker_node_name])
