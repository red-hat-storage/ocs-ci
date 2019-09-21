import logging
import pytest

from ocs_ci.ocs import node
from ocs_ci.framework.testlib import tier4, ignore_leftovers, ManageTest
from tests.sanity_helpers import Sanity

logger = logging.getLogger(__name__)


@tier4
@ignore_leftovers
class TestDetachAttachWorkerVolume(ManageTest):
    """
    Test class for detach and attach worker volume

    """
    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    @pytest.mark.polarion_id("OCS-1085")
    def test_detach_attach_worker_volume(self, nodes, pvc_factory, pod_factory):
        """
        Detach and attach worker volume

        - Detach the data volume from one of the worker nodes
        - Validate cluster functionality, without checking cluster and Ceph
          health (as one node volume is detached, the cluster will be unhealthy)
          by creating resources and running IO
        - Attach back the volume to the node
        - Restart the node so the volume will get re-mounted

        """
        # Requesting 1 worker node for the test as this case includes detach and
        # attach of data volume of 1 worker node
        worker = node.get_typed_nodes(num_of_nodes=1)
        assert worker, "Failed to find a worker node for the test"
        worker = worker[0]

        # Get the node's data volume
        data_volume = nodes.get_data_volume(worker)

        # Detach volume (logging is done inside the function)
        nodes.detach_volume(data_volume)

        # Validate cluster is still functional
        self.sanity_helpers.create_resources(pvc_factory, pod_factory)

        # Attach volume (logging is done inside the function)
        nodes.attach_volume(worker, data_volume)

        # Restart the instance so the volume will get re-mounted
        nodes.restart_nodes([worker])

        # Cluster health check
        self.sanity_helpers.health_check()

    @pytest.mark.polarion_id("OCS-1086")
    def test_detach_attach_2_workers_volumes(self, nodes, pvc_factory, pod_factory):
        """
        Detach and attach disk from 2 worker nodes

        - Detach the data volume from 2 of the worker nodes
        - Attach back the volume to the worker nodes
        - Restart the nodes so the volume will get re-mounted in each node
        - Check cluster health and functionality to make sure detach,
          attach and restart did not affect the cluster

        """
        # Requesting 2 worker nodes for the test as this case includes
        # detach and attach of data volume of 1 worker node
        workers = node.get_typed_nodes(num_of_nodes=2)
        assert workers, "Failed to find worker nodes for the test"

        for worker in workers:

            # Get the data volume
            data_volume = nodes.get_data_volume(worker)

            # Detach volume (logging is done inside the function)
            nodes.detach_volume(worker)

            # Attach volume (logging is done inside the function)
            nodes.attach_volume(worker, data_volume)

        # Restart the instances so the volume will get re-mounted
        nodes.restart_nodes(workers)

        # Validate cluster is still functional
        self.sanity_helpers.health_check()
        self.sanity_helpers.create_resources(pvc_factory, pod_factory)
