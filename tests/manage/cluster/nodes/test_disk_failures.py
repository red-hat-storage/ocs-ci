import logging
import pytest

from ocs_ci.ocs import node, constants
from ocs_ci.framework import config
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.resources.pod import get_all_pods
from ocs_ci.framework.testlib import tier4, ignore_leftovers, ManageTest, aws_platform_required
from tests.sanity_helpers import Sanity
from tests.helpers import wait_for_resource_count_change, get_admin_key

logger = logging.getLogger(__name__)


@tier4
@ignore_leftovers
class TestDetachAttachWorkerVolume(ManageTest):
    """
    Test class for detach and attach worker volume

    """
    @pytest.fixture(autouse=True)
    def teardown(self, request, nodes):
        """
        Restart nodes that are in status NotReady, for situations in
        which the test failed before restarting the node after detach volume,
        which leaves nodes in NotReady

        """
        def finalizer():
            not_ready_nodes = [
                n for n in node.get_node_objs() if n
                .ocp.get_resource_status(n.name) == constants.NODE_NOT_READY
            ]
            logger.warning(
                f"Nodes in NotReady status found: {[n.name for n in not_ready_nodes]}"
            )
            if not_ready_nodes:
                nodes.restart_nodes(not_ready_nodes)
                node.wait_for_nodes_status()
        request.addfinalizer(finalizer)

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
        - Wait for the volumes to be re-attached back to the worker node
        - Restart the node so the volume will get re-mounted

        """
        # Get a data volume
        data_volume = nodes.get_data_volumes()[0]

        # Get the worker node according to the volume attachment
        worker = nodes.get_node_by_attached_volume(data_volume)

        # Detach volume (logging is done inside the function)
        nodes.detach_volume(data_volume)

        # Validate cluster is still functional
        try:
            # In case the selected node that its volume disk was detached was the one
            # running the ceph tools pod, we'll need to wait for a new ct pod to start.
            # For that, a function that connects to the ct pod is being used to check if
            # it's alive
            _ = get_admin_key()
        except CommandFailed as ex:
            if "connection timed out" in str(ex):
                logger.info(
                    "Ceph tools box was running on the node that its data "
                    "volume has been detached. Hence, waiting for a new "
                    "Ceph tools box pod to spin up"
                )
                wait_for_resource_count_change(
                    func_to_use=get_all_pods, previous_num=1,
                    namespace=config.ENV_DATA['cluster_namespace'], timeout=120,
                    selector='app=rook-ceph-tools'
                )
            else:
                raise
        finally:
            self.sanity_helpers.create_resources(pvc_factory, pod_factory)

        # Wait for worker volume to be re-attached automatically to the node
        assert nodes.wait_for_volume_attach(data_volume), (
            f"Volume {data_volume.id} failed to be re-attached to a worker node"
        )

        # Restart the instance so the volume will get re-mounted
        nodes.restart_nodes([worker])

        # Cluster health check
        self.sanity_helpers.health_check()

    @pytest.mark.polarion_id("OCS-1086")
    def test_detach_attach_2_data_volumes(self, nodes, pvc_factory, pod_factory):
        """
        Detach and attach disk from 2 worker nodes

        - Detach the data 2 of the data volumes from their worker nodes
        - Wait for the volumes to be re-attached back to the worker nodes
        - Restart the nodes so the volume will get re-mounted in each node
        - Check cluster health and functionality to make sure detach,
          attach and restart did not affect the cluster

        """
        # Get 2 data volumes
        data_volumes = nodes.get_data_volumes()[:2]
        workers_and_volumes = [
            {'worker': nodes.get_node_by_attached_volume(vol), 'volume': vol}
            for vol in data_volumes
        ]

        for worker_and_volume in workers_and_volumes:
            # Detach the volume (logging is done inside the function)
            nodes.detach_volume(worker_and_volume['volume'])

        for worker_and_volume in workers_and_volumes:
            # Wait for worker volume to be re-attached automatically to the node
            assert nodes.wait_for_volume_attach(worker_and_volume['volume']), (
                f"Volume {worker_and_volume['volume']} "
                f"failed to be re-attached to a worker node"
            )

        # Restart the instances so the volume will get re-mounted
        nodes.restart_nodes(
            [worker_and_volume['worker'] for worker_and_volume in workers_and_volumes]
        )

        # Validate cluster is still functional
        self.sanity_helpers.health_check()
        self.sanity_helpers.create_resources(pvc_factory, pod_factory)
