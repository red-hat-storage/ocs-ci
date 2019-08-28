import logging
import pytest

from ocs_ci.ocs import node
from ocs_ci.framework.testlib import tier4, ignore_leftovers, ManageTest
from ocs_ci.framework import config
from ocs_ci.utility import aws
from tests.sanity_helpers import Sanity

logger = logging.getLogger(__name__)


@tier4
@pytest.mark.skipif(
    condition=config.ENV_DATA['platform'] != 'AWS',
    reason="Tests are not running on AWS deployed cluster"
)
@ignore_leftovers
class TestDetachAttachWorkerVolumeAWS(ManageTest):
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
    def test_detach_attach_worker_volume(self, aws_obj, pvc_factory, pod_factory):
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

        # Get the worker node's ec2 instance ID and name
        instance = aws.get_instances_ids_and_names([worker])
        assert instance, f"Failed to get ec2 instances for node {worker.name}"

        instance_id = [*instance][0]

        # Get the ec2 instance data volume Volume instance
        ec2_volume = aws.get_data_volumes(instance_id)[0]

        # Detach volume (logging is done inside the function)
        aws_obj.detach_volume(ec2_volume)

        # Validate cluster is still functional
        self.sanity_helpers.create_resources(pvc_factory, pod_factory)

        # Attach volume (logging is done inside the function)
        aws_obj.attach_volume(ec2_volume, instance_id)

        # Restart the instance so the volume will get re-mounted
        aws_obj.restart_ec2_instances(instances=instance, wait=True)

        # Cluster health check
        self.sanity_helpers.health_check()

    @pytest.mark.polarion_id("OCS-1086")
    def test_detach_attach_2_workers_volumes(self, aws_obj, pvc_factory, pod_factory):
        """
        Detach and attach disk from 2 worker nodes

        - Detach the data volume from 2 of the worker nodes
        - Attach back the volume to the worker nodes
        - Restart the nodes so the volume will get re-mounted in each node
        - Check cluster health and functionality to make sure detach,
          attach and restart did not affect the cluster

        """
        self.sanity_helpers.create_resources(pvc_factory, pod_factory)
        self.sanity_helpers.delete_resources()
        # Requesting 2 worker nodes for the test as this case includes
        # detach and attach of data volume of 1 worker node
        workers = node.get_typed_nodes(num_of_nodes=2)
        assert workers, "Failed to find worker nodes for the test"

        # Get the worker nodes ec2 instance IDs and names
        instances = aws.get_instances_ids_and_names(workers)
        assert instances, (
            f"Failed to get ec2 instances for node {[w.name for w in workers]}"
        )

        for instance in instances.items():
            instance_id = [*instance][0]

            # Get the ec2 instance data volume Volume instance
            ec2_volume = aws.get_data_volumes(instance_id)[0]

            # Detach volume (logging is done inside the function)
            aws_obj.detach_volume(ec2_volume)

            # Attach volume (logging is done inside the function)
            aws_obj.attach_volume(ec2_volume, instance_id)

        # Restart the instances so the volume will get re-mounted
        aws_obj.restart_ec2_instances(instances=instances, wait=True)

        # Validate cluster is still functional
        self.sanity_helpers.health_check()
        self.sanity_helpers.create_resources(pod_factory)

# TODO: Add test cases for VMWare and RHHI.Next
