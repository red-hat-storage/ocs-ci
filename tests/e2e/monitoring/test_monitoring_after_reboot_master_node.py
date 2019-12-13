import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import workloads, E2ETest, ignore_leftovers
from ocs_ci.utility import aws
from tests.sanity_helpers import Sanity
from ocs_ci.ocs.monitoring import check_pvcdata_collected_on_prometheus
from ocs_ci.ocs.node import wait_for_nodes_status, get_typed_nodes
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import CommandFailed


logger = logging.getLogger(__name__)


@retry((CommandFailed, TimeoutError), tries=10, delay=3, backoff=1)
def wait_for_master_node_to_be_running_state():
    """
    Waits for the all the nodes to be in running state
    """
    wait_for_nodes_status(timeout=900)


@pytest.mark.polarion_id("OCS-709")
@ignore_leftovers
class TestRebootMasterNodeAndInteractionWithPrometheus(E2ETest):
    """
    Rebooting master node shouldn't impact the data/metrics
    stored on persistent monitoring
    """

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance
        """
        self.sanity_helpers = Sanity()

    @pytest.fixture(autouse=True)
    def test_fixture(self, pod_factory, num_of_pod=1):
        """
        Create resources for tests
        """
        self.pod_objs = [
            pod_factory(
                interface=constants.CEPHBLOCKPOOL,
                status=constants.STATUS_RUNNING
            ) for _ in range(num_of_pod)
        ]

        # Check for the created pvc metrics on prometheus pod
        for pod_obj in self.pod_objs:
            assert check_pvcdata_collected_on_prometheus(pod_obj.pvc.name), (
                f"On prometheus pod for created pvc {pod_obj.pvc.name} related data is not collected"
            )

    @workloads
    def test_monitoring_after_rebooting_master_node(self, pod_factory):
        """
        Test case to validate reboot master node and its
        interaction with prometheus pods
        """
        aws_obj = aws.AWS()

        # Get the master node list
        master_nodes = get_typed_nodes(node_type='master')

        # Reboot one after one master nodes
        for node in master_nodes:
            instances = aws.get_instances_ids_and_names([node])
            aws_obj.restart_ec2_instances(instances=instances, wait=True, force=True)

            # Validate all nodes are in READY state
            wait_for_master_node_to_be_running_state()

        # Check the node are Ready state and check cluster is health ok
        self.sanity_helpers.health_check()

        # Check for the created pvc metrics after rebooting the master nodes
        for pod_obj in self.pod_objs:
            assert check_pvcdata_collected_on_prometheus(pod_obj.pvc.name), (
                f"On prometheus pod for created pvc {pod_obj.pvc.name} related data is not collected"
            )

        pod_obj = pod_factory(
            interface=constants.CEPHBLOCKPOOL,
            status=constants.STATUS_RUNNING
        )
        self.pod_objs.extend([pod_obj])

        # Check for the new created pvc metrics on prometheus pod
        assert check_pvcdata_collected_on_prometheus(pod_obj.pvc.name), (
            f"On prometheus pod for created pvc {pod_obj.pvc.name} related data is not collected"
        )
