import logging
import pytest

from ocs_ci.ocs import ocp, constants, defaults, node
from ocs_ci.framework.testlib import tier4, E2ETest, bugzilla
from tests.sanity_helpers import Sanity
from ocs_ci.ocs.monitoring import check_pvcdata_collected_on_prometheus
from ocs_ci.ocs.resources import pod

logger = logging.getLogger(__name__)


@pytest.fixture()
def test_fixture(pod_factory, num_of_pod=3):
    """
    Setup and teardown
    """
    pod_objs = [
        pod_factory(
            interface=constants.CEPHBLOCKPOOL,
            status=constants.STATUS_RUNNING
        ) for _ in range(num_of_pod)
    ]

    # Check for the created pvc metrics on prometheus pod
    for pod_obj in pod_objs:
        assert check_pvcdata_collected_on_prometheus(pod_obj.pvc.name), (
            f"On prometheus pod for created pvc {pod_obj.pvc.name} related data is not collected"
        )

    return pod_objs


@bugzilla('1744204')
@pytest.mark.polarion_id("OCS-579")
class TestDrainNodeWherePrometheusPodHosted(E2ETest):
    """
    When the node is drained where the prometheus pod is hosted,
    the pod should be re-spin on new healthy node.
    They should not be any loss of the data/metrics which was collected before.
    """

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance
        """
        self.sanity_helpers = Sanity()

    @tier4
    def test_monitoring_after_draining_node_where_prometheus_hosted(self, test_fixture, pod_factory):
        """
        Test case to validate when node is drained where prometheus
        is hosted, prometheus pod should re-spin on new healthy node
        and shouldn't be any data/metrics loss
        """
        pod_objs = test_fixture

        # # Get the worker node list
        # worker_nodes = node.get_typed_nodes(node_type='worker')

        # Get the prometheus pod
        pod_obj_list = pod.get_all_pods(namespace=defaults.OCS_MONITORING_NAMESPACE, selector=['prometheus'])

        for pod_obj in pod_obj_list:

            # Get the pvc which mounted on prometheus pod
            pod_info = pod_obj.get()
            pvc_name = pod_info['spec']['volumes'][0]['persistentVolumeClaim']['claimName']

            # Get the node where the prometheus pod is hosted
            prometheus_pod_obj = pod_obj.get()
            prometheus_node = prometheus_pod_obj['spec']['nodeName']

            # Drain node where the prometheus pod hosted
            node.drain_nodes([prometheus_node])

            # Validate node is in SchedulingDisabled state
            node.wait_for_nodes_status(
                [prometheus_node], status=constants.NODE_READY_SCHEDULING_DISABLED
            )

            # Validate all prometheus pod is running
            POD = ocp.OCP(kind=constants.POD, namespace=defaults.OCS_MONITORING_NAMESPACE)
            assert POD.wait_for_resource(
                condition='Running', selector=f'app=prometheus', timeout=60
            )

            # Validate prometheus pod is re-spinned on new healthy node
            pod_info = pod_obj.get()
            new_node = pod_info['spec']['nodeName']
            assert new_node not in prometheus_node, (
                f'Promethues pod not re-spinned on new node'
            )
            logger.info(f"Prometheus pod re-spinned on new node {new_node}")

            # Validate same pvc is mounted on prometheus pod
            assert pod_info['spec']['volumes'][0]['persistentVolumeClaim']['claimName'] in pvc_name, (
                f"Old pvc not found after restarting the prometheus pod {pod_obj.name}"
            )

            # Mark the nodes back to schedulable
            node.schedule_nodes([prometheus_node])

            # Check the node are Ready state and check cluster is health ok
            self.sanity_helpers.health_check()

        # Check for the created pvc metrics after rebooting the master nodes
        for pod_obj in pod_objs:
            assert check_pvcdata_collected_on_prometheus(pod_obj.pvc.name), (
                f"On prometheus pod for created pvc {pod_obj.pvc.name} related data is not collected"
            )
