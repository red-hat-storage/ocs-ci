import logging
import pytest

from ocs_ci.ocs import constants, defaults
from ocs_ci.framework.testlib import tier4, E2ETest, bugzilla
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.monitoring import check_pvcdata_collected_on_prometheus
from ocs_ci.utility import aws
from ocs_ci.ocs.node import wait_for_nodes_status
from tests.helpers import wait_for_resource_state
from tests.sanity_helpers import Sanity

logger = logging.getLogger(__name__)


@pytest.fixture()
def create_pods(pod_factory):
    """
    Create resource for the test
    """

    pod_objs = [
        pod_factory(
            interface=constants.CEPHBLOCKPOOL,
            status=constants.STATUS_RUNNING
        ) for _ in range(2)
    ]

    # Check for the created pvc metrics on prometheus pod
    for pod_obj in pod_objs:
        assert check_pvcdata_collected_on_prometheus(pod_obj.pvc.name), (
            f"On prometheus pod for created pvc {pod_obj.pvc.name} related data is not collected"
        )
    return pod_objs


@bugzilla('1750328')
@pytest.mark.polarion_id("OCS-711")
class TestWhenShutdownAndRecoverOfPrometheusNode(E2ETest):
    """
    Validate whether shutdown and recovery of a node running
    monitoring has no functional impact
    """
    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    @tier4
    def test_monitoring_shutdown_and_recovery_prometheus_node(self, create_pods, pod_factory):
        """
        Test case to validate when the prometheus pod is down and
        interaction with prometheus
        """
        pod_objs = create_pods

        aws_obj = aws.AWS()

        # Get all the openshift-monitoring pods
        monitoring_pod_obj_list = pod.get_all_pods(namespace=defaults.OCS_MONITORING_NAMESPACE)

        # Get all prometheus pods
        prometheus_pod_obj_list = pod.get_all_pods(namespace=defaults.OCS_MONITORING_NAMESPACE, selector=['prometheus'])

        for prometheus_pod_obj in prometheus_pod_obj_list:

            # Get the node where the prometheus pod is hosted
            prometheus_node_obj = pod.get_pod_node(prometheus_pod_obj)

            # Shutdown node where the prometheus pod is hosted
            instances = aws.get_instances_ids_and_names([prometheus_node_obj])
            aws_obj.stop_ec2_instances(instances=instances, wait=True, force=True)

            pod_obj = [
                pod_factory(
                    interface=constants.CEPHBLOCKPOOL,
                    status=constants.STATUS_RUNNING
                )
            ]
            pod_objs.extend(pod_obj)

            # Start the instance which was stopped
            aws_obj.start_ec2_instances(instances=instances, wait=True)

            # Validate all nodes are in READY state
            wait_for_nodes_status()

        # Check the node are Ready state and check cluster is health ok
        self.sanity_helpers.health_check()

        # ToDo: Add prometheus health check

        # Check all the monitoring pods are up
        for pod_obj in monitoring_pod_obj_list:
            wait_for_resource_state(resource=pod_obj, state=constants.STATUS_RUNNING)

        # Check for the created pvc metrics after shutdown and recovery of prometheus nodes
        for pod_obj in pod_objs:
            assert check_pvcdata_collected_on_prometheus(pod_obj.pvc.name), (
                f"On prometheus pod for created pvc {pod_obj.pvc.name} related data is not collected"
            )
