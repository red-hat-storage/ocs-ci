import logging
import pytest

from ocs_ci.ocs import constants, defaults
from ocs_ci.framework.testlib import tier4, E2ETest, ignore_leftovers
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
    Create resources for the test
    """
    pod_objs = [
        pod_factory(
            interface=constants.CEPHBLOCKPOOL,
            status=constants.STATUS_RUNNING
        )
    ]

    # Check for the created pvc metrics on prometheus pod
    for pod_obj in pod_objs:
        assert check_pvcdata_collected_on_prometheus(pod_obj.pvc.name), (
            f"On prometheus pod for created pvc {pod_obj.pvc.name} related data is not collected"
        )

    return pod_objs


@pytest.mark.polarion_id("OCS-606")
@ignore_leftovers
class TestWhenOneOfThePrometheusNodeDown(E2ETest):
    """
    When the nodes are down, there should not be any functional impact
    on monitoring pods. All the data/metrics should be collected correctly.
    """
    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    @tier4
    def test_monitoring_when_one_of_the_prometheus_node_down(self, create_pods, pod_factory):
        """
        Test case to validate when the prometheus pod is down and
        interaction with prometheus
        """
        pod_objs = create_pods

        aws_obj = aws.AWS()

        # Get all prometheus pods
        pod_obj_list = pod.get_all_pods(namespace=defaults.OCS_MONITORING_NAMESPACE, selector=['prometheus'])

        for pod_obj in pod_obj_list:

            # Get the node where the prometheus pod is hosted
            pod_node_obj = pod.get_pod_node(pod_obj)

            # Make one of the node down where the prometheus pod is hosted
            instances = aws.get_instances_ids_and_names([pod_node_obj])
            aws_obj.restart_ec2_instances(instances=instances, wait=True, force=True)

            # Validate all nodes are in READY state
            wait_for_nodes_status()

        # Check the node are Ready state and check cluster is health ok
        self.sanity_helpers.health_check()

        # Check all the prometheus pods are up
        for pod_obj in pod_obj_list:
            wait_for_resource_state(resource=pod_obj, state=constants.STATUS_RUNNING)

        pod_obj = [
            pod_factory(
                interface=constants.CEPHBLOCKPOOL,
                status=constants.STATUS_RUNNING
            )
        ]
        pod_objs.extend(pod_obj)

        # Check for the created pvc metrics after restarting node where prometheus pod is hosted
        for pod_obj in pod_objs:
            assert check_pvcdata_collected_on_prometheus(pod_obj.pvc.name), (
                f"On prometheus pod for created pvc {pod_obj.pvc.name} related data is not collected"
            )
            logger.info(f"On prometheus pod for created pvc {pod_obj.pvc.name} related data is collected")
