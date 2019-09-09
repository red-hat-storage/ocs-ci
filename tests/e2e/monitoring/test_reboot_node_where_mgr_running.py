import logging
import pytest

from ocs_ci.ocs import ocp, constants, defaults
from ocs_ci.framework.testlib import tier4, E2ETest
from ocs_ci.ocs.resources import pod
from ocs_ci.utility import aws
from tests.sanity_helpers import Sanity
from ocs_ci.ocs.monitoring import (
    check_pvcdata_collected_on_prometheus,
    check_ceph_health_status_metrics_on_prometheus
)
from ocs_ci.ocs.node import wait_for_nodes_status, get_typed_nodes
from ocs_ci.utility.retry import retry


logger = logging.getLogger(__name__)


@pytest.fixture()
def test_fixture(pod_factory, num_of_pod=2):
    """
    Setup and teardown
    """
    pod_objs = [
        pod_factory(interface=constants.CEPHBLOCKPOOL,
                    status=constants.STATUS_RUNNING
                    ) for _ in range(num_of_pod)
    ]

    # Check for the created pvc metrics on prometheus pod
    for pod_obj in pod_objs:
        assert check_pvcdata_collected_on_prometheus(pod_obj.pvc.name), (
            f"On prometheus pod for created pvc {pod_obj.pvc.name} related data is not collected"
        )

    return pod_objs


@retry(AssertionError, tries=10, delay=3, backoff=1)
def wait_to_update_in_prometheus_pod():

    logger.info(
        f"Verifying ceph health status metrics is updated after rebooting the node"
    )
    ocp_obj = ocp.OCP(kind=constants.POD, namespace=defaults.ROOK_CLUSTER_NAMESPACE)
    mgr_pod = (
        ocp_obj.get(selector=constants.MGR_APP_LABEL).get('items')[0].get('metadata').get('name')
    )
    assert check_ceph_health_status_metrics_on_prometheus(mgr_pod=mgr_pod), (
        f"Ceph health status metrics are not updated after the rebooting node where the mgr running"
    )
    logger.info("Ceph health status metrics is updated")


@pytest.mark.polarion_id("OCS-710")
class TestRebootNodeWhereMgrRunningAndInteractionWithPrometheus(E2ETest):
    """
    Rebooting node where mgr is running shouldn't impact the data/metrics
    stored on persistent monitoring
    """

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance
        """
        self.sanity_helpers = Sanity()

    @tier4
    def test_monitoring_after_rebooting_node_where_mgr_is_running(self, test_fixture):
        """
        Test case to validate rebooting a node where mgr is running
        should not delete the data collected on prometheus pod
        """
        pod_objs = test_fixture

        aws_obj = aws.AWS()

        # Get the worker node list
        workers = get_typed_nodes(node_type='worker')

        # Get the mgr pod obj
        mgr_pod_obj = pod.get_mgr_pods()

        # Get the node where the mgr pod is hosted
        mgr_node = mgr_pod_obj[0].get().get('spec').get('nodeName')
        mgr_node = [node for node in workers if node.get().get('metadata').get('name') == mgr_node]

        # Reboot the node where the mgr pod is hosted
        instances = aws.get_instances_ids_and_names(mgr_node)
        aws_obj.restart_ec2_instances(instances=instances, wait=True, force=True)

        # Validate all nodes are in READY state
        wait_for_nodes_status()

        # Check the node are Ready state and check cluster is health ok
        self.sanity_helpers.health_check()

        # Check for ceph health check metrics is updated with new mgr pod
        wait_to_update_in_prometheus_pod()

        # Check for the created pvc metrics after rebooting the node where mgr pod was running
        for pod_obj in pod_objs:
            assert check_pvcdata_collected_on_prometheus(pod_obj.pvc.name), (
                f"On prometheus pod for created pvc {pod_obj.pvc.name} related data is not collected"
            )
