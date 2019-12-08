import logging
import pytest

from ocs_ci.ocs import ocp, constants, defaults
from ocs_ci.framework.testlib import workloads, E2ETest, bugzilla
from ocs_ci.ocs.resources import pvc, pod
from tests import helpers
from ocs_ci.ocs.monitoring import check_pvcdata_collected_on_prometheus
from ocs_ci.utility import aws
from ocs_ci.ocs.node import wait_for_nodes_status, get_typed_nodes
from tests.helpers import wait_for_resource_state
from tests.sanity_helpers import Sanity

logger = logging.getLogger(__name__)


@pytest.fixture()
def test_fixture(request, storageclass_factory):
    """
    Setup and teardown
    """

    def teardown():

        # Delete created app pods and pvcs
        assert pod.delete_pods(pod_objs)
        assert pvc.delete_pvcs(pvc_objs)

        # Switch to default project
        ret = ocp.switch_to_default_rook_cluster_project()
        assert ret, 'Failed to switch to default rook cluster project'

        # Delete created projects
        for prj in namespace_list:
            prj.delete(resource_name=prj.namespace)

        # Validate all nodes are in READY state
        wait_for_nodes_status()

    request.addfinalizer(teardown)

    # Create a storage class
    sc = storageclass_factory()

    # Create projects
    namespace_list = helpers.create_multilpe_projects(number_of_project=1)

    # Create pvcs
    pvc_objs = [helpers.create_pvc(
        sc_name=sc.name, namespace=each_namespace.namespace
    ) for each_namespace in namespace_list]
    for pvc_obj in pvc_objs:
        helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND)
        pvc_obj.reload()

    # Create app pods
    pod_objs = [helpers.create_pod(
        interface_type=constants.CEPHBLOCKPOOL,
        pvc_name=each_pvc.name, namespace=each_pvc.namespace
    ) for each_pvc in pvc_objs]
    for pod_obj in pod_objs:
        helpers.wait_for_resource_state(pod_obj, constants.STATUS_RUNNING)
        pod_obj.reload()

    # Check for the created pvc metrics on prometheus pod
    for pvc_obj in pvc_objs:
        assert check_pvcdata_collected_on_prometheus(pvc_obj.name), (
            f"On prometheus pod for created pvc {pvc_obj.name} related data is not collected"
        )

    return namespace_list, pvc_objs, pod_objs, sc


@bugzilla('1751657')
@pytest.mark.polarion_id("OCS-606")
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

    @workloads
    def test_monitoring_when_one_of_the_prometheus_node_down(self, test_fixture):
        """
        Test case to validate when the prometheus pod is down and
        interaction with prometheus
        """
        namespace_list, pvc_objs, pod_objs, sc = test_fixture

        aws_obj = aws.AWS()

        # Get all the openshift-monitoring pods
        monitoring_pod_obj_list = pod.get_all_pods(namespace=defaults.OCS_MONITORING_NAMESPACE)

        # Get the worker node list
        workers = get_typed_nodes(node_type='worker')

        # Get all prometheus pods
        pod_obj_list = pod.get_all_pods(namespace=defaults.OCS_MONITORING_NAMESPACE, selector=['prometheus'])

        for pod_obj in pod_obj_list:

            # Get the node where the prometheus pod is hosted
            prometheus_pod_obj = pod_obj.get()
            prometheus_node = prometheus_pod_obj['spec']['nodeName']

            prometheus_node = [node for node in workers if node.get().get('metadata').get('name') == prometheus_node]

            # Make one of the node down where the prometheus pod is hosted
            instances = aws.get_instances_ids_and_names(prometheus_node)
            aws_obj.restart_ec2_instances(instances=instances, wait=True, force=True)

            # Validate all nodes are in READY state
            wait_for_nodes_status()

        # Check the node are Ready state and check cluster is health ok
        self.sanity_helpers.health_check()

        # Check all the monitoring pods are up
        for pod_obj in monitoring_pod_obj_list:
            wait_for_resource_state(resource=pod_obj, state=constants.STATUS_RUNNING)

        # Check for the created pvc metrics after nodes restarting
        for pvc_obj in pvc_objs:
            assert check_pvcdata_collected_on_prometheus(pvc_obj.name), (
                f"On prometheus pod for created pvc {pvc_obj.name} related data is not collected"
            )

        # Create projects after restarting nodes
        namespaces = helpers.create_multilpe_projects(number_of_project=1)
        namespace_list.extend(namespaces)

        # Create pvcs after restarting nodes
        pvcs = [helpers.create_pvc(
            sc_name=sc.name, namespace=each_namespace.namespace
        ) for each_namespace in namespaces]
        for pvc_obj in pvcs:
            helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND)
            pvc_obj.reload()
        pvc_objs.extend(pvcs)

        # Create app pods after restarting nodes
        pods = [helpers.create_pod(
            interface_type=constants.CEPHBLOCKPOOL,
            pvc_name=each_pvc.name, namespace=each_pvc.namespace
        ) for each_pvc in pvcs]
        for pod_obj in pods:
            helpers.wait_for_resource_state(pod_obj, constants.STATUS_RUNNING)
            pod_obj.reload()
        pod_objs.extend(pods)

        # Check for the created pvc metrics on prometheus pod after restarting nodes
        for pvc_obj in pvcs:
            assert check_pvcdata_collected_on_prometheus(pvc_obj.name), (
                f"On prometheus pod for created pvc {pvc_obj.name} related data is not collected"
            )
