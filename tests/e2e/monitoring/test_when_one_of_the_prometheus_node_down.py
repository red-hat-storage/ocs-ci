import logging

import pytest

from time import sleep

from ocs_ci.ocs import constants, ocp, defaults
from ocs_ci.framework.testlib import tier4, E2ETest
from ocs_ci.ocs.resources import pvc, pod
from tests.fixtures import (
    create_rbd_storageclass, create_ceph_block_pool,
    create_rbd_secret
)
from tests.helpers import create_pvc, create_pod, create_unique_resource_name
from ocs_ci.ocs.monitoring import (
    collected_metrics_for_created_pvc,
    get_kube_pod_spec_volumes_persistentvolumeclaims_info_metric
)
from ocs_ci.utility import aws
from tests.helpers import wait_for_resource_state

logger = logging.getLogger(__name__)


@pytest.fixture()
def test_fixture(request):
    """
    Setup and teardown
    """
    self = request.node.cls

    def finalizer():
        teardown(self)
    request.addfinalizer(finalizer)
    setup(self)


def setup(self):
    """
    Create project, pvc and an app pod
    """

    # Initializing
    self.namespace_list = []
    self.pvc_objs = []
    self.pod_objs = []

    assert create_multiple_project_and_pvc_and_check_metrics_are_collected(self)


def teardown(self):
    """
    Delete app pods and PVCs
    Delete project
    """
    # Delete created app pods and PVCs
    assert pod.delete_pods(self.pod_objs)
    assert pvc.delete_pvcs(self.pvc_objs)

    # Switch to default project
    ret = ocp.switch_to_default_rook_cluster_project()
    assert ret, 'Failed to switch to default rook cluster project'

    # Delete projects created
    for prj in self.namespace_list:
        prj_obj = ocp.OCP(kind='Project', namespace=prj)
        prj_obj.delete(resource_name=prj)


def create_multiple_project_and_pvc_and_check_metrics_are_collected(self):
    """
    Creates projects, pvcs and app pods
    """
    for i in range(5):
        # Create new project
        self.namespace = create_unique_resource_name('test', 'namespace')
        self.project_obj = ocp.OCP(kind='Project', namespace=self.namespace)
        assert self.project_obj.new_project(self.namespace), (
            f'Failed to create new project {self.namespace}'
        )
        # Create PVCs
        self.pvc_obj = create_pvc(
            sc_name=self.sc_obj.name, namespace=self.namespace
        )

        # Create pod
        self.pod_obj = create_pod(
            interface_type=constants.CEPHBLOCKPOOL,
            pvc_name=self.pvc_obj.name, namespace=self.namespace
        )

        self.namespace_list.append(self.namespace)
        self.pvc_objs.append(self.pvc_obj)
        self.pod_objs.append(self.pod_obj)

    # Check for the created pvc metrics is collected
    for pvc_obj in self.pvc_objs:
        assert collected_metrics_for_created_pvc(pvc_obj.name), (
            f"On prometheus pod for created pvc {pvc_obj.name} related data is not collected"
        )
    return True


def get_the_collected_metrics_for_pvcs_when_node_down():
    """
    Returns false if the metric/data are present
    """
    pvcs_data = get_kube_pod_spec_volumes_persistentvolumeclaims_info_metric()
    pvcs_list = pvcs_data['data']['result']
    if not pvcs_list:
        logger.info("When one of the node down where prometheus"
                    " hosted shouldn't be able to get the data/metrics")
        return True
    return False


@pytest.mark.usefixtures(
    create_rbd_secret.__name__,
    create_ceph_block_pool.__name__,
    create_rbd_storageclass.__name__,
    test_fixture.__name__
)
@pytest.mark.polarion_id("OCS-606")
class TestWhenOneOfThePrometheusNodeDown(E2ETest):
    """
    When the nodes are down, there should not be any functional impact
    on monitoring pods. All the data/metrics should be collected correctly.
    """
    @tier4
    def test_when_one_of_the_prometheus_node_down(self):
        """
        Test case to validate when the prometheus pod is down and
        interaction with prometheus
        """

        # Get the pod obj for of the prometheus pod, i.e prometheusk8s-0
        pod_obj = pod.get_pod_obj(name='prometheus-k8s-0', namespace=defaults.OCS_MONITORING_NAMESPACE)

        # Get the node where the prometheus pod is hosted
        prometheus_pod_obj = pod_obj.get()
        prometheus_node = prometheus_pod_obj['spec']['nodeName']

        # Get the node information
        nodes_obj = ocp.OCP(kind='node')
        nodes_list = nodes_obj.get()['items']
        instances = [node for node in nodes_list if node['metadata']['name'] == prometheus_node]

        # Make one of the node down where the prometheus pod is hosted
        aws_obj = aws.AWS()
        instance_dict = aws.get_instances_ids_and_names(instances)
        aws_obj.stop_ec2_instances(instances=instance_dict, wait=True)

        # Check for the created pvc metrics
        for pvc_obj in self.pvc_objs:
            assert collected_metrics_for_created_pvc(pvc_obj.name), (
                f"On prometheus pod for created pvc {pvc_obj.name} related data is not collected"
            )

        # Get all the openshift-monitoring pods
        monitoring_pod_obj_list = pod.get_all_pods(namespace=defaults.OCS_MONITORING_NAMESPACE)

        # Get all the openshift-storage pods
        ceph_pod_obj_list = pod.get_all_pods(namespace=defaults.OCS_MONITORING_NAMESPACE)

        # Make the node up which was down
        aws_obj.start_ec2_instances(instances=instance_dict, wait=True)

        wait_time = '60s'
        logging.info(f"Waiting for {wait_time} seconds")
        sleep(60)

        # Check all the monitoring pods are up
        for pod_obj in monitoring_pod_obj_list:
            assert wait_for_resource_state(resource=pod_obj, state=constants.STATUS_RUNNING)

        # Check all the openshift-storage pods are running
        for pod_obj in ceph_pod_obj_list:
            assert wait_for_resource_state(resource=pod_obj, state=constants.STATUS_RUNNING)

        # Once the node up, create new pvc and
        # also the pvc metrics which was created before the node down
        assert create_multiple_project_and_pvc_and_check_metrics_are_collected(self)
