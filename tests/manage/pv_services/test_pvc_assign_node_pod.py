import logging
import pytest

from ocs_ci.framework.testlib import ManageTest, tier1
from ocs_ci.ocs import constants
from tests import helpers

logger = logging.getLogger(__name__)


@pytest.fixture()
def resources(request):
    """
    Deletes the pod resources created during the test

    Returns:
        list: empty list of pod
    """
    pods = []

    def finalizer():
        for pod in pods:
            pod.delete()
            pod.ocp.wait_for_delete(pod.name)

    request.addfinalizer(finalizer)
    return pods


class TestNodeAssign(ManageTest):
    """
    Automates the following test cases:
    OCS-717 - RBD: Assign nodeName to a POD
    OCS-744 - CephFS: Assign nodeName to a POD
    """

    @tier1
    @pytest.mark.parametrize(
        argnames=["interface"],
        argvalues=[
            pytest.param(
                *[constants.CEPHBLOCKPOOL],
                marks=pytest.mark.polarion_id("OCS-717")
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM],
                marks=pytest.mark.polarion_id("OCS-744")
            )
        ]
    )
    def test_pvc_assign_node_pod(self, interface, pvc_factory, resources):
        """
        Test assign nodeName to a pod using RWO pvc
        """
        pods = resources
        worker_nodes_list = helpers.get_worker_nodes()

        # Create a RWO PVC
        pvc_obj = pvc_factory(
            interface=interface, status=constants.STATUS_BOUND
        )

        # Create a Pod with specified nodeName
        logger.info(
            f"Creating a pod on node: {worker_nodes_list[0]}"
            f" with pvc {pvc_obj.name}"
        )
        pod_obj = helpers.create_pod(
            interface_type=interface, pvc_name=pvc_obj.name, wait=False,
            namespace=pvc_obj.namespace, node_name=worker_nodes_list[0],
            pod_dict_path=constants.NGINX_POD_YAML
        )
        pods.append(pod_obj)
        assert helpers.wait_for_resource_state(
            resource=pod_obj, state=constants.STATUS_RUNNING, timeout=120
        )

        # Reload the new information and verify the nodeName
        pod_obj.reload()
        assert worker_nodes_list[0] == pod_obj.pod_data.get('spec').get('nodeName')
