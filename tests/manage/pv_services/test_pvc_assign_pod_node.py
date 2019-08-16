import logging
import pytest
import random

from ocs_ci.framework.testlib import ManageTest, tier1
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pod
from tests import helpers

logger = logging.getLogger(__name__)


def verify_pod_node(pod_obj, node_name):
    """
    Verifies that the pod is running on a particular node

    Args:
        pod_obj (Pod): The pod object
        node_name (str): The name of node to check

    Returns:
        bool: True if the pod is running on a particular node, False otherwise
    """

    logger.info(f"Checking whether the pod is running on node: {node_name}")
    actual_node = pod_obj.get().get('spec').get('nodeName')
    logger.info(f"The pod is running on node: {actual_node}")
    if actual_node == node_name:
        return True
    else:
        return False


class TestPvcAssignPodNode(ManageTest):
    """
    Automates the following test cases:
    OCS-717 - RBD: Assign nodeName to a POD using RWO PVC
    OCS-744 - CephFS: Assign nodeName to a POD using RWO PVC
    OCS-1258 - CephFS: Assign nodeName to a POD using RWX PVC
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
    def test_rwo_pvc_assign_pod_node(
        self, interface, pvc_factory, teardown_factory
    ):
        """
        Test assign nodeName to a pod using RWO pvc
        """
        worker_nodes_list = helpers.get_worker_nodes()

        # Create a RWO PVC
        pvc_obj = pvc_factory(
            interface=interface, access_mode=constants.ACCESS_MODE_RWO,
            status=constants.STATUS_BOUND
        )

        # Create a pod on a particular node
        selected_node = random.choice(worker_nodes_list)
        logger.info(
            f"Creating a pod on node: {selected_node} with pvc {pvc_obj.name}"
        )

        pod_obj = helpers.create_pod(
            interface_type=interface, pvc_name=pvc_obj.name, wait=False,
            namespace=pvc_obj.namespace, node_name=selected_node,
            pod_dict_path=constants.NGINX_POD_YAML
        )
        teardown_factory(pod_obj)

        # Confirm that the pod is running on the selected_node
        assert helpers.wait_for_resource_state(
            resource=pod_obj, state=constants.STATUS_RUNNING, timeout=120
        )
        assert verify_pod_node(pod_obj, selected_node), (
            'Pod is running on a different node than the selected node'
        )

        # Run IO
        logger.info(f"Running IO on pod {pod_obj.name}")
        pod_obj.run_io(storage_type='fs', size='512M', runtime=30)
        pod.get_fio_rw_iops(pod_obj)

    @tier1
    @pytest.mark.parametrize(
        argnames=["interface"],
        argvalues=[
            pytest.param(
                *[constants.CEPHFILESYSTEM],
                marks=pytest.mark.polarion_id("OCS-1258")
            )
        ]
    )
    def test_rwx_pvc_assign_pod_node(
        self, interface, pvc_factory, teardown_factory
    ):
        """
        Test assign nodeName to a pod using RWX pvc
        """
        worker_nodes_list = helpers.get_worker_nodes()

        # Create a RWX PVC
        pvc_obj = pvc_factory(
            interface=interface, access_mode=constants.ACCESS_MODE_RWX,
            status=constants.STATUS_BOUND
        )

        # Create two pods on selected nodes
        selected_nodes = random.sample(worker_nodes_list, k=2)

        logger.info(
            f"Creating first pod on node: {selected_nodes[0]} "
            f"with pvc {pvc_obj.name}"
        )
        pod_obj1 = helpers.create_pod(
            interface_type=interface, pvc_name=pvc_obj.name, wait=False,
            namespace=pvc_obj.namespace, node_name=selected_nodes[0],
            pod_dict_path=constants.NGINX_POD_YAML
        )
        teardown_factory(pod_obj1)

        logger.info(
            f"Creating second pod on node: {selected_nodes[1]} "
            f"with pvc {pvc_obj.name}"
        )
        pod_obj2 = helpers.create_pod(
            interface_type=interface, pvc_name=pvc_obj.name, wait=False,
            namespace=pvc_obj.namespace, node_name=selected_nodes[1],
            pod_dict_path=constants.NGINX_POD_YAML
        )
        teardown_factory(pod_obj2)

        # Confirm that both pods are running on the selected_nodes
        assert helpers.wait_for_resource_state(
            resource=pod_obj1, state=constants.STATUS_RUNNING, timeout=120
        )
        assert verify_pod_node(pod_obj1, selected_nodes[0]), (
            'First Pod is running on a different node than the selected node'
        )

        assert helpers.wait_for_resource_state(
            resource=pod_obj2, state=constants.STATUS_RUNNING, timeout=120
        )
        assert verify_pod_node(pod_obj2, selected_nodes[1]), (
            'Second Pod is running on a different node than the selected node'
        )

        # Run IOs
        logger.info(f"Running IO on first pod {pod_obj1.name}")
        pod_obj1.run_io(storage_type='fs', size='512M', runtime=30)
        logger.info(f"Running IO on second pod {pod_obj2.name}")
        pod_obj2.run_io(storage_type='fs', size='512M', runtime=30)

        pod.get_fio_rw_iops(pod_obj1)
        pod.get_fio_rw_iops(pod_obj2)
