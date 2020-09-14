import logging
import pytest
import random

from concurrent.futures import ThreadPoolExecutor
from ocs_ci.framework import config
from ocs_ci.framework.testlib import ManageTest, tier1, acceptance
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pod
from tests import helpers

logger = logging.getLogger(__name__)


class TestPvcAssignPodNode(ManageTest):
    """
    Automates the following test cases:
    OCS-717 - RBD: Assign nodeName to a POD using RWO PVC
    OCS-744 - CephFS: Assign nodeName to a POD using RWO PVC
    OCS-1258 - CephFS: Assign nodeName to a POD using RWX PVC
    OCS-1257 - RBD: Assign nodeName to a POD using RWX PVC
    """

    @acceptance
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
            interface_type=interface, pvc_name=pvc_obj.name,
            namespace=pvc_obj.namespace, node_name=selected_node,
            pod_dict_path=constants.NGINX_POD_YAML
        )
        teardown_factory(pod_obj)

        # Confirm that the pod is running on the selected_node
        helpers.wait_for_resource_state(
            resource=pod_obj, state=constants.STATUS_RUNNING, timeout=120
        )
        pod_obj.reload()
        assert pod.verify_node_name(pod_obj, selected_node), (
            'Pod is running on a different node than the selected node'
        )

        # Run IO
        logger.info(f"Running IO on pod {pod_obj.name}")
        pod_obj.run_io(storage_type='fs', size='512M', runtime=30)
        pod.get_fio_rw_iops(pod_obj)

    @acceptance
    @tier1
    @pytest.mark.skipif(
        config.ENV_DATA['platform'].lower() == 'ibm_cloud',
        reason=(
            "Skipping tests on IBM Cloud due to bug 1871314 "
            "https://bugzilla.redhat.com/show_bug.cgi?id=1871314"
        )
    )
    @pytest.mark.parametrize(
        argnames=["interface"],
        argvalues=[
            pytest.param(
                *[constants.CEPHBLOCKPOOL],
                marks=pytest.mark.polarion_id("OCS-1257")
            ),
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
        if interface == constants.CEPHBLOCKPOOL:
            volume_mode = 'Block'
            storage_type = 'block'
            block_pv = True
            pod_yaml = constants.CSI_RBD_RAW_BLOCK_POD_YAML
        else:
            volume_mode = ''
            storage_type = 'fs'
            block_pv = False
            pod_yaml = ''

        # Create a RWX PVC
        pvc_obj = pvc_factory(
            interface=interface, access_mode=constants.ACCESS_MODE_RWX,
            status=constants.STATUS_BOUND, volume_mode=volume_mode
        )

        # Create two pods on selected nodes
        pod_list = []
        selected_nodes = random.sample(worker_nodes_list, k=2)
        logger.info(
            f"Creating {len(selected_nodes)} pods with pvc {pvc_obj.name}"
        )
        for node in selected_nodes:
            logger.info(f"Creating pod on node: {node}")
            pod_obj = helpers.create_pod(
                interface_type=interface, pvc_name=pvc_obj.name,
                namespace=pvc_obj.namespace, node_name=node,
                pod_dict_path=pod_yaml, raw_block_pv=block_pv,
            )
            pod_list.append(pod_obj)
            teardown_factory(pod_obj)

        # Confirm that both pods are running on the selected_nodes
        logger.info('Checking whether pods are running on the selected nodes')
        for index in range(0, len(selected_nodes)):
            pod_obj = pod_list[index]
            selected_node = selected_nodes[index]
            helpers.wait_for_resource_state(
                resource=pod_obj, state=constants.STATUS_RUNNING,
                timeout=120
            )
            pod_obj.reload()
            assert pod.verify_node_name(pod_obj, selected_node), (
                f"Pod {pod_obj.name} is running on a different node "
                f"than the selected node"
            )

        # Run IOs on all pods. FIO Filename is kept same as pod name
        with ThreadPoolExecutor() as p:
            for pod_obj in pod_list:
                logger.info(f"Running IO on pod {pod_obj.name}")
                p.submit(
                    pod_obj.run_io, storage_type=storage_type, size='512M',
                    runtime=30, fio_filename=pod_obj.name
                )

        # Check IO from all pods
        for pod_obj in pod_list:
            pod.get_fio_rw_iops(pod_obj)
