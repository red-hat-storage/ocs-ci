import logging
import random
import pytest

from ocs_ci.ocs.resources import pod

from ocs_ci.ocs import node
from tests import helpers
from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import ManageTest, tier4, tier4a
from tests.helpers import get_worker_nodes
from tests.sanity_helpers import Sanity

log = logging.getLogger(__name__)


@tier4
@tier4a
class TestCreatePvcWithWorkerNodeDown(ManageTest):

    @pytest.fixture(autouse=True)
    def teardown(self, request, nodes):
        """
        Restart nodes that are in status NotReady, for situations in
        which the test failed before restarting the node,
        which leaves nodes in NotReady

        """

        def finalizer():
            not_ready_nodes = [
                n for n in node.get_node_objs() if n
                .ocp.get_resource_status(n.name) == constants.NODE_NOT_READY
            ]
            log.warning(
                f"Nodes in NotReady status found: {[n.name for n in not_ready_nodes]}"
            )
            if not_ready_nodes:
                nodes.restart_nodes(not_ready_nodes)
                node.wait_for_nodes_status()

        request.addfinalizer(finalizer)

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance
        """
        self.sanity_helpers = Sanity()

    @pytest.mark.polarion_id("OCS-1628")
    def test_create_delete_pvc_parallel(self, project_factory, pvc_factory, pod_factory, nodes):
        """
        PV provisioning with one worker down

        """

        # Get worker nodes
        worker_node_list = get_worker_nodes()
        log.info(f"Current available worker nodes are {worker_node_list}")
        mgr_pod_obj = pod.get_mgr_pods()
        worker_node_with_mgr_pod = pod.get_pod_node(mgr_pod_obj)
        selected_worker_node = random.choice(worker_node_list)
        log.info(f"Stopping Node {selected_worker_node}")
        selected_worker_node_obj = node.get_node_objs(selected_worker_node)
        log.info(selected_worker_node_obj[0].name)
        nodes.stop_nodes(selected_worker_node_obj)
        if selected_worker_node == worker_node_with_mgr_pod:
            log.info("Selected Worker node has mgr pod running")
            log.info("Deleting mgr pod to start mgr on other node")
            pod.delete_pods([mgr_pod_obj])
            pod.validate_pods_are_respinned_and_running_state([mgr_pod_obj])
        log.log("Checking if toolbox pod is accessable after stopping node")
        toolbox_pod_obj = pod.get_ceph_tools_pod()
        toolbox_node_name = pod.get_pod_node(toolbox_pod_obj).name
        if selected_worker_node == toolbox_node_name:
            helpers.wait_for_ct_pod_recovery()
        project_obj = project_factory()
        rbd_sc_obj = helpers.default_storage_class(interface_type=constants.CEPHBLOCKPOOL)
        cephfs_sc_obj = helpers.default_storage_class(interface_type=constants.CEPHFILESYSTEM)
        all_pvc_obj = list()
        log.info("Creating pvc in parallel")
        rbd_pvcs_obj = helpers.create_multiple_pvc_parallel(
            sc_obj=rbd_sc_obj, namespace=project_obj.namespace, number_of_pvc=25, size="4Gi",
            access_modes=[constants.ACCESS_MODE_RWO, constants.ACCESS_MODE_RWX]
        )
        cephfs_pvcs_obj = helpers.create_multiple_pvc_parallel(
            sc_obj=cephfs_sc_obj, namespace=project_obj.namespace, number_of_pvc=25, size="4Gi",
            access_modes=[constants.ACCESS_MODE_RWO, constants.ACCESS_MODE_RWX]
        )
        log.info("Creating resources to make sure everything works fine")

        self.sanity_helpers.create_resources(pvc_factory, pod_factory)

        all_pvc_obj.extend(rbd_pvcs_obj + cephfs_pvcs_obj)
        log.info("Deleting pvc in parallel")
        assert helpers.delete_objs_parallel(all_pvc_obj)

        log.info(f"Starting Node {selected_worker_node}")

        nodes.start_nodes(selected_worker_node_obj)
        log.info("Verifying All resources are Running and matches expected result")
        self.sanity_helpers.health_check(tries=60)
