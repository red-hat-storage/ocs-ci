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
        Make sure all nodes are up again

        """
        def finalizer():
            nodes.restart_nodes_teardown()

        request.addfinalizer(finalizer)

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance
        """
        self.sanity_helpers = Sanity()

    @pytest.mark.polarion_id("OCS-1628")
    def test_create_delete_pvc_parallel(self, project_factory, storageclass_factory, dc_pod_factory, nodes):
        """
        PV provisioning with one worker down

        """

        # Get worker nodes
        worker_node_list = get_worker_nodes()
        log.info(f"Current available worker nodes are {worker_node_list}")

        mgr_pod_obj = pod.get_mgr_pods()
        log.info(mgr_pod_obj[0].name)
        mgr_node_name = pod.get_pod_node(mgr_pod_obj[0]).name
        worker_node_list.remove(mgr_node_name)
        selected_worker_node = random.choice(worker_node_list)
        log.info(f"Stopping Node {selected_worker_node}")
        selected_worker_node_obj = node.get_node_objs(selected_worker_node)
        log.info(selected_worker_node_obj[0].name)
        nodes.stop_nodes(selected_worker_node_obj)
        toolbox_pod_obj = pod.get_ceph_tools_pod()
        toolbox_node_name = pod.get_pod_node(toolbox_pod_obj).name
        if selected_worker_node == toolbox_node_name:
            helpers.wait_for_ct_pod_recovery()
        project_obj = project_factory()
        rbd_sc_obj = helpers.default_storage_class(interface_type=constants.CEPHBLOCKPOOL)
        cephfs_sc_obj = helpers.default_storage_class(interface_type=constants.CEPHFILESYSTEM)
        all_pvc_obj, dc_pod_obj = list()
        log.info("Creating pvc in parallel")
        rbd_pvcs_obj = helpers.create_multiple_pvc_parallel(
            sc_obj=rbd_sc_obj, namespace=project_obj.namespace, number_of_pvc=25, size="10Gi",
            access_modes=[constants.ACCESS_MODE_RWO, constants.ACCESS_MODE_RWX]
        )
        cephfs_pvcs_obj = helpers.create_multiple_pvc_parallel(
            sc_obj=cephfs_sc_obj, namespace=project_obj.namespace, number_of_pvc=25, size="10Gi",
            access_modes=[constants.ACCESS_MODE_RWO, constants.ACCESS_MODE_RWX]
        )
        log.info("Creating dc pod backed with rbd pvc and running io")

        for dc_pod_count in range(2):
            rbd_dc_pod = dc_pod_factory(interface=constants.CEPHBLOCKPOOL, size=5)
            rbd_dc_pod.run_io(storage_type='fs', size='2G')
            dc_pod_obj.append(rbd_dc_pod)
        log.info("Creating dc pod backed with cephfs pvc and running io")
        for dc_pod_count in range(2):
            cephfs_dc_pod = dc_pod_factory(interface=constants.CEPHFILESYSTEM, size=5)
            cephfs_dc_pod.run_io(storage_type='fs', size='2G')
            dc_pod_obj.append(cephfs_dc_pod)

        all_pvc_obj.extend(rbd_pvcs_obj + cephfs_pvcs_obj)
        log.info("Deleting pvc in parallel")
        assert helpers.delete_objs_parallel(all_pvc_obj)

        for dc_pod_obj in dc_pod_obj:
            pod.get_fio_rw_iops(dc_pod_obj)
        log.info(f"Starting Node {selected_worker_node}")

        nodes.start_nodes(selected_worker_node_obj)
        log.info("Verifying All resources are Running and matches expected result")
        self.sanity_helpers.health_check(tries=60)
