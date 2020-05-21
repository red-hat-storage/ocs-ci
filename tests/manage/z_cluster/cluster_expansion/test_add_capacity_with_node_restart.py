import pytest
import logging
import time

from concurrent.futures import ThreadPoolExecutor
from ocs_ci.framework.testlib import ignore_leftovers, ManageTest, tier4
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources import pod as pod_helpers
from ocs_ci.ocs.resources import storage_cluster
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.ocs.node import get_typed_nodes, wait_for_nodes_status


@pytest.mark.parametrize(
    argnames=["node_type", "num_of_nodes", "workload_storageutilization_rbd"],
    argvalues=[
        pytest.param(
            *['worker', 1, (0.11, True, 120)],
            marks=pytest.mark.polarion_id("OCS-1313")
        ),
    ],
    indirect=["workload_storageutilization_rbd"],
)
@ignore_leftovers
@tier4
class TestAddCapacityNodeRestart(ManageTest):
    """
    Test add capacity when one of the nodes got restart
    in the middle of the process. Don't forget to include the flag '--cluster-name' when running the test
    """
    num_of_pvcs = 3
    pvc_size = 100

    def test_add_capacity_node_restart(
        self, nodes, multi_pvc_factory, pod_factory, workload_storageutilization_rbd,
        node_type, num_of_nodes,
    ):
        """
        test add capacity when one of the nodes got restart in the middle of the process
        """
        logging.info("Condition 1 to start the test is met: storageutilization is completed")
        # Please notice: When the branch 'wip-add-capacity-e_e' will be merged into master
        # the test will include more much data both before, and after calling 'add_capacity'function.

        node_list = get_typed_nodes(node_type=node_type, num_of_nodes=num_of_nodes)
        assert node_list, "Condition 2 to start test failed: No node to restart"

        max_osds = 12
        osd_pods_before = pod_helpers.get_osd_pods()
        assert len(osd_pods_before) < max_osds, (
            "Condition 3 to start test failed: We have maximum of osd's in the cluster")
        logging.info("All start conditions are met!")

        logging.info("Perform some IO operations in the background...")
        # The IOs here are not complete. When the branch 'wip-add-capacity-e_e' will be merged into master
        # I will use the functions from this branch.

        pvc_objs = multi_pvc_factory(
            size=self.pvc_size, num_of_pvc=self.num_of_pvcs
        )

        pod_objs = []

        with ThreadPoolExecutor(max_workers=self.num_of_pvcs) as executor:
            for pvc_obj in pvc_objs:
                executor.submit(pod_objs.append, (pod_factory(pvc=pvc_obj)))

        with ThreadPoolExecutor(max_workers=self.num_of_pvcs - 1) as executor:
            for pod_obj in pod_objs:
                executor.submit(
                    pod_obj.run_io, storage_type='fs', size=2, jobs=10, io_direction='wo', rate='200m')

        seconds_to_wait_for_io_operations = 40
        logging.info(f"Going to sleep for {seconds_to_wait_for_io_operations} seconds")
        time.sleep(seconds_to_wait_for_io_operations)

        osd_size = storage_cluster.get_osd_size()
        logging.info("Calling add_capacity function...")
        result = storage_cluster.add_capacity(osd_size)
        if result:
            logging.info("add capacity finished successfully")
        else:
            logging.info("add capacity failed")

        # Restart nodes while additional storage is being added
        logging.info("Restart nodes:")
        logging.info([n.name for n in node_list])
        nodes.restart_nodes(nodes=node_list)
        wait_for_nodes_status(node_names=[node.name for node in node_list])
        logging.info("Finished restarting the node list")

        # The exit criteria verification conditions here are not complete. When the branch
        # 'wip-add-capacity-e_e' will be merged into master I will use the functions from this branch.

        pod = OCP(
            kind=constants.POD, namespace=config.ENV_DATA['cluster_namespace']
        )
        pod.wait_for_resource(
            timeout=300,
            condition=constants.STATUS_RUNNING,
            selector='app=rook-ceph-osd',
            resource_count=result * 3
        )

        logging.info("Finished verifying add capacity osd storage with node restart")
        logging.info("Waiting for ceph health check to finished...")
        ceph_health_check(
            namespace=config.ENV_DATA['cluster_namespace'], tries=60
        )
