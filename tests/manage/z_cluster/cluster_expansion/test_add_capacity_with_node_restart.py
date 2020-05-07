import pytest
import logging
import time

from concurrent.futures import ThreadPoolExecutor
from ocs_ci.framework.pytest_customization.marks import polarion_id
from ocs_ci.framework.testlib import ignore_leftovers, ManageTest, tier4
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources import pod as pod_helpers
from ocs_ci.ocs.resources import storage_cluster
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.ocs.node import get_typed_nodes, wait_for_nodes_status
from tests.manage.z_cluster.cluster_expansion import temp_helper_file


@pytest.mark.parametrize(
    argnames=["node_type", "num_of_nodes"],
    argvalues=[
        pytest.param(
            *['worker', 1],
            marks=pytest.mark.polarion_id("OCS-1313")
        ),
     ]
)
@pytest.mark.parametrize(
    "workload_storageutilization_rbd",
    [(0.25, False, 120)],
    indirect=["workload_storageutilization_rbd"]
)
# @pytest.mark.parametrize(
#     "workload_storageutilization_cephfs",
#     [(0.05, False, 120)],
#     indirect=["workload_storageutilization_cephfs"]
# )
@ignore_leftovers
@tier4
@polarion_id('OCS-1313')
class TestAddCapacityNodeRestart(ManageTest):
    """
    Test add capacity when one of the nodes got restart
    in the middle of the process. Don't forget to include the flag '--cluster-name' when running the test
    """
    num_of_pvcs = 3
    pvc_size = 100

    def test_add_capacity_node_restart(
        self, nodes, multi_pvc_factory, pvc_factory, pod_factory, workload_storageutilization_rbd,
        node_type, num_of_nodes,
    ):
        """ test add capacity when one of the nodes got restart
        in the middle of the process
        """
        logging.info("Condition 1 to start the test is met: storageutilization is completed")
        used_capacity_after_utilizing = temp_helper_file.get_percent_used_capacity()
        logging.info(f"### used capacity after storageutilization = {used_capacity_after_utilizing}")

        seconds_to_wait_after_storageutilization = 60
        logging.info(f"wait {seconds_to_wait_after_storageutilization} seconds before starting the test")
        time.sleep(seconds_to_wait_after_storageutilization)

        assert temp_helper_file.check_pods_in_running_state(), \
            "Condition 2 to start test failed: one or more OCS pods are not in running state"

        node_list = get_typed_nodes(node_type=node_type, num_of_nodes=num_of_nodes)
        assert node_list, "Condition 3 to start test failed: No node to restart"

        osd_pods_before = pod_helpers.get_osd_pods()
        assert len(osd_pods_before) < 9, \
            "Condition 4 to start test failed: We have maximum of osd's in the cluster"
        logging.info("All start conditions are met!")

        total_space_before_expansion = temp_helper_file.get_total_space()
        logging.info('total space  before expansion = {}'.format(str(total_space_before_expansion)))

        logging.info("Perform some IO operations...")

        pvc_objs = multi_pvc_factory(
            size=self.pvc_size, num_of_pvc=self.num_of_pvcs
        )

        pod_objs = []

        with ThreadPoolExecutor(max_workers=self.num_of_pvcs) as executor:
            for pvc_obj in pvc_objs:
                executor.submit(pod_objs.append, (pod_factory(pvc=pvc_obj)))

        with ThreadPoolExecutor(max_workers=self.num_of_pvcs-1) as executor:
            for pod_obj in pod_objs:
                pod_io_task = executor.submit(
                    pod_obj.run_io, storage_type='fs', size=2, jobs=10, io_direction='wo', rate='200m')

        seconds_to_wait_for_io_operations = 90
        logging.info(f"Going to sleep for {seconds_to_wait_for_io_operations} seconds")
        time.sleep(seconds_to_wait_for_io_operations)
        used_capacity_after_io_operations = temp_helper_file.get_percent_used_capacity()
        logging.info(f"### used capacity after IO = {used_capacity_after_io_operations}")

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

        used_capacity_after_io_operations = temp_helper_file.get_percent_used_capacity()
        logging.info(f"### used capacity after node restart = {used_capacity_after_io_operations}")

        pod = OCP(
            kind=constants.POD, namespace=config.ENV_DATA['cluster_namespace']
        )
        pod.wait_for_resource(
            timeout=300,
            condition=constants.STATUS_RUNNING,
            selector='app=rook-ceph-osd',
            resource_count=result * 3
        )

        assert temp_helper_file.check_osd_pods_after_expansion(osd_pods_before), (
            "number of osd pods is not as expected"
        )
        assert temp_helper_file.check_total_space_after_expansion(total_space_before_expansion), (
            "Expected capacity mismatch"
        )
        assert temp_helper_file.check_osd_tree(), "Incorrect ceph osd tree format"

        logging.info("Finished verifying add capacity osd storage with node restart")
        logging.info("Waiting for ceph health check to finished...")
        ceph_health_check(
            namespace=config.ENV_DATA['cluster_namespace'], tries=60
        )
