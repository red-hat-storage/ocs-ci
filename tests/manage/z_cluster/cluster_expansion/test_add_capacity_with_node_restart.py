import pytest
import logging
import time

from concurrent.futures import ThreadPoolExecutor
from ocs_ci.framework.pytest_customization.marks import polarion_id
from ocs_ci.framework.testlib import ignore_leftovers, ManageTest, tier4
from ocs_ci.framework import config
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources import pod as pod_helpers
from ocs_ci.ocs.resources import storage_cluster
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.ocs.node import get_typed_nodes, wait_for_nodes_status


# The functions below I took from the branch 'wip-add-capacity-e_e'.
# When this branch will be merged into master I will use the functions from this branch.

def get_percent_used_capacity():
    """
    Function to calculate the percentage of used capacity in a cluster

    Returns:
        float: The percentage of the used capacity in the cluster
    """
    ct_pod = pod_helpers.get_ceph_tools_pod()
    output = ct_pod.exec_ceph_cmd(ceph_cmd='ceph df')
    total_used = (output.get('stats').get('total_used_raw_bytes'))
    total_avail = (output.get('stats').get('total_bytes'))
    return 100.0 * total_used / total_avail


def check_pods_in_running_state(namespace=defaults.ROOK_CLUSTER_NAMESPACE):
    """
    checks whether all the pods in a given namespace are in Running state or not
    Returns:
        Boolean: True, if all pods in Running state. False, otherwise
    """
    ret_val = True
    list_of_pods = pod_helpers.get_all_pods(namespace)
    ocp_pod_obj = OCP(kind=constants.POD, namespace=namespace)
    for p in list_of_pods:
        # we don't want to compare osd-prepare and canary pods as they get created freshly when an osd need to be added.
        if "rook-ceph-osd-prepare" not in p.name and "rook-ceph-drain-canary" not in p.name:
            status = ocp_pod_obj.get_resource(p.name, 'STATUS')
            if status not in "Running":
                logging.error(f"The pod {p.name} is in {status} state. Expected = Running")
                ret_val = False
    return ret_val


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
        used_capacity_after_utilizing = get_percent_used_capacity()
        logging.info(f"### used capacity after storageutilization = {used_capacity_after_utilizing}")

        seconds_to_wait_after_storageutilization = 60
        logging.info(f"wait {seconds_to_wait_after_storageutilization} seconds before starting the test")
        time.sleep(seconds_to_wait_after_storageutilization)

        assert check_pods_in_running_state(), (
            "Condition 2 to start test failed: one or more OCS pods are not in running state")

        node_list = get_typed_nodes(node_type=node_type, num_of_nodes=num_of_nodes)
        assert node_list, "Condition 3 to start test failed: No node to restart"

        osd_pods_before = pod_helpers.get_osd_pods()
        assert len(osd_pods_before) < 9, (
            "Condition 4 to start test failed: We have maximum of osd's in the cluster")
        logging.info("All start conditions are met!")

        logging.info("Perform some IO operations...")
        # The IOs here are not complete. When the branch 'wip-add-capacity-e_e' will be merged into master
        # I will use the functions from this branch.

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

        seconds_to_wait_for_io_operations = 60
        logging.info(f"Going to sleep for {seconds_to_wait_for_io_operations} seconds")
        time.sleep(seconds_to_wait_for_io_operations)
        used_capacity_after_io_operations = get_percent_used_capacity()
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

        used_capacity_after_io_operations = get_percent_used_capacity()
        logging.info(f"### used capacity after node restart = {used_capacity_after_io_operations}")

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
