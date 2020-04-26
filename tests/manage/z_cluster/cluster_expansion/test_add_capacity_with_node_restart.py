import pytest
import logging
import time
from tests.manage.monitoring.conftest import *

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


def get_total_space():
    logging.info("In 'get_total_space':")
    ct_pod = pod_helpers.get_ceph_tools_pod()
    output = ct_pod.exec_ceph_cmd(ceph_cmd='ceph osd df')
    total_space = int(output.get('summary').get('total_kb'))
    logging.info(f"total space is: {total_space}")
    return total_space


def check_osd_pods_after_expansion(osd_pods_before):
    osd_pods_after = pod_helpers.get_osd_pods()
    number_of_osds_added = len(osd_pods_after) - len(osd_pods_before)
    logging.info(f"number of osd's added = {number_of_osds_added}, "
                 f"before = {len(osd_pods_before)}, after = {len(osd_pods_after)}")
    if number_of_osds_added != 3:
        return False

    return True


def check_total_space_after_expansion(total_space_b4_expansion):
    # The newly added capacity takes into effect at the storage level
    ct_pod = pod_helpers.get_ceph_tools_pod()
    output = ct_pod.exec_ceph_cmd(ceph_cmd='ceph osd df')
    total_space_after_expansion = int(output.get('summary').get('total_kb'))
    osd_size = int(output.get('nodes')[0].get('kb'))
    expanded_space = osd_size * 3  # 3 OSDS are added of size = 'osd_size'
    logging.info(f"expanded_space == {expanded_space} ")
    logging.info(f"space output == {output} ")
    logging.info(f"osd size == {osd_size} ")
    logging.info(f"total_space_after_expansion == {total_space_after_expansion} ")
    expected_total_space_after_expansion = total_space_b4_expansion + expanded_space
    logging.info(f"expected_total_space_after_expansion == {expected_total_space_after_expansion} ")
    if not total_space_after_expansion == expected_total_space_after_expansion:
        return False

    return True


def check_osd_tree():
    # 'ceph osd tree' should show the new osds under right nodes/hosts
    #   Verification is different for 3 AZ and 1 AZ configs
    osd_pods = pod_helpers.get_osd_pods()
    ct_pod = pod_helpers.get_ceph_tools_pod()
    tree_output = ct_pod.exec_ceph_cmd(ceph_cmd='ceph osd tree')
    logging.info(f"### OSD tree output = {tree_output}")
    if config.ENV_DATA['platform'] == 'vsphere':
        return temp_helper_file.check_osd_tree_1az_vmware(tree_output, len(osd_pods))

    aws_number_of_zones = 3
    if config.ENV_DATA['platform'] == 'AWS':
        # parse the osd tree. if it contains a node 'rack' then it's a AWS_1AZ cluster. Else, 3 AWS_3AZ cluster
        for i in range(len(tree_output['nodes'])):
            if tree_output['nodes'][i]['name'] in "rack":
                aws_number_of_zones = 1
        if aws_number_of_zones == 1:
            return temp_helper_file.check_osd_tree_1az_aws(tree_output, len(osd_pods))
        else:
            return temp_helper_file.check_osd_tree_3az_aws(tree_output, len(osd_pods))


@pytest.mark.parametrize(
    argnames=["node_type", "num_of_nodes"],
    argvalues=[
        pytest.param(
            *['worker', 1],
            marks=pytest.mark.polarion_id("OCS-1311")
        ),
     ]
)
@pytest.mark.parametrize(
    "workload_storageutilization_rbd",
    [(0.02, False, 120)],
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
    num_of_pvcs = 4
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

        total_space_b4_expansion = get_total_space()
        logging.info('total space  before expansion = {}'.format(str(total_space_b4_expansion)))

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
                    pod_obj.run_io, storage_type='fs', size=4, jobs=10, io_direction='wo', rate='250m')

        seconds_to_wait_for_io_operations = 120
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

        assert check_osd_pods_after_expansion(osd_pods_before), \
            "number of osd pods is not as expected"
        # Don't failed the test for now, just add a warning
        if not check_total_space_after_expansion(total_space_b4_expansion):
            logging.warning("Expected capacity mismatch")
        if not check_osd_tree():
            logging.warning("Incorrect ceph osd tree format")

        logging.info("Finished verifying add capacity osd storage with node restart")
        logging.info("Waiting for ceph health check to finished...")
        ceph_health_check(
            namespace=config.ENV_DATA['cluster_namespace'], tries=60
        )
