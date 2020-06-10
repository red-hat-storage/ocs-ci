import pytest
import logging

from ocs_ci.framework.testlib import ignore_leftovers, ManageTest, tier4a
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources import pod as pod_helpers
from ocs_ci.ocs.resources import storage_cluster
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.ocs.node import get_typed_nodes


@pytest.mark.parametrize(
    argnames=["node_type", "num_of_nodes", "workload_storageutilization_rbd"],
    argvalues=[
        pytest.param(
            *[constants.WORKER_MACHINE, 1, (0.11, True, 120)],
            marks=pytest.mark.polarion_id("OCS-1313")
        ),
    ],
    indirect=["workload_storageutilization_rbd"],
)
@ignore_leftovers
@tier4a
class TestAddCapacityNodeRestart(ManageTest):
    """
    Test add capacity when one of the nodes got restart
    in the middle of the process.
    """

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

        node_list = get_typed_nodes(node_type=node_type, num_of_nodes=num_of_nodes, ocs_node=True)
        assert node_list, "Condition 2 to start test failed: No node to restart"

        max_osds = 15
        osd_pods_before = pod_helpers.get_osd_pods()
        assert len(osd_pods_before) < max_osds, (
            "Condition 3 to start test failed: We have maximum of osd's in the cluster")
        logging.info("All start conditions are met!")

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
        nodes.restart_nodes(nodes=node_list, wait=True, timeout=420)
        logging.info("Finished restarting the node list")

        # The exit criteria verification conditions here are not complete. When the branch
        # 'wip-add-capacity-e_e' will be merged into master I will use the functions from this branch.

        pod = OCP(
            kind=constants.POD, namespace=config.ENV_DATA['cluster_namespace']
        )
        pod.wait_for_resource(
            timeout=600,
            condition=constants.STATUS_RUNNING,
            selector='app=rook-ceph-osd',
            resource_count=result * 3
        )

        logging.info("Finished verifying add capacity osd storage with node restart")
        logging.info("Waiting for ceph health check to finished...")
        ceph_health_check(
            namespace=config.ENV_DATA['cluster_namespace'], tries=90
        )
