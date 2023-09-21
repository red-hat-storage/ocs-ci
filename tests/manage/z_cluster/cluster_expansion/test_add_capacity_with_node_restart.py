import pytest
import logging

from ocs_ci.framework.pytest_customization.marks import brown_squad
from ocs_ci.framework.testlib import (
    ignore_leftovers,
    ManageTest,
    tier4b,
    skipif_managed_service,
)
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources import pod as pod_helpers
from ocs_ci.ocs.resources import storage_cluster
from ocs_ci.ocs.node import get_ocs_nodes, wait_for_nodes_status
from ocs_ci.ocs.resources.storage_cluster import osd_encryption_verification
from ocs_ci.ocs.cluster import (
    check_ceph_health_after_add_capacity,
    is_flexible_scaling_enabled,
    is_vsphere_ipi_cluster,
)

logger = logging.getLogger(__name__)


@brown_squad
@pytest.mark.parametrize(
    argnames=["num_of_nodes", "workload_storageutilization_rbd"],
    argvalues=[
        pytest.param(
            *[1, (0.11, True, 120)], marks=pytest.mark.polarion_id("OCS-1313")
        ),
    ],
    indirect=["workload_storageutilization_rbd"],
)
@ignore_leftovers
@tier4b
@skipif_managed_service
class TestAddCapacityNodeRestart(ManageTest):
    """
    Test add capacity when one of the worker nodes got restart
    in the middle of the process.
    """

    def test_add_capacity_node_restart(
        self,
        add_capacity_setup,
        nodes,
        multi_pvc_factory,
        pod_factory,
        workload_storageutilization_rbd,
        num_of_nodes,
    ):
        """
        test add capacity when one of the worker nodes got restart in the middle of the process
        """
        logger.info(
            "Condition 1 to start the test is met: storageutilization is completed"
        )
        # Please notice: When the branch 'wip-add-capacity-e_e' will be merged into master
        # the test will include more much data both before, and after calling 'add_capacity'function.

        node_list = get_ocs_nodes(num_of_nodes=num_of_nodes)
        assert node_list, "Condition 2 to start test failed: No node to restart"

        max_osds = 15
        osd_pods_before = pod_helpers.get_osd_pods()
        assert (
            len(osd_pods_before) < max_osds
        ), "Condition 3 to start test failed: We have maximum of osd's in the cluster"
        logger.info("All start conditions are met!")

        osd_size = storage_cluster.get_osd_size()
        logger.info("Calling add_capacity function...")
        result = storage_cluster.add_capacity(osd_size)
        if result:
            logger.info("add capacity finished successfully")
        else:
            logger.info("add capacity failed")

        # Restart nodes while additional storage is being added
        node_names = [n.name for n in node_list]
        logger.info(f"Restart nodes: {node_names}")
        if is_vsphere_ipi_cluster():
            nodes.restart_nodes(nodes=node_list, wait=False)
            wait_for_nodes_status(node_names, constants.STATUS_READY, timeout=300)
        else:
            nodes.restart_nodes(nodes=node_list, wait=True)

        logger.info("Finished restarting the node list")

        # The exit criteria verification conditions here are not complete. When the branch
        # 'wip-add-capacity-e_e' will be merged into master I will use the functions from this branch.

        pod = OCP(kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"])
        if is_flexible_scaling_enabled():
            replica_count = 1
        else:
            replica_count = 3
        pod.wait_for_resource(
            timeout=600,
            condition=constants.STATUS_RUNNING,
            selector="app=rook-ceph-osd",
            resource_count=result * replica_count,
        )

        # Verify OSDs are encrypted
        if config.ENV_DATA.get("encryption_at_rest"):
            osd_encryption_verification()

        logger.info("Finished verifying add capacity osd storage with node restart")
        logger.info("Waiting for ceph health check to finished...")
        check_ceph_health_after_add_capacity()
