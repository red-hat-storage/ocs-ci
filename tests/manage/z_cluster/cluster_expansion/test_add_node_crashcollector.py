import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import skipif_openshift_dedicated
from ocs_ci.ocs.node import drain_nodes, schedule_nodes
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.node import (
    get_nodes_where_ocs_pods_running,
    get_node_rack,
    get_node_names,
    get_crashcollector_nodes,
)
from ocs_ci.framework.testlib import (
    tier2,
    ignore_leftovers,
    ManageTest,
    bugzilla,
    skipif_external_mode,
    vsphere_platform_required,
)

logger = logging.getLogger(__name__)


@tier2
@ignore_leftovers
@bugzilla("1898808")
@skipif_external_mode
@vsphere_platform_required
@skipif_openshift_dedicated
@pytest.mark.polarion_id("OCS-2594")
class TestAddNodeCrashCollector(ManageTest):
    """
    Add node with OCS label and verify crashcollector created on new node

    Test Procedure:
    1.Add worker node with OCS label
    2.Drain the 'old' node located in the same rack of ​​the new node
    3.Wait for 3 mon pods to be on running state
    4.Verify ceph-crashcollector pod running on worker node where "rook-ceph" pods are running.
    5.Schedule worker-node-x
    6.Wait for 3 osd pods to be on running state
    7.Verify ceph-crashcollector pod running on worker node where "rook-ceph" pods are running.

    """

    def test_add_node_crash_collector(self, add_nodes, node_drain_teardown):
        """
        Add node with OCS label and verify crashcollector created on new node

        """
        old_nodes = get_node_names()
        old_node_rack = get_node_rack()

        logger.info("Add one worker node with OCS label")
        add_nodes(ocs_nodes=True, node_count=1)

        logger.info("Get new worker node name")
        new_node = list(set(get_node_names()) - set(old_nodes))

        node_rack_dic = get_node_rack()
        new_node_rack = node_rack_dic[new_node[0]]
        for node, rack in old_node_rack.items():
            if rack == new_node_rack:
                drain_node = node

        drain_nodes([drain_node])

        logging.info("Wait for 3 mon pods to be on running state")
        pod = OCP(kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"])
        assert pod.wait_for_resource(
            condition="Running",
            selector=constants.MON_APP_LABEL,
            resource_count=3,
            timeout=1400,
        )
        logger.info(
            "Verify rook-ceph-crashcollector pod running on worker node"
            " where rook-ceph pods are running."
        )
        assert sorted(get_crashcollector_nodes()) == sorted(
            get_nodes_where_ocs_pods_running()
        ), (
            f"The crashcollector pod exists on "
            f"{get_crashcollector_nodes() - get_nodes_where_ocs_pods_running()} "
            f"even though rook-ceph pods are not running on this node"
        )

        schedule_nodes([drain_node])

        logging.info("Wait for 3 osd pods to be on running state")
        assert pod.wait_for_resource(
            condition="Running",
            selector=constants.OSD_APP_LABEL,
            resource_count=3,
            timeout=600,
        )

        logger.info(
            "Verify rook-ceph-crashcollector pod running on worker node where rook-ceph pods are running."
        )
        assert sorted(get_crashcollector_nodes()) == sorted(
            get_nodes_where_ocs_pods_running()
        ), (
            f"The crashcollector pod exists on "
            f"{get_crashcollector_nodes() - get_nodes_where_ocs_pods_running()} "
            f"even though rook-ceph pods are not running on this node"
        )
