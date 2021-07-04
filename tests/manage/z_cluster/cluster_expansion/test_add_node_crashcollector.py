import logging

from ocs_ci.framework.testlib import tier2, ignore_leftovers, ManageTest, bugzilla
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.framework.pytest_customization.marks import skipif_openshift_dedicated
from ocs_ci.ocs.resources.pod import get_pod_obj, get_all_pods
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.ocs.node import drain_nodes, schedule_nodes
from ocs_ci.framework import config
from ocs_ci.ocs.constants import OPENSHIFT_STORAGE_NAMESPACE
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import get_pod_node, get_mgr_pods

logger = logging.getLogger(__name__)


@tier2
@ignore_leftovers
@bugzilla("1898808")
@skipif_openshift_dedicated
class TestAddNodeCrashCollector(ManageTest):
    """
    Add node with OCS label and verify crashcollector created on new node

    Test Procedure:
    1.Get worker node where mgr pod running [worker-node-x]
    2.Add worker node with OCS label
    3.Check ceph status [wait_for_rebalance]
    4.Drain worker-node-x
    5.Wait for 3 mon pods to be on running state
    6.Verify ceph-crashcollector pod running on worker node where "rook-ceph" pods are running.
    7.Schedule worker-node-x
    8.Wait for 3 osd pods to be on running state
    9.Verify ceph-crashcollector pod running on worker node where "rook-ceph" pods are running.

    """

    def test_add_node_crash_collector(self, add_nodes, node_drain_teardown):
        """
        Add node with OCS label and verify crashcollector created on new node

        """
        logger.info("Get Node name where mgr pod running")
        mgr_pod_nodes = [get_pod_node(pod) for pod in get_mgr_pods()]
        mgr_pod_node_name = [node.name for node in mgr_pod_nodes]

        logger.info("Add one worker node with OCS label")
        add_nodes(ocs_nodes=False, node_count=1)
        ceph_cluster_obj = CephCluster()
        assert ceph_cluster_obj.wait_for_rebalance(
            timeout=3600
        ), "Data re-balance failed to complete"

        drain_nodes(mgr_pod_node_name)

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
        assert sorted(self.get_crashcollector_nodes()) == sorted(
            self.get_nodes_where_ocs_pods()
        ), (
            f"The crashcollector pod does not exist on "
            f"{self.get_nodes_where_ocs_pods() - self.get_crashcollector_nodes()} "
            f"even though rook-ceph pods are running on this node"
        )

        schedule_nodes(mgr_pod_node_name)

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
        assert sorted(self.get_crashcollector_nodes()) == sorted(
            self.get_nodes_where_ocs_pods()
        ), (
            f"The crashcollector pod does not exist on "
            f"{self.get_nodes_where_ocs_pods() - self.get_crashcollector_nodes()} "
            f"even though rook-ceph pods are running on this node"
        )

    def get_crashcollector_nodes(self):
        """
        Get the nodes names where crashcollector pods are running

        return:
            set: cluster names where crashcollector pods are running

        """
        crashcollector_pod_names = get_pod_name_by_pattern(pattern="crashcollector")
        crashcollector_pod_objs = [
            get_pod_obj(crashcollector_pod_name)
            for crashcollector_pod_name in crashcollector_pod_names
        ]
        crashcollector_ls = [
            crashcollector_pod_obj.data["spec"]["nodeName"]
            for crashcollector_pod_obj in crashcollector_pod_objs
        ]
        return set(crashcollector_ls)

    def get_nodes_where_ocs_pods(self):
        """
        Get the node names where rook ceph pods are running

        return:
            set: node names where rook ceph pods are running
        """
        pods_openshift_storage = get_all_pods(namespace=OPENSHIFT_STORAGE_NAMESPACE)
        ocs_nodes = list()
        for pod in pods_openshift_storage:
            if (
                "rook-ceph" in pod.name
                and "rook-ceph-operator" not in pod.name
                and "rook-ceph-tool" not in pod.name
            ):
                try:
                    ocs_nodes.append(pod.data["spec"]["nodeName"])
                except Exception as e:
                    logger.info(e)
        return set(ocs_nodes)
