import logging
from ocs_ci.framework import config
from ocs_ci.framework.testlib import ManageTest, tier1, acceptance, polarion_id
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import get_worker_nodes
from ocs_ci.ocs.resources.pod import get_pods_having_label

# from ocs_ci.utility.utils import TimeoutSampler
# from ocs_ci.helpers.helpers import wait_for_resource_state

logger = logging.getLogger(__name__)


class TestCSIADDonDaemonset(ManageTest):
    """
    Test class for CSI addon daemonset verification
    """

    @tier1
    @acceptance
    @polarion_id("OCS-7298")  # TODO Generarte Polarian ID and Replace
    def test_csi_addon_pods_on_worker_nodes(self):
        """
        Verify that the CSI addon pods are running on each worker node
        step:
        1. Get all worker nodes
        2. Get CSI addon daemonset pods
        3. Verify each worker node has a CSI addon pod
        """
        logger.info("Validating csi addon pods on each worker node")
        namespace = config.ENV_DATA["cluster_namespace"]

        worker_nodes = get_worker_nodes()
        logger.info(f"Current available worker nodes are {worker_nodes}")

        csi_addon_pods = get_pods_having_label(
            constants.CSI_RBD_ADDON_NODEPLUGIN_LABEL_420, namespace
        )
        assert len(csi_addon_pods) > 0, "csi addon pods not found"
        logger.info(f"Found {len(csi_addon_pods)} csi addon pods")

        assert len(csi_addon_pods) == len(worker_nodes), (
            f"Expected {len(worker_nodes)} csi addon pods, one per worker node, "
            f"found {len(csi_addon_pods)}"
        )
        # verify each node has csi addon pod
        worker_nodes_name = [node["metadata"]["name"] for node in worker_nodes]
        csi_pod_running_nodes_name = []
        for pod_obj in csi_addon_pods:
            csi_pod_running_node_name = pod_obj.data["spec"].get("nodeName")
            assert csi_pod_running_node_name in worker_nodes_name, (
                f"CSI addon pod {pod_obj['metadata']['name']} is running on "
                f"node {csi_pod_running_nodes_name} which is not a worker node"
            )
            csi_pod_running_nodes_name.append(csi_pod_running_node_name)

        pod_missed_node = set(worker_nodes_name) - set(csi_pod_running_nodes_name)
        assert (
            not pod_missed_node
        ), f"worker node {pod_missed_node} do not have CSI addon pods"
        logger.info("CSI addon pods running on each worker node")
