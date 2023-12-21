import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import (
    skipif_flexy_deployment,
    skipif_managed_service,
    skipif_hci_provider_and_client,
    skipif_multus_enabled,
    brown_squad,
)
from ocs_ci.framework.testlib import tier2, ManageTest, ignore_leftovers
from ocs_ci.ocs.node import get_worker_nodes_not_in_ocs
from ocs_ci.ocs.resources.pod import get_pod_node, get_plugin_pods

logger = logging.getLogger(__name__)


@brown_squad
# https://github.com/red-hat-storage/ocs-ci/issues/4802
@skipif_flexy_deployment
@skipif_managed_service
@skipif_hci_provider_and_client
@skipif_multus_enabled
@tier2
@pytest.mark.polarion_id("OCS-2490")
@pytest.mark.bugzilla("1794389")
@ignore_leftovers
class TestCheckTolerationForCephCsiDriverDs(ManageTest):
    """
    Check toleration for Ceph CSI driver DS on non ocs node
    """

    def test_ceph_csidriver_runs_on_non_ocs_nodes(
        self, pvc_factory, pod_factory, add_nodes
    ):
        """
        1. Add non ocs nodes
        2. Taint new nodes with app label
        3. Check if plugin pods running on new nodes
        4. Create app-pods on app_nodes
        """

        # Add worker nodes and tainting it as app_nodes
        add_nodes(ocs_nodes=False, taint_label="nodetype=app:NoSchedule")

        # Checks for new plugin pod respinning on new app-nodes
        app_nodes = [node.name for node in get_worker_nodes_not_in_ocs()]
        interfaces = [constants.CEPHFILESYSTEM, constants.CEPHBLOCKPOOL]
        logger.info("Checking for plugin pods on non-ocs worker nodes")
        for interface in interfaces:
            pod_objs = get_plugin_pods(interface)
            for pod_obj in pod_objs:
                node_obj = get_pod_node(pod_obj)
                try:
                    if node_obj.name in app_nodes:
                        logger.info(
                            f"The plugin pod {pod_obj.name} is running on app_node {node_obj.name}"
                        )
                        continue
                except Exception as e:
                    logger.info(f"Plugin pod was not found on {node_obj.name} - {e}")

        # Creates app-pods on app-nodes
        for node in app_nodes:
            pvc_obj = pvc_factory()
            pod_factory(pvc=pvc_obj, node_name=node)
