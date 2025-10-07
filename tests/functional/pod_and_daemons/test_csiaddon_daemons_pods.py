import logging
from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    ManageTest,
    tier1,
    acceptance,
    polarion_id,
    brown_squad,
    skipif_ocs_version,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import get_pods_having_label

logger = logging.getLogger(__name__)


@skipif_ocs_version("<4.20")
@polarion_id("OCS-7298")
class TestCSIADDonDaemonset(ManageTest):
    """
    Test class for CSI addon daemonset verification
    """

    @brown_squad
    @tier1
    @acceptance
    @polarion_id("OCS-7298")
    def test_csi_addon_pods_uses_pod_network(self):
        """
        Verify that CSI-addon used pod network instead of host network
        """
        logger.info(
            "Validating csi addon pods using pod-network instead of host-network "
        )
        namespace = config.ENV_DATA["cluster_namespace"]
        csi_addon_pods = get_pods_having_label(
            constants.CSI_RBD_ADDON_NODEPLUGIN_LABEL_420, namespace
        )
        for pod in csi_addon_pods:
            host_network = pod.get("spec").get("hostNetwork", False)
            assert (
                not host_network
            ), f" CSI-addon pod {pod.name} is using host network instead of pod network"
        logger.info(
            "CSI-addon DaemonSet pods using pod network instead of host-network"
        )
        

    @brown_squad
    @tier1
    @acceptance
    @polarion_id("OCS-7298")   
    def test_csi_addon_pods_containers_ready(self):
        """
        Verify that all containers in CSI-addon pods are in ready status
        Steps:
        1. Get all CSI Addons Pods
        2. Check each container in each pod
        3.Verify Container readiness status of each pod

        """
        logger.info("Validating containers in csi addon pods having ready status")
        namespace = config.ENV_DATA["cluster_namespace"]
        csi_addon_pods = get_pods_having_label(
            constants.CSI_RBD_ADDON_NODEPLUGIN_LABEL_420, namespace
        )
        for pod in csi_addon_pods:
            container_status_list = pod.get("status").get("containerStatuses")
            for container_status in container_status_list:
                assert container_status[
                    "ready"
                ], f"container {container_status['name']} in pod {pod.name} is not ready"
        logger.info("All containers in CSI-addon DaemonSet pods are ready")

