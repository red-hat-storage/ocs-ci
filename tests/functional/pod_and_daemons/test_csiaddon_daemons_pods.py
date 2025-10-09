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
from ocs_ci.ocs.resources.daemonset import DaemonSet

logger = logging.getLogger(__name__)


@tier1
@brown_squad
@skipif_ocs_version("<4.20")
@polarion_id("OCS-7298")
class TestCSIADDonDaemonset(ManageTest):
    """
    Test class for CSI addon daemonset verification
    """

    def test_csi_addon_daemonset_exists(self):
        """
        Verify that the CSI addon daemonset exists and is properly configured
        steps:
        1. Check if CSI addon Daemonset exists
        2. Verify daemonset configuration
        """
        daemonsets = DaemonSet(namespace=config.ENV_DATA["cluster_namespace"])
        logger.info("Validating existence of CSI Addon daemonset")

        # Verify Daemonset exists
        assert daemonsets.check_resource_existence(
            should_exist=True, resource_name=constants.DAEMONSET_CSI_RBD_CSI_ADDONS
        ), f"CSI addon daemonset '{constants.DAEMONSET_CSI_RBD_CSI_ADDONS}' does not exist"
        logger.info(
            f"CSIaddon daemonset '{constants.DAEMONSET_CSI_RBD_CSI_ADDONS}' exists"
        )

        # Verify daemonset configuration
        logger.info("Validating configuration of CSI Addon daemonset")
        daemonset_info = daemonsets.get(
            resource_name=constants.DAEMONSET_CSI_RBD_CSI_ADDONS
        )
        actual_labels = (
            daemonset_info.get("spec", {})
            .get("template", {})
            .get("metadata", {})
            .get("labels", {})
        )
        assert (
            actual_labels
        ), f" The Daemonset {constants.DAEMONSET_CSI_RBD_CSI_ADDONS} has label {actual_labels} "
        expected_label = constants.CSI_RBD_ADDON_NODEPLUGIN_LABEL_420.split("=")
        for key, value in actual_labels.items():
            assert expected_label == [
                key,
                value,
            ], f"expected label {constants.CSI_RBD_ADDON_NODEPLUGIN_LABEL_420} not found in daemonset labels"
        logger.info("CSI addon daemonset has correct labels")

    @acceptance
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

    def test_csi_addon_pods_uses_pod_network(self):
        """
        Verify that CSI-addon used pod network instead of host network
        """

        logger.info(
            "Validating csi addon pod using pod-network instead of host network"
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

    def test_csi_addon_daemonset_desired_vs_ready(self):
        """
        Verify that CSI addon DaemonSet has desired number of ready and available pods
        Step:
        1. Get CSI-addon DaemonSet status
        2. Compare desired Vs ready pod counts
        3. Verify all pods are available
        """
        logger.info(
            "Validating CSI-addon DaemonSet has correct number of Desired, ready and available pods"
        )
        csi_addon_daemonset = DaemonSet(
            resource_name=constants.DAEMONSET_CSI_RBD_CSI_ADDONS,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        csi_addon_daemonset_status = csi_addon_daemonset.get_status()

        number_ready = csi_addon_daemonset_status["numberReady"]
        number_available = csi_addon_daemonset_status["numberAvailable"]
        desired_number_ready = csi_addon_daemonset_status["desiredNumberScheduled"]

        assert (
            number_ready == desired_number_ready
        ), f"Expected {desired_number_ready} pods to be ready, but found {number_ready} pods ready"
        assert (
            number_available == desired_number_ready
        ), f"Expected {desired_number_ready} pods to be available, but found {number_available} pods available"
        logger.info(
            f"Verified CSI-addon DaemonSet status- Desired: {desired_number_ready}, "
            f"Ready: {number_ready}, Available: {number_available}"
        )
