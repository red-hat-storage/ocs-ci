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
from ocs_ci.ocs.ocp import OCP


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
        namespace = config.ENV_DATA["cluster_namespace"]
        daemonset_ocp = OCP(kind=constants.DAEMONSET, namespace=namespace)
        csi_addon_daemonset = daemonset_ocp.get(
            resource_name=constants.CSI_RBD_ADDON_NODEPLUGIN_LABEL_420
        )

        number_ready = csi_addon_daemonset["status"]["numberReady"]
        number_available = csi_addon_daemonset["status"]["numberAvailable"]
        desired_number_ready = csi_addon_daemonset["status"]["desiredNumberScheduled"]

        assert (
            number_ready == desired_number_ready
        ), f"Expected {desired_number_ready} pods to be ready, but found {number_ready} pods ready"
        assert number_available == desired_number_ready(
            f"Expected {desired_number_ready} pods to be available, but found {number_available} pods available"
        )
        logger.info(
            f"Verified CSI-addon DaemonSet status- Desired: {desired_number_ready}, "
            f"Ready: {number_ready}, Available: {number_available}"
        )
