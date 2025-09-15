import logging
from ocs_ci.framework import config
from ocs_ci.framework.testlib import ManageTest, tier1, acceptance, polarion_id
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP

# from ocs_ci.ocs.resources.pod import get_pods_having_label
# from ocs_ci.utility.utils import TimeoutSampler
# from ocs_ci.helpers.helpers import wait_for_resource_state

logger = logging.getLogger(__name__)


class TestCSIADDonDaemonset(ManageTest):
    """
    Test class for CSI addon daemonset verification
    """

    @tier1
    @acceptance
    @polarion_id("OCS-XXX")  # TODO Generarte Polarian ID and Replace
    def test_csi_addon_daemonset_exists(self):
        """
        Verify that the CSI addon daemonset exists and is properly configured
        steps:
        1. Check if CSI addon Daemonset exists
        2. Verify daemonset configuration
        3. Check daemonset status
        """
        namespace = config.ENV_DATA["cluster_namespace"]
        daemonset_ocp = OCP(kind=constants.DAEMONSET, namespace=namespace)
        logger.info("Validating existence of CSI Addon daemonset")

        # Verify Daemonset exists
        assert daemonset_ocp.check_resource_existence(
            resource_name=constants.DAEMONSET_CSI_ADDON
        ), f"CSI addon daemonset '{constants.DAEMONSET_CSI_ADDON}' does not exist"
        logger.info(f"CSIaddon daemonset '{constants.DAEMONSET_CSI_ADDON}' exists")

        # Verify daemonset configuration
        logger.info("Validating configuration of CSI Addon daemonset")
        daemonset_info = daemonset_ocp.get(resource_name=constants.DAEMONSET_CSI_ADDON)
        expected_label = {"app": constants.CSI_ADDON_RBD_LABEL}
        actual_labels = daemonset_info.get("metadata", {}).get("labels", {})
        for key, value in expected_label.items():
            assert (
                actual_labels.get(key) == value
            ), f"expected label {key}={value} not found in daemonset labels"
        logger.info("CSI addon daemonset has correct labels")
