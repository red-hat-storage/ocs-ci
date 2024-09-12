import logging
import time

import pytest

from ocs_ci.ocs import constants, ocp
from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    ManageTest,
    tier2,
    skipif_external_mode,
    brown_squad,
)

logger = logging.getLogger(__name__)


@tier2
@brown_squad
@skipif_external_mode
@pytest.mark.polarion_id("OCS-XXX")
class TestStorageclusterUpgradeParams(ManageTest):
    """
    Verify the upgrade storagecluster parameters move to cephcluster

    """

    UPGRADE_PARAMS = [
        {
            "sc_key": "waitTimeoutForHealthyOSDInMinutes",
            "value": "0.81",
            "default_value": "0.8",
        },
        {
            "sc_key": "skipUpgradeChecks",
            "value": "0.86",
            "default_value": "0.85",
        },
        {
            "sc_key": "continueUpgradeAfterChecksEvenIfNotHealthy",
            "value": "0.77",
            "default_value": "0.75",
        },
        {
            "sc_key": "upgradeOSDRequiresHealthyPGs",
            "value": "0.77",
            "default_value": "0.75",
        },
        {
            "sc_key": "osdMaintenanceTimeout",
            "value": "0.77",
            "default_value": "0.75",
        },
    ]

    @pytest.fixture(autouse=True)
    def teardown_fixture(self, request):
        """
        Teardown function

        """

        def finalizer():

            params_dict = {
                "waitTimeoutForHealthyOSDInMinutes": "10",
                "skipUpgradeChecks": "false",
                "continueUpgradeAfterChecksEvenIfNotHealthy": "false",
                "upgradeOSDRequiresHealthyPGs": "false",
                "osdMaintenanceTimeout": "30",
            }
            self.set_storage_cluster_upgrade_params(params_dict)

        request.addfinalizer(finalizer)

    def test_storagecluster_upgrade_params(self):
        """
        Procedure:
        1.Configure storagecluster
        2.Wait 2 seconds
        3.Verify upgrade parameters on cephcluster CR and storagecluster CR are same
        4.Configure the default params on storagecluster [treardown]

        """
        self.set_storage_cluster_upgrade_params()

        logger.info("Wait 2 sec the cephcluster will updated")
        time.sleep(2)

        logger.info(
            "Verify upgrade parameters on cephcluster CR and storagecluster CR are same"
        )
        cephcluster_obj = ocp.OCP(
            kind=constants.CEPH_CLUSTER,
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=constants.CEPH_CLUSTER_NAME,
        )
        for parameter in self.UPGRADE_PARAMS:
            if parameter == "osdMaintenanceTimeout":
                actual_value = cephcluster_obj.data["spec"]["disruptionManagement"][
                    parameter
                ]
            else:
                actual_value = cephcluster_obj.data["spec"][parameter]
            assert (
                str(actual_value).lower() == str(parameter["value"]).lower()
            ), f"The value of {parameter['sc_key']} is {actual_value} the expected value is"

    def set_storage_cluster_ceph_full_thresholds_params(self, default_values=False):
        """
        Configure StorageCluster CR with ceph full thresholds params
        Args:
            default_values(bool): parameters to set in StorageCluster under /spec/managedResources/cephCluster/
        """
        logger.info("Configure StorageCluster CR with upgrade params")
        storagecluster_obj = ocp.OCP(
            kind=constants.STORAGECLUSTER,
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=constants.DEFAULT_CLUSTERNAME,
        )
        for parameter in self.UPGRADE_PARAMS:
            sc_key = parameter["sc_key"]
            if default_values:
                parameter_value = parameter["default_value"]
            else:
                parameter_value = parameter["value"]
            param = (
                f'[{{"op": "add", "path": "/spec/managedResources/cephCluster/{sc_key}",'
                f' "value": {parameter_value}}}]'
            )
            storagecluster_obj.patch(params=param, format_type="json")
