import logging
import time

import pytest

from ocs_ci.ocs import constants, ocp
from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    ManageTest,
    tier2,
    skipif_external_mode,
)

logger = logging.getLogger(__name__)


@tier2
@skipif_external_mode
@pytest.mark.polarion_id("OCS-XXX")
class TestStorageclusterUpgradeParams(ManageTest):
    """
    Verify the upgrade storagecluster parameters move to cephcluster

    """

    @pytest.fixture(autouse=True)
    def teardown_fixture(self, request):
        """
        Teardown function

        """

        def finalizer():
            storagecluster_obj = ocp.OCP(
                kind=constants.STORAGECLUSTER,
                namespace=config.ENV_DATA["cluster_namespace"],
                resource_name=constants.DEFAULT_CLUSTERNAME,
            )
            params_dict = {
                "waitTimeoutForHealthyOSDInMinutes": "10",
                "skipUpgradeChecks": "false",
                "continueUpgradeAfterChecksEvenIfNotHealthy": "false",
                "upgradeOSDRequiresHealthyPGs": "false",
                "osdMaintenanceTimeout": "30",
            }

            for parameter, parameter_value in params_dict.items():
                param = (
                    f'[{{"op": "add", "path": "/spec/managedResources/cephCluster/{parameter}",'
                    f' "value": {parameter_value}}}]'
                )
                storagecluster_obj.patch(params=param, format_type="json")

        request.addfinalizer(finalizer)

    def test_storagecluster_upgrade_params(self):
        """
        Procedure:
        1.Configure storagecluster
        2.Wait 2 seconds
        3.Read cephcluster parameter
        4.Verify parameters on cephcluster CR same storagecluster CR
        5.Configure the default params on storagecluster [treardown]

        """
        storagecluster_obj = ocp.OCP(
            kind=constants.STORAGECLUSTER,
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=constants.DEFAULT_CLUSTERNAME,
        )
        params_dict = {
            "waitTimeoutForHealthyOSDInMinutes": "31",
            "skipUpgradeChecks": "true",
            "continueUpgradeAfterChecksEvenIfNotHealthy": "true",
            "upgradeOSDRequiresHealthyPGs": "true",
            "osdMaintenanceTimeout": "18",
        }

        for parameter, parameter_value in params_dict.items():
            param = (
                f'[{{"op": "add", "path": "/spec/managedResources/cephCluster/{parameter}",'
                f' "value": {parameter_value}}}]'
            )
            storagecluster_obj.patch(params=param, format_type="json")

        logger.info("Wait 2 sec the cephcluster will updated")
        time.sleep(2)

        cephcluster_obj = ocp.OCP(
            kind=constants.CEPH_CLUSTER,
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=constants.CEPH_CLUSTER_NAME,
        )
        for parameter, parameter_value in params_dict.items():
            if parameter == "osdMaintenanceTimeout":
                actual_value = cephcluster_obj.data["spec"]["disruptionManagement"][
                    parameter
                ]
            else:
                actual_value = cephcluster_obj.data["spec"][parameter]
            assert (
                str(actual_value).lower() == str(parameter_value).lower()
            ), f"The value of {parameter} is {actual_value} the expected value is {parameter_value}"
