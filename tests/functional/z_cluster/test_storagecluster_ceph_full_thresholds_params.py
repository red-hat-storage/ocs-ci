import logging
import time

import pytest

from ocs_ci.ocs import constants, ocp
from ocs_ci.framework import config
from ocs_ci.helpers.helpers import run_cmd_verify_cli_output
from ocs_ci.utility.utils import TimeoutSampler
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
class TestStorageClusterCephFullThresholdsParams(ManageTest):
    """
    Verify the ceph full thresholds storagecluster parameters move to cephcluster

    """

    TRESHOLDS = [
        {
            "sc_key": "backfillFullRatio",
            "value": "0.84",
            "default_value": "0.85",
            "ceph_key": "backfillfull_ratio",
        },
        {
            "sc_key": "fullRatio",
            "value": "0.79",
            "default_value": "0.8",
            "ceph_key": "full_ratio",
        },
        {
            "sc_key": "nearFullRatio",
            "value": "0.74",
            "default_value": "0.75",
            "ceph_key": "nearfull_ratio",
        },
    ]

    @pytest.fixture(autouse=True)
    def teardown_fixture(self, request):
        """
        Teardown function

        """

        def finalizer():
            self.set_storage_cluster_ceph_full_thresholds_params(default_values=True)

        request.addfinalizer(finalizer)

    def test_storagecluster_ceph_full_thresholds_params(self):
        """
        Procedure:
        1.Configure storagecluster CR
        2.Wait 2 seconds
        3.Verify ceph full thresholds parameters on cephcluster CR and storagecluster CR are same
        4.Verify parameters with ceph CLI 'ceph osd dump'
        5.Configure the default params on storagecluster [treardown]

        """
        self.set_storage_cluster_ceph_full_thresholds_params()

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
        for parameter in self.TRESHOLDS:
            actual_value = cephcluster_obj.data["spec"]["storage"][parameter["sc_key"]]
            assert (
                str(actual_value).lower() == str(parameter["value"]).lower()
            ), f"The value of {parameter['sc_key']} is {actual_value} the expected value is {parameter['value']}"

        sample = TimeoutSampler(
            timeout=600,
            sleep=10,
            func=run_cmd_verify_cli_output,
            cmd="ceph osd dump",
            expected_output_lst=(
                tuple(f"{d['ceph_key']} {d['value']}" for d in self.TRESHOLDS)
            ),
            cephtool_cmd=True,
            ocs_operator_cmd=False,
            debug_node=None,
        )
        if not sample.wait_for_func_status(True):
            raise Exception(
                "The ceph full thresholds storagecluster parameters are not updated in ceph tool"
            )

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
        for parameter in self.TRESHOLDS:
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
