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

    @pytest.fixture(autouse=True)
    def teardown_fixture(self, request):
        """
        Teardown function

        """

        def finalizer():
            params_dict = {
                "backfillFullRatio": "0.85",
                "fullRatio": "0.8",
                "nearFullRatio": "0.75",
            }
            self.set_storage_cluster_ceph_full_thresholds_params(params_dict)

        request.addfinalizer(finalizer)

    def test_storagecluster_ceph_full_thresholds_params(self):
        """
        Procedure:
        1.Configure storagecluster CR
        2.Wait 2 seconds
        3.Verify ceph full thresholds parameters on cephcluster CR and storagecluster CR are same
        4.Verify parameters with ceph api- 'ceph osd dump'
        5.Configure the default params on storagecluster [treardown]

        """
        params_dict = {
            "backfillFullRatio": "0.862",
            "fullRatio": "0.793",
            "nearFullRatio": "0.744",
        }
        self.set_storage_cluster_ceph_full_thresholds_params(params_dict)

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
        for parameter, parameter_value in params_dict.items():
            actual_value = cephcluster_obj.data["spec"]["storage"][parameter]
            assert (
                str(actual_value).lower() == str(parameter_value).lower()
            ), f"The value of {parameter} is {actual_value} the expected value is {parameter_value}"

        sample = TimeoutSampler(
            timeout=600,
            sleep=10,
            func=run_cmd_verify_cli_output,
            cmd="ceph osd dump",
            expected_output_lst=(
                "full_ratio 0.793",
                "backfillfull_ratio 0.862",
                "nearfull_ratio 0.744",
            ),
            cephtool_cmd=True,
            ocs_operator_cmd=False,
            debug_node=None,
        )
        if not sample.wait_for_func_status(True):
            raise Exception(
                "The ceph full thresholds storagecluster parameters are not updated in ceph tool"
            )

    def set_storage_cluster_ceph_full_thresholds_params(self, params_dict):
        """
        Configure StorageCluster CR with ceph full thresholds params

        Args:
            params_dict:

        """
        logger.info(f"Configure StorageCluster CR with upgrade params {params_dict}")
        storagecluster_obj = ocp.OCP(
            kind=constants.STORAGECLUSTER,
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=constants.DEFAULT_CLUSTERNAME,
        )
        for parameter, parameter_value in params_dict.items():
            param = (
                f'[{{"op": "add", "path": "/spec/managedResources/cephCluster/{parameter}",'
                f' "value": {parameter_value}}}]'
            )
            storagecluster_obj.patch(params=param, format_type="json")
