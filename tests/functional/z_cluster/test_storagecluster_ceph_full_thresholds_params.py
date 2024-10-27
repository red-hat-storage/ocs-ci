import logging
import time

import pytest

from ocs_ci.ocs import constants, ocp
from ocs_ci.framework import config
from ocs_ci.helpers.helpers import run_cmd_verify_cli_output
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.helpers.helpers import configure_cephcluster_params_in_storagecluster_cr
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
@pytest.mark.polarion_id("OCS-6224")
class TestStorageClusterCephFullThresholdsParams(ManageTest):
    """
    Verify the ceph full thresholds storagecluster parameters move to cephcluster

    """

    TRESHOLDS = [
        {
            "sc_key": "backfillFullRatio",
            "value": "0.81",
            "default_value": "0.8",
            "ceph_key": "backfillfull_ratio",
            "odf_cli_cmd": "backfillfull",
        },
        {
            "sc_key": "fullRatio",
            "value": "0.86",
            "default_value": "0.85",
            "ceph_key": "full_ratio",
            "odf_cli_cmd": "full",
        },
        {
            "sc_key": "nearFullRatio",
            "value": "0.77",
            "default_value": "0.75",
            "ceph_key": "nearfull_ratio",
            "odf_cli_cmd": "nearfull",
        },
    ]

    @pytest.fixture()
    def setup_odf_cli(self, odf_cli_setup):
        self.odf_cli_runner = odf_cli_setup

    @pytest.fixture(autouse=True)
    def teardown_fixture(self, request):
        """
        Teardown function

        """

        def finalizer():
            configure_cephcluster_params_in_storagecluster_cr(
                params=self.TRESHOLDS, default_values=True
            )

        request.addfinalizer(finalizer)

    def configure_ceph_full_thresholds(self):
        """
        Configure storagecluster CR with custom ceph full thresholds
        """
        configure_cephcluster_params_in_storagecluster_cr(
            params=self.TRESHOLDS, default_values=False
        )

        logger.info("Wait 2 sec for the cephcluster to be updated")
        time.sleep(2)

    def configure_ceph_full_thresholds_with_odf_cli(self):
        """
        Configure storagecluster CR with custom ceph full thresholds using odf cli
        """
        treshholds_name = [d["odf_cli_cmd"] for d in self.TRESHOLDS]
        logger.info(f"treshholds_name: {treshholds_name}")
        treshholds_value = [d["value"] for d in self.TRESHOLDS]
        logger.info(f"treshholds_value: {treshholds_value}")
        self.odf_cli_runner.run_set_ceph_fill_thresholds(
            thresholds_name=treshholds_name, value=treshholds_value
        )

    def validate_ceph_full_thresholds(self):
        """
        Validate ceph full thresholds parameters on cephcluster CR, storagecluster CR, and ceph CLI
        """
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
            ), f"The value of {parameter['sc_key']} is {actual_value}, the expected value is {parameter['value']}"

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

    @pytest.mark.parametrize("method", ["odf_cli", "oc"])
    def test_storagecluster_ceph_full_thresholds_params(self, method, setup_odf_cli):
        """
        Procedure:
        1. Configure storagecluster CR with custom ceph full thresholds
        2. Validate ceph full thresholds parameters on cephcluster CR, storagecluster CR, and ceph CLI
        3. Configure the default params on storagecluster [teardown]
        """
        if method == "odf_cli":
            self.configure_ceph_full_thresholds_with_odf_cli()
        elif method == "oc":
            self.configure_ceph_full_thresholds()
        self.validate_ceph_full_thresholds()
