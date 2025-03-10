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
    pre_upgrade,
    post_upgrade,
)

logger = logging.getLogger(__name__)

THRESHOLDS = [
    {
        "sc_key": "backfillFullRatio",
        "value": "0.81",
        "default_value": "0.8",
        "ceph_key": "backfillfull_ratio",
    },
    {
        "sc_key": "fullRatio",
        "value": "0.86",
        "default_value": "0.85",
        "ceph_key": "full_ratio",
    },
    {
        "sc_key": "nearFullRatio",
        "value": "0.77",
        "default_value": "0.75",
        "ceph_key": "nearfull_ratio",
    },
]


@pytest.fixture()
def thresholds_teardown_fixture(request):
    """
    Teardown function

    """

    def finalizer():
        configure_cephcluster_params_in_storagecluster_cr(
            params=THRESHOLDS, default_values=True
        )

    request.addfinalizer(finalizer)


@brown_squad
@skipif_external_mode
@pytest.mark.polarion_id("OCS-6224")
class TestStorageClusterCephFullThresholdsParams(ManageTest):
    """
    Verify the ceph full thresholds storagecluster parameters move to cephcluster

    """

    def setup_thresholds_params(self):
        configure_cephcluster_params_in_storagecluster_cr(
            params=THRESHOLDS, default_values=False
        )

        logger.info("Wait 2 sec the cephcluster will updated")
        time.sleep(2)

    def verify_thresholds_params(self):
        logger.info(
            "Verify upgrade parameters on cephcluster CR and storagecluster CR are same"
        )
        cephcluster_obj = ocp.OCP(
            kind=constants.CEPH_CLUSTER,
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=constants.CEPH_CLUSTER_NAME,
        )
        for parameter in THRESHOLDS:
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
                tuple(f"{d['ceph_key']} {d['value']}" for d in THRESHOLDS)
            ),
            cephtool_cmd=True,
            ocs_operator_cmd=False,
            debug_node=None,
        )
        if not sample.wait_for_func_status(True):
            raise Exception(
                "The ceph full thresholds storagecluster parameters are not updated in ceph tool"
            )

    @tier2
    def test_storagecluster_ceph_full_thresholds_params(
        self, thresholds_teardown_fixture
    ):
        """
        Procedure:
        1.Configure storagecluster CR
        2.Wait 2 seconds
        3.Verify ceph full thresholds parameters on cephcluster CR and storagecluster CR are same
        4.Verify parameters with ceph CLI 'ceph osd dump'
        5.Configure the default params on storagecluster [treardown]

        """
        self.setup_thresholds_params()
        self.verify_thresholds_params()

    @pre_upgrade
    def test_pre_upgrade_storagecluster_ceph_full_thresholds_params(self):
        """
        Procedure:
        1.Configure storagecluster CR
        2.Wait 2 seconds
        3.Verify ceph full thresholds parameters on cephcluster CR and storagecluster CR are same
        4.Verify parameters with ceph CLI 'ceph osd dump'

        """
        self.setup_thresholds_params()
        self.verify_thresholds_params()

    @post_upgrade
    def test_post_upgrade_storagecluster_ceph_full_thresholds_params(
        self, thresholds_teardown_fixture
    ):
        """
        Procedure:
        1.Verify ceph full thresholds parameters on cephcluster CR and storagecluster CR are same
        2.Verify parameters with ceph CLI 'ceph osd dump'

        """
        self.verify_thresholds_params()
