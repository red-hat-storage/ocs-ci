import logging

import pytest

from ocs_ci.helpers.helpers import (
    configure_cephcluster_params_in_storagecluster_cr,
    run_cmd_verify_cli_output,
)
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.resources.storage_cluster import get_cephcluster_storage_spec
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

    CEPHCLUSTER_RECONCILE_TIMEOUT = 60
    CEPH_OSD_DUMP_TIMEOUT = 600

    def setup_thresholds_params(self):
        configure_cephcluster_params_in_storagecluster_cr(
            params=THRESHOLDS, default_values=False
        )

    def verify_thresholds_params(self):
        logger.info(
            "Verify threshold parameters propagated from StorageCluster to CephCluster CR"
        )
        for storage_spec in TimeoutSampler(
            timeout=self.CEPHCLUSTER_RECONCILE_TIMEOUT,
            sleep=5,
            func=get_cephcluster_storage_spec,
        ):
            if storage_spec is None:
                continue
            mismatches = {}
            for parameter in THRESHOLDS:
                key = parameter["sc_key"]
                actual = storage_spec.get(key)
                if actual is None or str(actual).lower() != parameter["value"]:
                    mismatches[key] = actual
            if not mismatches:
                logger.info("All threshold parameters reconciled to CephCluster CR")
                break
            logger.info(
                "Waiting for CephCluster reconciliation, pending: %s", mismatches
            )

        for parameter in THRESHOLDS:
            actual_value = storage_spec.get(parameter["sc_key"])
            assert str(actual_value).lower() == str(parameter["value"]).lower(), (
                f"The value of {parameter['sc_key']} is {actual_value}, "
                f"the expected value is {parameter['value']}"
            )

        logger.info("Verify threshold parameters with ceph CLI 'ceph osd dump'")
        sample = TimeoutSampler(
            timeout=self.CEPH_OSD_DUMP_TIMEOUT,
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
        2.Verify ceph full thresholds parameters on cephcluster CR and storagecluster CR are same
        3.Verify parameters with ceph CLI 'ceph osd dump'
        4.Configure the default params on storagecluster [teardown]

        """
        self.setup_thresholds_params()
        self.verify_thresholds_params()

    @pre_upgrade
    def test_pre_upgrade_storagecluster_ceph_full_thresholds_params(self):
        """
        Procedure:
        1.Configure storagecluster CR
        2.Verify ceph full thresholds parameters on cephcluster CR and storagecluster CR are same
        3.Verify parameters with ceph CLI 'ceph osd dump'

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
