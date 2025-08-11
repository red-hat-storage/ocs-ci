import logging
import pytest

from ocs_ci.framework.testlib import (
    ManageTest,
    ignore_leftovers,
    libtest,
    brown_squad,
    runs_on_provider,
)
from ocs_ci.framework import config
from ocs_ci.helpers.osd_resize import CephCluster


log = logging.getLogger(__name__)


@brown_squad
@libtest
@ignore_leftovers
@runs_on_provider
class TestWaitForCephRebalance(ManageTest):
    """
    Test Wait for Ceph rebalance without having the IO in the background
    """

    @pytest.fixture(autouse=True)
    def setup(self):
        # Simulate the run when we don't have IO in the background
        self.original_io_in_bg = config.RUN.get("io_in_bg", False)
        config.RUN["io_in_bg"] = False

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        def finalizer():
            config.RUN["io_in_bg"] = self.original_io_in_bg

        request.addfinalizer(finalizer)

    def test_wait_for_ceph_rebalance(self):
        ceph_cluster_obj = CephCluster()
        assert ceph_cluster_obj.wait_for_rebalance(
            timeout=180
        ), "Data re-balance failed to complete"


@brown_squad
@libtest
@ignore_leftovers
@runs_on_provider
class TestWaitForCephRebalanceWithIO(ManageTest):
    """
    Test wait for Ceph rebalance when we run IO in the background
    """

    @pytest.fixture(autouse=True)
    def setup(self):
        # Simulate the run when we have IO in the background
        self.original_io_in_bg = config.RUN.get("io_in_bg", False)
        config.RUN["io_in_bg"] = True

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        def finalizer():
            config.RUN["io_in_bg"] = self.original_io_in_bg

        request.addfinalizer(finalizer)

    def test_wait_for_ceph_rebalance_with_io(self):
        ceph_cluster_obj = CephCluster()
        assert ceph_cluster_obj.wait_for_rebalance(
            timeout=180
        ), "Data re-balance failed to complete"


@brown_squad
@libtest
@ignore_leftovers
@runs_on_provider
class TestWaitForCephRebalanceHighRecoveryDisabled(ManageTest):
    """
    Test Ceph rebalance wait behavior when the high recovery profile is disabled
    """

    @pytest.fixture(autouse=True)
    def setup(self):
        # Simulate the run when the high recovery profile is disabled
        self.original_flag = config.ENV_DATA.get(
            "enable_high_recovery_during_rebalance", False
        )
        config.ENV_DATA["enable_high_recovery_during_rebalance"] = False

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        def finalizer():
            config.ENV_DATA["enable_high_recovery_during_rebalance"] = (
                self.original_flag
            )

        request.addfinalizer(finalizer)

    def test_wait_for_ceph_rebalance_high_recovery_disabled(self):
        ceph_cluster_obj = CephCluster()
        assert ceph_cluster_obj.wait_for_rebalance(
            timeout=180
        ), "Data re-balance failed to complete"
