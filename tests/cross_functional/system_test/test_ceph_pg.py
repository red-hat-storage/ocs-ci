import logging

from ocs_ci.ocs.cluster import (
    get_ceph_config_property,
    change_pool_target_size_ratio,
    get_autoscale_status_property,
    get_total_num_of_pgs,
)
from ocs_ci.helpers.ceph_helpers import update_mon_target_pg
from ocs_ci.framework.pytest_customization.marks import yellow_squad
from ocs_ci.framework.testlib import (
    ManageTest,
    acceptance,
    skipif_ocs_version,
    tier1,
    tier2,
)
from ocs_ci.ocs.resources.pod import restart_pods_having_label
from ocs_ci.framework import config
from ocs_ci.ocs import constants

logger = logging.getLogger(__name__)


@yellow_squad
class TestCephPg(ManageTest):
    """
    Tests realated to ceph pg_num and mon_target_pg_per_osd
    """

    @acceptance
    @skipif_ocs_version("<4.19")
    def test_mon_target_pg_per_osd(self):
        """
        Test the value of mon_target_pg_per_osd
        It should be 100 in Lean, 200 in Balanced and 400 in Performance profile
        """
        mon_target_pg = get_ceph_config_property("mon", "mon_target_pg_per_osd")
        mode = config.ENV_DATA.get("performance_profile")
        pgs_total = get_total_num_of_pgs()
        if mode == "lean":
            assert (
                int(mon_target_pg) == 100
            ), f"Mon_target_pg_per_osd is {mon_target_pg}. It should be 100 for Lean profile"
        elif mode == "balanced":
            assert (
                int(mon_target_pg) == 200
            ), f"Mon_target_pg_per_osd is {mon_target_pg}. It should be 100 for Balanced profile"
        else:
            assert (
                int(mon_target_pg) == 400
            ), f"Mon_target_pg_per_osd is {mon_target_pg}. It should be 400 for Performance profile"
            assert pgs_total > 250, f"Total pgs: {pgs_total}. Should be more than 250"

    @skipif_ocs_version("<4.19")
    @tier1
    def test_cephblockpool_pg_increase(self):
        """
        Test increasing target size ratio of the default cephblockpool
        while decreasing target size ratio of the default ceph filesystem pool
        makes ceph rebalance PGs in favor of the cephblockpool
        """
        cephblockpool_pgs_before = get_autoscale_status_property(
            constants.DEFAULT_CEPHBLOCKPOOL, constants.PG_NUM_FINAL
        )
        cephfspool_pgs_before = get_autoscale_status_property(
            constants.DEFAULT_CEPHFS_POOL, constants.PG_NUM_FINAL
        )
        logger.info(
            "PG numbers before target size change: cephblockpool "
            f"{cephblockpool_pgs_before}, cephfspool {cephfspool_pgs_before}"
        )
        change_pool_target_size_ratio(constants.DEFAULT_CEPHBLOCKPOOL, 0.99)
        change_pool_target_size_ratio(constants.DEFAULT_CEPHFS_POOL, 0.01)
        cephblockpool_pgs_after = get_autoscale_status_property(
            constants.DEFAULT_CEPHBLOCKPOOL, constants.PG_NUM_FINAL
        )
        cephfspool_pgs_after = get_autoscale_status_property(
            constants.DEFAULT_CEPHFS_POOL, constants.PG_NUM_FINAL
        )
        logger.info(
            "PG numbers after target size change: cephblockpool "
            f"{cephblockpool_pgs_after}, cephfspool {cephfspool_pgs_after}"
        )
        assert (int(cephblockpool_pgs_before) - int(cephfspool_pgs_before)) < int(
            cephblockpool_pgs_after
        ) - int(
            cephfspool_pgs_after
        ), "Pg count did not changet in favor of default cephblockpool"
        # change target size ratio back to the default 0.49
        change_pool_target_size_ratio(constants.DEFAULT_CEPHBLOCKPOOL, 0.49)
        change_pool_target_size_ratio(constants.DEFAULT_CEPHFS_POOL, 0.49)

    @skipif_ocs_version("<4.19")
    @tier1
    def test_cephfspool_pg_increase(self):
        """
        Test increasing target size ratio of the default ceph filesystem pool
        while decreasing target size ratio of the default cephblockpool
        makes ceph rebalance PGs in favor of the ceph filesystem pool
        """
        cephblockpool_pgs_before = get_autoscale_status_property(
            constants.DEFAULT_CEPHBLOCKPOOL, constants.PG_NUM_FINAL
        )
        cephfspool_pgs_before = get_autoscale_status_property(
            constants.DEFAULT_CEPHFS_POOL, constants.PG_NUM_FINAL
        )
        logger.info(
            "PG numbers before target size change: cephblockpool "
            f"{cephblockpool_pgs_before}, cephfspool {cephfspool_pgs_before}"
        )
        change_pool_target_size_ratio(constants.DEFAULT_CEPHBLOCKPOOL, 0.01)
        change_pool_target_size_ratio(constants.DEFAULT_CEPHFS_POOL, 0.99)
        cephblockpool_pgs_after = get_autoscale_status_property(
            constants.DEFAULT_CEPHBLOCKPOOL, constants.PG_NUM_FINAL
        )
        cephfspool_pgs_after = get_autoscale_status_property(
            constants.DEFAULT_CEPHFS_POOL, constants.PG_NUM_FINAL
        )
        logger.info(
            "PG numbers after target size change: cephblockpool "
            f"{cephblockpool_pgs_after}, cephfspool {cephfspool_pgs_after}"
        )
        assert (int(cephblockpool_pgs_before) - int(cephfspool_pgs_before)) > int(
            cephblockpool_pgs_after
        ) - int(
            cephfspool_pgs_after
        ), "Pg count did not changet in favor of default ceph filesystem pool"
        # change target size ratio back to the default 0.49
        change_pool_target_size_ratio(constants.DEFAULT_CEPHBLOCKPOOL, 0.49)
        change_pool_target_size_ratio(constants.DEFAULT_CEPHFS_POOL, 0.49)

    @skipif_ocs_version("<4.19")
    @tier1
    def test_metadata_pools_ratio(self):
        """
        Test verifies that metadata pools don't have
        the default target size ratio of 0.49
        """

        assert (
            get_autoscale_status_property(".mgr", "target_ratio") == 0
        ), "Target ratio of .mgr metadata pool should be 0.0"
        assert (
            get_autoscale_status_property(
                "ocs-storagecluster-cephfilesystem-metadata", "target_ratio"
            )
            == 0
        ), "Target ratio of ocs-storagecluster-cephfilesystem-metadata should be 0.0"

    @skipif_ocs_version("<4.19")
    @tier2
    def test_update_mon_target_pg(self):
        """
        Test verifies that mon_target_pg_per_osd can be changed
        by patching the storagecluster
        and that invalid values are not accepted
        """
        original_value = get_ceph_config_property("mon", "mon_target_pg_per_osd")
        assert update_mon_target_pg(400)
        assert update_mon_target_pg(100)
        assert not update_mon_target_pg("1OO")
        update_mon_target_pg(original_value)

    @skipif_ocs_version("<4.19")
    @tier2
    def test_update_mon_target_pg_with_mon_restart(self):
        """
        Test that after increasing mon_target_pg_per_osd
        and restarting mon pod the number of pgs is increased
        """
        original_value = get_ceph_config_property("mon", "mon_target_pg_per_osd")
        assert update_mon_target_pg(400)
        restart_pods_having_label(label=constants.MON_APP_LABEL)
        pgs_total = get_total_num_of_pgs()
        assert pgs_total > 250, f"Total pgs: {pgs_total}. Should be more than 250"
        update_mon_target_pg(original_value)
