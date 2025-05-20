from ocs_ci.ocs.cluster import get_ceph_config_property
from ocs_ci.framework.pytest_customization.marks import yellow_squad
from ocs_ci.framework.testlib import (
    ManageTest,
    acceptance,
    skipif_ocs_version,
)
from ocs_ci.framework import config


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
