from ocs_ci.ocs.cluster import get_ceph_config_dump_property
from ocs_ci.framework.pytest_customization.marks import yellow_squad
from ocs_ci.framework.testlib import (
    ManageTest,
    acceptance,
    skipif_ocs_version,
)


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
        It should be 400 for 4.19
        """
        mon_target_pg = get_ceph_config_dump_property("mon_target_pg_per_osd")
        assert (
            mon_target_pg
        ), f"Mon_target_pg_per_osd is {mon_target_pg}. It should be 400"
