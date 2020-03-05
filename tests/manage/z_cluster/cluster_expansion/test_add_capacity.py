import pytest

from ocs_ci.framework.pytest_customization.marks import polarion_id, tier3
from ocs_ci.framework.testlib import ignore_leftovers, ManageTest, tier1
from ocs_ci.ocs.resources import storage_cluster
from ocs_ci.utility.utils import ceph_health_check


@ignore_leftovers
@tier1
class TestAddCapacity(ManageTest):
    """
   Automates adding variable capacity to the cluster while IOs running
   """

    @pytest.mark.parametrize(
        argnames=[
            "capacity"
        ],
        argvalues=[
            pytest.param(
                *[2000], marks=[polarion_id('OCS-1191'), tier3]
            ),
        ]
    )
    def test_add_capacity(self):
        """
       Test to add variable capacity to the OSD cluster while IOs running
       """

        osd_size = storage_cluster.get_osd_size()
        result = storage_cluster.add_capacity(osd_size)
        ceph_health = ceph_health_check()
        assert result and ceph_health, "Test Failed, new pods failed reaching running state"
