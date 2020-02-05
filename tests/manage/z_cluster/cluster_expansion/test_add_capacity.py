import logging
import pytest
from ocs_ci.framework.pytest_customization.marks import polarion_id, tier3
from ocs_ci.framework.testlib import tier1, ignore_leftovers, ManageTest
from ocs_ci.ocs.resources import storage_cluster

logger = logging.getLogger(__name__)


@ignore_leftovers
@tier1
class TestAddCapacity(ManageTest):
    """
   Automates adding variable capacity to the cluster while IOs running
   """

    @pytest.mark.parametrize(
        argnames=[
            "capacity", "expected"
        ],
        argvalues=[
            pytest.param(*['2000Gi', False], marks=[polarion_id(''), tier3]),
            pytest.param(*['6144Gi', False], marks=[polarion_id(''), tier3]),
            pytest.param(*['2048Gi', True], marks=[polarion_id(''), tier1]),
        ]
    )
    def test_add_capacity(self, capacity, expected):
        """
       Test to add variable capacity to the OSD cluster while IOs running

       Args:
           capacity (String):the storage capacity of each OSD
           expected (Boolean): Expected boolean statment from add_capacity function
       """
        returned_bool = storage_cluster.add_capacity(capacity)
        assert returned_bool == expected(
            logger.info("Test Failed")
        )
