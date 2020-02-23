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
            "capacity"
        ],
        argvalues=[
            pytest.param(*[1], marks=[polarion_id(''), tier3]),
        ]
    )
    def test_add_capacity(self, capacity):
        """
       Test to add variable capacity to the OSD cluster while IOs running

       Args:
           capacity (int):the storage capacity as deviceSet number
       """
        ans = storage_cluster.add_capacity(capacity)
        assert ans(
            logger.info("Test Failed")
        )
