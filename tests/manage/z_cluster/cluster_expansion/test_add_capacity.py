import logging
import pytest
from builtins import len

from ocs_ci.framework.testlib import tier1, ignore_leftovers, ManageTest
from ocs_ci.ocs import machine as machine_utils
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs import defaults
from ocs_ci.framework import config
from ocs_ci.ocs.node import get_typed_nodes
from ocs_ci.ocs.resources import storage_cluster, pod
from tests import helpers

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
        argvalues=[pytest.param(*['2000Gi', False]),
                   pytest.param(*['6144Gi', False]),
                   pytest.param(*['2048Gi', True]),
        ]
    )
    def test_add_capacity(self, capacity, expected):
        """
        Test to add variable capacity to the OSD cluster while IOs running

        Args:
            capacity (String):the storage capacity of each OSD
            expected (Boolean): Expected boolean statment from add_capacity function
        """
        dt = config.ENV_DATA['deployment_type']
        if dt == 'ipi':
            returned_bool = storage_cluster.add_capacity(capacity)
            assert returned_bool == expected(
                logger.info("test failed successfully")
            )

        else:
            pytest.skip("UPI not yet supported")

