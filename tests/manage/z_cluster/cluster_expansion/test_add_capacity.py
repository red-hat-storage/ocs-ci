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
            "capacity"
        ],
        argvalues=[
            pytest.param(
                *['2048Gi'],
            ),
        ]
    )
    def test_add_capacity(self, capacity):
        """
        Test to add variable capacity to the OSD cluster while IOs running

        Args:
            capacity:the storage capacity of each OSD
        """
        dt = config.ENV_DATA['deployment_type']
        if dt == 'ipi':
            storage_cluster.add_capacity(capacity)

        else:
            pytest.skip("UPI not yet supported")
