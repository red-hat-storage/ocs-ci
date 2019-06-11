import os
import logging
import pytest

from ocs import ocp, defaults
from ocsci.config import ENV_DATA
from ocsci.testlib import tier1, ManageTest
from resources.ocs import OCS
from tests import helpers
import pdb

log = logging.getLogger(__name__)

POD = ocp.OCP(kind='Pod', namespace=ENV_DATA['cluster_namespace'])

@pytest.fixture(scope='class')
def test_fixture(request):
    """
    Create disks
    """
    self = request.node.cls

    def finalizer():
        teardown(self)
    request.addfinalizer(finalizer)
    setup(self)


def setup(self):
    """
    Setting up the environment for the test
    """
    pdb.set_trace()
    self.namespace = ocp.OCP(kind='namespace', namespace=ENV_DATA['cluster_namespace'])
    self.namespace.create(resource_name='ocs-368')
    pdb.set_trace()


def teardown(self):
    pdb.set_trace()
    self.namespace.delete(resource_name='ocs-368')
    pdb.set_trace()


@tier1
@pytest.mark.usefixtures(
    test_fixture.__name__,
)
class TestOcs368(ManageTest):
    def test_ocs_368(self):
        pdb.set_trace()
        pass
