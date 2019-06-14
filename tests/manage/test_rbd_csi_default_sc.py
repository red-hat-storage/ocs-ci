"""
Basic test for creating PVC with default StorageClass - RBD-CSI
"""

import logging
import pytest

from ocsci.testlib import tier1, ManageTest
from tests import helpers
from ocs import constants

log = logging.getLogger(__name__)


@pytest.fixture(scope='class')
def test_fixture(request):
    """
    This is a test fixture
    """
    def finalizer():
        teardown()
    request.addfinalizer(finalizer)
    setup()


def setup():
    """
    Setting up the environment - Creating Secret
    """
    global SECRET
    log.info("Creating RBD Secret")
    SECRET = helpers.create_secret(constants.CEPHBLOCKPOOL)


def teardown():
    """
    Tearing down the environment
    """
    log.info("Deleting PVC")
    PVC.delete()

    log.info("Deleting StorageClass")
    STORAGE_CLASS.delete()

    log.info("Deleting Secret")
    SECRET.delete()


@tier1
@pytest.mark.usefixtures(
    test_fixture.__name__,
)
class TestCaseOCS347(ManageTest):
    """
    Testing default storage class creation and pvc creation
    with default rbd pool

    https://polarion.engineering.redhat.com/polarion/#/project/
    OpenShiftContainerStorage/workitem?id=OCS-347
    """

    def test_ocs_347(self):
        global PVC, STORAGE_CLASS
        log.info("Creating RBD StorageClass")
        STORAGE_CLASS = helpers.create_storage_class(
            constants.CEPHBLOCKPOOL, 'rbd', SECRET.name
        )

        log.info("Creating a PVC")
        PVC = helpers.create_pvc(STORAGE_CLASS.name)
