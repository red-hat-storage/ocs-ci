"""
Automates the test OCS-297:-
1) Create a Storage Class
2) Create a PVC
3) Delete the corresponding storage class
"""
import logging
import pytest
from ocsci.testlib import tier1, ManageTest
from tests import helpers
from ocs_ci.ocs import constants


log = logging.getLogger(__name__)


@pytest.fixture(scope='class')
def test_fixture(request):
    """
    Deleting a Storage class
    """
    global SECRET, PVC, STORAGE_CLASS
    log.info("Creating RBD Secret")
    SECRET = helpers.create_secret(constants.CEPHBLOCKPOOL)

    log.info("Creating RBD StorageClass")
    STORAGE_CLASS = helpers.create_storage_class(
        constants.CEPHBLOCKPOOL, 'rbd', SECRET.name
    )

    def finalizer():
        teardown()
    request.addfinalizer(finalizer)


def teardown():
    """
    Remove the resources after execution of tests
    """
    log.info("Deleting PVC")
    assert PVC.delete()

    log.info("Deleting Secret")
    assert SECRET.delete()


@tier1
@pytest.mark.polarion_id("OCS-297")
class TestStorageClass(ManageTest):

    def test_storage_class(self):
        """
        Test to validate OCS-297
        """
        # Delete the storage class
        log.info("Deleting created storage class")
        assert STORAGE_CLASS.delete()
        log.info("Storage class deleted successfully")
