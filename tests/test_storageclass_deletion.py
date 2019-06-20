"""
Automates the test OCS-297:-
1) Create a Storage Class
2) Create a PVC
3) Delete the corresponding storage class
"""
import logging
import pytest

from ocsci.testlib import tier1, ManageTest
from ocs import constants
from tests import helpers


log = logging.getLogger(__name__)

@pytest.fixture(scope='class')
def test_fixture(request):
    """
    Deleting a Storage class
    """
    self = request.node.cls

    def finalizer():
        teardown(self)
    request.addfinalizer(finalizer)
    setup(self)


def teardown(self):
    """
    Remove the resources after execution of tests
    """

def setup(self):
    """
    Setting up the Environment : Creating project
    """
    log.info("Creating project")
    NAMESPACE.new_project(project_name)


@tier1
@pytest.mark.usefixtures(
    test_fixture.__name__,
)
class TestStorageClass(ManageTest):

    @tier1
    def test_storage_class(self, request):
        """
        Test to validate OCS-297
        """
        global PVC, STORAGE_CLASS, NAMESPACE
        log.info("Creating StorageClass")
        STORAGE_CLASS = helpers.create_storage_class(
            constants.CEPHBLOCKPOOL, 'rbd'
        )
        log.info("Creating a PVC")
        PVC = helpers.create_pvc(STORAGE_CLASS.name)
        log.info("Deleting PVC")
        PVC.delete()
        log.info("Deleting Storage Class")
        STORAGE_CLASS.delete()
        log.info("Deleting project")
        NAMESPACE.delete(resource_name=project_name)

