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
from ocs_ci.utility import templating
from ocs_ci.ocs.resources.pvc import PVC
from ocs_ci.ocs.resources.ocs import OCS


log = logging.getLogger(__name__)
SC_OBJ = None


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
    log.info("Deleting the pvc")
    self.pvc.delete()


def setup(self):
    """
    Setting up the Environment : Creating project
    """


@tier1
@pytest.mark.usefixtures(
    test_fixture.__name__,
)
class TestStorageClass(ManageTest):

    def test_storage_class(self):
        """
        Test to validate OCS-297
        """
        # Create a storage class
        log.info("Creating a Storage Class")
        self.sc_data = templating.load_yaml_to_dict(
            constants.CSI_RBD_STORAGECLASS_YAML
        )
        self.sc_data['metadata']['name'] = helpers.create_unique_resource_name(
            'test', 'csi-rbd'
        )
        global SC_OBJ
        SC_OBJ = OCS(**self.sc_data)
        assert SC_OBJ.create()
        log.info("Storage class created successfully")
        log.debug(self.sc_data)

        # Create a pvc
        log.info("Creating a pvc")
        self.rbd_pvc = templating.load_yaml_to_dict(constants.CSI_RBD_PVC_YAML)
        pvc = PVC(**self.rbd_pvc)
        pvc.create()

        # Delete the storage class
        log.info("Deleting created storage class")
        SC_OBJ.delete()
        log.info("Storage class deleted successfully")
