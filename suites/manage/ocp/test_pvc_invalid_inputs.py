import logging
import pytest

from ocs import defaults
from ocsci.config import ENV_DATA
from ocsci.testlib import tier1, ManageTest
from resources.ocs import OCS
from tests import helpers
from ocs.ocp import OCP
from ocs.exceptions import CommandFailed

log = logging.getLogger(__name__)

OCCLI = OCP(kind='service', namespace=ENV_DATA['cluster_namespace'])

SC_OBJ = None
SC_NAME = None


@pytest.fixture(scope='class')
def test_fixture(request):
    """
    This is a test fixture
    """
    self = request.node.cls

    def finalizer():
        teardown()
    request.addfinalizer(finalizer)
    setup(self)


def setup(self):
    """
    Setting up the environment for the test
    """
    # Create a storage class
    log.info("Creating a Storage Class")
    self.sc_data = defaults.STORAGECLASS_DICT.copy()
    self.sc_data['metadata']['name'] = helpers.create_unique_resource_name(
        'test', 'csi-rbd'
    )
    global SC_OBJ
    global SC_NAME
    SC_OBJ = OCS(**self.sc_data)
    SC_NAME = self.sc_data['metadata']['name']
    assert SC_OBJ.create()
    log.info(f"Storage class: {SC_NAME} created successfully")
    log.debug(self.sc_data)
    return SC_NAME


def teardown():
    """
    Tearing down the environment

    """
    log.info(f"Deleting created storage class: {SC_NAME}")
    SC_OBJ.delete()
    log.info(f"Storage class: {SC_NAME} deleted successfully")


@tier1
@pytest.mark.usefixtures(test_fixture.__name__)
class TestPvcCreationInvalidInputs(ManageTest):
    """
    This is the class for creating pvc with invalid inputs
    """
    def test_pvccreation_invalid_inputs(self):
        """
        Calling functions for pvc invalid name and size
        """
        create_pvc_invalid_name(pvcname='@123')
        create_pvc_invalid_size(pvcsize='t@st')


def create_pvc_invalid_name(pvcname):
    """
    Creates a pvc with an user provided data

    Args:
        pvcname (str): Name of the pvc to be created

    Returns:
        None
    """
    pvc_data = defaults.PVC_DICT.copy()
    pvc_data['metadata']['name'] = pvcname
    pvc_data['spec']['storageClassName'] = SC_NAME
    PVC_OBJ = OCS(**pvc_data)
    log.info(f"Creating a pvc with name {pvcname}")
    try:
        PVC_OBJ.create()
    except CommandFailed as ex:
        if "error" in str(ex):
            log.info(
                f"PVC creation failed with error \n {ex} \n as "
                "invalid pvc name is provided. EXPECTED"
            )
        else:
            assert (
                "PVC creation with invalid name succeeded : "
                "NOT expected"
            )


def create_pvc_invalid_size(pvcsize):
    """
    Creates a pvc with an user provided data

    Args:
        pvcsize (str): Size of the pvc to be created

    Returns:
        None
    """
    pvc_data = defaults.PVC_DICT.copy()
    pvc_data['metadata']['name'] = "auto"
    pvc_data['spec']['resources']['requests']['storage'] = pvcsize
    pvc_data['spec']['storageClassName'] = SC_NAME
    PVC_OBJ = OCS(**pvc_data)
    log.info(f"Creating a PVC with size {pvcsize}")
    try:
        PVC_OBJ.create()
    except CommandFailed as ex:
        if "error" in str(ex):
            log.info(
                f"PVC creation failed with error \n {ex} \n as "
                "invalid pvc size is provided. EXPECTED"
            )
        else:
            assert (
                "PVC creation with invalid size succeeded : "
                "NOT expected"
            )
