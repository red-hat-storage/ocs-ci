import logging

import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import tier3, ManageTest
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.resources.pvc import PVC
from ocs_ci.helpers import helpers
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility import templating

log = logging.getLogger(__name__)

SC_OBJ = None


@pytest.fixture(scope="class")
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
    self.sc_data = templating.load_yaml(constants.CSI_RBD_STORAGECLASS_YAML)
    self.sc_data["metadata"]["name"] = helpers.create_unique_resource_name(
        "test", "csi-rbd"
    )
    global SC_OBJ
    SC_OBJ = OCS(**self.sc_data)
    assert SC_OBJ.create()
    log.info(f"Storage class: {SC_OBJ.name} created successfully")
    log.debug(self.sc_data)


def teardown():
    """
    Tearing down the environment

    """
    log.info(f"Deleting created storage class: {SC_OBJ.name}")
    SC_OBJ.delete()
    log.info(f"Storage class: {SC_OBJ.name} deleted successfully")


@green_squad
@tier3
@pytest.mark.usefixtures(test_fixture.__name__)
@pytest.mark.polarion_id("OCS-284")
class TestPvcCreationInvalidInputs(ManageTest):
    """
    PVC creation with invaid inputs in pvc yaml
    """

    def test_pvccreation_invalid_inputs(self):
        """
        Calling functions for pvc invalid name and size
        """
        create_pvc_invalid_name(pvcname="@123")
        create_pvc_invalid_size(pvcsize="t@st")


def create_pvc_invalid_name(pvcname):
    """
    Creates a pvc with an user provided data

    Args:
        pvcname (str): Name of the pvc to be created

    Returns:
        None
    """
    pvc_data = templating.load_yaml(constants.CSI_PVC_YAML)
    pvc_data["metadata"]["name"] = pvcname
    pvc_data["spec"]["storageClassName"] = SC_OBJ.name
    pvc_obj = PVC(**pvc_data)
    log.info(f"Creating a pvc with name {pvcname}")
    try:
        pvc_obj.create()
    except CommandFailed as ex:
        error = (
            "subdomain must consist of lower case alphanumeric "
            "characters, '-' or '.', and must start and end with "
            "an alphanumeric character"
        )
        if error in str(ex):
            log.info(
                f"PVC creation failed with error \n {ex} \n as "
                "invalid pvc name is provided. EXPECTED"
            )
        else:
            assert "PVC creation with invalid name succeeded : " "NOT expected"


def create_pvc_invalid_size(pvcsize):
    """
    Creates a pvc with an user provided data

    Args:
        pvcsize (str): Size of the pvc to be created

    Returns:
        None
    """
    pvc_data = templating.load_yaml(constants.CSI_PVC_YAML)
    pvc_data["metadata"]["name"] = "auto"
    pvc_data["spec"]["resources"]["requests"]["storage"] = pvcsize
    pvc_data["spec"]["storageClassName"] = SC_OBJ.name
    pvc_obj = PVC(**pvc_data)
    log.info(f"Creating a PVC with size {pvcsize}")
    try:
        pvc_obj.create()
    except CommandFailed as ex:
        error = (
            "quantities must match the regular expression '^([+-]?[0-9.]"
            "+)([eEinumkKMGTP]*[-+]?[0-9]*)$'"
        )
        if error in str(ex):
            log.info(
                f"PVC creation failed with error \n {ex} \n as "
                "invalid pvc size is provided. EXPECTED"
            )
        else:
            assert "PVC creation with invalid size succeeded : " "NOT expected"
