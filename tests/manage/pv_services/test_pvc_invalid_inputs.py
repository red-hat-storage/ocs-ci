import logging

import pytest

from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.resources.pvc import PVC
from tests import helpers
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility import templating
from ocs_ci.ocs import constants, defaults
from ocs_ci.framework.testlib import ManageTest, tier3
log = logging.getLogger(__name__)

SC_OBJ = None


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
    self.sc_data = templating.load_yaml(
        constants.CSI_RBD_STORAGECLASS_YAML
    )
    self.sc_data['metadata']['name'] = helpers.create_unique_resource_name(
        'test', 'csi-rbd'
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


@tier3
@pytest.mark.usefixtures(test_fixture.__name__)
@pytest.mark.parametrize(
    argnames=['volume_mode', 'access_mode'],
    argvalues=[
        pytest.param(
            *['Block', constants.ACCESS_MODE_RWX],
            marks=[pytest.mark.polarion_id("OCS-875"), pytest.mark.polarion_id("OCS-883")]),
        pytest.param(*['Block', constants.ACCESS_MODE_RWX],
                     marks=pytest.mark.polarion_id("OCS-882")
                     ),
        pytest.param(*['None', constants.ACCESS_MODE_RWO],
                     marks=pytest.mark.polarion_id("OCS-284")
                     )
    ])
class TestPvcCreationInvalidInputs(ManageTest):
    """
    PVC creation with invaid inputs in pvc yaml
    """

    def test_pvccreation_invalid_inputs(self, volume_mode, access_mode):
        """
        Calling functions for pvc invalid name and size
        """
        create_pvc_invalid_name(pvcname='@123', volume_mode=volume_mode, access_mode=access_mode)
        create_pvc_invalid_size(pvcsize='t@st', volume_mode=volume_mode, access_mode=access_mode)
        create_pvc_invalid_namespace(pvcsize='t@st', volume_mode=volume_mode, access_mode=access_mode, namespace='xyz')


def create_pvc_invalid_name(pvcname, volume_mode=None, access_mode=constants.ACCESS_MODE_RWO,
                            namespace=defaults.ROOK_CLUSTER_NAMESPACE
                            ):
    """
    Creates a pvc with an user provided data

    Args:
        pvcname (str): Name of the pvc to be created
        access_mode (str): The access mode to be used for the PVC
        volume_mode (str): Volume mode for rbd RWX pvc i.e. 'Block'
        namespace (str): The namespace for the PVC creation
    Returns:
        None
    """
    pvc_data = templating.load_yaml(constants.CSI_PVC_YAML)
    pvc_data['metadata']['name'] = pvcname
    pvc_data['metadata']['namespace'] = namespace
    pvc_data['spec']['storageClassName'] = SC_OBJ.name
    pvc_data['spec']['accessModes'] = [access_mode]
    if volume_mode:
        pvc_data['spec']['volumeMode'] = volume_mode
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
            assert (
                "PVC creation with invalid name succeeded : "
                "NOT expected"
            )


def create_pvc_invalid_size(pvcsize, volume_mode=None, access_mode=constants.ACCESS_MODE_RWO,
                            namespace=defaults.ROOK_CLUSTER_NAMESPACE
                            ):
    """
    Creates a pvc with an user provided data

    Args:
        pvcsize (str): Size of the pvc to be created

    Returns:
        None
    """
    pvc_data = templating.load_yaml(constants.CSI_PVC_YAML)
    pvc_data['metadata']['name'] = "auto"
    pvc_data['metadata']['namespace'] = namespace
    pvc_data['spec']['resources']['requests']['storage'] = pvcsize
    pvc_data['spec']['storageClassName'] = SC_OBJ.name
    pvc_data['spec']['accessModes'] = [access_mode]
    if volume_mode:
        pvc_data['spec']['volumeMode'] = volume_mode
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
            assert (
                "PVC creation with invalid size succeeded : "
                "NOT expected"
            )


def create_pvc_invalid_namespace(pvcsize, namespace=None, volume_mode=None, access_mode=constants.ACCESS_MODE_RWO):
    """
    Creates a pvc with an user provided data

    Args:
        pvcsize (str): Size of the pvc to be created

    Returns:
        None
    """
    pvc_data = templating.load_yaml(constants.CSI_PVC_YAML)
    pvc_data['metadata']['name'] = "auto"
    if namespace:
        pvc_data['metadata']['namespace'] = namespace

    pvc_data['spec']['resources']['requests']['storage'] = pvcsize
    pvc_data['spec']['storageClassName'] = SC_OBJ.name
    pvc_data['spec']['accessModes'] = [access_mode]
    if volume_mode:
        pvc_data['spec']['volumeMode'] = volume_mode
    pvc_obj = PVC(**pvc_data)
    log.info(f"Creating a PVC with namespace {namespace}")

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
                "invalid namespace is provided. EXPECTED"
            )
        else:
            assert (
                "PVC creation with invalid namespace succeeded : "
                "NOT expected"
            )
