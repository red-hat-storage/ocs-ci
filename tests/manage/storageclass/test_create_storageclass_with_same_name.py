import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework import config
from ocs_ci.framework.testlib import tier1, ManageTest
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility import templating

log = logging.getLogger(__name__)

SC_OBJ = None


@pytest.fixture(scope="class")
def test_fixture(request):
    """
    This fixture defines the teardown function.
    """

    request.addfinalizer(teardown)


def teardown():
    """
    Tearing down the environment

    """
    if SC_OBJ:
        log.info(f"Deleting created storage class: {SC_OBJ.name}")
        SC_OBJ.delete()
        log.info(f"Storage class: {SC_OBJ.name} deleted successfully")


def create_storageclass(sc_name, expect_fail=False):
    """
    Function to create a storage class and check for
    duplicate storage class name

    Args:
        sc_name (str): name of the storageclass to be created
        expect_fail (bool): To catch the incorrect scenario if
            two SCs are indeed created with same name

    Returns:
        None

    """

    # Create a storage class
    sc_data = templating.load_yaml(constants.CSI_RBD_STORAGECLASS_YAML)
    sc_data["metadata"]["name"] = sc_name
    sc_data["parameters"]["clusterID"] = config.ENV_DATA["cluster_namespace"]

    global SC_OBJ
    SC_OBJ = OCS(**sc_data)

    # Check for expected failure with duplicate SC name
    try:
        SC_OBJ.create()
        assert not expect_fail, "SC creation with same name passed. Expected to fail !"
        log.info(f"Storage class: {SC_OBJ.name} created successfully !")
        log.debug(sc_data)

    except CommandFailed as ecf:
        assert "AlreadyExists" in str(ecf)
        log.info(
            f"Cannot create two StorageClasses with same name !"
            f" Error message:  \n"
            f"{ecf}"
        )


@tier1
@pytest.mark.usefixtures(
    test_fixture.__name__,
)
@pytest.mark.polarion_id("OCS-322")
class TestCreateSCSameName(ManageTest):
    def test_create_storageclass_with_same_name(self):
        """
        To test that Storageclass creation with duplicate names is not allowed
        """

        sc_name = "ocs-322-sc"
        create_storageclass(sc_name)
        log.info(
            f"Attempting to create another storageclass "
            f"with duplicate name {sc_name}"
        )
        create_storageclass(sc_name, expect_fail=True)
