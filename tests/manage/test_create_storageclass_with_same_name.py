import logging
import pytest

from tests import helpers
from ocs import defaults
from ocsci.testlib import tier1, ManageTest
from resources.ocs import OCS
from ocs.exceptions import CommandFailed
from ocsci import config

log = logging.getLogger(__name__)

SC_OBJ = None


@pytest.fixture(scope='class')
def test_fixture(request):
    """
    This fixture defines the teardown function.
    """

    request.addfinalizer(teardown)


def teardown():
    """
    Tearing down the environment

    """
    log.info(
        f"Deleting created storage class: {SC_OBJ.name}"
    )
    SC_OBJ.delete()
    log.info(
        f"Storage class: {SC_OBJ.name} deleted successfully"
    )


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
    namespace = config.ENV_DATA["cluster_namespace"]
    mons = (
        f'rook-ceph-mon-a.{namespace}'
        f'.svc.cluster.local:6789,'
        f'rook-ceph-mon-b.{namespace}.'
        f'svc.cluster.local:6789,'
        f'rook-ceph-mon-c.{namespace}'
        f'.svc.cluster.local:6789'
    )
    log.info("Creating a Storage Class")
    sc_data = helpers.get_crd_dict(defaults.CSI_RBD_STORAGECLASS_DICT)
    sc_data['metadata']['name'] = sc_name
    sc_data['parameters']['monitors'] = mons

    global SC_OBJ
    SC_OBJ = OCS(**sc_data)

    # Check for expected failure with duplicate SC name
    try:
        SC_OBJ.create()
        assert not expect_fail, (
            "SC creation with same name passed. Expected to fail !!"
        )
        log.info(
            f"Storage class: {SC_OBJ.name} created successfully !!"
        )
        log.debug(sc_data)

    except CommandFailed as ecf:
        assert "AlreadyExists" in str(ecf)
        log.error(
            f"Cannot create two StorageClasses with same name !! \n"
            f"{ecf}"
        )


@tier1
@pytest.mark.usefixtures(
    test_fixture.__name__,
)
@pytest.mark.polarion_id("OCS-322")
class TestCaseOCS322(ManageTest):
    def test_create_storageclass_with_same_name(self):
        """
        To test that Storageclass creation with duplicate names is not allowed

        TC Name = https://polarion.engineering.redhat.com/polarion/#/project/
        OpenShiftContainerStorage/workitem?id=OCS-322
        """

        sc_name = "ocs-322-sc"
        create_storageclass(sc_name)
        log.info(
            f"Attempting to create a storageclass "
            f"with duplicate name {sc_name}"
        )
        create_storageclass(sc_name, expect_fail=True)
