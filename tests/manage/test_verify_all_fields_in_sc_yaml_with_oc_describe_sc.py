import pytest
import logging

from tests import helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility import templating
from ocs_ci.framework.testlib import ManageTest, tier1
from tests.fixtures import create_ceph_block_pool

log = logging.getLogger(__name__)
RBD_SC_OBJ = None
CEPHFS_SC_OBJ = None


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
    Setting up the environment
    *. Creating RBD and CephFS Secret
    *. Creating Ceph block pool
    """
    global RBD_SECRET_OBJ
    log.info("Creating RBD Secret")
    RBD_SECRET_OBJ = helpers.create_secret(constants.CEPHBLOCKPOOL)

    log.info("Creating CEPHFS Secret")
    global CEPHFS_SECRET_OBJ
    CEPHFS_SECRET_OBJ = helpers.create_secret(constants.CEPHFILESYSTEM)

    log.info("Creating RBD Ceph pool")
    global POOL
    POOL = helpers.create_ceph_block_pool()


def teardown():
    """
    Tearing down the environment
    """
    log.info(f"Deleting RBD storage class: {RBD_SC_OBJ.name}")
    assert RBD_SC_OBJ.delete()
    log.info(f"Deleting CEPHFS storage class: {CEPHFS_SC_OBJ.name}")
    assert CEPHFS_SC_OBJ.delete()
    log.info("RBD and CephFS Storage classes deleted successfully")
    log.info(f"Deleting RBD Secret: {RBD_SECRET_OBJ}")
    assert RBD_SECRET_OBJ.delete()
    log.info(f"Deleting CEPHFS Secret: {CEPHFS_SECRET_OBJ}")
    assert CEPHFS_SECRET_OBJ.delete()
    log.info("RBD and CephFS Secret's deleted successfully")
    log.info(f"Deleting Ceph block pool {POOL}")
    assert POOL.delete()
    log.info(f"Ceph block pool:{POOL} deleted successfully")


@tier1
@pytest.mark.usefixtures(test_fixture.__name__)
@pytest.mark.usefixtures(create_ceph_block_pool.__name__)
@pytest.mark.polarion_id("OCS-521")
class TestVerifyAllFieldsInScYamlWithOcDescribe(ManageTest):
    """
    Test class for storageclass detail check by oc describe
    """
    def test_verify_all_fields_in_sc_yaml_with_oc_describe_RBD(self):
        """
        Test function for RBD
        """
        log.info("Creating a RBD Storage Class")
        self.sc_data = templating.load_yaml_to_dict(
            constants.CSI_RBD_STORAGECLASS_YAML
        )
        self.sc_data['metadata']['name'] = helpers.create_unique_resource_name(
            'test', 'csi-rbd'
        )
        global RBD_SC_OBJ
        RBD_SC_OBJ = OCS(**self.sc_data)
        assert RBD_SC_OBJ.create()
        log.info(f"Storage class: {RBD_SC_OBJ.name} created successfully")
        log.debug(self.sc_data)

        # Get oc describe sc output
        describe_out = RBD_SC_OBJ.get("sc")
        log.debug(describe_out)

        # Confirm that sc yaml details matches oc describe sc output
        value = {
            k: describe_out[k] for k in set(describe_out) - set(self.sc_data)
        }
        if len(value) == 1 and value['volumeBindingMode'] == 'Immediate':
            log.info("OC describe sc output matches storage class yaml")
        else:
            assert ("OC describe sc output didn't match storage class yaml")


    def test_verify_all_fields_in_sc_yaml_with_oc_describe_CEPHFS(self):
        """
        Test function for CEPHFS
        """
        log.info("Creating a CephFS Storage Class")
        self.sc_data = templating.load_yaml_to_dict(
            constants.CSI_CEPHFS_STORAGECLASS_YAML
        )
        self.sc_data['metadata']['name'] = helpers.create_unique_resource_name(
            'test', 'csi-cephfs'
        )
        global CEPHFS_SC_OBJ
        CEPHFS_SC_OBJ = OCS(**self.sc_data)
        assert CEPHFS_SC_OBJ.create()
        log.info(f"Storage class: {CEPHFS_SC_OBJ.name} created successfully")
        log.debug(self.sc_data)

        # Get oc describe sc output
        describe_out = CEPHFS_SC_OBJ.get("sc")
        log.debug(describe_out)

        # Confirm that sc yaml details matches oc describe sc output
        value = {
            k: describe_out[k] for k in set(describe_out) - set(self.sc_data)
        }
        if len(value) == 1 and value['volumeBindingMode'] == 'Immediate':
            log.info("OC describe sc output matches storage class yaml")
        else:
            assert ("OC describe sc output didn't match storage class yaml")
