import os
import logging
import ocs.defaults as defaults
import pytest
from ocsci import tier1, ManageTest
from ocs.ocp import OCP
from utility import utils, templating
from ocs.exceptions import CommandFailed

log = logging.getLogger(__name__)

RBD_SC_YAML = os.path.join("ocs-deployment", "storage-manifest.yaml")
TEMP_SC_YAML_FILE = '/tmp/tmp-storage-manifest.yaml'

RBD_PVC_YAML = os.path.join("ocs-deployment", "PersistentVolumeClaim.yaml")
TEMP_PVC_YAML_FILE = '/tmp/tmp-persistentVolumeClaim.yaml'

OCCLI = OCP(kind='service', namespace=defaults.ROOK_CLUSTER_NAMESPACE)


@pytest.fixture(scope='class')
def test_fixture(request):
    """
    This is a test fixture
    """

    def finalizer():
        teardown()
    request.addfinalizer(finalizer)


def teardown():
    """
    This is a tear down function

    """
    log.info("Deleting created temporary sc yaml file")
    assert OCCLI.delete(TEMP_SC_YAML_FILE)
    log.info("Successfully deleted temporary sc yaml file")
    utils.delete_file(TEMP_SC_YAML_FILE)
    utils.delete_file(TEMP_PVC_YAML_FILE)


@tier1
@pytest.mark.usefixtures(test_fixture.__name__)
class TestPvcCreationInvalidInputs(ManageTest):
    """
    This is a test for creating pvc with invalid inputs
    """
    def test_pvccreation_invalid_inputs(self):

        create_rbd_cephpool("autopool1119", "autosc1119")
        create_pvc_invalid_name(pvcname='@123')
        create_pvc_invalid_size(pvcsize='abcd')


def create_rbd_cephpool(poolname, storageclassname):
    """
    Creates rbd storage class and ceph pool

    Args:
        poolname (str): Name of the ceph pool to be created
        storageclassname (str): Name of the storage class

    Returns:
        None
    """

    data = {}
    data['metadata_name'] = poolname
    data['storage_class_name'] = storageclassname
    data['blockpool_name'] = poolname
    _templating = templating.Templating()
    tmp_yaml_file = _templating.render_template(RBD_SC_YAML, data)

    with open(TEMP_SC_YAML_FILE, 'w') as fd:
        fd.write(tmp_yaml_file)
        log.info(f"Creating RBD pool and storage class")
    assert OCCLI.create(TEMP_SC_YAML_FILE)
    log.info(
        f"RBD pool: {poolname} storage class: {storageclassname}"
        " created successfully"
    )
    log.debug(TEMP_SC_YAML_FILE)


def create_pvc_invalid_name(pvcname):
    """
    Creates a pvc with an user provided data

    Args:
        pvcname (str): Name of the pvc to be created

    Returns:
        None
    """
    data = {}
    data['pvc_name'] = pvcname
    _templating = templating.Templating()
    tmp_yaml_file = _templating.render_template(RBD_PVC_YAML, data)
    with open(TEMP_PVC_YAML_FILE, 'w') as fd:
        fd.write(tmp_yaml_file)
        log.info(f"Creating a pvc with name {pvcname}")
    log.info(tmp_yaml_file)
    try:
        OCCLI.create(
            yaml_file=tmp_yaml_file, resource_name="PersistentVolumeClaim"
        )
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
    data = {}
    data['pvc_size'] = pvcsize
    _templating = templating.Templating()
    tmp_yaml_file = _templating.render_template(RBD_PVC_YAML, data)
    with open(TEMP_PVC_YAML_FILE, 'w') as fd:
        fd.write(tmp_yaml_file)
        log.info(f"Creating a pvc with size {pvcsize}")
    log.debug(tmp_yaml_file)
    try:
        OCCLI.create(
            yaml_file=tmp_yaml_file, resource_name="PersistentVolumeClaim"
        )
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
