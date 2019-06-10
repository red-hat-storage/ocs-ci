"""
A test for creating a PV
"""
import logging
import yaml
import pytest

from ocsci.config import ENV_DATA
from ocs.defaults import TEMPLATE_PV_PVC_DIR
from ocsci.testlib import tier1, ManageTest
from ocs import exceptions
from ocs import ocp
from utility import utils, templating


log = logging.getLogger(__name__)

PV_YAML = f"{TEMPLATE_PV_PVC_DIR}/PersistentVolume.yaml"
TEMP_YAML_FILE = 'test.yaml'
VOLUME_DELETED = 'persistentvolume "{volume_name}" deleted'


OCP = ocp.OCP(
    kind='PersistentVolume', namespace=ENV_DATA['cluster_namespace']
)


@pytest.fixture(scope='class')
def test_fixture(request):
    """
    Create disks
    """
    self = request.node.cls

    def finalizer():
        teardown(self)
    request.addfinalizer(finalizer)


def teardown(self):
    """
    Tearing down the environment
    """
    assert delete_pv(self.pv_name)
    assert not verify_pv_exist(self.pv_name)
    utils.delete_file(TEMP_YAML_FILE)


def create_pv(pv_data):
    """
    Create a new Persistent Volume
    """
    file_y = templating.generate_yaml_from_jinja2_template_with_data(
        PV_YAML, **pv_data
    )
    with open(TEMP_YAML_FILE, 'w') as yaml_file:
        yaml.dump(file_y, yaml_file, default_flow_style=False)
        log.info(f"Creating new Persistent Volume")
    assert OCP.create(yaml_file=TEMP_YAML_FILE)
    return OCP.wait_for_resource(
        resource_name=pv_data['pv_name'], condition='Available'
    )


def delete_pv(pv_name):
    """
    Delete a Persistent Volume by given name
    """
    log.info(f"Deleting the Persistent Volume {pv_name}")
    stat = OCP.delete(TEMP_YAML_FILE)
    if stat in VOLUME_DELETED.format(volume_name=pv_name):
        return True
    return False


def verify_pv_exist(pv_name):
    """
    Verify a Persistent Volume exists by a given name
    """
    try:
        OCP.get(pv_name)
    except exceptions.CommandFailed:
        log.info(f"PV {pv_name} doesn't exist")
        return False
    log.info(f"PV {pv_name} exist")
    return True


@tier1
@pytest.mark.usefixtures(
    test_fixture.__name__,
)
class TestPvCreation(ManageTest):
    """
    Testing PV creation
    """
    pv_data = {}
    pv_name = 'my-pv1'
    pv_data['pv_name'] = pv_name
    pv_data['pv_size'] = '3Gi'

    def test_pv_creation(self):
        """
        Test PV creation
        """
        assert create_pv(self.pv_data)
        assert verify_pv_exist(self.pv_name)
