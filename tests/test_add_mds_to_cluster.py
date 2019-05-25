"""
A test for creating a CephFS
"""
import logging
import ocs.defaults as defaults
import yaml
import os
import pytest

from ocsci import tier1, ManageTest
from ocs import ocp
from munch import munchify
from utility import utils, templating

log = logging.getLogger(__name__)

CEPHFS_YAML = os.path.join("templates/ocs-deployment", "cephfilesystem.yaml")
TEMP_YAML_FILE = 'test_cephfilesystem.yaml'
CEPHFS_DELETED = '"{cephfs_name}" deleted'

CEPHFS = ocp.OCP(
    kind='CephFilesystem', namespace=defaults.ROOK_CLUSTER_NAMESPACE
)
POD = ocp.OCP(kind='Pod', namespace=defaults.ROOK_CLUSTER_NAMESPACE)


@pytest.fixture(scope='class')
def test_fixture(request):
    """
    Create disks
    """
    self = request.node.cls

    def finalizer():
        teardown(self)
    request.addfinalizer(finalizer)
    setup(self)


def setup(self):
    """
    Setting up the environment for the test
    """
    assert create_ceph_fs(self.fs_data)
    assert verify_fs_exist(2)


def teardown(self):
    """
    Tearing down the environment
    """
    assert delete_fs(self.fs_name)

    utils.delete_file(TEMP_YAML_FILE)


def create_ceph_fs(data):
    """
    Create a new Ceph File System
    """

    file_y = templating.generate_yaml_from_jinja2_template_with_data(
        CEPHFS_YAML, **data
    )
    with open(TEMP_YAML_FILE, 'w') as yaml_file:
        yaml.dump(file_y, yaml_file, default_flow_style=False)
    log.info(f"Creating a new Ceph FileSystem")
    assert CEPHFS.create(yaml_file=TEMP_YAML_FILE)
    assert POD.wait_for_resource(
        condition='Running', selector='app=rook-ceph-mds'
    )
    pods = POD.get(selector='app=rook-ceph-mds')['items']
    if len(pods) == 2:
        return True
    return False


def modify_fs(new_active_count):
    """
    Modifying a ceph FS yaml file
    """
    with open(TEMP_YAML_FILE, 'r') as yaml_file:
        cephfs_obj = munchify(yaml.safe_load(yaml_file))
    cephfs_obj.spec.metadataServer.activeCount = new_active_count
    with open(TEMP_YAML_FILE, 'w') as yaml_file:
        yaml.dump(cephfs_obj.toDict(), yaml_file, default_flow_style=False)
    log.info(f"Change the active_count to {new_active_count}")
    assert CEPHFS.apply(yaml_file=TEMP_YAML_FILE)
    return True


def delete_fs(fs_name):
    """
    Deleting a ceph FS
    """
    log.info(f"Deleting the file system")
    stat = CEPHFS.delete(resource_name=fs_name)
    if CEPHFS_DELETED.format(cephfs_name=fs_name) in stat:
        return POD.wait_for_resource(
            condition='', selector='app=rook-ceph-mds', to_delete=True
        )
    return False


def verify_fs_exist(pod_count):
    """
    Verifying if a ceph FS exist
    """
    assert POD.wait_for_resource(
        condition='Running', selector='app=rook-ceph-mds',
        resource_count=pod_count
    )
    pods = POD.get(selector='app=rook-ceph-mds')['items']
    if len(pods) == pod_count:
        return True
    return False


@tier1
@pytest.mark.usefixtures(
    test_fixture.__name__,
)
class TestCephFilesystemCreation(ManageTest):
    """
    Testing creation of Ceph FileSystem
    """
    fs_data = {}
    fs_name = 'my-cephfs1'
    fs_data['fs_name'] = fs_name
    new_active_count = 2

    def test_cephfilesystem_creation(self):
        """
        Creating a Ceph Filesystem
        """
        assert modify_fs(self.new_active_count)
        assert verify_fs_exist(self.new_active_count * 2)
