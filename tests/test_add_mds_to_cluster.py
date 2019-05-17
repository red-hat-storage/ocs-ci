"""
A test for creating a CephFS
"""
import logging
import ocs.defaults as defaults
import yaml
import os
import pytest

from ocs import ocp
from munch import munchify
from utility import utils, templating

log = logging.getLogger(__name__)

PV_YAML = os.path.join("templates/ocs-deployment", "cephfilesystem.yaml")
TEMP_YAML_FILE = 'test_cephfilesystem.yaml'
CEPHFS_DELETED = '"{cephfs_name}" deleted'

OCP = ocp.OCP(
    kind='CephFilesystem', namespace=defaults.ROOK_CLUSTER_NAMESPACE
)


def verify_fs_exist(self):
    """
    Verifying if a ceph FS exist
    """
    assert OCP.get(self.fs_name), f"CephFS {self.fs_name} doesn't exist"
    log.info(f"CephFS {self.fs_name} exist")


@pytest.fixture()
def create_ceph_fs(request):
    """
    Create a new Ceph File System
    """
    fs_data = {}
    request.node.cls.fs_name = 'my-cephfs1'
    fs_data['fs_name'] = request.node.cls.fs_name
    file_y = templating.generate_yaml_from_jinja2_template_with_data(
        PV_YAML, **fs_data
    )
    with open(TEMP_YAML_FILE, 'w') as yaml_file:
        yaml.dump(file_y, yaml_file, default_flow_style=False)
    log.info(f"Creating a new Ceph FileSystem")
    assert OCP.create(yaml_file=TEMP_YAML_FILE)
    assert verify_fs_exist(request.node.cls.fs_name)


@pytest.fixture()
def delete_fs(request):
    """
    Deleting a ceph FS
    """
    log.info(f"Deleting the file system")
    stat = OCP.delete(TEMP_YAML_FILE)
    assert not CEPHFS_DELETED.format(cephfs_name=request.node.cls.fs_name) in stat
    assert not verify_fs_exist(request.node.cls.fs_name)
    utils.delete_file(TEMP_YAML_FILE)


@pytest.mark.usefixtures(
    create_ceph_fs.__name__,
    delete_fs.__name__
)
class TestModifyFS:
    """
    TBD
    """
    new_active_count = '3'

    def verify_fs_exist(self):
        """
        Verifying if a ceph FS exist
        """
        assert OCP.get(self.fs_name), f"CephFS {self.fs_name} doesn't exist"
        log.info(f"CephFS {self.fs_name} exist")

    def test_modify_fs(self):
        """
        Modifying a ceph FS yaml file
        """
        with open(TEMP_YAML_FILE, 'r') as yaml_file:
            cephfs_obj = munchify(yaml.safe_load(yaml_file))
        cephfs_obj.spec.metadataServer.activeCount = int(new_active_count)
        with open(TEMP_YAML_FILE, 'w') as yaml_file:
            yaml.dump(cephfs_obj.toDict(), yaml_file, default_flow_style=False)
        log.info(f"Change the active_count to {new_active_count}")
        assert OCP.apply(yaml_file=TEMP_YAML_FILE)
