"""
A test for deleting an existing PVC and create a new PVC with the same name
"""
import base64
import logging
import os
import pytest
import yaml

from ocs import ocp
from ocsci.config import ENV_DATA
from ocsci.testlib import ManageTest, tier1
from utility import templating, utils

log = logging.getLogger(__name__)

TEMPLATES_DIR = "templates/csi-templates"
TEMP_YAML = os.path.join(TEMPLATES_DIR, "temp.yaml")

CBP = ocp.OCP(kind='CephBlockPool', namespace=ENV_DATA['cluster_namespace'])
CFS = ocp.OCP(kind='CephFilesystem', namespace=ENV_DATA['cluster_namespace'])
OCP = ocp.OCP(kind='Service', namespace=ENV_DATA['cluster_namespace'])
POD = ocp.OCP(kind='Pod', namespace=ENV_DATA['cluster_namespace'])
PVC = ocp.OCP(
    kind='PersistentVolumeClaim', namespace=ENV_DATA['cluster_namespace']
)
SECRET = ocp.OCP(kind='Secret', namespace='default')
SC = ocp.OCP(kind='StorageClass', namespace=ENV_DATA['cluster_namespace'])


@pytest.fixture(params=['rbd', 'cephfs'])
def test_fixture(request):
    """
    This is a test fixture
    """
    interface = request.param
    setup(interface)
    yield interface
    teardown(interface)


def setup(interface):
    """
    Setting up the environment for the test
    """
    log.info(f"Setting up environment for: {interface}")
    name = f"{interface}-test324"

    assert create_secret(interface=interface)
    if 'rbd' in interface:
        assert create_rbd_pool(pool_name=name)
        assert validate_pool_creation(pool_name=name)
        assert create_storageclass(interface=interface, pool_name=name)
    elif 'cephfs' in interface:
        assert create_ceph_fs(fs_name=name)
        assert validate_pool_creation(pool_name=f"{name}-data0")
        assert create_storageclass(
            interface=interface, pool_name=f"{name}-data0"
        )


def teardown(interface):
    """
    Tearing down the environment
    """
    log.info(f"Tearing down the environment of: {interface}")
    name = f"{interface}-test324"

    assert delete_pvc(interface=interface)
    assert delete_storageclass(interface=interface)
    if 'rbd' in interface:
        assert delete_rbd_pool(pool_name=name)
    elif 'cephfs' in interface:
        assert delete_ceph_fs(fs_name=name)

    assert delete_secret(interface=interface)
    utils.delete_file(TEMP_YAML)


def create_secret(interface):
    """
    Creates secret in oc using admin key
    """
    log.info(f"Creating secret for {interface}")
    secret_data = {'base64_encoded_admin_password': get_admin_key()}
    template = os.path.join(TEMPLATES_DIR, f"csi-{interface}-secret.yaml")
    dump_to_temp_yaml(template, TEMP_YAML, **secret_data)
    assert SECRET.create(yaml_file=TEMP_YAML)
    return True


def create_ceph_fs(fs_name):
    """
    Creates a new Ceph File System
    """
    log.info(f"Adding CephFS with name {fs_name}")
    fs_data = {'fs_name': fs_name}
    template = os.path.join("templates/ocs-deployment", "cephfilesystem.yaml")
    dump_to_temp_yaml(template, TEMP_YAML, **fs_data)
    assert CFS.create(yaml_file=TEMP_YAML)
    return POD.wait_for_resource(
        condition='Running', selector='app=rook-ceph-mds', resource_count=2
    )


def create_rbd_pool(pool_name):
    """
    Creates a rbd pool with specified name
    """
    log.info(f"Creating a new CephBlockPool with name {pool_name}")
    pool_data = {'rbd_pool': pool_name}
    template = os.path.join(TEMPLATES_DIR, "CephBlockPool.yaml")
    dump_to_temp_yaml(template, TEMP_YAML, **pool_data)
    assert CBP.create(yaml_file=TEMP_YAML)
    assert CBP.get(resource_name=pool_name)
    return True


def create_storageclass(interface, pool_name):
    """
    Creates a new StorageClass
    """
    log.info(
        f"Creating a storageclass for {interface} with {pool_name} as datapool"
    )
    sc_data = {}
    sc_data[f"{interface}_storageclass_name"] = f"csi-{interface}-sc"
    if 'rbd' in interface:
        sc_data['rbd_pool'] = pool_name
    elif 'cephfs' in interface:
        sc_data['ceph_data_pool'] = pool_name

    template = os.path.join(
        TEMPLATES_DIR, f"csi-{interface}-storageclass.yaml"
    )
    dump_to_temp_yaml(template, TEMP_YAML, **sc_data)
    assert SC.create(yaml_file=TEMP_YAML)
    assert SC.get(resource_name=f"csi-{interface}-sc")
    return True


def create_pvc(interface):
    """
    Creates a new PVC
    """
    log.info(f"Creating a PVC")
    pvc_data = {}
    pvc_data['pvc_name'] = f"csi-{interface}-pvc"
    pvc_data['user_namespace'] = ENV_DATA['cluster_namespace']
    pvc_data['sc_name'] = f"csi-{interface}-sc"

    template = os.path.join(TEMPLATES_DIR, "PersistentVolumeClaim.yaml")
    dump_to_temp_yaml(template, TEMP_YAML, **pvc_data)
    assert PVC.create(yaml_file=TEMP_YAML)
    return PVC.wait_for_resource(
        condition='Bound', resource_name=f"csi-{interface}-pvc"
    )


# TODO: Use function from ocp.py PR #136
def get_admin_key():
    """
    Fetches admin key secret from ceph
    """
    out = ocp.exec_ceph_cmd('ceph auth get-key client.admin')
    base64_output = base64.b64encode(out['key'].encode()).decode()
    return base64_output


def delete_pvc(interface):
    """
    Deletes an existing PVC
    """
    log.info(f"Deleting a PVC")
    assert PVC.delete(resource_name=f"csi-{interface}-pvc")
    return True


def delete_storageclass(interface):
    """
    Deletes an existing StorageClass
    """
    log.info(f"Deleting the storageclass for {interface}")
    assert SC.delete(resource_name=f"csi-{interface}-sc")
    return True


def delete_rbd_pool(pool_name):
    """
    Deletes CephBlockpool with specified name
    """
    log.info(f"Deleting CephBlockPool with name {pool_name}")
    assert CBP.delete(resource_name=pool_name)
    return True


def delete_ceph_fs(fs_name):
    """
    Deletes an existing Ceph File System
    """
    log.info(f"Deleting CephFS with name {fs_name}")
    assert CFS.delete(resource_name=fs_name)
    return CFS.wait_for_resource(
        condition='', selector='app=rook-ceph-mds', to_delete=True
    )


def delete_secret(interface):
    """
    Deletes secret in oc
    """
    log.info(f"Deleting secret for {interface}")
    assert SECRET.delete(resource_name=f"csi-{interface}-secret")
    return True


def validate_pool_creation(pool_name):
    """
    Validates pool creation at ceph side
    """
    out = ocp.exec_ceph_cmd("ceph osd lspools")
    for item in out:
        if item['poolname'] == pool_name:
            log.info(f"{pool_name} pool is created successfully at CEPH side")
            return True
    log.info(f"{pool_name} pool is not created at CEPH side")
    return False


# TODO: Use function from templating.py PR #132
def dump_to_temp_yaml(src_file, dst_file, **kwargs):
    """
    Dump a jinja2 template file content into a yaml file
    Args:
        src_file (str): Template Yaml file path
        dst_file: the path to the destination Yaml file
    """
    data = templating.generate_yaml_from_jinja2_template_with_data(
        src_file, **kwargs
    )
    with open(dst_file, 'w') as yaml_file:
        yaml.dump(data, yaml_file)


@tier1
class TestCaseOCS324(ManageTest):
    """
    Delete PVC and create a new PVC with same name
    https://polarion.engineering.redhat.com/polarion/#/project
    /OpenShiftContainerStorage/workitem?id=OCS-324
    TC Description: The purpose of this test case is to delete an existing PVC
    and create a new PVC with the same name.
    """
    def test_ocs_324(self, test_fixture):
        """
        TC OCS 324
        """
        assert create_pvc(interface=test_fixture)
        assert delete_pvc(interface=test_fixture)
        assert create_pvc(interface=test_fixture)
