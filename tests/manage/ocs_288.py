"""
A test for creating pvc with random sc
"""
import base64
import logging
import os
import random
import string

import pytest
import yaml

from ocs import defaults
from ocs import ocp
from ocsci import ManageTest, tier1
from utility import templating

log = logging.getLogger(__name__)
CEPH_FILESYSTEM = os.path.join(
    "templates/ocs-deployment", "cephfilesystem.yaml"
)
SC_RBD_YAML = os.path.join(
    "templates/CSI/rbd", "storageclass.yaml"
)
SECRET_RBD = os.path.join(
    "templates/CSI/rbd", "secret.yaml"
)
SC_CEPHFS_YAML = os.path.join(
    "templates/CSI/cephfs", "storageclass.yaml"
)
RBD_POOL_YAML = os.path.join(
    "templates/CSI/rbd", "CephBlockPool.yaml"
)
PVC_RBD_YAML = os.path.join(
    "templates/CSI/rbd", "pvc.yaml"
)
SECRET_CEPHFS = os.path.join(
    "templates/CSI/cephfs", "secret.yaml"
)
TEMP_YAML_FILE = 'temp.yaml'
TEMP_YAML_FILE_FS = 'temp_fs.yaml'

SC = ocp.OCP(
    kind='StorageClass', namespace=defaults.ROOK_CLUSTER_NAMESPACE
)
POOL = ocp.OCP(
    kind='CephBlockPool', namespace=defaults.ROOK_CLUSTER_NAMESPACE
)
POD = ocp.OCP(
    kind='Pod', namespace=defaults.ROOK_CLUSTER_NAMESPACE
)
SECRET = ocp.OCP(
    kind='Secret', namespace="default"
)
PVC = ocp.OCP(
    kind='PersistentVolumeClaim', namespace=defaults.ROOK_CLUSTER_NAMESPACE
)
CFS = ocp.OCP(
    kind='CephFilesystem', namespace=defaults.ROOK_CLUSTER_NAMESPACE
)


@pytest.fixture(scope='class')
def test_fixture(request):

    def finalizer():
        teardown()

    request.addfinalizer(finalizer)
    setup()


def setup():
    """
    Setting up the environment for the test
    """
    assert create_secret(file_name=SECRET_RBD)
    assert create_secret(file_name=SECRET_CEPHFS)
    assert create_cephfilesystem()
    assert create_multiple_rbd_storageclasses(count=5)
    assert create_storageclass_cephfs()


def teardown():
    """
    Tearing down the environment
    """
    assert delete_pvc()
    assert delete_cephblockpool()
    assert delete_storageclass()
    assert delete_cephfilesystem(file_name=TEMP_YAML_FILE_FS)
    assert delete_secret()


def delete_pvc():
    """
    Function to delete pvc
    :param pvc_list:
    :return:
    """
    pvc_list = get_pvc()
    for item in pvc_list:
        log.info(f"Deleting pvc {item}")
        assert PVC.delete(resource_name=item)
    return True


def delete_storageclass():
    """
    Function for deleting Storageclass and CephBlockPool
    :param storageclass_list:
    :return:
    """
    storageclass_list = get_storageclass()
    for item in storageclass_list:
        log.info(f"Deleting StorageClass with name {item}")
        assert SC.delete(resource_name=item)
    return True


def delete_cephblockpool():
    pool_list = get_cephblockpool()
    for item in pool_list:
        log.info(f"Deleting CephBlockPool with name {item}")
        assert POOL.delete(resource_name=item)
    return True


def delete_cephfilesystem(file_name):
    """
    Function to delete CephFileSystem
    :param file_name:
    :return:
    """
    log.info("Deleting CephFileSystem")
    assert CFS.delete(yaml_file=file_name)
    return True


def delete_secret():
    """
    Function to delete Secret
    :param secret_list:
    :return:
    """
    secret_list = get_secret()
    for item in secret_list:
        log.info(f"Deleting secret {item}")
        assert SECRET.delete(resource_name=item)
    return True


def create_rbd_pool(pool_name):
    """
    Create Blockpool with specified name

    :param pool_name:
    :return:
    """
    pool_data = {}
    pool_data['rbd_pool'] = pool_name
    file_y = templating.generate_yaml_from_jinja2_template_with_data(
        RBD_POOL_YAML, **pool_data
    )
    with open(TEMP_YAML_FILE, 'w') as yaml_file:
        yaml.dump(file_y, yaml_file, default_flow_style=False)
    log.info(f"Creating a new CephBlockPool with name {pool_name}")
    assert POOL.create(yaml_file=TEMP_YAML_FILE)
    return True


def validate_pool_creation(pool_name):
    """
    Check wether Blockpool and pool is created or not at ceph and as well
    k8s side

    :param pool_name:
    :return:
    """
    ceph_validate = False
    k8s_validate = False
    cmd = "ceph osd lspools"
    out = ocp.exec_ceph_cmd(cmd)
    for item in out:
        if item['poolname'] == pool_name:
            ceph_validate = True
    pool_obj = POOL.get()
    sample = pool_obj['items']
    for item in sample:
        if item.metadata.name == pool_name:
            k8s_validate = True
    if ceph_validate and k8s_validate:
        return True
    else:
        if not ceph_validate:
            log.error(f"{pool_name} pool wasn't created at CEPH side")
        if not k8s_validate:
            log.error(f"{pool_name} pool wasn't created at K8S side")
        return False


def validate_storageclass(sc_name):
    """
    Validate if storageClass is been created or not
    """
    sc_obj = SC.get(resource_name=sc_name)
    if sc_obj['metadata']['name']:
        log.info(f"StorageClass got created")
        return True
    return False


def create_cephfilesystem():
    """
    Function for deploying CephFileSystem (MDS)
    :param kwargs:
    :return:
    """
    file_y = templating.generate_yaml_from_jinja2_template_with_data(
        CEPH_FILESYSTEM
    )
    with open(TEMP_YAML_FILE_FS, 'w') as yaml_file:
        yaml.dump(file_y, yaml_file, default_flow_style=False)
    log.info(f"Creating a new CephFileSystem")
    assert CFS.create(yaml_file=TEMP_YAML_FILE_FS)
    fs_name = file_y.get('metadata')['name']
    POD.wait_for_resource(condition="Running", selector='app=rook-ceph-mds')
    assert validate_cephfilesystem(fs_name=fs_name)
    return True


def validate_cephfilesystem(fs_name):
    """
    Function for validating CephFileSystem Creation
    :param fs_name:
    :return:
    """
    ceph_validate = False
    k8s_validate = False
    cmd = "ceph fs ls"
    out = ocp.exec_ceph_cmd(cmd)
    if out:
        out = out[0]['name']
        if out == fs_name:
            log.info("FileSystem got created from Ceph Side")
            ceph_validate = True
        else:
            log.error("FileSystem was not present at Ceph Side")
            return False
    result = CFS.get(resource_name=fs_name)
    if result['metadata']['name']:
        log.info(f"Filesystem got created from kubernetes Side")
        k8s_validate = True
    else:
        log.error("Filesystem was not create at Kubernetes Side")
        return False
    return True if (ceph_validate and k8s_validate) else False


def create_multiple_rbd_storageclasses(count=1):
    """
    Function for creating multiple rbd storageclass
    :param count:
    :return:
    """
    for sc_count in range(count):
        kwargs = generate_pool_and_sc_names()
        pool_name = kwargs.get('rbd_pool')
        sc_name = kwargs.get('rbd_storageclass_name')
        assert create_rbd_pool(pool_name=pool_name)
        assert validate_pool_creation(pool_name)
        if validate_pool_creation(pool_name):
            log.info("Resource validation Passed")
        else:
            return False
        file_y = templating.generate_yaml_from_jinja2_template_with_data(
            SC_RBD_YAML, **kwargs
        )
        with open(TEMP_YAML_FILE, 'w') as yaml_file:
            yaml.dump(file_y, yaml_file, default_flow_style=False)
        log.info(f"Creating a new RBD StorageClass")
        assert SC.create(yaml_file=TEMP_YAML_FILE)
        assert validate_storageclass(sc_name)
    return True


def generate_pool_and_sc_names():
    """
    This will generate random pool_name and storageclass name
    :return:
    """
    sc_data = {}
    pool_name = "pool-" + ''.join(
        [random.choice(string.ascii_lowercase) for _ in range(4)])
    sc_name = "ocsci-csi-" + pool_name
    sc_data['rbd_storageclass_name'] = sc_name
    sc_data['rbd_pool'] = pool_name
    return sc_data


def get_cephblockpool():
    sc_obj = POOL.get()
    sample = sc_obj['items']
    pool_list = [
        item.metadata.name for item in sample
    ]
    return pool_list


def get_secret():
    sc_obj = SECRET.get()
    sample = sc_obj['items']
    conside_secret = "csi-"
    secret_list = [
        item.metadata.name for item in sample if (
            conside_secret in item.metadata.name
        )
    ]
    return secret_list


def get_pvc():
    sc_obj = PVC.get()
    sample = sc_obj['items']

    pvc_list = [
        item.metadata.name for item in sample
    ]
    return pvc_list


def get_storageclass():
    """
    Function for getting all storageclass
    :return:
    """
    sc_obj = SC.get()
    sample = sc_obj['items']

    ignore_sc = 'gp2'
    storageclass = [
        item.metadata.name for item in sample if (
            item.metadata.name not in ignore_sc
        )
    ]
    return storageclass


def create_secret(file_name):
    """
    This will create Secret file which will be used for creating StorageClass
    :return:
    """
    secret_data = {}
    user = "admin"
    admin_byte = base64.b64encode(user.encode("utf-8"))
    admin_base64 = str(admin_byte, "utf-8")
    cmd = f"ceph auth get-key client.admin"
    out = ocp.exec_ceph_cmd(cmd)
    admin_key = out.get('key')
    admin_key_byte = base64.b64encode(admin_key.encode("utf-8"))
    admin_key_base64 = str(admin_key_byte, "utf-8")
    secret_data['base64_encoded_admin_password'] = admin_key_base64
    secret_data['base64_encoded_admin'] = admin_base64
    file_y = templating.generate_yaml_from_jinja2_template_with_data(
        file_name, **secret_data
    )
    with open(TEMP_YAML_FILE, 'w') as yaml_file:
        yaml.dump(file_y, yaml_file, default_flow_style=False)
    assert SECRET.create(yaml_file=TEMP_YAML_FILE)
    return True


def create_pvc(storageclass_list, count=1):
    """
    Function for creating pvc and multiple pvc
    :param kwargs:
    :return:
    """
    for i in range(count):
        sc_name = random.choice(storageclass_list)
        pvc_data = {}
        pvc_data['rbd_storageclass_name'] = sc_name
        pvc_data['pvc_name'] = "rbd-pvc-" + ''.join(
            [random.choice(string.ascii_lowercase)
             for _ in range(3)]) + str(sc_name)
        file_y = templating.generate_yaml_from_jinja2_template_with_data(
            PVC_RBD_YAML, **pvc_data
        )
        with open(TEMP_YAML_FILE, 'w') as yaml_file:
            yaml.dump(file_y, yaml_file, default_flow_style=False)
        assert PVC.create(yaml_file=TEMP_YAML_FILE)
        pvc_name = pvc_data.get('pvc_name')
        PVC.wait_for_resource(resource_name=pvc_name, condition="Bound")
        log.info(f"{pvc_name} got Created and got Bounded")
    return True


def create_storageclass_cephfs():
    """
    Function for creating CephFs storageclass
    :return:
    """
    secret_data = {}
    cmd = "ceph fs ls"
    out = ocp.exec_ceph_cmd(cmd)
    if out:
        data_pool = out[0]['data_pools'][0]
        secret_data['ceph_data_pool'] = data_pool
    else:
        log.error(f"Failed to get cephfs data pool")
        return False
    file_y = templating.generate_yaml_from_jinja2_template_with_data(
        SC_CEPHFS_YAML, **secret_data
    )
    with open(TEMP_YAML_FILE, 'w') as yaml_file:
        yaml.dump(file_y, yaml_file, default_flow_style=False)
    log.info(f"Creating a new CEPHFS StorageClass")
    assert SC.create(yaml_file=TEMP_YAML_FILE)
    sc_name = file_y.get('metadata')['name']
    assert validate_storageclass(sc_name)
    return True


@tier1
@pytest.mark.usefixtures(
    test_fixture.__name__,
)
class TestCaseOCS288(ManageTest):
    def test_ocs_288(self):
        storageclass_list = get_storageclass()
        if len(storageclass_list):
            assert create_pvc(storageclass_list, count=20)
        else:
            log.error("No Storageclass Found")
            return False
