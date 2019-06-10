"""
A test for creating pvc with random sc
"""
import logging
import os
import random
import string

import pytest
import yaml

from ocs import defaults
from ocs import ocp
from ocsci.config import ENV_DATA
from ocsci.testlib import tier1, ManageTest
from resources import pod
from resources.ocs import OCS
from tests import helpers
from utility import templating

log = logging.getLogger(__name__)
TEMPLATE_DIR = "templates"

RBD_POOL_YAML = os.path.join(
    "templates/ocs-deployment", "cephblockpool.yaml"
)

TEMP_YAML_FILE = 'temp.yaml'

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
tools_pod = pod.get_ceph_tools_pod()


@pytest.fixture(scope='class')
def ocs288_fixture(request):
    def finalizer():
        teardown()

    request.addfinalizer(finalizer)
    setup()


def setup():
    """
    Setting up the environment for the test
    """
    log.info("Creating RBD Secret")
    assert create_secret_rbd()
    log.info("Creating CEPHFS Secret")
    assert create_secret_cephfs()
    log.info("Creating CEPH FileSystem")
    assert create_cephfilesystem()
    log.info("Creating RBD Storageclass")
    assert create_multiple_rbd_storageclasses(count=5)
    log.info("Creating CEPHFS Storageclass")
    assert create_storageclass_cephfs()


def teardown():
    """
    Tearing down the environment
    """
    global RBD_SECRET_OBJ, CEPHFS_SECRET_OBJ, CEPHFS_OBJ, CEPHFS_SC_OBJ
    log.info("Deleting PVC")
    assert delete_pvc()
    log.info("Deleting CEPH BLOCK POOL")
    assert delete_cephblockpool()
    log.info("Deleting RBD Secret")
    RBD_SECRET_OBJ.delete()
    log.info("Deleting CEPHFS Secret")
    CEPHFS_SECRET_OBJ.delete()
    log.info("Deleting CEPH FILESYSTEM")
    CEPHFS_OBJ.delete()
    log.info("Deleting Cephfs Storageclass")
    CEPHFS_SC_OBJ.delete()
    log.info("Deleting RBD Storageclass")
    assert delete_storageclass()


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


def create_rbd_pool(pool_name):
    """
    Create Blockpool with specified name

    :param pool_name:
    :return:
    """

    pool_data = {}

    pool_data['cephblockpool_name'] = pool_name
    pool_data['rook_api_version'] = defaults.ROOK_API_VERSION

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
    cmd = "ceph osd pool ls"
    out = tools_pod.exec_ceph_cmd(ceph_cmd=cmd)
    for item in out:
        if item == pool_name:
            ceph_validate = True
    pool_obj = POOL.get()
    sample = pool_obj['items']
    for item in sample:
        if item.get('metadata')['name'] == pool_name:
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
    fs_data = defaults.CEPHFILESYSTEM_DICT.copy()
    fs_data['metadata']['name'] = helpers.create_unique_resource_name(
        'test', 'cephfs'
    )
    log.info(fs_data)
    fs_data['metadata']['namespace'] = ENV_DATA['cluster_namespace']
    global CEPHFS_OBJ
    CEPHFS_OBJ = OCS(**fs_data)
    CEPHFS_OBJ.create()
    assert POD.wait_for_resource(
        condition='Running', selector='app=rook-ceph-mds'
    )
    assert validate_cephfilesystem(fs_name=fs_data['metadata']['name'])
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
    log.info(fs_name)
    out = tools_pod.exec_ceph_cmd(ceph_cmd=cmd)
    if out:
        out = out[0]['name']
        log.info(out)
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

    mons = (
        f'rook-ceph-mon-a.{ENV_DATA["cluster_namespace"]}'
        f'.svc.cluster.local:6789,'
        f'rook-ceph-mon-b.{ENV_DATA["cluster_namespace"]}.'
        f'svc.cluster.local:6789,'
        f'rook-ceph-mon-c.{ENV_DATA["cluster_namespace"]}'
        f'.svc.cluster.local:6789'
    )

    for sc_count in range(count):
        kwargs = generate_pool_and_sc_names()
        pool_name = kwargs.get('pool')
        sc_name = kwargs.get('name')
        assert create_rbd_pool(pool_name=pool_name)
        assert validate_pool_creation(pool_name)
        if validate_pool_creation(pool_name):
            log.info("Resource validation Passed")
        else:
            return False

        rbd_sc = defaults.RBD_SC.copy()
        rbd_sc['metadata']['name'] = sc_name
        rbd_sc['parameters']['pool'] = pool_name
        rbd_sc['parameters']['monitors'] = mons
        global RBD_SC_OBJ
        RBD_SC_OBJ = OCS(**rbd_sc)
        log.info(f"Creating a new RBD StorageClass")
        log.info(rbd_sc)
        RBD_SC_OBJ.create()
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
    sc_data['name'] = sc_name
    sc_data['pool'] = pool_name
    return sc_data


def get_cephblockpool():
    sc_obj = POOL.get()
    sample = sc_obj['items']
    pool_list = [
        item.get('metadata')['name'] for item in sample
    ]
    return pool_list


def get_secret():
    sc_obj = SECRET.get()
    sample = sc_obj['items']
    conside_secret = "csi-"
    secret_list = [
        item.get('metadata')['name'] for item in sample if (
            conside_secret in item.metadata.name
        )
    ]
    return secret_list


def get_pvc():
    sc_obj = PVC.get()
    sample = sc_obj['items']

    pvc_list = [
        item.get('metadata')['name'] for item in sample
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
        item.get('metadata')['name'] for item in sample if (
            item.get('metadata')['name'] not in ignore_sc
        )
    ]
    return storageclass


def create_secret_rbd():
    """
    This will create Secret file which will be used for creating StorageClass
    :return:
    """
    secret_data = defaults.RBD_SECRET.copy()
    admin_key = pod.get_admin_key_from_ceph_tools()
    secret_data['data']['admin'] = admin_key
    del secret_data['data']['kubernetes']
    global RBD_SECRET_OBJ
    log.info(secret_data)
    RBD_SECRET_OBJ = OCS(**secret_data)
    RBD_SECRET_OBJ.create()

    return True


def create_secret_cephfs():
    """
    This will create Secret file which will be used for creating StorageClass
    :return:
    """
    secret_data = defaults.CEPHFS_SECRET.copy()
    admin_key = pod.get_admin_key_from_ceph_tools()
    del secret_data['data']['userID']
    del secret_data['data']['userKey']
    secret_data['data']['adminID'] = defaults.ADMIN_BASE64
    secret_data['data']['adminKey'] = admin_key
    global CEPHFS_SECRET_OBJ
    CEPHFS_SECRET_OBJ = OCS(**secret_data)
    CEPHFS_SECRET_OBJ.create()

    return True


def create_pvc(storageclass_list, count=1):
    """
    Function for creating pvc and multiple pvc
    :param kwargs:
    :return:
    """
    for i in range(count):
        pvc_data = defaults.RBD_PVC.copy()
        sc_name = random.choice(storageclass_list)
        pvc_data['spec']['storageClassName'] = sc_name
        pvc_data['metadata']['name'] = "rbd-pvc-" + ''.join(
            [random.choice(string.ascii_lowercase)
             for _ in range(3)]) + str(sc_name)
        pvc_name = pvc_data['metadata']['name']
        global PVC_OBJ
        PVC_OBJ = OCS(**pvc_data)
        log.info(f"Creating a new PVC with name {pvc_name}")
        PVC_OBJ.create()
        PVC.wait_for_resource(resource_name=pvc_name, condition="Bound")
        log.info(f"{pvc_name} got Created and got Bounded")
    return True


def create_storageclass_cephfs():
    """
    Function for creating CephFs storageclass
    :return:
    """
    mons = (
        f'rook-ceph-mon-a.{ENV_DATA["cluster_namespace"]}'
        f'.svc.cluster.local:6789,'
        f'rook-ceph-mon-b.{ENV_DATA["cluster_namespace"]}.'
        f'svc.cluster.local:6789,'
        f'rook-ceph-mon-c.{ENV_DATA["cluster_namespace"]}'
        f'.svc.cluster.local:6789'
    )
    sc_data = defaults.CEPHFS_SC.copy()
    cmd = "ceph fs ls"
    out = tools_pod.exec_ceph_cmd(ceph_cmd=cmd)
    log.info(out)
    if out:
        data_pool = out[0]['data_pools'][0]
        sc_data['parameters']['pool'] = data_pool
    else:
        log.error(f"Failed to get cephfs data pool")
        return False
    sc_data['parameters']['monitors'] = mons
    global CEPHFS_SC_OBJ
    CEPHFS_SC_OBJ = OCS(**sc_data)
    log.info(f"Creating a new CEPHFS StorageClass")
    CEPHFS_SC_OBJ.create()
    sc_name = sc_data['metadata']['name']
    assert validate_storageclass(sc_name)
    return True


@tier1
@pytest.mark.usefixtures(
    ocs288_fixture.__name__,
)
class TestCaseOCS288(ManageTest):
    def test_ocs_288(self):
        storageclass_list = get_storageclass()
        if len(storageclass_list):
            assert create_pvc(storageclass_list, count=20)
        else:
            log.error("No Storageclass Found")
            return False
