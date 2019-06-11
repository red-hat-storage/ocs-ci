"""
A test for creating pvc with random sc
"""
import logging
import os
import random

import pytest

from ocs import defaults, constants
from ocs import ocp
from ocsci.config import ENV_DATA
from ocsci.testlib import tier1, ManageTest
from resources import pod
from resources.ocs import OCS
from tests import helpers

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
    global RBD_SECRET_OBJ
    RBD_SECRET_OBJ = helpers.create_secret(constants.CEPHBLOCKPOOL)

    log.info("Creating CEPHFS Secret")
    global CEPHFS_SECRET_OBJ
    CEPHFS_SECRET_OBJ = helpers.create_secret(constants.CEPHFILESYSTEM)

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
    global RBD_SECRET_OBJ, CEPHFS_SECRET_OBJ, CEPHFS_OBJ
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
    log.info("Deleting Storageclass")
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
    """
    Function for deleting CephBlockPool
    :return:
    """
    pool_list = get_cephblockpool()
    for item in pool_list:
        log.info(f"Deleting CephBlockPool with name {item}")
        assert POOL.delete(resource_name=item)
    return True


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
    tools_pod = pod.get_ceph_tools_pod()
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


def create_multiple_rbd_storageclasses(count=1, ):
    """
    Function for creating multiple rbd storageclass
    :param count:
    :return:
    """

    for sc_count in range(count):
        log.info("Creating CephBlockPool")
        pool_obj = helpers.create_ceph_block_pool()
        helpers.create_storage_class(
            constants.CEPHBLOCKPOOL,
            interface_name=pool_obj.name,
            secret_name=RBD_SECRET_OBJ.name
        )

    return True


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


def create_pvc(storageclass_list, count=1):
    """
    Function for creating pvc and multiple pvc
    :param kwargs:
    :return:
    """
    for i in range(count):
        sc_name = random.choice(storageclass_list)
        pvc_obj = helpers.create_pvc(sc_name)
        log.info(f"{pvc_obj.name} got Created and got Bounded")
    return True


def create_storageclass_cephfs():
    """
    Function for creating CephFs storageclass
    :return:
    """
    helpers.create_storage_class(
        constants.CEPHFILESYSTEM,
        helpers.get_cephfs_data_pool_name(),
        CEPHFS_SECRET_OBJ.name
    )

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
