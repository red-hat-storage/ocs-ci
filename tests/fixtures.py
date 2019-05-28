import os
import time
import pytest
import logging
import ocs.ocp
import ocs.defaults as defaults
from utility import templating

logger = logging.getLogger(__name__)


# Ceph Block Pool
CBP = ocs.ocp.OCP(
    kind='CephBlockPool', namespace=defaults.ROOK_CLUSTER_NAMESPACE
)
PVC = ocs.ocp.OCP(
    kind='PersistentVolumeClaim', namespace=defaults.ROOK_CLUSTER_NAMESPACE
)
SC = ocs.ocp.OCP(
    kind='StorageClass', namespace=defaults.ROOK_CLUSTER_NAMESPACE
)
Pod = ocs.ocp.OCP(
    kind='Pod', namespace=defaults.ROOK_CLUSTER_NAMESPACE
)
OCP = ocs.ocp.OCP(
    kind='Service', namespace=defaults.ROOK_CLUSTER_NAMESPACE
)

TEMP_YAML = os.path.join("templates/ocs-deployment", "temp.yaml")
TEMPLATES_DIR = "templates/ocs-deployment"
PROJECT_NAME = 'test-project'


def create_ceph_block_pool():
    """
    Create a Ceph block pool

    """
    template = os.path.join(TEMPLATES_DIR, "CephBlockPool.yaml")
    logger.info(f'Creating a Ceph Block Pool')

    templating.dump_to_temp_yaml(template, TEMP_YAML)
    assert CBP.create(yaml_file=TEMP_YAML)
    open(TEMP_YAML, 'w').close()
    # TODO:
    # wait()
    time.sleep(15)



def create_storageclass():
    """
    Create a storage class

    """
    template = os.path.join(TEMPLATES_DIR, "StorageClass.yaml")
    logger.info(f'Creating a storage class')

    templating.dump_to_temp_yaml(template, TEMP_YAML)
    assert SC.create(yaml_file=TEMP_YAML)
    open(TEMP_YAML, 'w').close()
    # TODO:
    # wait()
    time.sleep(15)


def create_pvc():
    """
    Create a persistent Volume Claim

    """
    template = os.path.join(TEMPLATES_DIR, "PersistentVolumeClaim.yaml")
    logger.info(f'Creating a PVC')

    templating.dump_to_temp_yaml(template, TEMP_YAML)

    assert PVC.create(yaml_file=TEMP_YAML)
    open(TEMP_YAML, 'w').close()
    # TODO:
    # wait()


def create_pod():
    """
    Create a pod

    """
    template = os.path.join(TEMPLATES_DIR, "Pod.yaml")
    logger.info(f'Creating a pod')

    templating.dump_to_temp_yaml(template, TEMP_YAML)

    assert PVC.create(yaml_file=TEMP_YAML)
    open(TEMP_YAML, 'w').close()
    # TODO:
    # wait()
    time.sleep(30)


def delete_ceph_block_pool():
    """
    Delete the Ceph block pool

    """
    template = os.path.join(TEMPLATES_DIR, "CephBlockPool.yaml")
    logger.info(f"Deleting Ceph Block Pool")
    templating.dump_to_temp_yaml(template, TEMP_YAML)
    assert CBP.delete(yaml_file=TEMP_YAML)
    open(TEMP_YAML, 'w').close()


def delete_storageclass():
    """
    Delete the storage class

    """
    template = os.path.join(TEMPLATES_DIR, "StorageClass.yaml")
    logger.info(f"Deleting storage class")
    templating.dump_to_temp_yaml(template, TEMP_YAML)
    assert SC.delete(yaml_file=TEMP_YAML)
    open(TEMP_YAML, 'w').close()


def delete_pvc():
    """
    Delete the persistent volume claim

    """
    template = os.path.join(TEMPLATES_DIR, "PersistentVolumeClaim.yaml")
    logger.info(f"Deleting PVC")
    templating.dump_to_temp_yaml(template, TEMP_YAML)
    assert PVC.delete(yaml_file=TEMP_YAML)
    open(TEMP_YAML, 'w').close()


def delete_pod():
    """
    Delete the pod

    """
    template = os.path.join(TEMPLATES_DIR, "Pod.yaml")
    logger.info(f"Deleting a pod")
    templating.dump_to_temp_yaml(template, TEMP_YAML)
    assert Pod.delete(yaml_file=TEMP_YAML)
    open(TEMP_YAML, 'w').close()



def teardown():
    """
    Tearing down the environment
    """
    delete_pod()
    delete_storageclass()
    delete_ceph_block_pool()


@pytest.fixture(scope='class')
def test_fixture(request):
    """
    Create disks
    """

    def finalizer():
        """

        Returns:

        """
        teardown()
    request.addfinalizer(finalizer)
    create_ceph_block_pool()
    create_storageclass()
    create_pvc()
    create_pod()
