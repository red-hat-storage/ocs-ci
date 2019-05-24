import os
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


@pytest.fixture()
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


@pytest.fixture()
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


@pytest.fixture()
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


@pytest.fixture()
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


@pytest.fixture()
def delete_ceph_block_pool(request):
    """
    Delete the Ceph block pool

    """
    def finalizer(request):
        template = os.path.join(TEMPLATES_DIR, "CephBlockPool.yaml")
        logger.info(f"Deleting Ceph Block Pool")
        templating.dump_to_temp_yaml(template, TEMP_YAML)
        assert CBP.delete(yaml_file=TEMP_YAML)
        open(TEMP_YAML, 'w').close()
    request.addfinalizer(finalizer)


@pytest.fixture()
def delete_storageclass(request):
    """
    Delete the storage class

    """
    def finalizer(request):
        template = os.path.join(TEMPLATES_DIR, "StorageClass.yaml")
        logger.info(f"Deleting storage class")
        templating.dump_to_temp_yaml(template, TEMP_YAML)
        assert SC.delete(yaml_file=TEMP_YAML)
        open(TEMP_YAML, 'w').close()
    request.addfinalizer(finalizer)


@pytest.fixture()
def delete_pvc(request):
    """
    Delete the persistent volume claim

    """
    def finalizer(request):
        template = os.path.join(TEMPLATES_DIR, "PersistentVolumeClaim.yaml")
        logger.info(f"Deleting PVC")
        templating.dump_to_temp_yaml(template, TEMP_YAML)
        assert PVC.delete(yaml_file=TEMP_YAML)
        open(TEMP_YAML, 'w').close()
    request.addfinalizer(finalizer)


@pytest.fixture()
def delete_pod(request):
    """
    Delete the pod

    """
    def finalizer(request):
        template = os.path.join(TEMPLATES_DIR, "Pod.yaml")
        logger.info(f"Deleting a pod")
        templating.dump_to_temp_yaml(template, TEMP_YAML)
        assert Pod.delete(yaml_file=TEMP_YAML)
        open(TEMP_YAML, 'w').close()
    request.addfinalizer(finalizer)
