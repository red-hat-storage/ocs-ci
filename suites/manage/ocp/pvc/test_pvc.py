"""
PVC test plan
"""
import os
import logging
import yaml
import pytest

import suites.helpers as helpers
import oc.openshift_ops


ocp = oc.openshift_ops.OCP()
logger = logging.getLogger(__name__)


@pytest.fixture()
def create_ceph_block_pool(request):
    """
    Create a Ceph block pool
    """

    def finalizer():
        """
        Delete the Ceph block pool created during setup
        """
        pass

    pool_body = os.path.join("templates/ocs-deployment", "CephBlockPool.yaml")
    pool_dict = yaml.safe_load(pool_body)
    pool_name = pool_dict['pool_name']
    logger.info(f"Creating a Ceph block pool {pool_name}")
    assert ocp.create_block_pool(pool_body=pool_dict), (
        f"Failed to create Ceph block pool {pool_name}"
    )
    request.addfinalizer(finalizer)


@pytest.fixture()
def create_storageclass(request):
    """
    Create a storage class
    """

    def finalizer():
        """
        Delete the storage class created during setup
        """
        pass

    sc_body = os.path.join("templates/ocs-deployment", "StorageClass.yaml")
    sc_dict = yaml.safe_load(sc_body)
    request.node.cls.sc_name = sc_dict['pool_name']
    logger.info(f"Creating a storage class {request.node.cls.sc_name}")
    assert ocp.create_storage_class(sc_body=sc_dict), (
        f"Failed to create a Ceph block pool {request.node.cls.sc_name}"
    )
    request.addfinalizer(finalizer)


@pytest.fixture()
def create_project(request):
    """
    Create a project
    """

    def finalizer():
        """
        Delete the project created during setup
        """
        pass

    project_name = 'test_project'
    logger.info(f"Creating a project {project_name}")
    assert ocp.create_project(project=project_name)
    request.addfinalizer(finalizer)


@pytest.fixture()
def create_pvc(request):
    """
    Create a PVC
    """

    def finalizer():
        """
        Delete the PVC created during setup
        """
        pass

    pvc_body = os.path.join("templates/ocs-deployment", "PersistentVolumeClaim.yaml")
    pvc_dict = yaml.safe_load(pvc_body)
    pvc_dict['storageClassName'] = request.node.cls.sc_name
    pvc_name = pvc_dict['name']
    logger.info(f"Creating a PVC {pvc_name}")
    assert ocp.create_pvc(pvc_body=pvc_dict), (
        f"Failed to create PVC {pvc_name}"
    )
    request.addfinalizer(finalizer)


@pytest.fixture()
def create_pod(request):
    """
    Create a pod
    """
    pod_body = os.path.join("templates/ocs-deployment", "Pod.yaml")
    pod_dict = yaml.safe_load(pod_body)
    pod_name = pod_dict['name']
    logger.info(f"Creating a pod {pod_name}")
    assert ocp.create_pvc(pvc_body=pod_name), (
        f"Failed to create pod {pod_name}"
    )


@pytest.mark.usefixtures(
    create_ceph_block_pool.__name__,
    create_storageclass.__name__,
    create_project.__name__,
    create_pvc.__name__,
    create_pod.__name__,
)
class TestCaseOCS371:
    """
    Delete PVC while IO is in progress
    https://polarion.engineering.redhat.com/polarion/#/
    project/OpenShiftContainerStorage/workitem?id=OCS-371
    """
    def test_delete_pvc_while_io_is_in_progress(self):
        """
        Delete PVC while IO is in progress
        """
        helpers.run_io_in_background()
        # assert ocp.delete_pod

