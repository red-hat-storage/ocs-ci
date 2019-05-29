import pytest
import logging
from tests import helpers

logger = logging.getLogger(__name__)


@pytest.fixture()
def create_ceph_block_pool(request):
    """
    Create a Ceph block pool

    """
    class_instance = request.node.cls

    def finalizer():
        """
        Delete the Ceph block pool
        """
        helpers.delete_ceph_block_pool(
            class_instance.cbp_name, class_instance.project_name
        )

    request.addfinalizer(finalizer)
    class_instance.cbp_name = helpers.create_unique_resource_name(
        'test', 'cephblockpool'
    )
    helpers.create_ceph_block_pool(
        class_instance.cbp_name, class_instance.project_name
    )


@pytest.fixture()
def create_storageclass(request):
    """
    Create a storage class

    """
    class_instance = request.node.cls

    def finalizer():
        """
        Delete the storage class
        """
        helpers.delete_storage_class(
            class_instance.sc_name, class_instance.project_name,
            cbp_name=class_instance.cbp_name
        )

    request.addfinalizer(finalizer)

    class_instance.sc_name = helpers.create_unique_resource_name(
        'test', 'storageclass'
    )
    helpers.create_storageclass(
        class_instance.sc_name, class_instance.project_name,
        cbp_name=class_instance.cbp_name
    )


@pytest.fixture()
def create_pvc(request):
    """
    Create a persistent Volume Claim

    """
    class_instance = request.node.cls

    def finalizer():
        """
        Delete the PVC
        """
        helpers.delete_pvc(
            class_instance.pvc_name, class_instance.project_name
        )

    request.addfinalizer(finalizer)

    class_instance.pvc_name = helpers.create_unique_resource_name(
        'test', 'pvc'
    )
    helpers.create_ceph_block_pool(
        class_instance.pvc_name, class_instance.project_name
    )


@pytest.fixture()
def create_pod(request):
    """
    Create a pod

    """
    class_instance = request.node.cls

    def finalizer():
        """
        Delete the pod
        """
        helpers.delete_pod(
            class_instance.pod_name, class_instance.project_name
        )

    request.addfinalizer(finalizer)
    class_instance.pod_name = helpers.create_unique_resource_name(
        'test', 'pod'
    )
    helpers.create_pod(
        class_instance.pod_name, class_instance.project_name,
        pvc_name=class_instance.pvc_name
    )


@pytest.fixture()
def create_project(request):
    """
    Create a project

    """
    class_instance = request.node.cls

    def finalizer():
        """
        Delete the project
        """
        helpers.delete_project(class_instance.project_name)

    request.addfinalizer(finalizer)

    class_instance.project_name = helpers.create_unique_resource_name(
        'test', 'project'
    )
    helpers.create_project(class_instance.project_name)
