import pytest
from tests import helpers
from ocs_ci.ocs import constants, defaults
from ocs_ci.utility import templating


@pytest.fixture()
def create_rbd_secret(request):
    """
    Create a rbd secret
    """
    class_instance = request.node.cls

    def finalizer():
        """
        Delete the project
        """
        if class_instance.rbd_secret_obj.get():
            class_instance.rbd_secret_obj.delete()

    request.addfinalizer(finalizer)

    class_instance.rbd_secret_obj = helpers.create_secret(
        interface_type=constants.CEPHBLOCKPOOL
    )
    assert class_instance.rbd_secret_obj, "Failed to create rbd secret"


@pytest.fixture()
def create_cephfs_secret(request):
    """
    Create a cephfs secret
    """
    class_instance = request.node.cls

    def finalizer():
        """
        Delete the project
        """
        if class_instance.cephfs_secret_obj.get():
            class_instance.cephfs_secret_obj.delete()

    request.addfinalizer(finalizer)

    class_instance.cephfs_secret_obj = helpers.create_secret(
        interface_type=constants.CEPHFILESYSTEM
    )
    assert class_instance.cephfs_secret_obj, "Failed to create cephfs secret"


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
        if class_instance.cbp_obj.get():
            class_instance.cbp_obj.delete()

    request.addfinalizer(finalizer)

    class_instance.cbp_obj = helpers.create_ceph_block_pool()
    assert class_instance.cbp_obj, "Failed to create block pool"


@pytest.fixture()
def create_rbd_storageclass(request):
    """
    Create a storage class
    """
    class_instance = request.node.cls

    def finalizer():
        """
        Delete the storage class
        """
        if class_instance.sc_obj.get():
            class_instance.sc_obj.delete()

    request.addfinalizer(finalizer)

    class_instance.sc_obj = helpers.create_storage_class(
        interface_type=constants.CEPHBLOCKPOOL,
        interface_name=class_instance.cbp_obj.name,
        secret_name=class_instance.secret_obj.name
    )
    assert class_instance.sc_obj, "Failed to create storage class"


@pytest.fixture()
def create_pvc(request):
    """
    Create a persistent Volume Claim
    """
    class_instance = request.node.cls

    class_instance.pvc_obj = helpers.create_pvc(
        sc_name=class_instance.sc_obj.name
    )


@pytest.fixture()
def create_pod(request):
    """
    Create a pod
    """
    class_instance = request.node.cls

    pod_data = templating.load_yaml_to_dict(constants.CSI_RBD_POD_YAML)
    pod_data['metadata']['name'] = helpers.create_unique_resource_name(
        'test', 'pod'
    )
    pod_data['metadata']['namespace'] = defaults.ROOK_CLUSTER_NAMESPACE
    pod_data['spec']['volumes'][0]['persistentVolumeClaim']['claimName'] = class_instance.pvc_obj.name
    class_instance.pod_obj = helpers.create_pod(**pod_data)
