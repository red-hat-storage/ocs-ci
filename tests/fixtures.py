import pytest
from tests import helpers
from ocs_ci.ocs import constants, ocp


@pytest.fixture()
def create_rbd_secret(request):
    """
    Create an RBD secret
    """
    class_instance = request.node.cls

    def finalizer():
        """
        Delete the RBD secret
        """
        if hasattr(class_instance, 'rbd_secret_obj'):
            class_instance.rbd_secret_obj.delete()

    request.addfinalizer(finalizer)

    class_instance.rbd_secret_obj = helpers.create_secret(
        interface_type=constants.CEPHBLOCKPOOL
    )
    assert class_instance.rbd_secret_obj, "Failed to create secret"


@pytest.fixture()
def create_cephfs_secret(request):
    """
    Create a CephFS secret
    """
    class_instance = request.node.cls

    def finalizer():
        """
        Delete the FS secret
        """
        if hasattr(class_instance, 'cephfs_secret_obj'):
            class_instance.cephfs_secret_obj.delete()

    request.addfinalizer(finalizer)

    class_instance.cephfs_secret_obj = helpers.create_secret(
        interface_type=constants.CEPHFILESYSTEM
    )
    assert class_instance.cephfs_secret_obj, f"Failed to create secret"


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
        if hasattr(class_instance, 'cbp_obj'):
            class_instance.cbp_obj.delete()

    request.addfinalizer(finalizer)

    class_instance.cbp_obj = helpers.create_ceph_block_pool()
    assert class_instance.cbp_obj, "Failed to create block pool"


@pytest.fixture()
def create_rbd_storageclass(request):
    """
    Create an RBD storage class
    """
    class_instance = request.node.cls

    def finalizer():
        """
        Delete the RBD storage class
        """
        if class_instance.sc_obj.get():
            class_instance.sc_obj.delete()

    request.addfinalizer(finalizer)

    class_instance.sc_obj = helpers.create_storage_class(
        interface_type=constants.CEPHBLOCKPOOL,
        interface_name=class_instance.cbp_obj.name,
        secret_name=class_instance.rbd_secret_obj.name,
    )
    assert class_instance.sc_obj, "Failed to create storage class"


@pytest.fixture()
def create_cephfs_storageclass(request):
    """
    Create a CephFS storage class
    """
    class_instance = request.node.cls

    def finalizer():
        """
        Delete the CephFS storage class
        """
        if class_instance.sc_obj.get():
            class_instance.sc_obj.delete()

    request.addfinalizer(finalizer)

    class_instance.sc_obj = helpers.create_storage_class(
        interface_type=constants.CEPHFILESYSTEM,
        interface_name=helpers.get_cephfs_data_pool_name(),
        secret_name=class_instance.cephfs_secret_obj.name
    )
    assert class_instance.sc_obj, f"Failed to create storage class"


@pytest.fixture()
def create_project(request):
    """
    Create a new project
    """
    class_instance = request.node.cls

    def finalizer():
        """
        Delete the project
        """
        from ipdb import set_trace; set_trace()
        class_instance.project_obj.delete(resource_name=class_instance.namespace)

    request.addfinalizer(finalizer)

    class_instance.namespace = helpers.create_unique_resource_name(
        'test', 'namespace'
    )
    class_instance.project_obj = ocp.OCP(
        kind='Project', namespace=class_instance.namespace
    )

    assert class_instance.project_obj.new_project(class_instance.namespace), (
        f'Failed to create new project {class_instance.namespace}'
    )


@pytest.fixture()
def create_pvc(request):
    """
    Create a persistent Volume Claim
    """
    class_instance = request.node.cls

    class_instance.pvc_obj = helpers.create_pvc(
        sc_name=class_instance.sc_obj.name, namespace=class_instance.namespace
    )


@pytest.fixture()
def delete_pvc(request):
    """
    Delete a persistent Volume Claim
    """
    class_instance = request.node.cls

    def finalizer():
        """
        Delete the PVC
        """
        if hasattr(class_instance, 'pvc_obj'):
            class_instance.pvc_obj.delete()

    request.addfinalizer(finalizer)


@pytest.fixture()
def create_rbd_pod(request):
    """
    Create a pod
    """
    class_instance = request.node.cls
    class_instance.pod_obj = helpers.create_pod(
        interface_type=constants.CEPHBLOCKPOOL,
        pvc_name=class_instance.pvc_obj.name,
        namespace=class_instance.namespace
    )


@pytest.fixture()
def delete_pod(request):
    """
    Delete a pod
    """
    class_instance = request.node.cls

    def finalizer():
        """
        Delete the pod
        """
        if hasattr(class_instance, 'pod_obj'):
            class_instance.pod_obj.delete()

    request.addfinalizer(finalizer)


@pytest.fixture()
def create_pvcs(request):
    """
    Create multiple PVCs
    """
    class_instance = request.node.cls

    def finalizer():
        """
        Delete multiple PVCs
        """
        if hasattr(class_instance, 'pvc_objs'):
            for pvc_obj in class_instance.pvc_objs:
                pvc_obj.delete()
            for pvc_obj in class_instance.pvc_objs:
                assert pvc_obj.wait_for_delete(pvc_obj.name), (
                    f"PVC {pvc_obj.name} failed to be deleted"
                )

    request.addfinalizer(finalizer)

    class_instance.pvc_objs = helpers.create_multiple_pvcs(
        sc_name=class_instance.sc_obj.name, number_of_pvc=class_instance.num_of_pvcs,
        size=class_instance.pvc_size, namespace=class_instance.namespace
    )


@pytest.fixture()
def create_pods(request):
    """
    Create multiple pods
    """
    class_instance = request.node.cls

    def finalizer():
        """
        Delete multiple pods
        """
        if hasattr(class_instance, 'pod_objs'):
            for pod in class_instance.pod_objs:
                pod.delete()

    request.addfinalizer(finalizer)

    class_instance.pod_objs = [
        helpers.create_pod(
            interface_type=class_instance.interface, pvc_name=pvc_obj.name,
            wait=False, namespace=class_instance.namespace
        ) for pvc_obj in class_instance.pvc_objs
    ]
    for pod in class_instance.pod_objs:
        assert helpers.wait_for_resource_state(
            pod, constants.STATUS_RUNNING
        ), f"Pod {pod} failed to reach {constants.STATUS_RUNNING}"
