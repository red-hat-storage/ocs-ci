import pytest
from tests import helpers
from ocs_ci.ocs import constants, ocp, defaults
from ocs_ci.ocs.resources import ocs


# Secret section
@pytest.fixture()
def create_rbd_secret(request):
    """
    Create an RBD secret

    Modifies:
        OCS object: rbd_secret_obj
    """
    class_instance = request.node.cls

    def finalizer():
        """
        Delete the RBD secret
        """
        if hasattr(class_instance, 'rbd_secret_obj'):
            class_instance.rbd_secret_obj.delete()
            class_instance.rbd_secret_obj.ocp.wait_for_delete(
                class_instance.rbd_secret_obj.name
            )

    request.addfinalizer(finalizer)

    class_instance.rbd_secret_obj = helpers.create_secret(
        interface_type=constants.CEPHBLOCKPOOL
    )
    assert class_instance.rbd_secret_obj, "Failed to create secret"


@pytest.fixture()
def create_cephfs_secret(request):
    """
    Create a CephFS secret

    Modifies:
        OCS object: cephfs_secret_obj
    """
    class_instance = request.node.cls

    def finalizer():
        """
        Delete the FS secret
        """
        if hasattr(class_instance, 'cephfs_secret_obj'):
            class_instance.cephfs_secret_obj.delete()
            class_instance.cephfs_secret_obj.ocp.wait_for_delete(
                class_instance.cephfs_secret_obj.name
            )

    request.addfinalizer(finalizer)

    class_instance.cephfs_secret_obj = helpers.create_secret(
        interface_type=constants.CEPHFILESYSTEM
    )
    assert class_instance.cephfs_secret_obj, f"Failed to create secret"


@pytest.fixture()
def create_interface_based_secret(request):
    """
    Create a CephFS or RBD secret

    Modifies:
        OCS object: secret_obj
    """
    class_instance = request.node.cls
    interface = class_instance.interface

    def finalizer():
        """
        Delete the secret
        """
        if hasattr(class_instance, 'secret_obj'):
            class_instance.secret_obj.delete()
            class_instance.secret_obj.ocp.wait_for_delete(
                class_instance.secret_obj.name
            )

    request.addfinalizer(finalizer)

    class_instance.secret_obj = helpers.create_secret(
        interface_type=interface
    )
    assert class_instance.secret_obj, f"Failed to create {interface} secret"


# Interface section
@pytest.fixture()
def create_ceph_block_pool(request):
    """
    Create a Ceph block pool

    Modifies:
        OCS object: cbp_obj
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
def create_ceph_file_system(request):
    """
    Create a Ceph file system

    Modifies:
        OCS object: cfs_obj
    """
    class_instance = request.node.cls

    def finalizer():
        """
        Delete the Ceph block pool
        """
        if hasattr(class_instance, 'cfs_obj'):
            class_instance.cfs_obj.delete()

    request.addfinalizer(finalizer)

    class_instance.cfs_obj = helpers.create_ceph_file_system()
    assert class_instance.cfs_obj, "Failed to create ceph file system"


@pytest.fixture()
def create_interface_based_ceph_pool(request):
    """
    Create a Ceph File System or Ceph Block Pool backend

    Modifies:
        OCS object: ceph_pool
    """
    class_instance = request.node.cls
    interface = class_instance.interface

    def finalizer():
        """
        Delete the Ceph block pool
        * Ceph file system should not be deleted
        """
        if interface == constants.CEPHBLOCKPOOL:
            if hasattr(class_instance, 'ceph_pool'):
                class_instance.ceph_pool.delete()

    request.addfinalizer(finalizer)

    if interface == constants.CEPHBLOCKPOOL:
        class_instance.ceph_pool = helpers.create_ceph_block_pool()
        assert class_instance.ceph_pool, "Failed to create block pool"
    elif interface == constants.CEPHFILESYSTEM:
        cfs = ocp.OCP(kind=constants.CEPHFILESYSTEM).get(
            defaults.CEPHFILESYSTEM_NAME
        )
        class_instance.ceph_pool = ocs.OCS(**cfs)
        assert class_instance.ceph_pool, "Failed to create file system pool"


# Storage class section
@pytest.fixture()
def create_rbd_storageclass(request):
    """
    Create an RBD storage class

    Requires:
        OCS object: cbp_obj
        OCS object: rbd_secret_obj

    Modifies:
        OCS object: rbd_sc_obj
    """
    class_instance = request.node.cls

    def finalizer():
        """
        Delete the RBD storage class
        """
        if hasattr(class_instance, 'rbd_sc_obj'):
            class_instance.rbd_sc_obj.delete()
            class_instance.rbd_sc_obj.ocp.wait_for_delete(
                class_instance.rbd_sc_obj.name
            )

    request.addfinalizer(finalizer)

    class_instance.rbd_sc_obj = helpers.create_storage_class(
        interface_type=constants.CEPHBLOCKPOOL,
        interface_name=class_instance.cbp_obj.name,
        secret_name=class_instance.rbd_secret_obj.name,
    )
    assert class_instance.rbd_sc_obj, "Failed to create RBD storage class"


@pytest.fixture()
def create_cephfs_storageclass(request):
    """
    Create a CephFS storage class

    Requires:
        OCS object: cephfs_secret_obj

    Modifies:
        OCS object: fs_sc_obj
    """
    class_instance = request.node.cls

    def finalizer():
        """
        Delete the CephFS storage class
        """
        if hasattr(class_instance, 'fs_sc_obj'):
            class_instance.fs_sc_obj.delete()
            class_instance.fs_sc_obj.ocp.wait_for_delete(
                class_instance.fs_sc_obj.name
            )

    request.addfinalizer(finalizer)

    class_instance.fs_sc_obj = helpers.create_storage_class(
        interface_type=constants.CEPHFILESYSTEM,
        interface_name=helpers.get_cephfs_data_pool_name(),
        secret_name=class_instance.cephfs_secret_obj.name
    )
    assert class_instance.fs_sc_obj, f"Failed to create FS storage class"


@pytest.fixture()
def create_interface_based_storageclass(
    request, create_interface_based_secret, create_interface_based_ceph_pool
):
    """
    Create a CephFS or RBD secret

    Requires:
        OCS object: ceph_pool in case of interface == constants.CEPHBLOCKPOOL
        OCS object: secret_obj

    Modifies:
        OCS object: sc_obj
    """
    class_instance = request.node.cls
    interface = class_instance.interface

    def finalizer():
        """
        Delete the storage class
        """
        if class_instance.sc_obj.get():
            class_instance.sc_obj.delete()
            class_instance.sc_obj.ocp.wait_for_delete(
                class_instance.sc_obj.name
            )

    request.addfinalizer(finalizer)
    class_instance.sc_obj = helpers.create_storage_class(
        interface_type=interface,
        interface_name=class_instance.ceph_pool.name,
        secret_name=class_instance.secret_obj.name,
        )
    assert class_instance.sc_obj, f"Failed to create {interface} storage class"


@pytest.fixture()
def create_project(request):
    """
    Create a new project

    Modifies:
        OCS object: project_obj
    """
    class_instance = request.node.cls

    def finalizer():
        """
        Delete the project
        """
        ocp.switch_to_default_rook_cluster_project()
        class_instance.project_obj.delete(
            resource_name=class_instance.namespace
        )
        class_instance.project_obj.wait_for_delete(class_instance.namespace)

    request.addfinalizer(finalizer)

    class_instance.project_obj = helpers.create_project()
    class_instance.namespace = class_instance.project_obj.namespace


@pytest.fixture()
def create_pvc(request):
    """
    Create a persistent Volume Claim

    Modifies:
        OCS object: pvc_obj
    """
    class_instance = request.node.cls

    class_instance.pvc_obj = helpers.create_pvc(
        sc_name=class_instance.sc_obj.name, namespace=class_instance.namespace
    )


@pytest.fixture()
def delete_pvc(request):
    """
    Delete a persistent Volume Claim

    Requires:
        OCS object: pvc_obj
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

    Requires:
        OCS object: pvc_obj
        str: namespace

    Modifies:
        OCS object: pod_obj
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

    Requires:
        OCS object: pod_obj
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

    Requires:
        OCS object: sc_obj
        int: num_of_pvcs
        str: pvc_size
        str: namespace

    Modifies:
        OCS object: pvc_objs
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
                pvc_obj.ocp.wait_for_delete(pvc_obj.name)

    request.addfinalizer(finalizer)

    class_instance.pvc_objs = helpers.create_multiple_pvcs(
        sc_name=class_instance.sc_obj.name,
        number_of_pvc=class_instance.num_of_pvcs,
        size=class_instance.pvc_size,
        namespace=class_instance.namespace
    )


@pytest.fixture()
def create_pods(request):
    """
    Create multiple pods

    Requires:
        str: interface
        OCS object: pvc_obj
        str: namespace

    Modifies:
        OCS object: pod_objs
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


@pytest.fixture()
def create_pod(request, delete_pod):
    """
    Create multiple pods

    Requires:
        str: interface
        OCS object: pvc_obj
        str: namespace

    Modifies:
        OCS object: pod_obj
    """
    class_instance = request.node.cls
    interface = class_instance.interface

    class_instance.pod_obj = helpers.create_pod(
        interface_type=interface,
        pvc_name=class_instance.pvc_obj.name,
        namespace=class_instance.namespace
    )


@pytest.fixture(
    params=[
        pytest.param({'interface': constants.CEPHBLOCKPOOL}),
        pytest.param({'interface': constants.CEPHFILESYSTEM})
    ],
    ids=["RBD", "CephFS"]
)
def interface_iterate(request):
    """
    Iterate over interfaces
    This fixture should be the first fixture that is being called

    Modifies:
        str: interface
    """
    class_instance = request.node.cls
    class_instance.interface = request.param['interface']
