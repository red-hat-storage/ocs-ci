import pytest
from tests import helpers
from ocs_ci.ocs import constants


@pytest.fixture()
def rbd_secret(request):
    """
    Create an RBD secret
    """
    rbd_secret_obj = helpers.create_secret(
        interface_type=constants.CEPHBLOCKPOOL
    )
    assert rbd_secret_obj, "Failed to create secret"

    def finalizer():
        """
        Delete the RBD secret
        """
        rbd_secret_obj.delete()

    request.addfinalizer(finalizer)
    return rbd_secret_obj


@pytest.fixture()
def cephfs_secret(request):
    """
    Create a CephFS secret
    """
    cephfs_secret_obj = helpers.create_secret(
        interface_type=constants.CEPHFILESYSTEM
    )
    assert cephfs_secret_obj, f"Failed to create secret"

    def finalizer():
        """
        Delete the FS secret
        """
        cephfs_secret_obj.delete()

    request.addfinalizer(finalizer)
    return cephfs_secret_obj


@pytest.fixture()
def ceph_block_pool(request):
    """
    Create a Ceph block pool
    """
    cbp_obj = helpers.create_ceph_block_pool()
    assert cbp_obj, "Failed to create block pool"

    def finalizer():
        """
        Delete the Ceph block pool
        """
        cbp_obj.delete()

    request.addfinalizer(finalizer)
    return cbp_obj


@pytest.fixture()
def rbd_storageclass(request, ceph_block_pool, rbd_secret):
    """
    Create an RBD storage class
    """
    sc_obj = helpers.create_storage_class(
        interface_type=constants.CEPHBLOCKPOOL,
        interface_name=ceph_block_pool.name,
        secret_name=rbd_secret.name
    )
    assert sc_obj, "Failed to create storage class"

    def finalizer():
        """
        Delete the RBD storage class
        """
        sc_obj.delete()

    request.addfinalizer(finalizer)
    return sc_obj


@pytest.fixture()
def cephfs_storageclass(request, cephfs_secret):
    """
    Create a CephFS storage class
    """
    sc_obj = helpers.create_storage_class(
        interface_type=constants.CEPHFILESYSTEM,
        interface_name=helpers.get_cephfs_data_pool_name(),
        secret_name=cephfs_secret.name
    )
    assert sc_obj, f"Failed to create storage class"

    def finalizer():
        """
        Delete the CephFS storage class
        """
        sc_obj.delete()

    request.addfinalizer(finalizer)
    return sc_obj


@pytest.fixture()
def rbd_pvc(request, rbd_storageclass):
    """
    Create a persistent Volume Claim using RBD
    """
    pvc_obj = helpers.create_pvc(
        sc_name=rbd_storageclass.name
    )
    assert pvc_obj, f"Failed to create PVC"

    def finalizer():
        """
        Delete the PVC
        """
        pvc_obj.delete()

    request.addfinalizer(finalizer)
    return pvc_obj


@pytest.fixture()
def cephfs_pvc(request, cephfs_pvc):
    """
    Create a persistent Volume Claim using CephFS
    """
    pvc_obj = helpers.create_pvc(
        sc_name=cephfs_pvc.name
    )
    assert pvc_obj, f"Failed to create PVC"

    def finalizer():
        """
        Delete the PVC
        """
        pvc_obj.delete()

    request.addfinalizer(finalizer)
    return pvc_obj


@pytest.fixture()
def rbd_pod(request, rbd_pvc):
    """
    Create a pod
    """
    pod_obj = helpers.create_pod(
        interface_type=constants.CEPHBLOCKPOOL,
        pvc_name=rbd_pvc.name
    )
    assert pod_obj, f"Failed to create RBD pod"

    def finalizer():
        """
        Delete the pod
        """
        pod_obj.delete()

    request.addfinalizer(finalizer)
    return pod_obj
