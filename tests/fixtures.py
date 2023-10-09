import pytest

from ocs_ci.ocs.resources.pod import delete_deploymentconfig_pods
from ocs_ci.helpers import helpers
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
        if hasattr(class_instance, "rbd_secret_obj"):
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
    """
    class_instance = request.node.cls

    def finalizer():
        """
        Delete the FS secret
        """
        if hasattr(class_instance, "cephfs_secret_obj"):
            class_instance.cephfs_secret_obj.delete()
            class_instance.cephfs_secret_obj.ocp.wait_for_delete(
                class_instance.cephfs_secret_obj.name
            )

    request.addfinalizer(finalizer)

    class_instance.cephfs_secret_obj = helpers.create_secret(
        interface_type=constants.CEPHFILESYSTEM
    )
    assert class_instance.cephfs_secret_obj, "Failed to create secret"


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
        if hasattr(class_instance, "cbp_obj"):
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
            class_instance.sc_obj.ocp.wait_for_delete(class_instance.sc_obj.name)

    request.addfinalizer(finalizer)

    if not hasattr(class_instance, "reclaim_policy"):
        class_instance.reclaim_policy = constants.RECLAIM_POLICY_DELETE

    class_instance.sc_obj = helpers.create_storage_class(
        interface_type=constants.CEPHBLOCKPOOL,
        interface_name=class_instance.cbp_obj.name,
        secret_name=class_instance.rbd_secret_obj.name,
        reclaim_policy=class_instance.reclaim_policy,
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
            class_instance.sc_obj.ocp.wait_for_delete(class_instance.sc_obj.name)

    request.addfinalizer(finalizer)

    class_instance.sc_obj = helpers.create_storage_class(
        interface_type=constants.CEPHFILESYSTEM,
        interface_name=helpers.get_cephfs_data_pool_name(),
        secret_name=class_instance.cephfs_secret_obj.name,
    )
    assert class_instance.sc_obj, "Failed to create storage class"


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
        ocp.switch_to_default_rook_cluster_project()
        class_instance.project_obj.delete(resource_name=class_instance.namespace)
        class_instance.project_obj.wait_for_delete(class_instance.namespace)

    request.addfinalizer(finalizer)

    class_instance.project_obj = helpers.create_project()
    class_instance.namespace = class_instance.project_obj.namespace


@pytest.fixture()
def create_pvc(request):
    """
    Create a persistent Volume Claim
    """
    class_instance = request.node.cls

    class_instance.pvc_obj = helpers.create_pvc(
        sc_name=class_instance.sc_obj.name, namespace=class_instance.namespace
    )
    helpers.wait_for_resource_state(class_instance.pvc_obj, constants.STATUS_BOUND)
    class_instance.pvc_obj.reload()


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
        if hasattr(class_instance, "pvc_obj"):
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
        namespace=class_instance.namespace,
    )
    helpers.wait_for_resource_state(class_instance.pod_obj, constants.STATUS_RUNNING)
    class_instance.pod_obj.reload()


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
        if hasattr(class_instance, "pod_obj"):
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
        if hasattr(class_instance, "pvc_objs"):
            for pvc_obj in class_instance.pvc_objs:
                pvc_obj.reload()
                backed_pv_name = pvc_obj.backed_pv
                pvc_obj.delete()
            for pvc_obj in class_instance.pvc_objs:
                pvc_obj.ocp.wait_for_delete(pvc_obj.name)
                helpers.validate_pv_delete(backed_pv_name)

    request.addfinalizer(finalizer)

    class_instance.pvc_objs, _ = helpers.create_multiple_pvcs(
        sc_name=class_instance.sc_obj.name,
        number_of_pvc=class_instance.num_of_pvcs,
        size=class_instance.pvc_size,
        namespace=class_instance.namespace,
    )
    for pvc_obj in class_instance.pvc_objs:
        helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND)
        pvc_obj.reload()


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
        if hasattr(class_instance, "pod_objs"):
            for pod in class_instance.pod_objs:
                pod.delete()

    request.addfinalizer(finalizer)

    class_instance.pod_objs = list()
    for pvc_obj in class_instance.pvc_objs:
        class_instance.pod_objs.append(
            helpers.create_pod(
                interface_type=class_instance.interface,
                pvc_name=pvc_obj.name,
                do_reload=False,
                namespace=class_instance.namespace,
            )
        )

    for pod in class_instance.pod_objs:
        helpers.wait_for_resource_state(pod, constants.STATUS_RUNNING)


@pytest.fixture()
def create_dc_pods(request):
    """
    Create multiple deploymentconfig pods
    """
    class_instance = request.node.cls

    def finalizer():
        """
        Delete multiple dc pods
        """
        if hasattr(class_instance, "dc_pod_objs"):
            for pod in class_instance.dc_pod_objs:
                delete_deploymentconfig_pods(pod_obj=pod)

    request.addfinalizer(finalizer)

    class_instance.dc_pod_objs = [
        helpers.create_pod(
            interface_type=class_instance.interface,
            pvc_name=pvc_obj.name,
            do_reload=False,
            namespace=class_instance.namespace,
            sa_name=class_instance.sa_obj.name,
            dc_deployment=True,
            replica_count=class_instance.replica_count,
        )
        for pvc_obj in class_instance.pvc_objs
    ]

    for pod in class_instance.dc_pod_objs:
        helpers.wait_for_resource_state(pod, constants.STATUS_RUNNING, timeout=180)


@pytest.fixture()
def create_serviceaccount(request):
    """
    Create a service account
    """
    class_instance = request.node.cls

    def finalizer():
        """
        Delete the service account
        """
        helpers.remove_scc_policy(
            sa_name=class_instance.sa_obj.name,
            namespace=class_instance.project_obj.namespace,
        )
        class_instance.sa_obj.delete()

    request.addfinalizer(finalizer)

    class_instance.sa_obj = helpers.create_serviceaccount(
        namespace=class_instance.project_obj.namespace,
    )
    helpers.add_scc_policy(
        sa_name=class_instance.sa_obj.name,
        namespace=class_instance.project_obj.namespace,
    )
    assert class_instance.sa_obj, "Failed to create serviceaccount"
