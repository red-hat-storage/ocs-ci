import pytest
from tests import helpers
from ocs_ci.ocs import constants, ocp


@pytest.fixture()
def secret(request, interface_type):
    """
    Create a secret

    Returns:
        OCS: An instance of secret OCS

    """
    secrets = list()

    def finalizer():
        """
        Delete the RBD secret
        """
        for secret in secrets:
            secret.delete()
            secret.ocp.wait_for_delete(secret.name)

    request.addfinalizer(finalizer)
    secrets.append(helpers.create_secret(interface_type=interface_type))

    return secrets


@pytest.fixture()
def pool(request, interface):
    """
    Create a pool

    Returns:

    """
    pools = list()

    def finalizer():
        """
        Delete the Ceph block pool
        """
        if interface == constants.CEPHBLOCKPOOL:
            for pool in pools:
                pool.delete()
                pool.ocp.wait_for_delete(pool.name)

    request.addfinalizer(finalizer)

    if interface == constants.CEPHBLOCKPOOL:
        pools.append(helpers.create_ceph_block_pool())
    elif interface == constants.CEPHFILESYSTEM:
        pools.append(helpers.get_cephfs_data_pool_name())
    return pools


@pytest.fixture()
def storageclass(request, secret, pool, interface_type):
    """
    Create storage class

    Returns:
        OCS: An instance of storageclass OCS

    """
    storageclasses = list()

    def finalizer():
        """
        Delete the storage class

        """
        for sc in storageclasses:
            sc.delete()
            sc.ocp.wait_for_delete(sc.name)

    request.addfinalizer(finalizer)
    storageclasses.append(
        helpers.create_storage_class(
            interface_type=interface_type,
            interface_name=pool[0].name,
            secret_name=secret[0].name,
        )
    )
    return storageclasses


@pytest.fixture()
def project(request):
    """
    Create a new project

    Returns:
        OCP: An instance of project OCP
    """
    projects = list()

    def finalizer():
        """
        Delete the project

        """
        ocp.switch_to_default_rook_cluster_project()
        for project in projects:
            project.delete(project.namespace)
            project.wait_for_delete(project.namespace)

    request.addfinalizer(finalizer)

    projects.append(helpers.create_project())
    return projects


@pytest.fixture()
def pvc(request, project, storageclass):
    """
    Create a persistent Volume Claim

    Returns:
        PVC: An instance of PVC

    """
    pvcs = list()

    def finalizer():
        """
        Delete the PVC

        """
        for pvc in pvcs:
            pvc.delete()
            pvc.ocp.wait_for_delete(pvc.name)

        request.addfinalizer(finalizer)

    pvcs.append(
        helpers.create_pvc(
            sc_name=storageclass[0].name, namespace=project[0].namespace
        )
    )
    return pvcs


@pytest.fixture()
def pod(request, pvc, interface_type, project):
    """
    Create a pod

    Returns:
        Pod: An instance of Pod

    """
    pods = list()

    def finalizer():
        """
        Delete the pod

        """
        for pod in pods:
            pod.delete()
            pod.ocp.wait_for_delete(pod.name)

    request.addfinazlizer(finalizer)

    pods.append(
        helpers.create_pod(
            interface_type=interface_type,
            pvc_name=pvc[0].name, namespace=project[0].namespace
        )
    )

    return pods


@pytest.fixture()
def pvcs(request, storageclass, num_of_pvcs, pvc_size, project):
    """
    Create multiple PVCs

    Return:
        list: PVC objects

    """
    pvcs = list()

    def finalizer():
        """
        Delete multiple PVCs
        """
        for pvc in pvcs:
            pvc.delete()
        for pvc in pvcs:
            pvc.ocp.wait_for_delete(pvc.name)

    request.addfinalizer(finalizer)

    pvcs = helpers.create_multiple_pvcs(
        sc_name=storageclass[0].name, number_of_pvc=num_of_pvcs,
        size=pvc_size, namespace=project[0].namespace
    )
    return pvcs


@pytest.fixture()
def pods(request, pvcs, project, interface_type):
    """
    Create multiple pods

    Returns:
        list: Pod instances

    """
    pods = list()

    def finalizer():
        """
        Delete multiple pods

        """
        for pod in pods:
            pod.delete()
            pod.ocp.wait_for_delete(pod.name)

    request.addfinalizer(finalizer)

    pods = [
        helpers.create_pod(
            interface_type=interface_type, pvc_name=pvc.name,
            wait=False, namespace=project[0].namespace
        ) for pvc in pvcs
    ]
    for pod in pods:
        assert helpers.wait_for_resource_state(
            pod, constants.STATUS_RUNNING
        ), f"Pod {pod} failed to reach {constants.STATUS_RUNNING}"

    return pods
