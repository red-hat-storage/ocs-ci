import logging
import pytest

from ocs_ci.ocs import constants
from tests import helpers

log = logging.getLogger(__name__)


@pytest.fixture(scope='session')
def pre_upgrade_rbd_pods(request):
    """
    Generate RBD pods for tests before upgrade is executed.

    Returns:
        list: List of pods with RBD interface
    """
    log.info('Creating RBD resources for upgrade testing')
    rbd_pool = helpers.create_ceph_block_pool()
    rbd_secret_obj = helpers.create_secret(constants.CEPHBLOCKPOOL)
    rbd_sc_obj = helpers.create_storage_class(
        interface_type=constants.CEPHBLOCKPOOL,
        interface_name=rbd_pool.name,
        secret_name=rbd_secret_obj.name

    )
    rbd_pvc_obj = helpers.create_pvc(
        sc_name=rbd_sc_obj.name
    )
    helpers.wait_for_resource_state(rbd_pvc_obj, constants.STATUS_BOUND)
    rbd_pvc_obj.reload()
    if rbd_pvc_obj.backed_pv is None:
        rbd_pvc_obj.reload()

    pods = [
        helpers.create_pod(
            interface_type=constants.CEPHBLOCKPOOL,
            pvc_name=rbd_pvc_obj.name
        ) for _ in range(2)
    ]
    for pod in pods:
        helpers.wait_for_resource_state(pod, constants.STATUS_RUNNING)
        pod.reload()
    log.info('RBD resources for upgrade testing created')

    def teardown():
        """
        Destroy all created resources
        """
        log.info('Destroying RBD resources for upgrade testing')
        for pod in pods:
            pod.delete()
        rbd_pvc_obj.delete()
        rbd_sc_obj.delete()
        rbd_secret_obj.delete()
        log.info('RBD resources for upgrade testing destroyed')

    request.addfinalizer(teardown)
    return pods


@pytest.fixture(scope='session')
def pre_upgrade_cephfs_pods(request):
    """
    Generate CephFS pods for tests before upgrade is executed.

    Returns:
        list: List of pods with CephFS interface
    """
    log.info('Creating CephFS resources for upgrade testing')
    cephfs_secret_obj = helpers.create_secret(constants.CEPHFILESYSTEM)
    cephfs_sc_obj = helpers.create_storage_class(
        interface_type=constants.CEPHFILESYSTEM,
        interface_name=helpers.get_cephfs_data_pool_name(),
        secret_name=cephfs_secret_obj.name

    )
    cephfs_pvc_obj = helpers.create_pvc(
        sc_name=cephfs_sc_obj.name
    )
    helpers.wait_for_resource_state(cephfs_pvc_obj, constants.STATUS_BOUND)
    cephfs_pvc_obj.reload()
    if cephfs_pvc_obj.backed_pv is None:
        cephfs_pvc_obj.reload()

    pods = [
        helpers.create_pod(
            interface_type=constants.CEPHFILESYSTEM,
            pvc_name=rbd_pvc_obj.name
        ) for _ in range(2)
    ]
    for pod in pods:
        helpers.wait_for_resource_state(pod, constants.STATUS_RUNNING)
        pod.reload()
    log.info('CephFS resources for upgrade testing created')

    def teardown():
        """
        Destroy all created resources
        """
        log.info('Destroying CephFS resources for upgrade testing')
        for pod in pods:
            pod.delete()
        cephfs_pvc_obj.delete()
        cephfs_sc_obj.delete()
        cephfs_secret_obj.delete()
        log.info('CephFS resources for upgrade testing destroyed')

    request.addfinalizer(teardown)
    return pods


@pytest.fixture
def post_upgrade_pods(pod_factory):
    """
    Generate pods for tests.

    Returns:
        list: List of pods with RBD and CephFS interface
    """
    rbd_pods = [pod_factory(constants.CEPHBLOCKPOOL) for _ in range(2)]
    cephfs_pods = [pod_factory(constants.CEPHFILESYSTEM) for _ in range(2)]
    return rbd_pods + cephfs_pods
