import logging
import pytest

from ocs_ci.ocs import constants

log = logging.getLogger(__name__)


@pytest.fixture(scope='session')
def pre_upgrade_pods(request, pod_factory_session):
    """
    Generate RBD and CephFS pods for tests before upgrade is executed.

    Returns:
        list: List of pods with RBD interface
    """
    rbd_pods = [
        pod_factory_session(
            interface=constants.CEPHBLOCKPOOL
        ) for _ in range(2)
    ]
    cephfs_pods = [
        pod_factory_session(
            interface=constants.CEPHFILESYSTEM
        ) for _ in range(2)
    ]
    return rbd_pods + cephfs_pods


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
