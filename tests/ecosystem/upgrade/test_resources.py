import logging
import pytest
import threading

from ocs_ci.framework.pytest_customization.marks import (
    order_pre_upgrade, order_post_upgrade
)
from ocs_ci.ocs import constants

log = logging.getLogger(__name__)


@pytest.fixture(scale='session')
def pre_upgrade_pods(pod_factory):
    """
    Generate pods for tests before upgrade is executed.

    Returns:
        list: List of pods with RBD and CephFS interface
    """
    rbd_pods = [pod_factory(constants.CEPHBLOCKPOOL) for _ in range(2)]
    cephfs_pods = [pod_factory(constants.CEPHFILESYSTEM) for _ in range(2)]
    return rbd_pods + cephfs_pods

@pytest.fixture()
def post_upgrade_pods(pod_factory):
    """
    Generate pods for tests.

    Returns:
        list: List of pods with RBD and CephFS interface
    """
    rbd_pods = [pod_factory(constants.CEPHBLOCKPOOL) for _ in range(2)]
    cephfs_pods = [pod_factory(constants.CEPHFILESYSTEM) for _ in range(2)]
    return rbd_pods + cephfs_pods

@order_pre_upgrade
def test_pre_upgrade_pods(pre_upgrade_pods):
    """
    Confirm that there are pods created before upgrade.
    """
    assert pre_upgrade_pods

@order_post_upgrade
@pytest.mark.polarion_id("OCS-1862")
def test_pod_io(pre_upgrade_pods, post_upgrade_pods):
    """
    Test IO on multiple pods at the same time.
    """
    pods = pre_upgrade_pods + post_upgrade_pods
    for pod in pods:
          log.info(f"Running fio on {pod.name}")
          pod.run_io(
              storage_type='fs',
              size='1GB',
              runtime=30,
          )
    for pod in pods:
          log.info("Waiting for results from {pod.name}")
          fio_result = pod.get_fio_results()
          reads = fio_result.get('jobs')[0].get('read').get('iops')
          writes = fio_result.get('jobs')[0].get('write').get('iops')
          assert reads, f"There are no reads from pod {pod.name}"
          assert writes, f"There are no writes from pod {pod.name}"
