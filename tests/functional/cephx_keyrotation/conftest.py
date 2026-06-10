import logging

import pytest

from ocs_ci.helpers.cephx_keyrotation_helper import CephXKeyRotation

log = logging.getLogger(__name__)


@pytest.fixture(scope="class")
def cephx_keyrotation_setup():
    """
    Prepare cluster for CephX key rotation TC-01:
      - enable daemon KeyGeneration policy on CephCluster
      - ensure RBD mirror daemon is running
      - wait for daemons and cluster Ready state
    """
    rotator = CephXKeyRotation()
    rotator.ensure_daemon_key_rotation_enabled(key_generation=1)
    rotator.ensure_rbd_mirror()
    rotator.wait_for_rook_daemon_pods_ready()
    rotator.wait_for_cluster_ready()

    initial_generation = rotator.get_spec_key_generation(rotator.COMPONENT_DAEMON)
    if initial_generation >= 1:
        rotator.wait_for_daemon_rotation(initial_generation, timeout=900)
        rotator.wait_for_filesystem_daemon_rotation(initial_generation, timeout=900)
        rotator.wait_for_rbd_mirror_daemon_rotation(initial_generation, timeout=900)
        rotator.wait_for_mon_rotation(initial_generation, timeout=900)

    return rotator
