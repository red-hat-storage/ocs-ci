import logging

import pytest

from ocs_ci.helpers.cephx_keyrotation_helper import CephXKeyRotation

log = logging.getLogger(__name__)


@pytest.fixture(scope="class")
def cephx_keyrotation_setup():
    """
    Prepare cluster for CephX key rotation TC-01:
      - enable daemon KeyGeneration policy on CephCluster
      - wait for mon/mgr/osd/mds daemons and cluster Ready state
    """
    rotator = CephXKeyRotation()
    rotator.ensure_daemon_key_rotation_enabled(key_generation=1)
    rotator.wait_for_rook_daemon_pods_ready()
    rotator.wait_for_cluster_ready()

    initial_generation = rotator.get_spec_key_generation(rotator.COMPONENT_DAEMON)
    if initial_generation >= 1:
        rotator.wait_for_rook_daemon_rotation(initial_generation, timeout=900)

    return rotator


@pytest.fixture(scope="class")
def cephx_bootstrap_setup():
    """
    Prepare cluster for bootstrap CephX key cleanup verification:
      - wait for mon/mgr/osd/mds daemons and cluster Ready state
    """
    rotator = CephXKeyRotation()
    rotator.wait_for_rook_daemon_pods_ready()
    rotator.wait_for_cluster_ready()
    return rotator


@pytest.fixture(scope="class")
def cephx_rotation_disabled_setup():
    """
    Prepare cluster for CephX policy-disabled verification:
      - disable daemon/csi/rbdMirrorPeer keyRotationPolicy on CephCluster
      - wait for mon/mgr/osd/mds daemons and cluster Ready state
    """
    rotator = CephXKeyRotation()
    rotator.ensure_key_rotation_disabled()
    rotator.assert_key_rotation_disabled()
    rotator.wait_for_rook_daemon_pods_ready()
    rotator.wait_for_cluster_ready()
    return rotator
