import logging

import pytest

from ocs_ci.helpers.cephx_keyrotation_helper import CephXKeyRotation

log = logging.getLogger(__name__)


def _align_daemon_key_generations():
    """Align StorageCluster daemon keyGeneration to CephCluster (teardown)."""
    try:
        rotator = CephXKeyRotation()
        aligned = rotator.ensure_daemon_key_generations_aligned()
        log.info(
            "Teardown: StorageCluster/CephCluster daemon keyGeneration aligned "
            "at %s",
            aligned,
        )
    except Exception as exc:
        log.warning(
            "Teardown: failed to align StorageCluster/CephCluster daemon "
            "keyGeneration: %s",
            exc,
        )


@pytest.fixture(autouse=True)
def cephx_align_key_generations_teardown(request):
    """
    After every CephX test, ensure StorageCluster and CephCluster daemon
    keyGeneration values match (CephCluster is the source of truth).
    """
    request.addfinalizer(_align_daemon_key_generations)


@pytest.fixture(scope="class")
def cephx_keyrotation_setup():
    """
    Prepare cluster for CephX key rotation TC-01:
      - enable daemon KeyGeneration policy on StorageCluster
      - wait for mon/mgr/osd/mds daemons and cluster Ready state

    Enabling at DESIRED_CEPHX_KEY_GEN / DEFAULT_DAEMON_KEY_GENERATION only
    updates StorageCluster; CephCluster does not Progress and status may stay
    at keyGeneration 1. Do not wait for status to reach the desired baseline.
    """
    rotator = CephXKeyRotation()
    rotator.ensure_daemon_key_rotation_enabled(
        key_generation=CephXKeyRotation.DEFAULT_DAEMON_KEY_GENERATION
    )
    rotator.wait_for_rook_daemon_pods_ready()
    rotator.wait_for_cluster_ready()
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
      - disable daemon keyRotationPolicy on StorageCluster cephCluster
      - disable rbdMirrorPeer keyRotationPolicy on StorageCluster cephRBDMirror
      - disable csi keyRotationPolicy on CephCluster
      - wait for mon/mgr/osd/mds daemons and cluster Ready state
    """
    rotator = CephXKeyRotation()
    rotator.ensure_key_rotation_disabled()
    rotator.assert_key_rotation_disabled()
    rotator.wait_for_rook_daemon_pods_ready()
    rotator.wait_for_cluster_ready()
    return rotator
