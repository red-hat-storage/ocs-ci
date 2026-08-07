import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.helpers.cephx_keyrotation_helper import CephXKeyRotation
from ocs_ci.utility.utils import ceph_health_check

log = logging.getLogger(__name__)

_CEPHX_NEGATIVE_RESTORE_CLASSES = (
    "TestCephXKeyRotationNegative",
    "TestCephXKeyRotationNegativeEncryptedCSI",
)


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


@pytest.fixture(autouse=True)
def cephx_negative_restore_cluster_state(request):
    """
    Shared teardown for disruptive CephX negative tests.

    Restores mon quorum, OSD mark-in, and deleted OSD auth entities, then waits
    for full cluster recovery and health when any restore ran.
    """
    cls = getattr(request, "cls", None)
    if cls is None or cls.__name__ not in _CEPHX_NEGATIVE_RESTORE_CLASSES:
        yield
        return

    instance = request.instance
    instance._scaled_mon_deployments = []
    instance._osd_marked_out = None
    instance._deleted_osd_auth_entity = None

    def finalizer():
        rotator = CephXKeyRotation()
        namespace = config.ENV_DATA["cluster_namespace"]
        restored = False
        # Full recovery wait for mon/auth restore only. OSD mark-in already
        # waits inside restore_osd_and_wait_for_recovery.
        need_full_recovery = False
        teardown_exc = None

        def _record_teardown_error(exc):
            nonlocal teardown_exc
            if teardown_exc is None:
                teardown_exc = exc

        if instance._scaled_mon_deployments:
            log.info(
                "Teardown: restoring mon deployments "
                f"{instance._scaled_mon_deployments}"
            )
            try:
                rotator.restore_mon_deployments(instance._scaled_mon_deployments)
                restored = True
                need_full_recovery = True
            except Exception as exc:
                log.warning("Teardown: failed to restore mon deployments: %s", exc)
                _record_teardown_error(exc)
            instance._scaled_mon_deployments = []
        if instance._osd_marked_out is not None:
            log.info(
                f"Teardown: marking osd.{instance._osd_marked_out} back in and "
                "waiting for full cluster recovery"
            )
            try:
                rotator.restore_osd_and_wait_for_recovery(
                    instance._osd_marked_out, timeout=1500
                )
                restored = True
            except Exception as exc:
                log.warning(
                    "Teardown: failed to restore osd.%s: %s",
                    instance._osd_marked_out,
                    exc,
                )
                _record_teardown_error(exc)
            instance._osd_marked_out = None
        if instance._deleted_osd_auth_entity:
            entity = instance._deleted_osd_auth_entity
            log.info(f"Teardown: ensuring deleted OSD auth entity {entity} is restored")
            try:
                if rotator.ensure_osd_auth_entity_restored(entity):
                    rotator.trigger_cephcluster_reconcile()
                    need_full_recovery = True
                restored = True
            except Exception as exc:
                log.warning(
                    f"Teardown: failed to restore OSD auth entity {entity}: {exc}"
                )
                _record_teardown_error(exc)
            instance._deleted_osd_auth_entity = None
        if need_full_recovery:
            try:
                rotator.wait_for_cluster_fully_recovered(timeout=1500)
            except Exception as exc:
                log.warning(
                    "Teardown: wait_for_cluster_fully_recovered failed: %s", exc
                )
                _record_teardown_error(exc)
        if restored:
            try:
                ceph_health_check(namespace=namespace)
            except Exception as exc:
                log.warning("Teardown: ceph_health_check failed: %s", exc)
                _record_teardown_error(exc)
        try:
            rotator.ensure_daemon_key_generations_aligned()
        except Exception as exc:
            log.warning("Teardown: failed to align daemon keyGeneration: %s", exc)
            _record_teardown_error(exc)
        if teardown_exc is not None:
            raise teardown_exc

    request.addfinalizer(finalizer)
    yield


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
