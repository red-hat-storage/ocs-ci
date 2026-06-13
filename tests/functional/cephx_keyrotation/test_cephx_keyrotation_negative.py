"""
CephX Key Rotation — Negative Test Cases

TC-21: Operator crash between Mon auth rotation and Kubernetes secret update.
TC-22: Mon key rotation when Mons are not in quorum.
TC-23: OSD key rotation blocked when PGs are not clean.
TC-24: Single OSD rotation failure fails the entire CephCluster reconcile.
"""

import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    ignore_leftovers,
    skipif_external_mode,
    skipif_ocs_version,
    tier2,
)
from ocs_ci.helpers.cephx_keyrotation_helper import CephXKeyRotation
from ocs_ci.helpers.helpers import get_last_log_time_date
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import get_mon_pods
from ocs_ci.utility.utils import ceph_health_check

log = logging.getLogger(__name__)

MIN_MON_COUNT = 3
MIN_OSD_COUNT = 3
MON_QUORUM_BROKEN_MAX = 1


@skipif_external_mode
@skipif_ocs_version("<4.19")
@green_squad
@ignore_leftovers
class TestCephXKeyRotationNegative:
    @pytest.fixture(autouse=True)
    def _restore_cluster_state(self, request):
        """Restore mon quorum and OSD mark-in after disruptive negative tests."""
        self._scaled_mon_deployments = []
        self._osd_marked_out = None

        def finalizer():
            rotator = CephXKeyRotation()
            if self._scaled_mon_deployments:
                log.info(
                    "Teardown: restoring mon deployments "
                    f"{self._scaled_mon_deployments}"
                )
                rotator.restore_mon_deployments(self._scaled_mon_deployments)
            if self._osd_marked_out is not None:
                log.info(f"Teardown: marking osd.{self._osd_marked_out} back in")
                rotator.set_osd_in(self._osd_marked_out)
                rotator.wait_for_pgs_active_clean(timeout=900)

        request.addfinalizer(finalizer)

    @tier2
    def test_cephx_operator_crash_during_mon_rotation(self, cephx_keyrotation_setup):
        """
        TC-21: Operator crash between Mon Ceph auth rotation and secret update.

        Kill rook-ceph-operator after mon auth rotation starts but before mon
        secrets are updated; verify recovery and cluster health.
        """
        rotator = cephx_keyrotation_setup
        namespace = config.ENV_DATA["cluster_namespace"]

        if len(rotator.get_mon_deployment_names()) < MIN_MON_COUNT:
            pytest.skip(f"Need at least {MIN_MON_COUNT} mon deployments for this test")
        if not rotator.is_mon_key_rotation_supported():
            pytest.skip("MON CephX key rotation is not supported on this cluster")

        ceph_health_check(namespace=namespace)

        target_generation = rotator.kill_operator_during_mon_rotation(timeout=900)
        rotator.recover_after_operator_crash_during_mon_rotation(timeout=1500)
        rotator.wait_for_rook_daemon_rotation(target_generation, timeout=1500)

        ceph_health_check(namespace=namespace)
        log.info("Operator recovered successfully after crash during mon key rotation")

    @tier2
    def test_cephx_mon_rotation_without_quorum(self, cephx_keyrotation_setup):
        """
        TC-22: Mon key rotation is blocked when Mons are not in quorum.

        Scale down two Mons, attempt rotation, verify no key changes, restore
        quorum, and verify a subsequent rotation succeeds.
        """
        rotator = cephx_keyrotation_setup
        namespace = config.ENV_DATA["cluster_namespace"]

        if len(rotator.get_mon_deployment_names()) < MIN_MON_COUNT:
            pytest.skip(f"Need at least {MIN_MON_COUNT} mon deployments for this test")

        ceph_health_check(namespace=namespace)

        baseline_generations = rotator.record_all_cephx_status_generations()
        auth_entities = rotator.discover_all_rotation_auth_entities()
        if not auth_entities:
            pytest.skip("No Ceph auth entities found for rotation verification")

        pre_auth_keys = rotator.capture_auth_keys(
            auth_entities, label="before quorum break"
        )
        operator_log_marker = get_last_log_time_date()

        self._scaled_mon_deployments = rotator.break_mon_quorum(mons_to_stop=2)
        rotator.wait_for_mon_quorum_count_at_most(MON_QUORUM_BROKEN_MAX, timeout=300)

        rotator.rotate_daemon_keys()
        rotator.trigger_cephcluster_reconcile()
        rotator.assert_reported_cephx_generations_unchanged(
            baseline_generations,
            context="while mon quorum is broken",
        )
        rotator.assert_auth_keys_unchanged(
            pre_auth_keys,
            entities=auth_entities,
            context="while mon quorum is broken",
        )
        rotator.verify_operator_logs_contain_any_pattern(
            constants.CEPHX_ROTATION_QUORUM_ERROR_PATTERNS,
            since_time=operator_log_marker,
        )

        rotator.restore_mon_deployments(self._scaled_mon_deployments)
        self._scaled_mon_deployments = []

        target_generation = rotator.rotate_daemon_keys()
        rotator.wait_for_rook_daemon_rotation(target_generation, timeout=1500)
        rotator.verify_auth_keys_changed(pre_auth_keys, entities=auth_entities)
        rotator.verify_pods_no_auth_bad_key(get_mon_pods(namespace=namespace))

        ceph_health_check(namespace=namespace)
        log.info("Mon key rotation blocked without quorum and succeeded after restore")

    @tier2
    def test_cephx_osd_rotation_blocked_by_unhealthy_pgs(self, cephx_keyrotation_setup):
        """
        TC-23: OSD key rotation is deferred when PGs are not active+clean.

        Mark an OSD out, verify rotation is skipped, restore the OSD, and
        verify rotation proceeds once PGs heal.
        """
        rotator = cephx_keyrotation_setup
        namespace = config.ENV_DATA["cluster_namespace"]

        osd_entities = rotator.discover_osd_auth_entities()
        if len(osd_entities) < MIN_OSD_COUNT:
            pytest.skip(f"Need at least {MIN_OSD_COUNT} OSDs for this test")

        ceph_health_check(namespace=namespace)
        rotator.wait_for_pgs_active_clean()

        pre_osd_generation = rotator.get_status_key_generation("osd")
        pre_osd_keys = rotator.capture_auth_keys(osd_entities, label="before osd out")
        operator_log_marker = get_last_log_time_date()

        osd_id = int(osd_entities[0].split(".")[-1])
        self._osd_marked_out = osd_id
        rotator.set_osd_out(osd_id)
        rotator.wait_for_pgs_not_clean(timeout=300)

        rotator.rotate_daemon_keys()
        rotator.trigger_cephcluster_reconcile()
        rotator.assert_auth_keys_unchanged(
            pre_osd_keys,
            entities=osd_entities,
            context="while PGs are not clean",
        )
        assert (
            rotator.get_status_key_generation("osd") == pre_osd_generation
        ), "OSD keyGeneration changed while PGs were not clean"
        rotator.verify_operator_logs_contain_any_pattern(
            constants.CEPHX_OSD_ROTATION_DEFERRED_PATTERNS,
            since_time=operator_log_marker,
        )

        rotator.set_osd_in(osd_id)
        self._osd_marked_out = None
        rotator.wait_for_pgs_active_clean(timeout=900)

        target_generation = rotator.rotate_daemon_keys()
        rotator.wait_for_osd_rotation(target_generation, timeout=1500)
        rotator.verify_auth_keys_changed(pre_osd_keys, entities=osd_entities)
        assert rotator.get_status_key_generation("osd") > pre_osd_generation

        ceph_health_check(namespace=namespace)
        log.info("OSD key rotation deferred until PGs healed, then succeeded")

    @tier2
    def test_cephx_osd_rotation_failure_fails_reconcile(self, cephx_keyrotation_setup):
        """
        TC-24: A single OSD rotation failure fails CephCluster reconcile.

        Inject an OSD auth deletion mid-rotation, verify reconcile failure and
        partial rotation, then verify recovery on the next reconcile.
        """
        rotator = cephx_keyrotation_setup
        namespace = config.ENV_DATA["cluster_namespace"]

        osd_entities = rotator.discover_osd_auth_entities()
        if len(osd_entities) < MIN_OSD_COUNT:
            pytest.skip(f"Need at least {MIN_OSD_COUNT} OSDs for this test")

        ceph_health_check(namespace=namespace)
        rotator.wait_for_pgs_active_clean()

        pre_osd_generation = rotator.get_status_key_generation("osd")
        pre_osd_keys = rotator.capture_auth_keys(
            osd_entities, label="before injected osd failure"
        )
        pre_cephx_status = rotator.capture_osd_deployment_cephx_status()
        operator_log_marker = get_last_log_time_date()

        target_generation = rotator.rotate_daemon_keys()
        failed_entity = rotator.inject_osd_auth_rotation_failure(
            pre_osd_keys, timeout=900
        )

        rotator.wait_for_cephcluster_reconcile_failure(timeout=600)
        rotator.verify_operator_logs_contain_any_pattern(
            constants.CEPHX_RECONCILE_FAILURE_PATTERNS,
            since_time=operator_log_marker,
        )

        mid_rotation_keys = rotator.capture_auth_keys(osd_entities)
        rotated_entities = [
            entity
            for entity in osd_entities
            if pre_osd_keys.get(entity) != mid_rotation_keys.get(entity)
        ]
        unchanged_entities = [
            entity
            for entity in osd_entities
            if pre_osd_keys.get(entity) == mid_rotation_keys.get(entity)
        ]
        assert (
            rotated_entities
        ), "Expected at least one OSD key to rotate before failure"
        assert failed_entity in unchanged_entities or not mid_rotation_keys.get(
            failed_entity
        ), f"Failed OSD entity {failed_entity} should retain prior/missing key"

        rotator.trigger_cephcluster_reconcile()
        rotator.wait_for_osd_rotation(target_generation, timeout=1500)
        rotator.wait_for_rook_daemon_rotation(target_generation, timeout=1500)
        post_osd_keys = rotator.verify_auth_keys_changed(
            pre_osd_keys, entities=osd_entities
        )
        assert all(
            pre_osd_keys.get(entity) != post_osd_keys.get(entity)
            for entity in osd_entities
            if pre_osd_keys.get(entity)
        ), "Not all OSD auth keys reached the target generation after recovery"
        assert rotator.get_status_key_generation("osd") > pre_osd_generation
        rotator.assert_osd_deployment_cephx_status_updated(
            pre_cephx_status, target_generation
        )

        ceph_health_check(namespace=namespace)
        rotator.wait_for_cluster_ready()
        log.info("CephCluster reconcile recovered after single OSD rotation failure")
