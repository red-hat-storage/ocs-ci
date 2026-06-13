"""
CephX Key Rotation — Negative Test Cases

TC-21: Operator crash between Mon auth rotation and Kubernetes secret update.
TC-22: Mon key rotation when Mons are not in quorum.
TC-23: OSD key rotation blocked when PGs are not clean.
TC-24: Single OSD rotation failure fails the entire CephCluster reconcile.
TC-32: Brownfield OSD deployments with empty cephx-status annotations.
TC-33: Operator restart mid OSD rotation checkpoints remaining OSDs.
TC-34: Lockbox key preserved when lockbox rotation init container fails.
TC-NEG-15: Disk-based encrypted OSD deployments carry encrypted=true label.
TC-36: Bootstrap key deletion is idempotent on already-deleted keys.
TC-37: CSI key rotation with priorKeyCount 0 and mounted volumes.
"""

import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    encryption_at_rest_required,
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
MIN_OSD_COUNT_PARTIAL_ROTATION = 5
MIN_ROTATED_OSDS_BEFORE_OPERATOR_KILL = 3
MON_QUORUM_BROKEN_MAX = 1
MIN_ENCRYPTED_OSD_COUNT = 1
MIN_DISK_ENCRYPTED_OSD_COUNT = 1
CSI_IO_FILE = "/mnt/rbd/csi_prior_key_test"
CSI_IO_BS = "4k"
CSI_IO_COUNT = 10000
CSI_WORKLOAD_PVC_SIZE = 10
CSI_POST_ROTATION_PVC_SIZE = 5


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

    @tier2
    def test_cephx_bootstrap_deletion_idempotent(self, cephx_bootstrap_setup):
        """
        TC-36: Bootstrap key deletion is idempotent on already-deleted keys.

        Wait for bootstrap cleanup, restart the operator, and verify no errors
        are logged when deletion is attempted again.
        """
        rotator = cephx_bootstrap_setup
        namespace = config.ENV_DATA["cluster_namespace"]

        ceph_health_check(namespace=namespace)
        rotator.trigger_cephcluster_reconcile()
        rotator.wait_for_post_mon_startup_bootstrap_cleanup(timeout=900)
        rotator.verify_bootstrap_deletion_idempotent_after_operator_restart()

        ceph_health_check(namespace=namespace)
        log.info("Bootstrap key deletion idempotency verified after operator restart")


@skipif_external_mode
@skipif_ocs_version("<4.19")
@green_squad
@ignore_leftovers
class TestCephXKeyRotationNegativeOSD:
    @tier2
    def test_cephx_brownfield_osd_empty_cephx_status(self, cephx_keyrotation_setup):
        """
        TC-32: Brownfield OSD deployments start with empty cephx-status.

        Simulate brownfield OSDs by clearing cephx-status annotations, trigger
        rotation, verify annotations are populated, then verify a subsequent
        rotation behaves like a greenfield deployment.
        """
        rotator = cephx_keyrotation_setup
        namespace = config.ENV_DATA["cluster_namespace"]

        osd_entities = rotator.discover_osd_auth_entities()
        if len(osd_entities) < MIN_OSD_COUNT:
            pytest.skip(f"Need at least {MIN_OSD_COUNT} OSDs for this test")

        ceph_health_check(namespace=namespace)
        rotator.wait_for_pgs_active_clean()

        pre_osd_keys = rotator.capture_auth_keys(
            osd_entities, label="before brownfield"
        )
        pre_cephx_status = rotator.capture_osd_deployment_cephx_status()

        rotator.clear_osd_deployment_cephx_status_annotations()
        rotator.assert_osd_deployments_have_empty_cephx_status()

        first_target = rotator.rotate_daemon_keys()
        rotator.wait_for_osd_rotation(first_target, timeout=1500)
        rotator.verify_auth_keys_changed(pre_osd_keys, entities=osd_entities)
        rotator.assert_osd_deployment_cephx_status_updated(
            {name: {} for name in pre_cephx_status},
            first_target,
        )
        rotator.assert_all_osd_deployments_cephx_status_at_generation(first_target)

        brownfield_post_keys = rotator.capture_auth_keys(
            osd_entities, label="after brownfield rotation"
        )
        brownfield_post_status = rotator.capture_osd_deployment_cephx_status()

        second_target = rotator.rotate_daemon_keys()
        rotator.wait_for_osd_rotation(second_target, timeout=1500)
        rotator.verify_auth_keys_changed(brownfield_post_keys, entities=osd_entities)
        rotator.assert_osd_deployment_cephx_status_updated(
            brownfield_post_status, second_target
        )

        ceph_health_check(namespace=namespace)
        rotator.wait_for_pgs_active_clean()
        log.info(
            "Brownfield OSD cephx-status simulation and subsequent rotation "
            "completed successfully"
        )

    @tier2
    def test_cephx_operator_restart_during_partial_osd_rotation(
        self, cephx_keyrotation_setup
    ):
        """
        TC-33: Operator restart mid OSD rotation only rotates remaining OSDs.

        Kill the operator after a partial OSD cephx-status checkpoint, verify
        already-rotated OSDs are not rotated again, then verify all OSDs reach
        the target generation.
        """
        rotator = cephx_keyrotation_setup
        namespace = config.ENV_DATA["cluster_namespace"]

        baseline_cephx_status = rotator.capture_osd_deployment_cephx_status()
        osd_count = len(baseline_cephx_status)
        if osd_count < MIN_OSD_COUNT_PARTIAL_ROTATION:
            pytest.skip(
                f"Need at least {MIN_OSD_COUNT_PARTIAL_ROTATION} OSD deployments; "
                f"found {osd_count}"
            )

        osd_entities = rotator.discover_osd_auth_entities()
        pre_osd_keys = rotator.capture_auth_keys(
            osd_entities, label="before partial osd rotation"
        )

        ceph_health_check(namespace=namespace)
        rotator.wait_for_pgs_active_clean()

        min_rotated = min(MIN_ROTATED_OSDS_BEFORE_OPERATOR_KILL, osd_count - 1)
        target_generation, rotated_deployments = (
            rotator.kill_operator_during_partial_osd_rotation(
                baseline_cephx_status,
                min_rotated=min_rotated,
                timeout=1500,
            )
        )
        assert rotated_deployments, "Expected partial OSD rotation before operator kill"

        checkpoint_entities = rotator.map_osd_deployments_to_auth_entities(
            rotated_deployments
        )
        checkpoint_keys = rotator.capture_auth_keys(
            checkpoint_entities, label="checkpoint osd keys"
        )
        checkpoint_status = rotator.capture_osd_deployment_cephx_status()

        rotator.wait_for_rook_ceph_operator_ready()
        rotator.assert_osd_deployment_cephx_status_unchanged_for(
            rotated_deployments, checkpoint_status
        )

        rotator.wait_for_osd_rotation(target_generation, timeout=1500)
        rotator.assert_all_osd_deployments_cephx_status_at_generation(target_generation)
        rotator.assert_auth_keys_unchanged_for(
            checkpoint_keys, entities=checkpoint_entities
        )
        rotator.verify_auth_keys_changed(pre_osd_keys, entities=osd_entities)

        ceph_health_check(namespace=namespace)
        rotator.wait_for_pgs_active_clean()
        log.info(
            "Partial OSD rotation checkpoint preserved across operator restart; "
            f"rotated before kill: {rotated_deployments}"
        )


@skipif_external_mode
@skipif_ocs_version("<4.19")
@green_squad
@ignore_leftovers
class TestCephXKeyRotationNegativeEncryptedCSI:
    @pytest.fixture(autouse=True)
    def _restore_cluster_state(self, request):
        """Restore mon quorum after disruptive encrypted OSD negative tests."""
        self._scaled_mon_deployments = []

        def finalizer():
            rotator = CephXKeyRotation()
            if self._scaled_mon_deployments:
                log.info(
                    "Teardown: restoring mon deployments "
                    f"{self._scaled_mon_deployments}"
                )
                rotator.restore_mon_deployments(self._scaled_mon_deployments)

        request.addfinalizer(finalizer)

    @tier2
    @encryption_at_rest_required
    def test_cephx_lockbox_rotation_failure_preserves_key(
        self, cephx_keyrotation_setup
    ):
        """
        TC-34: Lockbox key is preserved when lockbox rotation init fails.

        Break mon quorum during lockbox rotation, verify lockbox keys are not
        lost, restore quorum, and verify encrypted OSDs complete rotation.
        """
        rotator = cephx_keyrotation_setup
        namespace = config.ENV_DATA["cluster_namespace"]

        encrypted_deployments = rotator.capture_encrypted_osd_deployments()
        if len(encrypted_deployments) < MIN_ENCRYPTED_OSD_COUNT:
            pytest.skip(
                f"Need at least {MIN_ENCRYPTED_OSD_COUNT} encrypted OSD "
                f"deployment(s); found {len(encrypted_deployments)}"
            )

        lockbox_entities = rotator.discover_lockbox_auth_entities()
        if not lockbox_entities:
            pytest.skip("No client.osd-lockbox.* auth entities found on cluster")

        if len(rotator.get_mon_deployment_names()) < MIN_MON_COUNT:
            pytest.skip(f"Need at least {MIN_MON_COUNT} mon deployments for this test")

        ceph_health_check(namespace=namespace)
        pre_lockbox_keys = rotator.capture_auth_keys(
            lockbox_entities, label="lockbox keys before disruption"
        )

        target_generation, self._scaled_mon_deployments = (
            rotator.break_mon_quorum_during_lockbox_rotation(mons_to_stop=2)
        )
        rotator.wait_for_mon_quorum_count_at_most(MON_QUORUM_BROKEN_MAX, timeout=300)

        rotator.assert_lockbox_auth_keys_present(lockbox_entities)
        rotator.assert_auth_keys_unchanged(
            pre_lockbox_keys,
            entities=lockbox_entities,
            context="during lockbox rotation disruption",
        )
        rotator.verify_osd_lockbox_init_container_disruption_logs()

        rotator.restore_mon_deployments(self._scaled_mon_deployments)
        self._scaled_mon_deployments = []
        rotator.wait_for_pgs_active_clean(timeout=900)

        rotator.wait_for_osd_rotation(target_generation, timeout=1500)
        rotator.verify_auth_keys_changed(pre_lockbox_keys, entities=lockbox_entities)

        encrypted_osd_pods = rotator.get_encrypted_osd_pods()
        rotator.verify_osd_activate_lockbox_logs(encrypted_osd_pods)
        rotator.verify_encrypted_osd_pods_running(encrypted_osd_pods)

        ceph_health_check(namespace=namespace)
        log.info("Lockbox keys preserved through rotation failure and recovered")

    @tier2
    @encryption_at_rest_required
    def test_cephx_disk_encrypted_osd_label_and_lockbox_rotation(
        self, cephx_keyrotation_setup
    ):
        """
        TC-NEG-15: Disk-based encrypted OSD deployments are labeled encrypted=true.

        Verify host/disk-based encrypted OSDs carry the encrypted label and
        receive lockbox key rotation (not silently skipped).
        """
        rotator = cephx_keyrotation_setup
        namespace = config.ENV_DATA["cluster_namespace"]

        disk_encrypted = rotator.get_disk_based_encrypted_osd_deployments()
        if len(disk_encrypted) < MIN_DISK_ENCRYPTED_OSD_COUNT:
            pytest.skip(
                "No disk-based encrypted OSD deployments found; "
                "need host/disk encrypted OSDs for this test"
            )

        rotator.assert_encrypted_osd_labels(disk_encrypted)
        log.info(
            "Disk-based encrypted OSD deployments: "
            + ", ".join(
                f"{name} (osd_id={info['osd_id']})"
                for name, info in sorted(disk_encrypted.items())
            )
        )

        lockbox_entities = rotator.discover_lockbox_auth_entities()
        if not lockbox_entities:
            pytest.skip("No client.osd-lockbox.* auth entities found on cluster")

        pre_lockbox_keys = rotator.capture_auth_keys(
            lockbox_entities, label="disk encrypted lockbox keys before rotation"
        )
        operator_log_marker = get_last_log_time_date()

        ceph_health_check(namespace=namespace)
        target_generation = rotator.rotate_daemon_keys()
        rotator.wait_for_osd_rotation(target_generation, timeout=1500)
        rotator.verify_auth_keys_changed(pre_lockbox_keys, entities=lockbox_entities)

        operator_logs = rotator.get_operator_logs_since(operator_log_marker)
        disk_lockbox_logs = [
            line for line in operator_logs if constants.OSD_LOCKBOX_OPERATOR_LOG in line
        ]
        assert disk_lockbox_logs, (
            "Operator did not log lockbox rotation for encrypted OSDs; "
            "disk-based OSDs may have been skipped"
        )
        log.info(
            f"Operator logged {len(disk_lockbox_logs)} encrypted OSD lockbox "
            "rotation line(s) for disk-based OSDs"
        )

        ceph_health_check(namespace=namespace)
        log.info("Disk-based encrypted OSD label and lockbox rotation verified")

    @tier2
    def test_cephx_csi_rotation_prior_key_count_zero_with_mounted_volume(
        self, cephx_keyrotation_setup, deployment_pod_factory
    ):
        """
        TC-37: CSI rotation with priorKeyCount 0 while a volume stays mounted.

        Documents that deleting old CSI keys immediately may disrupt mounted
        volumes; verifies new PVC provisioning still works after rotation.
        """
        rotator = cephx_keyrotation_setup
        namespace = config.ENV_DATA["cluster_namespace"]

        csi_entities = rotator.discover_csi_auth_entities()
        if not csi_entities:
            pytest.skip("No CSI Ceph auth entities found on cluster")

        ceph_health_check(namespace=namespace)

        workload_pod = deployment_pod_factory(
            interface=constants.CEPHBLOCKPOOL,
            size=CSI_WORKLOAD_PVC_SIZE,
        )
        io_thread = rotator.start_dd_io_in_background(
            workload_pod, CSI_IO_FILE, bs=CSI_IO_BS, count=CSI_IO_COUNT
        )

        pre_csi_keys = rotator.capture_auth_keys(
            csi_entities, label="csi keys before priorKeyCount=0 rotation"
        )
        csi_log_marker = get_last_log_time_date()

        target_generation = rotator.rotate_csi_keys(keep_prior_key_count_max=0)
        log.info(
            f"Triggered CSI rotation to generation {target_generation} with "
            "keepPriorKeyCountMax=0"
        )

        auth_errors = rotator.verify_csi_node_plugin_logs_for_auth_errors(
            since_time=csi_log_marker
        )
        if auth_errors:
            log.warning(
                "AUTH_BAD_KEY observed on CSI node plugins after deleting old "
                f"CSI keys (expected risk with priorKeyCount=0): "
                f"{len(auth_errors)} line(s)"
            )

        if io_thread.is_alive():
            log.info("Mounted volume I/O remained active during CSI rotation")
            rotator.stop_dd_io(workload_pod, CSI_IO_FILE)
            rotator.verify_io_file_readable(workload_pod, CSI_IO_FILE)
        else:
            log.warning(
                "Mounted volume I/O stopped during CSI rotation with "
                "priorKeyCount=0 (documented trade-off)"
            )

        rotator.wait_for_csi_rotation(target_generation, timeout=1500)
        rotator.verify_auth_keys_changed(pre_csi_keys, entities=csi_entities)

        new_pod = deployment_pod_factory(
            interface=constants.CEPHBLOCKPOOL,
            size=CSI_POST_ROTATION_PVC_SIZE,
        )
        new_pod.exec_cmd_on_pod(
            "dd if=/dev/urandom of=/mnt/post_rotation_csi_test "
            "bs=4k count=100 status=none",
            out_yaml_format=False,
        )
        new_pod.exec_cmd_on_pod(
            "test -s /mnt/post_rotation_csi_test", out_yaml_format=False
        )

        ceph_health_check(namespace=namespace)
        log.info("CSI rotation with priorKeyCount=0 completed; new PVC mount verified")
