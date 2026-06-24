"""
CephX Key Rotation — Daemon Rotation and Cluster Health

TC-01/02/03: Rook daemon rotation, consecutive rotations, OSD init container.
Idempotency after operator re-reconcile.
I/O continuity during full key rotation.
"""

import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    skipif_external_mode,
    skipif_ocs_version,
    tier1,
)
from ocs_ci.helpers.helpers import get_last_log_time_date
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.resources.pod import (
    get_mgr_pods,
    get_mon_pods,
    get_osd_pods,
)
from ocs_ci.utility.utils import ceph_health_check

log = logging.getLogger(__name__)

CONSECUTIVE_ROTATION_COUNT = 3
MIN_OSD_COUNT = 2
IDEMPOTENCY_SETTLE_SECONDS = 120
RBD_IO_FILE = "/mnt/rbd/testfile"
CEPHFS_IO_FILE = "/mnt/cephfs/testfile"
DD_BS = "4k"
DD_COUNT = 10000
WORKLOAD_PVC_SIZE = 10
POST_ROTATION_PVC_SIZE = 5


@skipif_external_mode
@skipif_ocs_version("<4.19")
@green_squad
class TestCephXKeyRotation:
    @tier1
    def test_cephx_key_rotation_all_rook_daemons(self, cephx_keyrotation_setup):
        """
        TC-01: Rotate CephX keys for MON, MGR, OSD, and MDS daemons.

        Steps:
            1. Record keyGeneration, auth keys, pod names, and cephx-key-identifier
               annotations for MON, MGR, OSD, and MDS.
            2. Trigger daemon key rotation (increment desired key generation).
            3. Wait for status.cephx updates on CephCluster and CephFilesystem;
               wait for daemon pod restarts.
            4. Verify new keys, updated annotations, unchanged capabilities, and
               cluster health (HEALTH_OK, Ready).
        """
        rotator = cephx_keyrotation_setup
        namespace = config.ENV_DATA["cluster_namespace"]

        log.info("Recording pre-rotation CephX key generation values")
        pre_mgr_generation = rotator.get_status_key_generation("mgr")
        pre_mon_generation = rotator.get_status_key_generation("mon")
        pre_osd_generation = rotator.get_status_key_generation("osd")
        pre_mds_generation = rotator.get_filesystem_daemon_key_generation()
        mon_rotation_supported = rotator.is_mon_key_rotation_supported()
        mon_auth_verifiable = rotator.is_mon_auth_verifiable()
        log.info(
            f"Pre-rotation keyGeneration: mon={pre_mon_generation} "
            f"mgr={pre_mgr_generation} osd={pre_osd_generation} "
            f"mds={pre_mds_generation}"
        )

        auth_entities = rotator.discover_rook_daemon_auth_entities()
        for daemon, entities in auth_entities.items():
            if daemon == "mon" and not entities:
                log.info(
                    "MON auth entities not in ceph auth store; MON rotation "
                    "will be verified via status.cephx.mon and mon pod restarts"
                )
                continue
            assert entities, f"No Ceph auth entities found for {daemon}"
            log.info(f"Pre-rotation {daemon} auth entities: {', '.join(entities)}")

        all_entities = rotator.flatten_daemon_auth_entities(auth_entities)
        pre_auth_keys = rotator.capture_auth_keys(all_entities, label="before rotation")
        pre_auth_caps = rotator.capture_auth_caps(all_entities)
        pre_pod_states = rotator.capture_all_daemon_pod_states()

        for daemon, pods in pre_pod_states.items():
            assert pods, f"No Running pods found for {daemon} before rotation"
            log.info(
                f"Pre-rotation {daemon} pods: "
                f"{', '.join(f'{name} (cephx-key-identifier={ann})' for name, ann in pods.items())}"
            )

        ceph_health_check(namespace=namespace)

        target_generation = rotator.rotate_daemon_keys()
        log.info(f"Triggered daemon CephX rotation to generation {target_generation}")

        rotator.wait_for_rook_daemon_rotation(target_generation)
        post_pod_states = rotator.wait_for_all_daemon_pod_restarts(pre_pod_states)

        log.info("Verifying post-rotation keyGeneration values")
        rotator.assert_rook_daemon_generations(
            target_generation, mon_rotation_supported
        )

        post_auth_keys = rotator.verify_auth_keys_changed(
            pre_auth_keys, entities=all_entities
        )
        rotator.log_auth_key_snapshot("after rotation", post_auth_keys)
        rotator.verify_auth_caps_unchanged(pre_auth_caps, entities=all_entities)
        rotator.log_generation_status("Post-rotation")

        for daemon, pods in post_pod_states.items():
            for pod_name, annotation in pods.items():
                if annotation is None and daemon == "mon" and not mon_auth_verifiable:
                    log.warning(
                        f"Pod {pod_name} ({daemon}) missing cephx-key-identifier; "
                        "MON auth is not verifiable on this cluster"
                    )
                    continue
                assert (
                    annotation is not None
                ), f"Pod {pod_name} ({daemon}) missing cephx-key-identifier annotation"
            log.info(
                f"Post-rotation {daemon} pods: "
                f"{', '.join(f'{name} (cephx-key-identifier={ann})' for name, ann in pods.items())}"
            )

        ceph_health_check(namespace=namespace)
        rotator.wait_for_cluster_ready()

        if mon_rotation_supported:
            assert rotator.get_status_key_generation("mon") > pre_mon_generation
        assert rotator.get_status_key_generation("mgr") > pre_mgr_generation
        assert rotator.get_status_key_generation("osd") > pre_osd_generation
        assert rotator.get_filesystem_daemon_key_generation() > pre_mds_generation

        log.info("CephX key rotation for MON/MGR/OSD/MDS completed successfully")

    @tier1
    def test_cephx_key_rotation_consecutive_rotations(self, cephx_keyrotation_setup):
        """
        TC-02: Verify multiple consecutive daemon CephX key rotations.

        Steps:
            1. Record starting keyGeneration values and auth keys for all daemons.
            2. For each of *CONSECUTIVE_ROTATION_COUNT* iterations:
               a. Trigger daemon key rotation.
               b. Wait for status.cephx updates and pod restarts.
               c. Verify keyGeneration incremented and auth keys changed.
               d. Verify no auth key value is reused across any prior generation.
               e. Verify cluster health (HEALTH_OK) and Ready state.
        """
        rotator = cephx_keyrotation_setup
        namespace = config.ENV_DATA["cluster_namespace"]
        mon_rotation_supported = rotator.is_mon_key_rotation_supported()

        auth_entities = rotator.discover_rook_daemon_auth_entities()
        for daemon, entities in auth_entities.items():
            if daemon == "mon" and not entities:
                log.info(
                    "MON auth entities not in ceph auth store; MON rotation "
                    "will be verified via status.cephx.mon and mon pod restarts"
                )
                continue
            assert entities, f"No Ceph auth entities found for {daemon}"
            log.info(f"Auth entities for {daemon}: {', '.join(entities)}")

        all_entities = rotator.flatten_daemon_auth_entities(auth_entities)
        assert all_entities, "No Ceph auth entities found for daemon verification"

        ceph_health_check(namespace=namespace)
        rotator.log_generation_status("Initial")

        current_keys = rotator.capture_auth_keys(all_entities, label="initial")
        key_history = {entity: {key} for entity, key in current_keys.items() if key}

        for rotation_index in range(1, CONSECUTIVE_ROTATION_COUNT + 1):
            log.info(
                f"Starting consecutive rotation {rotation_index}/"
                f"{CONSECUTIVE_ROTATION_COUNT}"
            )

            pre_auth_keys = current_keys
            pre_generations = rotator.record_daemon_generations()
            pre_pod_states = rotator.capture_all_daemon_pod_states()

            for daemon, pods in pre_pod_states.items():
                assert pods, f"No Running pods found for {daemon} before rotation"

            target_generation = rotator.rotate_daemon_keys()
            log.info(
                f"Triggered daemon CephX rotation to generation {target_generation}"
            )

            rotator.wait_for_rook_daemon_rotation(target_generation)
            rotator.wait_for_all_daemon_pod_restarts(pre_pod_states)

            rotator.assert_rook_daemon_generations(
                target_generation, mon_rotation_supported
            )
            rotator.assert_generations_increased(
                pre_generations, mon_rotation_supported
            )
            rotator.log_generation_status(f"After rotation {rotation_index}")

            current_keys = rotator.verify_auth_keys_changed(
                pre_auth_keys, entities=all_entities
            )
            rotator.log_auth_key_snapshot(
                f"after rotation {rotation_index}", current_keys
            )

            for entity, key in current_keys.items():
                if not key:
                    continue
                prior_keys = key_history.setdefault(entity, set())
                assert key not in prior_keys, (
                    f"CephX key for {entity} was reused during rotation "
                    f"{rotation_index} (generation {target_generation})"
                )
                prior_keys.add(key)

            ceph_health_check(namespace=namespace)
            rotator.wait_for_cluster_ready()
            log.info(
                f"Consecutive rotation {rotation_index}/{CONSECUTIVE_ROTATION_COUNT} "
                f"completed at keyGeneration {target_generation}"
            )

        log.info(
            f"Completed {CONSECUTIVE_ROTATION_COUNT} consecutive CephX key "
            "rotations for MON/MGR/OSD/MDS successfully"
        )

    @tier1
    def test_cephx_key_rotation_osd_init_container(self, cephx_keyrotation_setup):
        """
        TC-03: Verify OSD CephX key rotation and init container key load.

        Steps:
            1. Record baseline OSD auth keys, deployment cephx-status annotations,
               and pod cephx-key-identifier values.
            2. Trigger daemon key rotation (rotates all OSD auth entities).
            3. Wait for OSD status.cephx and pod restarts.
            4. Verify cephx-keyring-update init container logs and updated keys.
            5. Verify PGs are active+clean and cluster health is OK.
            6. Reconcile the same keyGeneration and verify idempotency.
        """
        rotator = cephx_keyrotation_setup
        namespace = config.ENV_DATA["cluster_namespace"]

        osd_entities = rotator.discover_osd_auth_entities()
        assert len(osd_entities) >= MIN_OSD_COUNT, (
            f"Expected at least {MIN_OSD_COUNT} OSD auth entities, "
            f"found {len(osd_entities)}"
        )
        log.info(f"OSD auth entities: {', '.join(osd_entities)}")

        osd_store_types = rotator.capture_osd_store_types()
        log.info(
            "OSD store types: "
            + ", ".join(
                f"{name}={store_type}"
                for name, store_type in sorted(osd_store_types.items())
            )
        )
        store_values = set(osd_store_types.values())
        if store_values == {"pvc"}:
            log.info("All OSDs are PVC-backed on this cluster")
        elif store_values == {"host"}:
            log.info("All OSDs are host-based on this cluster")
        else:
            log.info("Cluster has a mix of host-based and PVC-based OSDs")

        pre_osd_generation = rotator.get_status_key_generation("osd")
        log.info(f"Pre-rotation OSD keyGeneration: {pre_osd_generation}")

        pre_auth_keys = rotator.capture_auth_keys(osd_entities, label="before rotation")
        pre_cephx_status = rotator.capture_osd_deployment_cephx_status()
        for deployment_name, status in pre_cephx_status.items():
            log.info(
                f"Pre-rotation {deployment_name} cephx-status: {status or '<empty>'}"
            )

        pre_pod_states = rotator.capture_daemon_pod_state(constants.OSD_APP_LABEL)
        assert pre_pod_states, "No Running OSD pods found before rotation"
        log.info(
            "Pre-rotation OSD pods: "
            + ", ".join(
                f"{name} (cephx-key-identifier={annotation})"
                for name, annotation in pre_pod_states.items()
            )
        )

        ceph_health_check(namespace=namespace)

        target_generation = rotator.rotate_daemon_keys()
        log.info(f"Triggered daemon CephX rotation to generation {target_generation}")

        rotator.wait_for_osd_rotation(target_generation)
        post_pod_states = rotator.wait_for_pod_restarts(
            pre_pod_states, constants.OSD_APP_LABEL
        )

        assert (
            rotator.get_status_key_generation("osd") >= target_generation
        ), "CephCluster status.cephx.osd keyGeneration did not reach target"
        log.info(
            f"Post-rotation OSD keyGeneration: "
            f"{rotator.get_status_key_generation('osd')}"
        )

        rotator.verify_osd_cephx_init_container_logs(get_osd_pods(namespace=namespace))

        post_auth_keys = rotator.verify_auth_keys_changed(
            pre_auth_keys, entities=osd_entities
        )
        rotator.log_auth_key_snapshot("after rotation", post_auth_keys)

        rotator.assert_osd_deployment_cephx_status_updated(
            pre_cephx_status, target_generation
        )
        post_cephx_status = rotator.capture_osd_deployment_cephx_status()
        for deployment_name, status in post_cephx_status.items():
            log.info(
                f"Post-rotation {deployment_name} cephx-status: {status or '<empty>'}"
            )

        for pod_name, annotation in post_pod_states.items():
            assert (
                annotation is not None
            ), f"OSD pod {pod_name} missing cephx-key-identifier annotation"
        log.info(
            "Post-rotation OSD pods: "
            + ", ".join(
                f"{name} (cephx-key-identifier={annotation})"
                for name, annotation in post_pod_states.items()
            )
        )

        rotator.wait_for_pgs_active_clean()
        ceph_health_check(namespace=namespace)
        rotator.wait_for_cluster_ready()

        assert rotator.get_status_key_generation("osd") > pre_osd_generation

        rotator.verify_daemon_rotation_idempotent(
            target_generation,
            post_auth_keys,
            post_pod_states,
            osd_entities,
        )

        log.info("OSD CephX key rotation completed successfully")


@skipif_external_mode
@skipif_ocs_version("<4.19")
@green_squad
class TestCephXKeyRotationIdempotency:
    @tier1
    def test_cephx_key_rotation_rereconcile_idempotent(self, cephx_keyrotation_setup):
        """
        Verify operator re-reconcile does not trigger a second CephX key rotation.

        Steps:
            1. Ensure CephX key rotation is enabled on the cluster.
            2. Trigger full key rotation and wait for completion.
            3. Record keyGeneration values, auth keys, pod state, and OSD cephx-status.
            4. Restart rook-ceph-operator and wait for reconciliation.
            5. Re-check generations, auth keys, pod state, and operator logs.

        Expected:
            - keyGeneration values remain unchanged after re-reconcile.
            - Auth keys remain unchanged.
            - Operator logs do not show rotation messages on re-reconcile.
            - Daemon pods do not restart unnecessarily.
            - OSD cephx-status annotations prevent redundant rotation.
        """
        rotator = cephx_keyrotation_setup
        namespace = config.ENV_DATA["cluster_namespace"]

        ceph_health_check(namespace=namespace)

        log.info("Triggering full CephX key rotation")
        generations = rotator.rotate_all_keys()
        log.info(f"Requested key generations: {generations}")
        rotator.wait_for_all_key_rotations(generations, timeout=1500)

        baseline_generations = rotator.record_all_cephx_status_generations()
        rotator.log_generation_status("Post-rotation baseline")
        log.info(f"Baseline CephX status generations: {baseline_generations}")

        auth_entities = rotator.discover_all_rotation_auth_entities()
        if not auth_entities:
            pytest.skip("No Ceph auth entities found for idempotency verification")

        log.info(
            "Auth entities tracked for idempotency verification: "
            f"{', '.join(auth_entities)}"
        )
        post_rotation_auth_keys = rotator.capture_auth_keys(
            auth_entities, label="after rotation before operator restart"
        )
        post_rotation_pod_states = rotator.capture_all_daemon_pod_states()
        post_rotation_osd_cephx_status = rotator.capture_osd_deployment_cephx_status()

        operator_log_marker = get_last_log_time_date()
        previous_operator_pod = rotator.restart_rook_ceph_operator()

        rotator.verify_key_rotation_idempotent_after_operator_restart(
            baseline_generations,
            post_rotation_auth_keys,
            auth_entities,
            post_rotation_pod_states,
            osd_cephx_status=post_rotation_osd_cephx_status,
            operator_log_since=operator_log_marker,
            previous_operator_pod_name=previous_operator_pod,
            settle_timeout=IDEMPOTENCY_SETTLE_SECONDS,
        )

        ceph_health_check(namespace=namespace)
        rotator.wait_for_pgs_active_clean()
        log.info(
            "CephX key rotation idempotency after operator re-reconcile verified "
            f"(restarted operator pod {previous_operator_pod})"
        )


@skipif_external_mode
@skipif_ocs_version("<4.19")
@green_squad
class TestCephXKeyRotationIOContinuity:
    @tier1
    def test_cephx_key_rotation_io_continuity(
        self, cephx_keyrotation_setup, deployment_pod_factory
    ):
        """
        Verify cluster health and I/O continuity during full CephX key rotation.

        Steps:
            1. Create RBD and CephFS PVCs mounted to pods; start continuous dd I/O.
            2. Trigger full key rotation (daemon, CSI, RBD mirror peer).
            3. Monitor I/O during rotation — verify background threads stay alive.
            4. After rotation, verify I/O files are intact and readable.
            5. Verify no AUTH_BAD_KEY errors in workload and daemon pod logs.
            6. Create new PVCs and pods; verify post-rotation provisioning works.
            7. Verify PGs are active+clean and cluster health is OK.
        """
        rotator = cephx_keyrotation_setup
        namespace = config.ENV_DATA["cluster_namespace"]

        ceph_health_check(namespace=namespace)

        log.info("Creating RBD and CephFS deployment pods for continuous I/O")
        rbd_pod = deployment_pod_factory(
            interface=constants.CEPHBLOCKPOOL,
            size=WORKLOAD_PVC_SIZE,
        )
        cephfs_pod = deployment_pod_factory(
            interface=constants.CEPHFILESYSTEM,
            size=WORKLOAD_PVC_SIZE,
        )

        rbd_io_thread = rotator.start_dd_io_in_background(
            rbd_pod, RBD_IO_FILE, bs=DD_BS, count=DD_COUNT
        )
        cephfs_io_thread = rotator.start_dd_io_in_background(
            cephfs_pod, CEPHFS_IO_FILE, bs=DD_BS, count=DD_COUNT
        )

        log.info("Triggering full CephX key rotation for all components")
        generations = rotator.rotate_all_keys()
        log.info(f"Requested key generations: {generations}")

        rotator.wait_for_all_key_rotations(generations, timeout=1500)

        if not rbd_io_thread.is_alive():
            raise UnexpectedBehaviour(
                f"RBD I/O stopped unexpectedly during rotation on pod {rbd_pod.name}"
            )
        if not cephfs_io_thread.is_alive():
            raise UnexpectedBehaviour(
                "CephFS I/O stopped unexpectedly during rotation on pod "
                f"{cephfs_pod.name}"
            )
        log.info("Continuous I/O remained active throughout key rotation")

        rotator.stop_dd_io(rbd_pod, RBD_IO_FILE)
        rotator.stop_dd_io(cephfs_pod, CEPHFS_IO_FILE)

        rotator.verify_io_file_readable(rbd_pod, RBD_IO_FILE)
        rotator.verify_io_file_readable(cephfs_pod, CEPHFS_IO_FILE)

        daemon_pods = []
        for getter in (get_mon_pods, get_mgr_pods, get_osd_pods):
            daemon_pods.extend(getter(namespace=namespace))
        rotator.verify_pods_no_auth_bad_key([rbd_pod, cephfs_pod] + daemon_pods)

        log.info("Creating new RBD and CephFS workloads after key rotation")
        new_rbd_pod = deployment_pod_factory(
            interface=constants.CEPHBLOCKPOOL,
            size=POST_ROTATION_PVC_SIZE,
        )
        new_cephfs_pod = deployment_pod_factory(
            interface=constants.CEPHFILESYSTEM,
            size=POST_ROTATION_PVC_SIZE,
        )
        new_rbd_pod.exec_cmd_on_pod(
            "dd if=/dev/urandom of=/mnt/post_rotation_test bs=4k count=100 status=none",
            out_yaml_format=False,
        )
        new_cephfs_pod.exec_cmd_on_pod(
            "dd if=/dev/urandom of=/mnt/post_rotation_test bs=4k count=100 status=none",
            out_yaml_format=False,
        )
        new_rbd_pod.exec_cmd_on_pod(
            "test -s /mnt/post_rotation_test", out_yaml_format=False
        )
        new_cephfs_pod.exec_cmd_on_pod(
            "test -s /mnt/post_rotation_test", out_yaml_format=False
        )
        log.info("Post-rotation PVC provisioning and I/O verified successfully")

        ceph_health_check(namespace=namespace)
        log.info(
            "Cluster health and I/O continuity verified during full CephX key rotation"
        )
