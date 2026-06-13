"""
CephX Key Rotation — Consecutive Rotations for All Daemons

Standalone test that iteratively rotates daemon CephX keys and verifies each
rotation increments keyGeneration, produces unique auth keys, restarts pods, and
keeps the cluster healthy.
"""

import logging

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    skipif_external_mode,
    skipif_ocs_version,
    tier1,
)
from ocs_ci.utility.utils import ceph_health_check

log = logging.getLogger(__name__)

CONSECUTIVE_ROTATION_COUNT = 3


@skipif_external_mode
@skipif_ocs_version("<4.19")
@green_squad
class TestCephXKeyRotationConsecutiveRotations:
    @tier1
    def test_cephx_key_rotation_consecutive_rotations(self, cephx_keyrotation_setup):
        """
        Verify multiple consecutive daemon CephX key rotations.

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
