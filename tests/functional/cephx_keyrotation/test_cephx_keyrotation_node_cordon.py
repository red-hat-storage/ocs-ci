"""
CephX Key Rotation — Node Cordon Scenarios

Verify daemon CephX key rotation while a node is cordoned:
  - Cordon one OSD node, rotate daemon keys, verify rotation for all daemons.
  - Uncordon the node, rotate daemon keys again, verify rotation succeeds.
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
from ocs_ci.ocs.node import (
    get_osd_running_nodes,
    schedule_nodes,
    unschedule_nodes,
)
from ocs_ci.ocs.resources.pod import get_osd_pods
from ocs_ci.utility.utils import ceph_health_check

log = logging.getLogger(__name__)

MIN_OSD_NODES = 3


@skipif_external_mode
@skipif_ocs_version("<4.19")
@green_squad
@ignore_leftovers
class TestCephXKeyRotationNodeCordon:
    @pytest.fixture(autouse=True)
    def _restore_node_scheduling(self, request):
        """Uncordon any node left cordoned after a test failure."""
        self._cordoned_node = None

        def finalizer():
            if self._cordoned_node:
                log.info(f"Teardown: uncordoning node {self._cordoned_node}")
                schedule_nodes([self._cordoned_node])
            try:
                CephXKeyRotation().ensure_daemon_key_generations_aligned()
            except Exception as exc:
                log.warning("Teardown: failed to align daemon keyGeneration: %s", exc)

        request.addfinalizer(finalizer)

    @tier2
    def test_cephx_key_rotation_with_node_cordon_and_uncordon(
        self, cephx_keyrotation_setup
    ):
        """
        Verify daemon CephX key rotation completes while one OSD node is
        cordoned, then succeeds again after the node is uncordoned.

        Steps:
            1. Record pre-rotation auth keys and daemon pod states.
            2. Cordon one OSD-hosting node.
            3. Trigger daemon key rotation and wait for completion.
            4. Verify all daemon keys rotated and cluster is healthy.
            5. Uncordon the node.
            6. Trigger a second daemon key rotation and wait for completion.
            7. Verify all daemon keys rotated again and cluster is healthy.
        """
        rotator = cephx_keyrotation_setup
        namespace = config.ENV_DATA["cluster_namespace"]
        mon_rotation_supported = rotator.is_mon_key_rotation_supported()

        osd_nodes = get_osd_running_nodes()
        if len(osd_nodes) < MIN_OSD_NODES:
            pytest.skip(
                f"Need at least {MIN_OSD_NODES} OSD nodes; found {len(osd_nodes)}"
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

        all_entities = rotator.flatten_daemon_auth_entities(auth_entities)
        ceph_health_check(namespace=namespace)

        # --- Phase 1: Cordon one node and rotate ---
        cordon_target = osd_nodes[0]
        log.info(f"Cordoning OSD node: {cordon_target}")
        unschedule_nodes([cordon_target])
        self._cordoned_node = cordon_target

        pre_generations_1 = rotator.record_daemon_generations()
        pre_auth_keys_1 = rotator.capture_auth_keys(
            all_entities, label="before rotation (node cordoned)"
        )
        pre_pod_states_1 = rotator.capture_all_daemon_pod_states()
        for daemon, pods in pre_pod_states_1.items():
            assert pods, f"No Running pods found for {daemon} before cordoned rotation"

        target_gen_1 = rotator.rotate_daemon_keys()
        log.info(
            f"Triggered daemon CephX rotation to generation {target_gen_1} "
            f"(node {cordon_target} cordoned)"
        )

        rotator.wait_for_rook_daemon_rotation(target_gen_1, timeout=1500)
        rotator.wait_for_all_daemon_pod_restarts(pre_pod_states_1)

        rotator.assert_rook_daemon_generations(target_gen_1, mon_rotation_supported)
        rotator.assert_generations_increased(pre_generations_1, mon_rotation_supported)
        rotator.log_generation_status("After rotation with node cordoned")

        post_auth_keys_1 = rotator.verify_auth_keys_changed(
            pre_auth_keys_1, entities=all_entities
        )
        rotator.log_auth_key_snapshot("after cordoned rotation", post_auth_keys_1)

        rotator.wait_for_pgs_active_clean()
        ceph_health_check(namespace=namespace)
        rotator.wait_for_cluster_ready()
        log.info(
            "Daemon CephX rotation completed successfully with node "
            f"{cordon_target} cordoned"
        )

        # --- Phase 2: Uncordon the node and rotate again ---
        log.info(f"Uncordoning OSD node: {cordon_target}")
        schedule_nodes([cordon_target])
        self._cordoned_node = None

        rotator.wait_for_rook_daemon_pods_ready()
        rotator.wait_for_pgs_active_clean()
        ceph_health_check(namespace=namespace)

        pre_generations_2 = rotator.record_daemon_generations()
        pre_auth_keys_2 = rotator.capture_auth_keys(
            all_entities, label="before rotation (node uncordoned)"
        )
        pre_pod_states_2 = rotator.capture_all_daemon_pod_states()
        for daemon, pods in pre_pod_states_2.items():
            assert (
                pods
            ), f"No Running pods found for {daemon} before uncordoned rotation"

        target_gen_2 = rotator.rotate_daemon_keys()
        log.info(
            f"Triggered daemon CephX rotation to generation {target_gen_2} "
            f"(node {cordon_target} uncordoned)"
        )

        rotator.wait_for_rook_daemon_rotation(target_gen_2, timeout=1500)
        rotator.wait_for_all_daemon_pod_restarts(pre_pod_states_2)

        rotator.assert_rook_daemon_generations(target_gen_2, mon_rotation_supported)
        rotator.assert_generations_increased(pre_generations_2, mon_rotation_supported)
        rotator.log_generation_status("After rotation with node uncordoned")

        post_auth_keys_2 = rotator.verify_auth_keys_changed(
            pre_auth_keys_2, entities=all_entities
        )
        rotator.log_auth_key_snapshot("after uncordoned rotation", post_auth_keys_2)

        for entity in all_entities:
            key_1 = post_auth_keys_1.get(entity)
            key_2 = post_auth_keys_2.get(entity)
            if key_1 and key_2:
                assert key_1 != key_2, (
                    f"Auth key for {entity} was not rotated in the second "
                    "rotation after uncordon"
                )

        rotator.wait_for_pgs_active_clean()
        ceph_health_check(namespace=namespace)
        rotator.wait_for_cluster_ready()

        osd_pods = get_osd_pods(namespace=namespace)
        all_daemon_pods = []
        all_daemon_pods.extend(osd_pods)
        rotator.verify_pods_no_auth_bad_key(all_daemon_pods)

        log.info(
            "CephX daemon key rotation with node cordon/uncordon completed "
            "successfully across two rotation cycles"
        )
