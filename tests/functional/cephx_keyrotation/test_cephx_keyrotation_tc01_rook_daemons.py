"""
TC-01: CephX Key Rotation — Rook Daemons (Mon, MGR, OSD, MDS)

Verify that CephX key rotation works for Rook-managed MON, MGR, OSD, and MDS
daemons: generations increment, auth keys change, pods restart with updated
cephx-key-identifier annotations, capabilities are unchanged, and the cluster
stays healthy.
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


@skipif_external_mode
@skipif_ocs_version("<4.19")
@green_squad
class TestCephXKeyRotationRookDaemons:
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

        auth_entities = rotator.discover_rook_daemon_auth_entities()
        for daemon, entities in auth_entities.items():
            if daemon == "mon" and not entities:
                if mon_rotation_supported:
                    assert entities, (
                        "MON key rotation is reported on CephCluster but no MON "
                        "auth entities were found"
                    )
                log.info(
                    "No MON auth entities in this Ceph version; "
                    "skipping MON auth key verification"
                )
                continue
            assert entities, f"No Ceph auth entities found for {daemon}"
            log.info("Pre-rotation %s auth entities: %s", daemon, ", ".join(entities))

        all_entities = [
            entity
            for daemon, entities in auth_entities.items()
            for entity in entities
            if not (daemon == "mon" and not entities)
        ]
        pre_auth_keys = rotator.capture_auth_keys(all_entities)
        pre_auth_caps = rotator.capture_auth_caps(all_entities)
        pre_pod_states = rotator.capture_all_daemon_pod_states()

        for daemon, pods in pre_pod_states.items():
            assert pods, f"No Running pods found for {daemon} before rotation"
            log.info(
                "Pre-rotation %s pods: %s",
                daemon,
                ", ".join(
                    f"{name} (cephx-key-identifier={ann})" for name, ann in pods.items()
                ),
            )

        ceph_health_check(namespace=namespace)

        target_generation = rotator.rotate_daemon_keys()
        log.info("Triggered daemon CephX rotation to generation %s", target_generation)

        rotator.wait_for_rook_daemon_rotation(target_generation)
        post_pod_states = rotator.wait_for_all_daemon_pod_restarts(pre_pod_states)

        log.info("Verifying post-rotation keyGeneration values")
        assert (
            rotator.get_status_key_generation("mgr") >= target_generation
        ), "MGR keyGeneration did not reach target"
        assert (
            rotator.get_status_key_generation("osd") >= target_generation
        ), "OSD keyGeneration did not reach target"
        if mon_rotation_supported:
            assert (
                rotator.get_status_key_generation("mon") >= target_generation
            ), "MON keyGeneration did not reach target"
        assert (
            rotator.get_filesystem_daemon_key_generation() >= target_generation
        ), "MDS (CephFilesystem) keyGeneration did not reach target"

        rotator.verify_auth_keys_changed(pre_auth_keys, entities=all_entities)
        rotator.verify_auth_caps_unchanged(pre_auth_caps, entities=all_entities)

        for daemon, pods in post_pod_states.items():
            for pod_name, annotation in pods.items():
                assert (
                    annotation is not None
                ), f"Pod {pod_name} ({daemon}) missing cephx-key-identifier annotation"
            log.info(
                "Post-rotation %s pods: %s",
                daemon,
                ", ".join(
                    f"{name} (cephx-key-identifier={ann})" for name, ann in pods.items()
                ),
            )

        ceph_health_check(namespace=namespace)
        rotator.wait_for_cluster_ready()

        if mon_rotation_supported:
            assert rotator.get_status_key_generation("mon") > pre_mon_generation
        assert rotator.get_status_key_generation("mgr") > pre_mgr_generation
        assert rotator.get_status_key_generation("osd") > pre_osd_generation
        assert rotator.get_filesystem_daemon_key_generation() > pre_mds_generation

        log.info("TC-01 CephX key rotation for MON/MGR/OSD/MDS completed successfully")
