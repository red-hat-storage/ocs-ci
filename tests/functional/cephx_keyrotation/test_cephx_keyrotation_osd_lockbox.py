"""
OSD Lockbox CephX Key Rotation for Encrypted OSDs

Verify that lockbox CephX keys (client.osd-lockbox.<UUID>) are rotated correctly
for encrypted OSDs: encrypted labels, updated lockbox keys, activate init container
key load, operator rotation logs, and cluster health after rotation.
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
from ocs_ci.ocs import constants
from ocs_ci.utility.utils import ceph_health_check

log = logging.getLogger(__name__)

MIN_ENCRYPTED_OSD_COUNT = 1


@skipif_external_mode
@skipif_ocs_version("<4.19")
@green_squad
class TestCephXKeyRotationOSDLockbox:
    @tier1
    def test_cephx_key_rotation_osd_lockbox_encrypted(self, cephx_keyrotation_setup):
        """
        Verify lockbox CephX key rotation for encrypted OSDs.

        Steps:
            1. Discover encrypted OSD deployments and lockbox auth entities.
            2. Verify ``encrypted=true`` labels and record baseline lockbox keys.
            3. Trigger daemon key rotation.
            4. Wait for OSD pod restarts and verify lockbox keys changed.
            5. Verify activate init container and operator lockbox rotation logs.
            6. Verify encrypted OSD pods remain Running/Ready and PGs are clean.

        Requires a cluster with encrypted OSDs (host-based and/or PVC-based).
        """
        rotator = cephx_keyrotation_setup
        namespace = config.ENV_DATA["cluster_namespace"]

        encrypted_deployments = rotator.capture_encrypted_osd_deployments()
        if len(encrypted_deployments) < MIN_ENCRYPTED_OSD_COUNT:
            pytest.skip(
                f"Cluster has {len(encrypted_deployments)} encrypted OSD "
                f"deployment(s); need at least {MIN_ENCRYPTED_OSD_COUNT}"
            )

        rotator.assert_encrypted_osd_labels(encrypted_deployments)
        log.info(
            "Encrypted OSD deployments: "
            + ", ".join(
                f"{name} (osd_id={info['osd_id']}, store={info['store_type']})"
                for name, info in sorted(encrypted_deployments.items())
            )
        )
        store_types = {info["store_type"] for info in encrypted_deployments.values()}
        if store_types == {"pvc"}:
            log.info("All encrypted OSDs are PVC-backed on this cluster")
        elif store_types == {"host"}:
            log.info("All encrypted OSDs are host/disk-based on this cluster")
        else:
            log.info("Cluster has a mix of host-based and PVC-based encrypted OSDs")

        lockbox_entities = rotator.discover_lockbox_auth_entities()
        if not lockbox_entities:
            pytest.skip("No client.osd-lockbox.* auth entities found on cluster")

        log.info(f"Lockbox auth entities: {', '.join(lockbox_entities)}")
        pre_osd_generation = rotator.get_status_key_generation("osd")
        pre_auth_keys = rotator.capture_auth_keys(
            lockbox_entities, label="lockbox keys before rotation"
        )
        pre_pod_states = rotator.capture_daemon_pod_state(constants.OSD_APP_LABEL)

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

        post_auth_keys = rotator.verify_auth_keys_changed(
            pre_auth_keys, entities=lockbox_entities
        )
        rotator.log_auth_key_snapshot("lockbox keys after rotation", post_auth_keys)

        encrypted_osd_pods = rotator.get_encrypted_osd_pods()
        rotator.verify_osd_activate_lockbox_logs(encrypted_osd_pods)
        rotator.verify_operator_lockbox_rotation_logs(len(encrypted_deployments))
        rotator.verify_encrypted_osd_pods_running(encrypted_osd_pods)

        for pod_name, annotation in post_pod_states.items():
            if pod_name not in {pod.name for pod in encrypted_osd_pods}:
                continue
            assert (
                annotation is not None
            ), f"Encrypted OSD pod {pod_name} missing cephx-key-identifier annotation"

        rotator.wait_for_pgs_active_clean()
        ceph_health_check(namespace=namespace)
        rotator.wait_for_cluster_ready()

        assert rotator.get_status_key_generation("osd") > pre_osd_generation
        log.info("Encrypted OSD lockbox CephX key rotation completed successfully")
