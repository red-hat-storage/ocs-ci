"""
CephX Key Rotation — Security Configuration

Policy disabled and allowedCiphers defaults/custom StorageCluster config.
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
from ocs_ci.helpers.cephx_keyrotation_helper import CephXKeyRotation
from ocs_ci.ocs import constants
from ocs_ci.utility.utils import ceph_health_check

log = logging.getLogger(__name__)

RECONCILE_CYCLES = 3
RECONCILE_SLEEP_SECONDS = 60
EXPECTED_INITIAL_GENERATION = 2


@skipif_external_mode
@skipif_ocs_version("<4.19")
@green_squad
@pytest.mark.order("first")
class TestCephXKeyRotationPolicyDisabled:
    @tier1
    def test_cephx_key_rotation_policy_disabled_no_rotation(
        self, cephx_rotation_disabled_setup
    ):
        """
        Verify Disabled keyRotationPolicy prevents CephX key rotation.

        Steps:
            1. Ensure keyRotationPolicy is Disabled for daemon (and other components).
            2. Record keyGeneration, daemon auth keys, pod state, and bootstrap keys.
            3. Trigger multiple CephCluster reconciles.
            4. Verify generations, daemon auth keys, pods, and bootstrap keys are unchanged.
        """
        rotator = cephx_rotation_disabled_setup
        namespace = config.ENV_DATA["cluster_namespace"]

        ceph_health_check(namespace=namespace)
        rotator.assert_key_rotation_disabled()

        baseline_generations = rotator.record_all_cephx_status_generations()
        rotator.log_generation_status("Baseline")
        log.info(f"Baseline CephX status generations: {baseline_generations}")

        for entity, generation in baseline_generations.items():
            if entity.startswith("spec_"):
                continue
            if generation and generation != EXPECTED_INITIAL_GENERATION:
                log.warning(
                    f"Baseline {entity} keyGeneration={generation}; "
                    f"expected initial creation generation "
                    f"{EXPECTED_INITIAL_GENERATION} on fresh clusters"
                )

        auth_entities = rotator.flatten_daemon_auth_entities(
            rotator.discover_rook_daemon_auth_entities()
        )
        if not auth_entities:
            pytest.skip("No Ceph auth entities found for rotation verification")

        log.info(
            "Daemon auth entities tracked for no-rotation verification: "
            f"{', '.join(auth_entities)}"
        )
        pre_auth_keys = rotator.capture_auth_keys(
            auth_entities, label="before reconcile cycles"
        )
        pre_pod_states = rotator.capture_all_daemon_pod_states()
        pre_bootstrap_entities = rotator.discover_bootstrap_auth_entities()
        if pre_bootstrap_entities:
            log.info(
                "Bootstrap keys present before reconcile: "
                f"{', '.join(pre_bootstrap_entities)}"
            )
        else:
            log.info("No bootstrap keys present before reconcile")

        rotator.trigger_reconciliation_cycles(
            cycles=RECONCILE_CYCLES,
            sleep_between=RECONCILE_SLEEP_SECONDS,
        )

        rotator.assert_cephx_status_generations_unchanged(baseline_generations)
        rotator.assert_auth_keys_unchanged(pre_auth_keys, entities=auth_entities)
        rotator.assert_all_daemon_pod_states_unchanged(pre_pod_states, settle_time=30)
        rotator.assert_bootstrap_keys_unchanged(pre_bootstrap_entities)

        ceph_health_check(namespace=namespace)
        rotator.wait_for_cluster_ready()
        rotator.wait_for_pgs_active_clean()

        log.info(
            "CephX key rotation policy Disabled verification completed successfully"
        )


@skipif_external_mode
@skipif_ocs_version("<4.19")
@green_squad
class TestCephXAllowedCiphers:
    @pytest.fixture(autouse=True)
    def _teardown(self, request):
        """Restore StorageCluster security after custom cipher test."""
        request.node._cephx_reconcile_strategy_restore = None

        def fin():
            rotator = CephXKeyRotation()
            log.info(
                "Teardown: removing StorageCluster managedResources.cephCluster.security"
            )
            try:
                rotator.remove_storagecluster_cephcluster_security()
                restore_strategy = getattr(
                    request.node, "_cephx_reconcile_strategy_restore", None
                )
                if restore_strategy is not None:
                    log.info(
                        "Teardown: restoring StorageCluster "
                        f"cephCluster.reconcileStrategy={restore_strategy}"
                    )
                    rotator.patch_storagecluster_cephcluster_reconcile_strategy(
                        restore_strategy
                    )
                rotator.wait_for_allowed_ciphers(
                    constants.CEPHX_DEFAULT_ALLOWED_CIPHERS,
                    timeout=600,
                )
                rotator.ensure_daemon_key_generations_aligned()
            except Exception as exc:
                log.warning(
                    "Teardown restore of default allowedCiphers failed: %s", exc
                )

        request.addfinalizer(fin)

    @tier1
    def test_cephx_allowed_ciphers_configuration(self, cephx_bootstrap_setup, request):
        """
        Verify default CephCluster allowedCiphers and StorageCluster custom config.

        Part A — Default ciphers:
            1. Confirm StorageCluster does not specify allowedCiphers.
            2. Verify CephCluster spec.security.cephx.allowedCiphers defaults to
               ["aes", "aes256k"].

        Part B — Custom StorageCluster ciphers:
            3. Patch StorageCluster with custom allowedCiphers (["aes256k"]).
            4. Verify StorageCluster retains the custom value.
            5. Verify CephCluster keeps the default ["aes", "aes256k"] (operator
               does not propagate StorageCluster allowedCiphers overrides).
        """
        rotator = cephx_bootstrap_setup
        namespace = config.ENV_DATA["cluster_namespace"]

        ceph_health_check(namespace=namespace)

        sc_ciphers = rotator.get_storagecluster_allowed_ciphers()
        if sc_ciphers is not None:
            log.warning(
                "StorageCluster already specifies allowedCiphers=%s; "
                "expected unset for Part A default verification",
                sc_ciphers,
            )
        else:
            log.info("StorageCluster does not specify allowedCiphers (defaults apply)")

        rotator.wait_for_allowed_ciphers(
            constants.CEPHX_DEFAULT_ALLOWED_CIPHERS,
            timeout=600,
        )
        rotator.assert_allowed_ciphers(constants.CEPHX_DEFAULT_ALLOWED_CIPHERS)
        rotator.assert_cephcluster_security_populated()

        log.info(
            "Part A passed: default allowedCiphers=%s on CephCluster",
            list(constants.CEPHX_DEFAULT_ALLOWED_CIPHERS),
        )

        reconcile_strategy = rotator.get_cephcluster_reconcile_strategy()
        if reconcile_strategy == "ignore":
            log.info(
                "StorageCluster cephCluster.reconcileStrategy is ignore; "
                "temporarily setting manage so reconcile can evaluate custom "
                "allowedCiphers without changing CephCluster defaults"
            )
            request.node._cephx_reconcile_strategy_restore = reconcile_strategy
            rotator.patch_storagecluster_cephcluster_reconcile_strategy("manage")

        rotator.patch_storagecluster_allowed_ciphers(
            constants.CEPHX_CUSTOM_ALLOWED_CIPHERS
        )
        rotator.wait_for_allowed_ciphers(
            constants.CEPHX_CUSTOM_ALLOWED_CIPHERS,
            timeout=600,
            source="storagecluster",
        )
        rotator.assert_allowed_ciphers(
            constants.CEPHX_CUSTOM_ALLOWED_CIPHERS,
            source="storagecluster",
        )
        rotator.wait_for_storagecluster_reconciliation(timeout=600)
        # Operator keeps CephCluster on the default cipher list; custom values
        # on StorageCluster are accepted but not mirrored to CephCluster.
        rotator.assert_allowed_ciphers(constants.CEPHX_DEFAULT_ALLOWED_CIPHERS)
        rotator.assert_cephcluster_security_populated()

        ceph_health_check(namespace=namespace)
        log.info(
            "Part B passed: StorageCluster allowedCiphers=%s; CephCluster "
            "remained at defaults %s",
            list(constants.CEPHX_CUSTOM_ALLOWED_CIPHERS),
            list(constants.CEPHX_DEFAULT_ALLOWED_CIPHERS),
        )
