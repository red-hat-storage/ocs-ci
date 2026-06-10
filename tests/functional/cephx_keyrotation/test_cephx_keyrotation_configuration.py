"""
CephX Key Rotation — Security Configuration

Policy disabled, allowedCiphers passthrough, and custom keyType selection.
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
EXPECTED_INITIAL_GENERATION = 1


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
            1. Ensure keyRotationPolicy is Disabled for daemon, CSI, and RBD mirror.
            2. Record keyGeneration, auth keys, pod state, and bootstrap keys.
            3. Trigger multiple CephCluster reconciles.
            4. Verify generations, auth keys, pods, and bootstrap keys are unchanged.
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

        auth_entities = rotator.discover_all_rotation_auth_entities()
        if not auth_entities:
            pytest.skip("No Ceph auth entities found for rotation verification")

        log.info(
            "Auth entities tracked for no-rotation verification: "
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
        """Restore StorageCluster security passthrough after custom cipher test."""

        def fin():
            rotator = CephXKeyRotation()
            log.info(
                "Teardown: removing StorageCluster managedResources.cephCluster.security"
            )
            try:
                rotator.remove_storagecluster_cephcluster_security()
                rotator.wait_for_allowed_ciphers(
                    constants.CEPHX_DEFAULT_ALLOWED_CIPHERS,
                    timeout=600,
                )
            except Exception as exc:
                log.warning(
                    "Teardown restore of default allowedCiphers failed: %s", exc
                )

        request.addfinalizer(fin)

    @tier1
    def test_cephx_allowed_ciphers_configuration(self, cephx_bootstrap_setup):
        """
        Verify default and custom allowedCiphers passthrough from StorageCluster.

        Part A — Default ciphers:
            1. Confirm StorageCluster does not specify allowedCiphers.
            2. Verify CephCluster spec.security.cephx.allowedCiphers defaults to
               ["aes", "aes256k"].

        Part B — Custom ciphers:
            3. Patch StorageCluster with custom allowedCiphers (["aes256k"]).
            4. Wait for reconciliation and verify CephCluster mirrors the override.
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
            log.info(
                "StorageCluster does not specify allowedCiphers (default passthrough)"
            )

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

        rotator.patch_storagecluster_allowed_ciphers(
            constants.CEPHX_CUSTOM_ALLOWED_CIPHERS
        )
        rotator.wait_for_allowed_ciphers(
            constants.CEPHX_CUSTOM_ALLOWED_CIPHERS,
            timeout=600,
            source="storagecluster",
        )
        rotator.wait_for_allowed_ciphers(constants.CEPHX_CUSTOM_ALLOWED_CIPHERS)
        rotator.assert_allowed_ciphers(constants.CEPHX_CUSTOM_ALLOWED_CIPHERS)
        rotator.assert_allowed_ciphers(
            constants.CEPHX_CUSTOM_ALLOWED_CIPHERS,
            source="storagecluster",
        )
        rotator.assert_cephcluster_security_populated()

        ceph_health_check(namespace=namespace)
        log.info(
            "Part B passed: custom allowedCiphers=%s propagated to CephCluster",
            list(constants.CEPHX_CUSTOM_ALLOWED_CIPHERS),
        )


@skipif_external_mode
@skipif_ocs_version("<4.19")
@green_squad
class TestCephXKeyType:
    @pytest.fixture(autouse=True)
    def _teardown(self, request):
        """Remove custom keyType from the CephCluster after the test."""

        def fin():
            rotator = CephXKeyRotation()
            log.info(
                "Teardown: removing CephCluster spec.security.cephx.daemon.keyType"
            )
            try:
                rotator.remove_cephcluster_key_type()
            except Exception as exc:
                log.warning("Teardown remove keyType skipped or failed: %s", exc)

        request.addfinalizer(fin)

    @tier1
    def test_cephx_key_type_custom_selection(self, cephx_keyrotation_setup):
        """
        Verify custom CephX keyType selection and daemon rotation.

        Steps:
            1. Wait for cluster/daemons Ready (fixture enables daemon rotation).
            2. Set CephCluster spec.security.cephx.daemon.keyType to aes256k.
            3. Trigger daemon CephX key rotation for all daemons.
            4. Verify rotated service daemon keys use aes256k.
            5. Verify AUTH_INSECURE_SERVICE_KEY_TYPE is reconciled and daemons stay healthy.
        """
        rotator = cephx_keyrotation_setup
        namespace = config.ENV_DATA["cluster_namespace"]
        key_type = constants.CEPHX_CUSTOM_KEY_TYPE

        ceph_health_check(namespace=namespace)

        if rotator.get_spec_key_type() is not None:
            log.warning(
                "CephCluster already has keyType=%s before test start",
                rotator.get_spec_key_type(),
            )

        rotator.patch_cephcluster_key_type(key_type)
        rotator.wait_for_cephcluster_key_type(key_type)

        auth_entities = rotator.discover_rook_daemon_auth_entities()
        service_entities = rotator.flatten_daemon_auth_entities(auth_entities)
        if not service_entities:
            pytest.skip("No discoverable service daemon auth entities on cluster")

        log.info(
            "Service daemon auth entities for key type verification: "
            f"{', '.join(service_entities)}"
        )

        target_generation = rotator.get_next_key_generation(rotator.COMPONENT_DAEMON)
        log.info(
            f"Triggering daemon CephX key rotation to generation "
            f"{target_generation} with keyType={key_type}"
        )
        rotator.rotate_daemon_keys(target_generation)
        rotator.wait_for_rook_daemon_rotation(target_generation, timeout=1200)

        rotator.assert_auth_entities_key_type(service_entities, key_type)
        rotator.wait_for_auth_insecure_service_key_type_cleared(timeout=1200)
        rotator.verify_operator_auth_rotate_key_type_logs(key_type)

        rotator.wait_for_rook_daemon_pods_ready()
        rotator.wait_for_cluster_ready()
        rotator.wait_for_pgs_active_clean()
        ceph_health_check(namespace=namespace)

        log.info(
            f"CephX keyType={key_type} rotation verification completed successfully"
        )
