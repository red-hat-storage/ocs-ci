"""Daemon-focused CephX rotation, waits, discovery, and bootstrap helpers."""

import logging
import time

from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import (
    get_pods_having_label,
)
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


class CephXDaemonRotation:
    """Daemon-specific CephX rotation, waits, pod state, and bootstrap logic."""

    def get_current_rook_daemon_key_generation(self):
        """
        Return the highest daemon keyGeneration across spec, status, and desired.

        Includes MON, MGR, and OSD on CephCluster plus MDS on CephFilesystem,
        and ocs-operator DESIRED_CEPHX_KEY_GEN (greenfield baseline).
        """
        current = self.get_spec_key_generation(self.COMPONENT_DAEMON)
        for entity in self.ROOK_DAEMON_STATUS_ENTITIES:
            current = max(current, self.get_status_key_generation(entity))
        current = max(current, self.get_filesystem_daemon_key_generation())
        return max(current, self.get_desired_cephx_key_gen())

    def get_next_rook_daemon_key_generation(self):
        """
        Return a generation value to trigger rotation for Rook daemons only.

        Considers MON, MGR, and OSD on CephCluster plus MDS on CephFilesystem
        and DESIRED_CEPHX_KEY_GEN.
        """
        return self.get_current_rook_daemon_key_generation() + 1

    def rotate_rook_daemon_keys(self, key_generation=None, wait_for_rotation=True):
        """
        Rotate CephX keys for Rook-managed daemons: MON, MGR, OSD, and MDS.

        Triggers StorageCluster ``security.cephx.daemon`` reconciliation;
        generation calculation and completion checks use only those four
        daemon types (not admin, crashCollector, or cephExporter).

        Args:
            key_generation (int): Desired generation. When omitted, computed via
                :meth:`get_next_rook_daemon_key_generation`.
            wait_for_rotation (bool): When False, patch only and return without
                waiting for CephCluster/StorageCluster Ready.
        """
        if key_generation is None:
            key_generation = self.get_next_rook_daemon_key_generation()
        return self.rotate_component_keys(
            self.COMPONENT_DAEMON,
            key_generation=key_generation,
            wait_for_rotation=wait_for_rotation,
        )

    def wait_for_filesystem_daemon_rotation(
        self, expected_generation, timeout=900, sleep=15
    ):
        """Wait until CephFilesystem ``status.cephx.daemon.keyGeneration`` matches."""
        return self._wait_for_cr_daemon_rotation(
            self._get_cephfilesystem_obj(),
            expected_generation,
            timeout,
            sleep,
            label=f"CephFilesystem/{self.cephfilesystem_name}",
        )

    def wait_for_rook_daemon_rotation(
        self, expected_generation, timeout=1200, sleep=15
    ):
        """
        Wait for Rook daemon CephX rotation: MON, MGR, OSD, and MDS only.

        MON/MGR/OSD are tracked on CephCluster ``status.cephx``; MDS is tracked
        on CephFilesystem ``status.cephx.daemon``.
        """
        self._wait_for_status_entities(
            ["mgr", "osd"],
            expected_generation,
            timeout,
            sleep,
            label="CephCluster mgr/osd",
        )
        self.wait_for_mon_rotation(expected_generation, timeout, sleep)
        self.wait_for_filesystem_daemon_rotation(expected_generation, timeout, sleep)

    def get_filesystem_status_cephx(self):
        """Return ``status.cephx`` from the CephFilesystem CR."""
        fs_obj = self._get_cephfilesystem_obj()
        fs_obj.reload_data()
        return fs_obj.data.get("status", {}).get("cephx", {}) or {}

    def get_filesystem_daemon_key_generation(self):
        """Return ``status.cephx.daemon.keyGeneration`` from CephFilesystem."""
        daemon_status = self.get_filesystem_status_cephx().get("daemon") or {}
        return int(daemon_status.get("keyGeneration", 0) or 0)

    def ensure_daemon_key_rotation_enabled(self, key_generation=None):
        """
        Ensure StorageCluster daemon CephX uses KeyGeneration policy.

        On a fresh cluster (no security block), the default first generation
        is :attr:`DEFAULT_DAEMON_KEY_GENERATION` (2), matching
        ``DESIRED_CEPHX_KEY_GEN``. Enabling at that baseline only updates
        StorageCluster; CephCluster does not Progress (already at desired).

        Happy-path enable never decreases generation; pass an explicit lower
        value through :meth:`rotate_component_keys` for negative tests.

        Args:
            key_generation (int): Minimum desired generation in spec. Defaults
                to :attr:`DEFAULT_DAEMON_KEY_GENERATION`. When the cluster
                already has a higher generation, the existing generation is
                preserved.

        Returns:
            int: Generation configured in StorageCluster spec.
        """
        if key_generation is None:
            key_generation = self.DEFAULT_DAEMON_KEY_GENERATION

        spec_generation = self.get_spec_key_generation(self.COMPONENT_DAEMON)
        desired_baseline = self.get_desired_cephx_key_gen()
        daemon_spec = self.get_storagecluster_component_spec(self.COMPONENT_DAEMON)
        policy = daemon_spec.get("keyRotationPolicy")

        # Spec (not status) decides whether StorageCluster already has the
        # policy/generation. Status may remain 1 while desired/spec is 2.
        if (
            policy == self.KEY_ROTATION_POLICY_KEY_GENERATION
            and spec_generation >= key_generation
        ):
            log.info(
                "Daemon CephX key rotation already enabled at generation "
                f"{spec_generation}"
            )
            return spec_generation

        effective_generation = max(key_generation, spec_generation, desired_baseline)
        if effective_generation > key_generation:
            log.info(
                "Preserving daemon keyGeneration %s (requested minimum %s); "
                "happy-path enable does not decrease keyGeneration",
                effective_generation,
                key_generation,
            )

        log.info(
            "Enabling daemon CephX KeyGeneration policy at generation "
            f"{effective_generation} via StorageCluster"
        )
        return self.rotate_component_keys(
            self.COMPONENT_DAEMON, key_generation=effective_generation
        )

    def assert_all_daemon_pod_states_unchanged(
        self, before_states, settle_time=30, context="while key rotation is Disabled"
    ):
        """Assert MON/MGR/OSD/MDS pods were not restarted for key rotation."""
        if settle_time:
            time.sleep(settle_time)
        after_states = self.capture_all_daemon_pod_states()
        restarted = []
        for daemon, before in before_states.items():
            after = after_states.get(daemon, {})
            if before != after:
                restarted.append(daemon)
        if restarted:
            raise UnexpectedBehaviour(
                f"Daemon pods changed {context} "
                f"(possible rotation restart): {', '.join(restarted)}"
            )
        log.info("Daemon pod names and cephx-key-identifier annotations unchanged")

    def assert_bootstrap_keys_unchanged(self, pre_bootstrap_entities):
        """Assert bootstrap keys were not prematurely deleted."""
        pre = set(pre_bootstrap_entities)
        post = set(self.discover_bootstrap_auth_entities())
        deleted = sorted(pre - post)
        if deleted:
            raise UnexpectedBehaviour(
                "Bootstrap CephX keys prematurely deleted while rotation is "
                f"Disabled: {', '.join(deleted)}"
            )
        log.info("Bootstrap CephX keys unchanged")

    def wait_for_rook_daemon_pods_ready(self, timeout=600):
        """Wait until MON, MGR, OSD, and MDS pods are Running."""
        for daemon, label in constants.ROOK_CEPHX_KEYROTATION_DAEMON_LABELS.items():
            log.info(f"Waiting for {daemon} pods ({label}) to be Running")

            def _pods_running(lbl=label):
                pods = get_pods_having_label(
                    lbl, namespace=self.namespace, statuses=[constants.STATUS_RUNNING]
                )
                return bool(pods)

            for _ in TimeoutSampler(timeout, 15, _pods_running):
                break

    def _discover_mgr_auth_entities(self, toolbox_pod=None):
        """Discover MGR auth entities, falling back to ``ceph mgr dump``."""
        entities = self.list_auth_entities("mgr.", toolbox_pod)
        if entities:
            return entities

        toolbox = toolbox_pod or self.get_ceph_cli_pod()
        mgr_dump = toolbox.exec_ceph_cmd("ceph mgr dump")
        discovered = []
        active = mgr_dump.get("active_name")
        if active:
            entity = f"mgr.{active}"
            if self._auth_entity_exists(entity, toolbox_pod):
                discovered.append(entity)
        for standby in mgr_dump.get("standbys", []) or []:
            standby_name = standby.get("name") if isinstance(standby, dict) else standby
            if not standby_name:
                continue
            entity = f"mgr.{standby_name}"
            if self._auth_entity_exists(entity, toolbox_pod):
                discovered.append(entity)
        return sorted(set(discovered))

    def _discover_mds_auth_entities(self, toolbox_pod=None):
        """Discover MDS auth entities for the configured CephFilesystem."""
        mds_prefix = f"mds.{self.cephfilesystem_name}"
        entities = self.list_auth_entities(mds_prefix, toolbox_pod)
        if entities:
            return entities

        discovered = []
        for suffix in ("a", "b"):
            entity = f"{mds_prefix}-{suffix}"
            if self._auth_entity_exists(entity, toolbox_pod):
                discovered.append(entity)
        return sorted(discovered)

    def discover_rook_daemon_auth_entities(self, toolbox_pod=None):
        """
        Discover MON, MGR, OSD, and MDS auth entities for TC-01.

        Returns:
            dict: daemon type to list of entity names.
        """
        return {
            "mon": self._discover_mon_auth_entities(toolbox_pod),
            "mgr": self._discover_mgr_auth_entities(toolbox_pod),
            "osd": self._discover_osd_auth_entities(toolbox_pod),
            "mds": self._discover_mds_auth_entities(toolbox_pod),
        }

    @staticmethod
    def flatten_daemon_auth_entities(auth_entities):
        """
        Return auth entity names for all daemons with discoverable entities.

        Args:
            auth_entities (dict): Output of :meth:`discover_rook_daemon_auth_entities`.

        Returns:
            list[str]: Flat list of Ceph auth entity names.
        """
        return [
            entity
            for daemon, entities in auth_entities.items()
            for entity in entities
            if not (daemon == "mon" and not entities)
        ]

    def record_daemon_generations(self):
        """
        Snapshot current rook daemon keyGeneration values from status.

        Returns:
            dict: mon, mgr, osd, and mds (CephFilesystem) generations.
        """
        return {
            "mon": self.get_status_key_generation("mon"),
            "mgr": self.get_status_key_generation("mgr"),
            "osd": self.get_status_key_generation("osd"),
            "mds": self.get_filesystem_daemon_key_generation(),
        }

    def log_generation_status(self, label):
        """Log mon/mgr/osd/mds keyGeneration values under *label*."""
        generations = self.record_daemon_generations()
        log.info(
            f"{label} keyGeneration: mon={generations['mon']} "
            f"mgr={generations['mgr']} osd={generations['osd']} "
            f"mds={generations['mds']}"
        )

    def assert_rook_daemon_generations(
        self, target_generation, mon_rotation_supported=None
    ):
        """
        Assert CephCluster and CephFilesystem daemon keyGeneration reached target.

        Args:
            target_generation (int): Expected minimum generation.
            mon_rotation_supported (bool): When True, also assert MON generation.
                Auto-detected when omitted.
        """
        if mon_rotation_supported is None:
            mon_rotation_supported = self.is_mon_key_rotation_supported()
        assert (
            self.get_status_key_generation("mgr") >= target_generation
        ), "MGR keyGeneration did not reach target"
        assert (
            self.get_status_key_generation("osd") >= target_generation
        ), "OSD keyGeneration did not reach target"
        if mon_rotation_supported:
            assert (
                self.get_status_key_generation("mon") >= target_generation
            ), "MON keyGeneration did not reach target"
        assert (
            self.get_filesystem_daemon_key_generation() >= target_generation
        ), "MDS (CephFilesystem) keyGeneration did not reach target"

    def assert_generations_increased(self, before, mon_rotation_supported=None):
        """
        Assert each daemon type keyGeneration increased after a rotation.

        Args:
            before (dict): Output of :meth:`record_daemon_generations`.
            mon_rotation_supported (bool): When True, also assert MON increased.
                Auto-detected when omitted.
        """
        if mon_rotation_supported is None:
            mon_rotation_supported = self.is_mon_key_rotation_supported()
        assert (
            self.get_status_key_generation("mgr") > before["mgr"]
        ), "MGR keyGeneration did not increase"
        assert (
            self.get_status_key_generation("osd") > before["osd"]
        ), "OSD keyGeneration did not increase"
        if mon_rotation_supported:
            assert (
                self.get_status_key_generation("mon") > before["mon"]
            ), "MON keyGeneration did not increase"
        assert (
            self.get_filesystem_daemon_key_generation() > before["mds"]
        ), "MDS keyGeneration did not increase"

    def verify_bootstrap_deletion_idempotent_after_operator_restart(self, timeout=600):
        """Restart operator and verify bootstrap cleanup is idempotent."""
        self.assert_bootstrap_keys_absent(constants.CEPHX_BOOTSTRAP_KEYS_TO_CLEANUP)
        operator_log_marker = None
        from ocs_ci.helpers.helpers import get_last_log_time_date

        operator_log_marker = get_last_log_time_date()
        previous_operator = self.restart_rook_ceph_operator()
        self.wait_for_rook_ceph_operator_ready(previous_pod_name=previous_operator)
        self.wait_for_cluster_ready(timeout=timeout)
        self.assert_bootstrap_keys_absent(constants.CEPHX_BOOTSTRAP_KEYS_TO_CLEANUP)
        self.verify_no_bootstrap_deletion_errors()
        self.verify_operator_logs_do_not_contain_warnings(
            constants.CEPHX_BOOTSTRAP_DELETION_WARNING_PATTERNS,
            since_time=operator_log_marker,
            require_match=False,
        )
        log.info(
            "Bootstrap key deletion is idempotent after operator restart "
            f"(restarted pod {previous_operator})"
        )

    def verify_daemon_rotation_idempotent(
        self,
        current_generation,
        auth_keys,
        pod_states,
        entities,
        settle_timeout=120,
    ):
        """
        Reconcile the same daemon keyGeneration and verify no further rotation occurs.

        Args:
            current_generation (int): Generation already applied in spec/status.
            auth_keys (dict): Entity to key mapping after rotation.
            pod_states (dict): OSD pod name to annotation map from
                :meth:`capture_daemon_pod_state` (OSD pods do not use
                cephx-key-identifier; restart detection is by pod name).
            entities (list): Auth entities to re-check.
            settle_timeout (int): Seconds to wait for a spurious reconcile.
        """
        log.info(
            f"Verifying idempotent reconcile at daemon keyGeneration "
            f"{current_generation}"
        )
        self.rotate_component_keys(
            self.COMPONENT_DAEMON, key_generation=current_generation
        )
        time.sleep(settle_timeout)

        new_keys = self.capture_auth_keys(entities)
        unchanged_keys = [
            entity
            for entity in entities
            if auth_keys.get(entity) and auth_keys[entity] == new_keys.get(entity)
        ]
        if len(unchanged_keys) != len(
            [entity for entity in entities if auth_keys.get(entity)]
        ):
            changed = [
                entity
                for entity in entities
                if auth_keys.get(entity) != new_keys.get(entity)
            ]
            raise UnexpectedBehaviour(
                f"Re-reconcile changed CephX keys for: {', '.join(changed)}"
            )

        if self.get_status_key_generation("osd") != current_generation:
            raise UnexpectedBehaviour(
                f"Re-reconcile changed OSD keyGeneration "
                f"(expected {current_generation}, "
                f"got {self.get_status_key_generation('osd')})"
            )

        current_pod_states = self.capture_daemon_pod_state(constants.OSD_APP_LABEL)
        if current_pod_states != pod_states:
            raise UnexpectedBehaviour(
                "Re-reconcile triggered OSD pod restarts or annotation changes"
            )
        log.info(f"Daemon keyGeneration {current_generation} reconcile is idempotent")

    def discover_bootstrap_auth_entities(self, toolbox_pod=None):
        """Return sorted ``client.bootstrap-*`` auth entity names."""
        return self.list_auth_entities(
            constants.CEPHX_BOOTSTRAP_AUTH_PREFIX, toolbox_pod
        )

    def assert_bootstrap_keys_absent(self, entities=None, toolbox_pod=None):
        """
        Assert bootstrap CephX keys are not present in the auth store.

        Args:
            entities (list): Bootstrap entities to check (defaults to all known).
        """
        entities = entities or list(constants.CEPHX_BOOTSTRAP_KEYS_TO_CLEANUP)
        present = [
            entity
            for entity in entities
            if self._auth_entity_exists(entity, toolbox_pod)
        ]
        if present:
            raise UnexpectedBehaviour(
                f"Bootstrap CephX keys still present: {', '.join(present)}"
            )
        log.info(f"Bootstrap CephX keys absent as expected: {', '.join(entities)}")

    def wait_for_bootstrap_keys_absent(
        self, entities=None, timeout=600, sleep=15, toolbox_pod=None
    ):
        """Wait until bootstrap auth entities are removed from the auth store."""
        entities = entities or list(constants.CEPHX_BOOTSTRAP_KEYS_TO_CLEANUP)
        log.info(
            f"Waiting for bootstrap keys to be absent: {', '.join(entities)} "
            f"(timeout={timeout}s)"
        )

        def _keys_absent():
            present = [
                entity
                for entity in entities
                if self._auth_entity_exists(entity, toolbox_pod)
            ]
            if present:
                log.debug(f"Bootstrap keys still present: {', '.join(present)}")
                return False
            return True

        for absent in TimeoutSampler(timeout, sleep, _keys_absent):
            if absent:
                log.info("Bootstrap CephX keys are absent")
                return True

        raise UnexpectedBehaviour(f"Bootstrap CephX keys not removed within {timeout}s")

    def wait_for_bootstrap_key_present(
        self, entity, timeout=300, sleep=10, toolbox_pod=None
    ):
        """Wait until a bootstrap auth entity appears (e.g. during OSD provisioning)."""
        log.info(f"Waiting for bootstrap auth entity {entity} to appear")

        def _key_present():
            return self._auth_entity_exists(entity, toolbox_pod)

        for present in TimeoutSampler(timeout, sleep, _key_present):
            if present:
                log.info(f"Bootstrap auth entity {entity} is present")
                return True

        log.info(f"Bootstrap auth entity {entity} did not appear within {timeout}s")
        return False

    def verify_key_rotation_idempotent_after_operator_restart(
        self,
        baseline_generations,
        auth_keys,
        auth_entities,
        pod_states,
        osd_cephx_status=None,
        operator_log_since=None,
        previous_operator_pod_name=None,
        settle_timeout=120,
    ):
        """
        Verify CephX state is unchanged after rook-ceph-operator re-reconcile.

        Args:
            baseline_generations (dict): From :meth:`record_all_cephx_status_generations`.
            auth_keys (dict): Entity to key mapping captured after rotation.
            auth_entities (list): Auth entities to re-check.
            pod_states (dict): From :meth:`capture_all_daemon_pod_states`.
            osd_cephx_status (dict): From :meth:`capture_osd_deployment_cephx_status`.
            operator_log_since (datetime): Scan operator logs after this timestamp.
            previous_operator_pod_name (str): Deleted operator pod name.
            settle_timeout (int): Seconds to wait before post-reconcile checks.
        """
        idempotency_context = "after operator re-reconcile"
        log.info("Verifying CephX key rotation idempotency after operator restart")
        if settle_timeout:
            time.sleep(settle_timeout)

        self.wait_for_rook_ceph_operator_ready(
            previous_pod_name=previous_operator_pod_name
        )
        self.wait_for_cluster_ready()
        self.assert_cephx_status_generations_unchanged(
            baseline_generations, context=idempotency_context
        )
        self.assert_auth_keys_unchanged(
            auth_keys,
            entities=auth_entities,
            context=idempotency_context,
        )
        self.assert_all_daemon_pod_states_unchanged(
            pod_states,
            settle_time=0,
            context=idempotency_context,
        )
        if osd_cephx_status is not None:
            self.assert_osd_deployment_cephx_status_unchanged(osd_cephx_status)
        if operator_log_since is not None:
            self.verify_operator_no_key_rotation_logs(operator_log_since)
        log.info("CephX key rotation is idempotent after operator re-reconcile")

    def wait_for_post_mon_startup_bootstrap_cleanup(self, timeout=900, sleep=15):
        """
        Wait for post-mon-startup bootstrap key cleanup to finish.

        Non-OSD bootstrap keys are removed by Rook after cluster startup actions.
        """
        return self.wait_for_bootstrap_keys_absent(
            constants.CEPHX_BOOTSTRAP_NON_OSD_KEYS,
            timeout=timeout,
            sleep=sleep,
        )

    def wait_for_bootstrap_osd_key_absent(self, timeout=900, sleep=15):
        """Wait until ``client.bootstrap-osd`` is removed after OSD provisioning."""
        return self.wait_for_bootstrap_keys_absent(
            ["client.bootstrap-osd"],
            timeout=timeout,
            sleep=sleep,
        )

    def verify_operator_bootstrap_deletion_logs(self, bootstrap_entities):
        """
        Verify rook-ceph-operator logged successful bootstrap key deletion.

        Args:
            bootstrap_entities (list): Entity names expected to have deletion logs.
        """
        if not bootstrap_entities:
            log.info(
                "No bootstrap keys were present before cleanup; "
                "skipping operator deletion log verification"
            )
            return

        from ocs_ci.helpers.helpers import get_logs_rook_ceph_operator

        operator_logs = get_logs_rook_ceph_operator()
        missing = []
        for entity in bootstrap_entities:
            if any(
                entity in line
                and constants.CEPHX_BOOTSTRAP_DELETED_OPERATOR_LOG in line
                and constants.CEPHX_BOOTSTRAP_OPERATOR_LOG_TOKEN in line
                for line in operator_logs.splitlines()
            ):
                log.info(f"Operator log confirms deletion of {entity}")
                continue
            missing.append(entity)

        if missing:
            raise UnexpectedBehaviour(
                "Operator logs missing successful bootstrap key deletion for: "
                f"{', '.join(missing)}"
            )

    def verify_no_bootstrap_deletion_errors(self):
        """
        Verify operator did not log non-idempotent bootstrap key deletion errors.

        ENOENT/not-found style failures are acceptable when keys are already gone.
        """
        from ocs_ci.helpers.helpers import get_logs_rook_ceph_operator

        operator_logs = get_logs_rook_ceph_operator()
        errors = []
        for line in operator_logs.splitlines():
            lower_line = line.lower()
            if "bootstrap" not in lower_line or "failed to delete" not in lower_line:
                continue
            if "enoent" in lower_line or "not found" in lower_line:
                continue
            errors.append(line.strip())

        if errors:
            raise UnexpectedBehaviour(
                "Unexpected bootstrap key deletion errors in operator logs: "
                f"{'; '.join(errors[:5])}"
            )
        log.info("No unexpected bootstrap key deletion errors in operator logs")

    def capture_daemon_pod_state(self, label):
        """
        Record Running pod names and cephx-key-identifier annotations for *label*.

        Note:
            OSD pods do not carry ``cephx-key-identifier`` (Rook uses Deployment
            ``cephx-status`` plus the ``cephx-keyring-update`` init container).
            For OSDs the annotation value is typically ``None``.

        Returns:
            dict: pod name to annotation value (may be None).
        """
        pods = get_pods_having_label(
            label, namespace=self.namespace, statuses=[constants.STATUS_RUNNING]
        )
        state = {}
        for pod in pods:
            name = pod["metadata"]["name"]
            annotations = pod["metadata"].get("annotations") or {}
            state[name] = annotations.get(constants.CEPHX_KEY_IDENTIFIER_ANNOTATION)
        return state

    def capture_all_daemon_pod_states(self):
        """Capture pod state for MON, MGR, OSD, and MDS daemons."""
        return {
            daemon: self.capture_daemon_pod_state(label)
            for daemon, label in constants.ROOK_CEPHX_KEYROTATION_DAEMON_LABELS.items()
        }

    def wait_for_pod_restarts(self, before_state, label, timeout=900, sleep=15):
        """
        Wait until all Running pods for *label* have new names or annotations.

        Args:
            before_state (dict): Output of :meth:`capture_daemon_pod_state`.
        """
        log.info(
            f"Waiting for pod restarts (label={label}, "
            f"prior pods={', '.join(before_state) or 'none'})"
        )

        def _pods_restarted():
            current = self.capture_daemon_pod_state(label)
            if not current:
                return False
            # Require every currently Running pod to be new or annotation-changed;
            # a single restarted peer must not short-circuit the wait.
            for pod_name, annotation in current.items():
                if pod_name in before_state and before_state[pod_name] == annotation:
                    return False
            return True

        for restarted in TimeoutSampler(timeout, sleep, _pods_restarted):
            if restarted:
                log.info(f"Pods restarted for label {label}")
                return self.capture_daemon_pod_state(label)

        raise UnexpectedBehaviour(
            f"Pods with label {label} did not restart within {timeout}s"
        )

    def wait_for_all_daemon_pod_restarts(self, before_states, timeout=900, sleep=15):
        """Wait for MON, MGR, OSD, and MDS pod restarts."""
        after_states = {}
        for daemon, label in constants.ROOK_CEPHX_KEYROTATION_DAEMON_LABELS.items():
            after_states[daemon] = self.wait_for_pod_restarts(
                before_states.get(daemon, {}),
                label,
                timeout=timeout,
                sleep=sleep,
            )
        return after_states

    def _get_cephfilesystem_obj(self):
        if self._cephfilesystem_obj is None:
            self._cephfilesystem_obj = OCP(
                kind=constants.CEPHFILESYSTEM,
                resource_name=self.cephfilesystem_name,
                namespace=self.namespace,
            )
        return self._cephfilesystem_obj

    def _wait_for_cr_daemon_rotation(
        self, cr_obj, expected_generation, timeout, sleep, label
    ):
        log.info(
            f"Waiting for CephX daemon rotation on {label} to reach "
            f"generation {expected_generation}"
        )

        def _daemon_ready():
            cr_obj.reload_data()
            cephx = cr_obj.data.get("status", {}).get("cephx", {}) or {}
            generation = int((cephx.get("daemon") or {}).get("keyGeneration", 0) or 0)
            if generation < expected_generation:
                log.debug(
                    f"{label} daemon keyGeneration={generation} "
                    f"(want >= {expected_generation})"
                )
                return False
            return True

        for ready in TimeoutSampler(timeout, sleep, _daemon_ready):
            if ready:
                log.info(
                    f"CephX daemon rotation on {label} reached "
                    f"generation {expected_generation}"
                )
                return True

        raise UnexpectedBehaviour(
            f"CephX daemon rotation on {label} did not reach generation "
            f"{expected_generation} within {timeout}s"
        )
