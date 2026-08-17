"""MON CephX helpers: quorum, secrets, and operator crash recovery."""

import logging
import re

from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import (
    get_deployments_having_label,
    get_mon_pods,
    get_operator_pods,
)
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


class CephXMONHelper:
    """MON quorum, secret, and operator-crash recovery helpers."""

    def wait_for_mon_rotation(self, expected_generation, timeout=900, sleep=15):
        """Wait until ``status.cephx.mon.keyGeneration`` matches (when supported)."""
        if not self.is_mon_key_rotation_supported():
            log.info("MON CephX key rotation status not reported; skipping wait")
            return False
        return self._wait_for_status_entities(
            ["mon"],
            expected_generation,
            timeout,
            sleep,
            label="mon",
        )

    @retry(UnexpectedBehaviour, tries=5, delay=20)
    def is_mon_key_rotation_supported(self):
        """
        Return True when CephCluster reports MON ``status.cephx.mon.keyGeneration``.

        Raises ``UnexpectedBehaviour`` while ``status.cephx.mon`` is absent so the
        retry decorator can wait for CephCluster to report MON status. Returns
        False only when ``mon`` is present but has no ``keyGeneration`` (Rook
        reports ``mon: {}`` when MON rotation is unsupported).

        Note: Prefer verifying keys via the shared ``mon.`` entity
        (``ceph auth get-key mon.``). Use :meth:`is_mon_auth_verifiable` before
        asserting on MON auth keys.
        """
        status_cephx = self.get_status_cephx()
        if "mon" not in status_cephx:
            raise UnexpectedBehaviour("CephCluster status.cephx.mon not yet reported")
        mon_status = status_cephx.get("mon") or {}
        return bool(mon_status.get("keyGeneration"))

    def is_mon_auth_verifiable(self, toolbox_pod=None):
        """Return True when MON auth entities are readable from the auth store."""
        return bool(self._discover_mon_auth_entities(toolbox_pod))

    def _discover_mon_auth_entities(self, toolbox_pod=None):
        """
        Discover MON auth entities.

        Prefer the shared ``mon.`` entity (``ceph auth get-key mon.``), which is
        the CephX key used by monitors on current ODF clusters. Fall back to
        per-mon entities from ``ceph auth ls`` / ``ceph mon dump`` when ``mon.``
        is absent.
        """
        if self._auth_entity_exists("mon.", toolbox_pod):
            return ["mon."]

        entities = self.list_auth_entities("mon.", toolbox_pod)
        if entities:
            return entities

        toolbox = toolbox_pod or self.get_ceph_cli_pod()
        mon_dump = toolbox.exec_ceph_cmd("ceph mon dump")
        discovered = []
        for mon in mon_dump.get("mons", []):
            name = mon.get("name")
            if not name:
                continue
            entity = f"mon.{name}"
            if self._auth_entity_exists(entity, toolbox_pod):
                discovered.append(entity)
        return sorted(discovered)

    def get_mon_deployment_names(self):
        """Return sorted rook-ceph-mon deployment names."""
        deployments = get_deployments_having_label(
            constants.MON_APP_LABEL, self.namespace
        )
        return sorted(dep["metadata"]["name"] for dep in deployments)

    def scale_mon_deployments(self, deployment_names, replicas):
        """Scale mon deployments to *replicas*."""
        from ocs_ci.helpers.helpers import modify_deployment_replica_count

        for deployment_name in deployment_names:
            log.info(f"Scaling {deployment_name} to {replicas} replicas")
            assert modify_deployment_replica_count(
                deployment_name, replicas, namespace=self.namespace
            ), f"Failed to scale {deployment_name} to {replicas}"

    def restore_mon_deployments(self, deployment_names=None):
        """Scale mon deployments back to one replica and wait for quorum."""
        from ocs_ci.helpers.ceph_helpers import wait_for_mons_in_quorum

        deployment_names = deployment_names or self.get_mon_deployment_names()
        self.scale_mon_deployments(deployment_names, 1)
        wait_for_mons_in_quorum(len(deployment_names), timeout=600)

    def break_mon_quorum(self, mons_to_stop=2):
        """
        Scale down *mons_to_stop* mon deployments to break quorum.

        Returns:
            list: Mon deployment names scaled to zero (keeps the first mon up).
        """
        mon_deployments = self.get_mon_deployment_names()
        if len(mon_deployments) < mons_to_stop + 1:
            raise UnexpectedBehaviour(
                f"Need at least {mons_to_stop + 1} mon deployments; "
                f"found {len(mon_deployments)}"
            )
        scaled_down = mon_deployments[1 : mons_to_stop + 1]
        self.scale_mon_deployments(scaled_down, 0)
        return scaled_down

    def get_running_mon_pod_count(self):
        """
        Return the number of mon pods currently in Running state.

        Prefer this over ``ceph mon stat`` when quorum may already be broken:
        Ceph CLI against the tools pod hangs without mon majority.
        """
        running = [
            mon_pod
            for mon_pod in get_mon_pods(namespace=self.namespace)
            if mon_pod.status() == constants.STATUS_RUNNING
        ]
        log.info(
            f"Running mon pods ({len(running)}): "
            f"{[mon_pod.name for mon_pod in running]}"
        )
        return len(running)

    def wait_for_mon_quorum_count_at_most(self, max_count, timeout=300, sleep=15):
        """
        Wait until at most *max_count* mon pods are Running.

        Uses the Kubernetes API rather than ``ceph mon stat``. After scaling
        mons below majority, Ceph CLI hangs, so running-pod count is the
        reliable signal that quorum has been broken for negative tests.
        """
        log.info(f"Waiting for running mon pod count <= {max_count}")

        def _running_mons_reduced():
            count = self.get_running_mon_pod_count()
            return count <= max_count

        for ready in TimeoutSampler(timeout, sleep, _running_mons_reduced):
            if ready:
                return self.get_running_mon_pod_count()

        raise UnexpectedBehaviour(
            f"Running mon pod count did not drop to {max_count} within {timeout}s"
        )

    def assert_mon_pods_not_crashlooping(self):
        """Assert no mon pods are in CrashLoopBackOff."""
        crashloop_pods = []
        for mon_pod in get_mon_pods(namespace=self.namespace):
            pod_data = mon_pod.get()
            for container_status in (
                pod_data.get("status", {}).get("containerStatuses", []) or []
            ):
                waiting = container_status.get("state", {}).get("waiting", {})
                if waiting.get("reason") == constants.STATUS_CLBO:
                    crashloop_pods.append(mon_pod.name)
        if crashloop_pods:
            raise UnexpectedBehaviour(
                f"Mon pods in CrashLoopBackOff: {', '.join(crashloop_pods)}"
            )
        log.info("No mon pods are in CrashLoopBackOff")

    def _parse_mon_keys_from_keyring(self, keyring_text):
        """Return mon entity to key mapping parsed from a Ceph keyring."""
        keys = {}
        current_entity = None
        for line in keyring_text.splitlines():
            # Any section header ends the previous entity scope. Only ``mon.*``
            # sections (shared ``[mon.]`` or per-mon ``[mon.a]``) stay active so
            # later sections like ``[client.admin]`` cannot overwrite mon keys.
            section_match = re.match(r"\[([^\]]+)\]", line.strip())
            if section_match:
                entity = section_match.group(1)
                current_entity = entity if entity.startswith("mon.") else None
                continue
            if current_entity and "key" in line:
                key_match = re.search(r"key\s*=\s*(\S+)", line)
                if key_match:
                    keys[current_entity] = key_match.group(1)
        return keys

    def get_mon_keys_from_secrets(self):
        """Return mon entity to key mapping from rook mon Kubernetes secrets."""
        import base64

        secret_keys = {}
        for secret_name in (
            constants.MANAGED_MON_SECRET,
            constants.MANAGED_MONS_KEYRING_SECRET,
        ):
            secret_obj = OCP(
                kind=constants.SECRET,
                resource_name=secret_name,
                namespace=self.namespace,
            )
            secret_data = secret_obj.get().get("data", {})
            keyring_b64 = secret_data.get("keyring") or secret_data.get("adminKeyring")
            if not keyring_b64:
                continue
            keyring_text = base64.b64decode(keyring_b64).decode()
            secret_keys.update(self._parse_mon_keys_from_keyring(keyring_text))
        return secret_keys

    def assert_mon_secret_keys_unchanged(
        self, old_keys, context="while mon quorum is broken"
    ):
        """
        Assert rook mon Kubernetes secret keys did not change.

        Prefer this over :meth:`assert_auth_keys_unchanged` when mon quorum is
        broken: ``ceph auth get-key`` cannot reach the cluster without quorum.
        """
        new_keys = self.get_mon_keys_from_secrets()
        self.log_auth_key_comparison(old_keys, new_keys)
        entities = sorted(set(old_keys) | set(new_keys))
        changed = [
            entity
            for entity in entities
            if old_keys.get(entity) != new_keys.get(entity)
        ]
        if changed:
            raise UnexpectedBehaviour(
                f"Mon Kubernetes secret keys changed {context}: {', '.join(changed)}"
            )
        log.info(
            f"Mon Kubernetes secret keys unchanged for entities: "
            f"{', '.join(entities) if entities else '<none>'}"
        )

    def get_mon_keys_from_ceph_auth(self, toolbox_pod=None):
        """Return mon entity to key mapping from the Ceph auth store."""
        toolbox = toolbox_pod or self.get_ceph_cli_pod()
        mon_entities = self.discover_rook_daemon_auth_entities(toolbox).get("mon", [])
        if not mon_entities:
            mon_entities = [
                line.split()[0]
                for line in toolbox.exec_cmd_on_pod(
                    "ceph auth ls", out_yaml_format=False
                ).splitlines()
                if line.startswith("mon.")
            ]
        return self.capture_auth_keys(mon_entities, toolbox_pod=toolbox)

    def verify_mon_secrets_match_ceph_auth(self, toolbox_pod=None):
        """Assert Kubernetes mon secrets match Ceph auth store mon keys."""
        ceph_keys = self.get_mon_keys_from_ceph_auth(toolbox_pod)
        secret_keys = self.get_mon_keys_from_secrets()
        if not ceph_keys:
            log.warning(
                "No mon auth entities found in Ceph; skipping secret comparison"
            )
            return
        mismatched = []
        for entity, ceph_key in ceph_keys.items():
            secret_key = secret_keys.get(entity)
            if not secret_key:
                mismatched.append(f"{entity}=<missing in secret>")
            elif secret_key != ceph_key:
                mismatched.append(f"{entity}=<mismatch>")
        if mismatched:
            raise UnexpectedBehaviour(
                "Mon Kubernetes secrets do not match Ceph auth store: "
                f"{', '.join(mismatched)}"
            )
        log.info("Mon Kubernetes secrets match Ceph auth store")

    def kill_operator_during_mon_rotation(self, timeout=900, poll_interval=2):
        """
        Trigger daemon rotation and kill rook-ceph-operator while mon rotation
        is in progress.

        Patches StorageCluster without waiting for Ready, then force-deletes the
        operator once CephCluster is Progressing and mon auth rotation appears
        in operator logs since the trigger.

        Returns:
            int: Requested daemon key generation.
        """
        from ocs_ci.helpers.helpers import get_last_log_time_date

        operator_log_marker = get_last_log_time_date()
        target_generation = self.rotate_daemon_keys(wait_for_rotation=False)
        operator_pods = get_operator_pods(namespace=self.namespace)
        if not operator_pods:
            raise UnexpectedBehaviour("rook-ceph-operator pod not found")
        operator_pod = operator_pods[0]
        operator_ocp = OCP(kind=constants.POD, namespace=self.namespace)

        log.info(
            "Waiting to kill rook-ceph-operator while mon key rotation is in progress"
        )

        def _kill_when_mon_rotating():
            phase = self.get_cephcluster_phase()
            logs = self.get_operator_logs_since(operator_log_marker)
            logs_text = "\n".join(logs)
            mon_rotating = bool(
                re.search(
                    constants.CEPHX_MON_AUTH_ROTATION_LOG_PATTERN, logs_text, re.I
                )
            )
            if phase == constants.STATUS_PROGRESSING and mon_rotating:
                log.info(
                    f"Killing rook-ceph-operator pod {operator_pod.name} during "
                    f"mon key rotation (CephCluster phase={phase})"
                )
                operator_ocp.delete(
                    resource_name=operator_pod.name, force=True, wait=False
                )
                return True
            log.debug(
                f"Mon rotation kill gate: phase={phase}, "
                f"mon_auth_log={mon_rotating}"
            )
            return False

        for done in TimeoutSampler(timeout, poll_interval, _kill_when_mon_rotating):
            if done:
                return target_generation

        raise UnexpectedBehaviour(
            "Timed out waiting for mon key rotation in progress before "
            "killing rook-ceph-operator"
        )

    def recover_after_operator_crash_during_mon_rotation(self, timeout=1500):
        """
        Wait for operator and cluster recovery after a mid-mon-rotation crash.

        Does not gate on operator log patterns; callers should verify daemon
        key rotation separately via status generation and auth key comparison.
        """
        self.wait_for_rook_ceph_operator_ready()
        self.assert_mon_pods_not_crashlooping()
        from ocs_ci.helpers.ceph_helpers import wait_for_mons_in_quorum

        wait_for_mons_in_quorum(len(self.get_mon_deployment_names()), timeout=timeout)
        self.wait_for_cluster_ready(timeout=timeout)
        self.wait_for_pgs_active_clean(timeout=timeout)
        self.verify_mon_secrets_match_ceph_auth()
