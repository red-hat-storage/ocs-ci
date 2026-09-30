"""Cluster readiness, reconcile, operator, and PG helpers for CephX tests."""

import json
import logging
import time

from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed, UnexpectedBehaviour
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import (
    Pod,
    get_operator_pods,
    get_pod_logs,
    get_pods_having_label,
    wait_for_matching_pattern_in_pod_logs,
)
from ocs_ci.ocs.resources.storage_cluster import StorageCluster
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


class CephXClusterHelper:
    """Cluster Ready/reconcile, operator lifecycle, and PG wait helpers."""

    def get_cephcluster_phase(self):
        """Return CephCluster ``status.phase``."""
        status = self._get_cluster_dict().get("status") or {}
        return status.get("phase")

    def get_storagecluster_phase(self):
        """Return StorageCluster ``status.phase``."""
        status = self._get_storage_cluster_dict().get("status") or {}
        return status.get("phase")

    def assert_decreasing_daemon_key_generation_rejected(self, lower_generation=None):
        """
        Assert StorageCluster rejects decreasing daemon ``keyGeneration``.

        Patches ``managedResources.cephCluster.security.cephx.daemon.keyGeneration``
        to a value lower than the current StorageCluster generation and expects
        admission validation failure containing
        ``constants.CEPHX_KEY_GENERATION_DECREASE_ERROR``.

        Args:
            lower_generation (int): Explicit lower generation. Defaults to
                current StorageCluster generation - 1.

        Returns:
            int: The rejected lower generation that was attempted.

        Raises:
            UnexpectedBehaviour: If the patch succeeds or fails with an
                unexpected error.
        """
        current_generation = self.get_spec_key_generation(self.COMPONENT_DAEMON)
        if current_generation < 1:
            raise UnexpectedBehaviour(
                "StorageCluster daemon keyGeneration is unset; cannot verify "
                "decrease rejection"
            )

        if lower_generation is None:
            lower_generation = current_generation - 1
        lower_generation = int(lower_generation)
        if lower_generation >= current_generation:
            raise UnexpectedBehaviour(
                f"lower_generation={lower_generation} must be < current "
                f"StorageCluster keyGeneration={current_generation}"
            )

        component_config = dict(
            self.get_storagecluster_component_spec(self.COMPONENT_DAEMON)
        )
        component_config["keyRotationPolicy"] = self.KEY_ROTATION_POLICY_KEY_GENERATION
        component_config["keyGeneration"] = lower_generation

        log.info(
            "Attempting to decrease StorageCluster daemon keyGeneration "
            f"{current_generation} -> {lower_generation} (expect rejection)"
        )
        try:
            self.patch_storagecluster_cephx_component(
                self.COMPONENT_DAEMON, component_config
            )
        except CommandFailed as exc:
            err = str(exc)
            if constants.CEPHX_KEY_GENERATION_DECREASE_ERROR not in err:
                raise UnexpectedBehaviour(
                    "Expected StorageCluster admission error containing "
                    f"'{constants.CEPHX_KEY_GENERATION_DECREASE_ERROR}', got: {err}"
                ) from exc
            log.info(
                "StorageCluster correctly rejected daemon keyGeneration decrease "
                f"to {lower_generation}"
            )
            return lower_generation

        raise UnexpectedBehaviour(
            "StorageCluster accepted a decreased daemon keyGeneration "
            f"({current_generation} -> {lower_generation}); expected rejection"
        )

    def assert_invalid_daemon_key_generation_type_rejected(
        self, invalid_value, expected_json_type
    ):
        """
        Assert StorageCluster rejects non-integer daemon ``keyGeneration``.

        Patches only
        ``/spec/managedResources/cephCluster/security/cephx/daemon/keyGeneration``
        with a non-integer JSON value.

        Expected rejection: non-null values fail with an OpenAPI type error
        (``must be of type integer: "<expected_json_type>"``); ``null`` fails
        with ``keyGeneration cannot be removed once set`` (null is treated as
        removing the field once it has been set).

        Args:
            invalid_value: Value to patch (e.g. ``"abc"``, ``True``, ``None``).
            expected_json_type (str): Reported JSON type in the OpenAPI error
                (``string`` or ``boolean``). Ignored when ``invalid_value`` is
                ``None`` (null uses the remove-once-set validation path).

        Returns:
            str: Admission error text.

        Raises:
            UnexpectedBehaviour: If the patch succeeds or fails unexpectedly.
        """
        daemon_spec = self.get_storagecluster_component_spec(self.COMPONENT_DAEMON)
        path = "/spec/managedResources/cephCluster/security/cephx/daemon/keyGeneration"
        op = "replace" if "keyGeneration" in daemon_spec else "add"
        # Ensure parent daemon object exists before adding keyGeneration.
        if op == "add" and not daemon_spec:
            restore_generation = max(
                self.DEFAULT_DAEMON_KEY_GENERATION,
                self.get_desired_cephx_key_gen(),
            )
            self.patch_storagecluster_cephx_component(
                self.COMPONENT_DAEMON,
                {
                    "keyRotationPolicy": self.KEY_ROTATION_POLICY_KEY_GENERATION,
                    "keyGeneration": restore_generation,
                },
            )
            op = "replace"

        patch_ops = [{"op": op, "path": path, "value": invalid_value}]
        value_repr = (
            "null"
            if invalid_value is None
            else (
                str(invalid_value).lower()
                if isinstance(invalid_value, bool)
                else repr(invalid_value)
            )
        )
        expect_remove_error = invalid_value is None
        expected_error = (
            constants.CEPHX_KEY_GENERATION_REMOVE_ERROR
            if expect_remove_error
            else constants.CEPHX_KEY_GENERATION_TYPE_ERROR
        )
        log.info(
            "Attempting invalid StorageCluster daemon keyGeneration=%s "
            "(expect rejection containing %r%s)",
            value_repr,
            expected_error,
            ("" if expect_remove_error else f", json type={expected_json_type}"),
        )
        try:
            self._get_storagecluster_ocp().patch(
                params=json.dumps(patch_ops), format_type="json"
            )
        except CommandFailed as exc:
            err = str(exc)
            if expected_error not in err:
                raise UnexpectedBehaviour(
                    "Expected StorageCluster validation error containing "
                    f"'{expected_error}', got: {err}"
                ) from exc
            if not expect_remove_error:
                type_token = f'"{expected_json_type}"'
                if type_token not in err and expected_json_type not in err:
                    raise UnexpectedBehaviour(
                        "Expected type validation error to mention JSON type "
                        f"{type_token}, got: {err}"
                    ) from exc
            log.info(
                "StorageCluster correctly rejected invalid daemon "
                f"keyGeneration={value_repr}"
            )
            return err

        raise UnexpectedBehaviour(
            "StorageCluster accepted non-integer daemon keyGeneration="
            f"{value_repr}; expected validation rejection containing "
            f"'{expected_error}'"
        )

    def get_cephcluster_daemon_key_generation(self):
        """
        Return CephCluster daemon ``keyGeneration`` for SC recovery.

        Prefers ``spec.security.cephx.daemon.keyGeneration`` when set; otherwise
        uses the highest reported ``status.cephx`` daemon generation.
        """
        cc_daemon = self.get_spec_cephx().get("daemon") or {}
        if cc_daemon.get("keyGeneration") is not None:
            return int(cc_daemon["keyGeneration"])
        return int(self.get_component_status_key_generation(self.COMPONENT_DAEMON) or 0)

    def recover_storagecluster_daemon_key_generation_from_cephcluster(self):
        """
        Restore StorageCluster daemon ``keyGeneration`` from CephCluster.

        Used when a bad patch (e.g. ``null``) deleted the field from
        StorageCluster and left the cluster in Error. Copies the CephCluster
        daemon generation onto StorageCluster and waits for Ready.
        """
        self.ensure_daemon_key_generations_aligned()

    def ensure_daemon_key_generations_aligned(self):
        """
        Ensure StorageCluster daemon ``keyGeneration`` matches CephCluster.

        CephCluster is the source of truth (spec.daemon.keyGeneration when set,
        otherwise highest status.cephx daemon generation). StorageCluster is
        patched to that value. If a direct replace is rejected because
        keyGeneration cannot be decreased, the field is removed and re-added.

        Returns:
            int: Aligned daemon keyGeneration.

        Raises:
            UnexpectedBehaviour: If CephCluster has no generation or StorageCluster
                still mismatches after alignment.
        """
        path = "/spec/managedResources/cephCluster/security/cephx/daemon/keyGeneration"
        cc_generation = self.get_cephcluster_daemon_key_generation()
        if cc_generation < 1:
            raise UnexpectedBehaviour(
                "Cannot align StorageCluster daemon keyGeneration: "
                "CephCluster has no daemon keyGeneration in spec or status"
            )

        daemon_spec = self.get_storagecluster_component_spec(self.COMPONENT_DAEMON)
        sc_generation = int(daemon_spec.get("keyGeneration", 0) or 0)
        if "keyGeneration" in daemon_spec and sc_generation == cc_generation:
            log.info(
                "StorageCluster and CephCluster daemon keyGeneration already "
                f"aligned at {cc_generation}"
            )
            return cc_generation

        # Parent daemon object must exist before add/replace of keyGeneration.
        if not daemon_spec:
            self.patch_storagecluster_cephx_component(
                self.COMPONENT_DAEMON,
                {
                    "keyRotationPolicy": self.KEY_ROTATION_POLICY_KEY_GENERATION,
                    "keyGeneration": cc_generation,
                },
            )
            self.wait_for_cluster_ready()
            aligned = self.get_spec_key_generation(self.COMPONENT_DAEMON)
            if aligned != cc_generation:
                raise UnexpectedBehaviour(
                    "Failed to align StorageCluster daemon keyGeneration: "
                    f"StorageCluster={aligned}, CephCluster={cc_generation}"
                )
            log.info(
                "Aligned StorageCluster daemon keyGeneration to CephCluster "
                f"value {cc_generation}"
            )
            return cc_generation

        sc_obj = self._get_storagecluster_ocp()
        log.info(
            "Aligning StorageCluster daemon keyGeneration "
            f"({sc_generation if 'keyGeneration' in daemon_spec else None}) "
            f"to CephCluster value {cc_generation}"
        )
        try:
            op = "replace" if "keyGeneration" in daemon_spec else "add"
            sc_obj.patch(
                params=json.dumps([{"op": op, "path": path, "value": cc_generation}]),
                format_type="json",
            )
        except CommandFailed as exc:
            err = str(exc)
            if (
                "keyGeneration" in daemon_spec
                and sc_generation > cc_generation
                and constants.CEPHX_KEY_GENERATION_DECREASE_ERROR in err
            ):
                log.warning(
                    "Direct keyGeneration decrease rejected; removing and "
                    f"re-adding keyGeneration={cc_generation}"
                )
                sc_obj.patch(
                    params=json.dumps([{"op": "remove", "path": path}]),
                    format_type="json",
                )
                sc_obj.patch(
                    params=json.dumps(
                        [{"op": "add", "path": path, "value": cc_generation}]
                    ),
                    format_type="json",
                )
            else:
                raise

        self._storagecluster_obj = None
        self.wait_for_cluster_ready()
        aligned = self.get_spec_key_generation(self.COMPONENT_DAEMON)
        if aligned != cc_generation:
            raise UnexpectedBehaviour(
                "Failed to align StorageCluster daemon keyGeneration: "
                f"StorageCluster={aligned}, CephCluster={cc_generation}"
            )
        log.info(
            "Aligned StorageCluster daemon keyGeneration to CephCluster "
            f"value {cc_generation}"
        )
        return cc_generation

    def record_all_cephx_status_generations(self):
        """Snapshot status/spec keyGeneration values for rotation components."""
        generations = {}
        status = self.get_status_cephx()
        for entity in self.CEPHX_STATUS_GENERATION_ENTITIES:
            entry = status.get(entity) or {}
            generations[entity] = int(entry.get("keyGeneration", 0) or 0)
        generations["filesystem_daemon"] = self.get_filesystem_daemon_key_generation()
        for component in self.ROTATION_COMPONENTS:
            generations[f"spec_{component}"] = self.get_spec_key_generation(component)
        return generations

    def assert_cephx_status_generations_unchanged(
        self, baseline, context="while rotation is Disabled"
    ):
        """Assert CephX keyGeneration values did not change."""
        current = self.record_all_cephx_status_generations()
        changed = {
            name: {"before": baseline[name], "after": current[name]}
            for name in baseline
            if baseline[name] != current[name]
        }
        if changed:
            raise UnexpectedBehaviour(
                f"CephX keyGeneration values changed {context}: {changed}"
            )
        log.info("CephX keyGeneration values unchanged")

    def assert_reported_cephx_generations_unchanged(
        self, baseline, context="while rotation is blocked"
    ):
        """Assert reported ``status.cephx`` generations did not change (ignore spec)."""
        current = self.record_all_cephx_status_generations()
        changed = {
            name: {"before": baseline[name], "after": current[name]}
            for name in baseline
            if not name.startswith("spec_") and baseline[name] != current[name]
        }
        if changed:
            raise UnexpectedBehaviour(
                f"Reported CephX keyGeneration values changed {context}: {changed}"
            )
        log.info("Reported CephX keyGeneration values unchanged")

    def trigger_reconciliation_cycles(self, cycles=3, sleep_between=60):
        """Trigger multiple CephCluster reconciles."""
        for cycle in range(1, cycles + 1):
            log.info(f"Triggering CephCluster reconcile cycle {cycle}/{cycles}")
            self.trigger_cephcluster_reconcile()
            if cycle < cycles:
                time.sleep(sleep_between)

    def wait_for_cluster_ready(self, timeout=900):
        """Wait until CephCluster and StorageCluster reach Ready phase."""
        cephcluster = OCP(
            kind=constants.CEPH_CLUSTER,
            namespace=self.namespace,
            resource_name=self.ceph_cluster_name,
        )
        # CephCluster has status.phase but generic OCP defaults _has_phase to False.
        cephcluster._has_phase = True
        log.info(f"Waiting for CephCluster {self.ceph_cluster_name} to be Ready")
        cephcluster.wait_for_phase(phase=constants.STATUS_READY, timeout=timeout)

        storage_cluster = StorageCluster(
            resource_name=constants.DEFAULT_CLUSTERNAME,
            namespace=self.namespace,
        )
        log.info("Waiting for StorageCluster to be Ready")
        storage_cluster.wait_for_phase(phase=constants.STATUS_READY, timeout=timeout)

    def wait_for_cluster_fully_recovered(self, timeout=1500, sleep=15):
        """
        Wait until PGs are active+clean and CephCluster/StorageCluster are Ready.

        Use after daemon CephX rotation or after restoring intentionally
        disrupted OSDs/MONs so subsequent health checks do not race recovery.
        """
        log.info(
            "Waiting for full cluster recovery "
            f"(PGs active+clean, cluster Ready; timeout={timeout}s)"
        )
        self.wait_for_pgs_active_clean(timeout=timeout, sleep=sleep)
        self.wait_for_cluster_ready(timeout=timeout)

    def wait_for_cephcluster_rotation(self, timeout=1500, sleep=15):
        """
        Wait until CephCluster finishes CephX rotation reconcile.

        Expected flow after a StorageCluster-initiated rotation:
        ``Progressing`` (per-component messages such as Processing OSD…) →
        ``Ready``. ``Error`` fails immediately.

        ``Ready`` is accepted only after ``Progressing`` was observed so a
        pre-reconcile Ready phase is not mistaken for completion.

        Args:
            timeout (int): Max seconds to wait (default 25 minutes).
            sleep (int): Seconds between polls (default 15).

        Returns:
            bool: True when CephCluster is Ready after Progressing.

        Raises:
            UnexpectedBehaviour: If phase is Error, or Ready is not reached
                within *timeout* after Progressing was seen.
        """
        cephcluster = OCP(
            kind=constants.CEPH_CLUSTER,
            namespace=self.namespace,
            resource_name=self.ceph_cluster_name,
        )
        log.info(
            f"Waiting for CephCluster {self.ceph_cluster_name} rotation "
            f"(Progressing→Ready; Error fails; timeout={timeout}s, "
            f"poll every {sleep}s)"
        )
        seen_progressing = False
        last_phase = None
        last_message = ""

        def _poll_rotation_state():
            """
            Returns:
                str: ``ready``, ``error``, or ``waiting``.
            """
            nonlocal seen_progressing, last_phase, last_message
            cephcluster.reload_data()
            status = cephcluster.data.get("status") or {}
            phase = status.get("phase")
            message = status.get("message", "") or ""
            last_phase = phase
            last_message = message
            msg_suffix = f" message={message}" if message else ""

            if phase == constants.STATUS_ERROR:
                log.error(
                    f"CephCluster {self.ceph_cluster_name} entered Error during "
                    f"CephX key rotation{msg_suffix}"
                )
                return "error"

            if phase == constants.STATUS_PROGRESSING:
                if not seen_progressing:
                    log.info(
                        f"CephCluster {self.ceph_cluster_name} entered "
                        f"Progressing{msg_suffix}"
                    )
                else:
                    log.info(
                        f"CephCluster {self.ceph_cluster_name} still "
                        f"Progressing{msg_suffix}"
                    )
                seen_progressing = True
                return "waiting"

            if phase == constants.STATUS_READY:
                if seen_progressing:
                    log.info(
                        f"CephCluster {self.ceph_cluster_name} rotation "
                        f"completed; phase=Ready{msg_suffix}"
                    )
                    return "ready"
                log.info(
                    f"CephCluster {self.ceph_cluster_name} phase=Ready "
                    "(pre-reconcile); waiting for Progressing"
                )
                return "waiting"

            log.info(
                f"CephCluster {self.ceph_cluster_name} phase={phase}"
                f"{msg_suffix}; waiting for Progressing→Ready"
            )
            return "waiting"

        for state in TimeoutSampler(timeout, sleep, _poll_rotation_state):
            if state == "error":
                raise UnexpectedBehaviour(
                    f"CephCluster {self.ceph_cluster_name} entered Error during "
                    f"CephX key rotation (phase={last_phase}"
                    f"{f' message={last_message}' if last_message else ''})"
                )
            if state == "ready":
                return True

        raise UnexpectedBehaviour(
            f"CephCluster {self.ceph_cluster_name} did not complete CephX "
            f"rotation within {timeout}s "
            f"(seen_progressing={seen_progressing}, last phase={last_phase}, "
            f"message={last_message})"
        )

    def wait_for_storagecluster_reconciliation(self, timeout=600, sleep=10):
        """
        Wait until StorageCluster reconciliation completes after a CephX patch.

        During key rotation reconcile the StorageCluster may temporarily enter
        ``Error`` (or ``Progressing``). Keep polling until phase is ``Ready``.

        Args:
            timeout (int): Max seconds to wait (default 10 minutes).
            sleep (int): Seconds between polls (default 10).

        Returns:
            bool: True when StorageCluster is Ready.

        Raises:
            UnexpectedBehaviour: If Ready is not reached within *timeout*.
        """
        storage_cluster = StorageCluster(
            resource_name=constants.DEFAULT_CLUSTERNAME,
            namespace=self.namespace,
        )
        log.info(
            "Waiting for StorageCluster reconciliation after CephX key rotation "
            f"(timeout={timeout}s, poll every {sleep}s; Error phase is tolerated)"
        )

        def _is_ready():
            storage_cluster.reload_data()
            phase = (storage_cluster.data.get("status") or {}).get("phase")
            message = (storage_cluster.data.get("status") or {}).get("message", "")
            if phase == constants.STATUS_READY:
                log.info("StorageCluster reconciliation completed; phase=Ready")
                return True
            log.info(
                f"StorageCluster phase={phase}"
                f"{f' message={message}' if message else ''}; "
                "waiting for Ready (temporary Error during reconcile is expected)"
            )
            return False

        for ready in TimeoutSampler(timeout, sleep, _is_ready):
            if ready:
                return True

        storage_cluster.reload_data()
        phase = (storage_cluster.data.get("status") or {}).get("phase")
        message = (storage_cluster.data.get("status") or {}).get("message", "")
        raise UnexpectedBehaviour(
            "StorageCluster did not reach Ready within "
            f"{timeout}s after CephX key rotation "
            f"(last phase={phase}, message={message})"
        )

    def verify_csi_node_plugin_logs_for_auth_errors(self, since_time=None):
        """
        Collect AUTH_BAD_KEY lines from CSI RBD node plugin logs.

        Returns:
            list: Matching log lines (may be non-empty when old CSI keys are deleted).
        """
        from ocs_ci.helpers.helpers import get_event_line_datetime

        matches = []
        for csi_pod in self.get_csi_node_plugin_pods():
            logs = get_pod_logs(
                pod_name=csi_pod.name,
                namespace=self.namespace,
            )
            if since_time:
                filtered = []
                for line in logs.splitlines():
                    log_time = get_event_line_datetime(line)
                    if log_time and log_time > since_time:
                        filtered.append(line)
                logs = "\n".join(filtered)
            for line in logs.splitlines():
                if constants.AUTH_BAD_KEY_LOG in line:
                    matches.append(f"{csi_pod.name}: {line.strip()}")
        if matches:
            log.warning(
                "CSI node plugin AUTH_BAD_KEY log lines:\n" + "\n".join(matches[:10])
            )
        else:
            log.info("No AUTH_BAD_KEY lines found in CSI node plugin logs")
        return matches

    def verify_operator_logs_do_not_contain_warnings(
        self, patterns, since_time=None, require_match=False
    ):
        """Fail if operator logs since *since_time* contain warning-level patterns."""
        from ocs_ci.helpers.helpers import get_logs_rook_ceph_operator

        logs = (
            self.get_operator_logs_since(since_time)
            if since_time
            else get_logs_rook_ceph_operator().splitlines()
        )
        matches = []
        for line in logs:
            lower_line = line.lower()
            if "warning" not in lower_line and " error" not in lower_line:
                continue
            if any(pattern.lower() in lower_line for pattern in patterns):
                matches.append(line)
        if matches and require_match:
            raise UnexpectedBehaviour(
                f"Expected warning patterns in operator logs: {patterns}"
            )
        if matches and not require_match:
            sample = "\n".join(matches[:5])
            raise UnexpectedBehaviour(
                "Unexpected bootstrap deletion warnings in operator logs:\n" f"{sample}"
            )

    def get_csi_node_plugin_pods(self):
        """Return CSI RBD node plugin pods for the cluster namespace."""
        pods = get_pods_having_label(
            constants.CSI_RBDPLUGIN_LABEL, namespace=self.namespace
        )
        if not pods:
            pods = get_pods_having_label(
                constants.CSI_RBDPLUGIN_LABEL_419, namespace=self.namespace
            )
        return [Pod(**pod) for pod in pods]

    def wait_for_pgs_active_clean(self, timeout=600, sleep=15):
        """Wait until all PGs are in active+clean state."""
        from ocs_ci.ocs.cluster import CephCluster

        ceph_cluster = CephCluster()
        log.info("Waiting for all PGs to reach active+clean state")

        for ready in TimeoutSampler(timeout, sleep, ceph_cluster.get_rebalance_status):
            if ready:
                log.info("All PGs are active+clean")
                return True

        raise UnexpectedBehaviour(
            f"PGs did not reach active+clean state within {timeout}s"
        )

    def trigger_cephcluster_reconcile(self):
        """Annotate the CephCluster to trigger a Rook operator reconcile."""
        annotation = f"ocs-ci/reconcile-trigger={int(time.time())}"
        log.info(f"Triggering CephCluster reconcile via annotation {annotation}")
        self.cephcluster_obj.annotate(annotation=annotation)

    def restart_rook_ceph_operator(self):
        """
        Restart the rook-ceph-operator by deleting its pod.

        Returns:
            str: Name of the deleted operator pod.
        """
        operator_pods = get_operator_pods(namespace=self.namespace)
        if not operator_pods:
            raise UnexpectedBehaviour(
                f"rook-ceph-operator pod not found in {self.namespace}"
            )
        operator_pod = operator_pods[0]
        operator_name = operator_pod.name
        log.info(f"Restarting rook-ceph-operator pod {operator_name}")
        operator_pod.delete()
        return operator_name

    def wait_for_rook_ceph_operator_ready(
        self, previous_pod_name=None, timeout=300, sleep=15
    ):
        """
        Wait until rook-ceph-operator is Running after a restart.

        Args:
            previous_pod_name (str): Prior pod name; wait until a new pod is Running.
        """
        log.info(
            "Waiting for rook-ceph-operator to be Running "
            f"(previous pod={previous_pod_name or 'unknown'})"
        )

        def _operator_ready():
            pods = get_operator_pods(namespace=self.namespace)
            if not pods:
                return False
            pod = pods[0]
            phase = pod.data.get("status", {}).get("phase")
            if phase != constants.STATUS_RUNNING:
                return False
            if previous_pod_name and pod.name == previous_pod_name:
                return False
            return True

        for ready in TimeoutSampler(timeout, sleep, _operator_ready):
            if ready:
                operator_pod = get_operator_pods(namespace=self.namespace)[0]
                log.info(f"rook-ceph-operator pod {operator_pod.name} is Running")
                return operator_pod

        raise UnexpectedBehaviour(
            f"rook-ceph-operator did not become Running within {timeout}s"
        )

    def get_operator_logs_since(self, since_time):
        """Return rook-ceph-operator log lines newer than *since_time*."""
        from ocs_ci.helpers.helpers import (
            get_event_line_datetime,
            get_logs_rook_ceph_operator,
        )

        new_logs = []
        for line in get_logs_rook_ceph_operator().splitlines():
            log_time = get_event_line_datetime(line)
            if since_time and log_time and log_time > since_time:
                new_logs.append(line)
        return new_logs

    def verify_operator_no_key_rotation_logs(
        self,
        since_time,
        rotation_patterns=None,
    ):
        """
        Assert rook-ceph-operator did not log CephX key rotation after *since_time*.

        Args:
            since_time (datetime): Only scan operator logs newer than this timestamp.
            rotation_patterns (tuple): Substrings that indicate rotation activity.
        """
        rotation_patterns = (
            rotation_patterns or constants.CEPHX_KEY_ROTATION_OPERATOR_LOG_PATTERNS
        )
        matches = []
        for line in self.get_operator_logs_since(since_time):
            lower_line = line.lower()
            if any(pattern.lower() in lower_line for pattern in rotation_patterns):
                matches.append(line)

        if matches:
            sample = "\n".join(matches[:5])
            raise UnexpectedBehaviour(
                "rook-ceph-operator logged CephX key rotation after re-reconcile:\n"
                f"{sample}"
            )
        log.info(
            "No CephX key rotation messages in rook-ceph-operator logs "
            "after re-reconcile"
        )

    def wait_for_operator_log_pattern(self, pattern, timeout=300, sleep=5, since=None):
        """Wait until *pattern* appears in rook-ceph-operator logs."""
        operator_pods = get_operator_pods(namespace=self.namespace)
        if not operator_pods:
            raise UnexpectedBehaviour("rook-ceph-operator pod not found")
        return wait_for_matching_pattern_in_pod_logs(
            pod_name=operator_pods[0].name,
            pattern=pattern,
            namespace=self.namespace,
            since=since,
            timeout=timeout,
            sleep=sleep,
        )

    def verify_operator_logs_contain_any_pattern(
        self, patterns, since_time=None, require_match=True
    ):
        """Assert operator logs since *since_time* contain at least one *patterns*."""
        from ocs_ci.helpers.helpers import get_logs_rook_ceph_operator

        if since_time:
            logs = self.get_operator_logs_since(since_time)
        else:
            logs = get_logs_rook_ceph_operator().splitlines()

        matches = []
        for line in logs:
            lower_line = line.lower()
            if any(pattern.lower() in lower_line for pattern in patterns):
                matches.append(line)
        if require_match and not matches:
            raise UnexpectedBehaviour(
                f"rook-ceph-operator logs missing expected patterns: {patterns}"
            )
        if matches:
            log.info(f"Found {len(matches)} operator log lines matching {patterns}")
            for line in matches[:5]:
                log.info(f"  operator: {line}")
        return matches

    def wait_for_cephcluster_reconcile_failure(
        self,
        timeout=600,
        sleep=15,
        message_patterns=None,
        operator_log_since=None,
    ):
        """
        Wait until CephCluster reports a reconcile failure.

        Args:
            timeout (int): Max seconds to wait.
            sleep (int): Poll interval in seconds.
            message_patterns (iterable|None): If set, status.message (or a
                failing Ready-condition message) must contain at least one
                pattern (case-insensitive). Without patterns, any non-Ready
                phase matches — which can false-positive on normal
                Progressing reconcile — so callers that inject a specific
                fault should pass patterns for that fault.
            operator_log_since: Optional log marker from
                ``get_last_log_time_date()``. When set with
                *message_patterns*, rook-ceph-operator logs since the marker
                are also accepted as failure evidence while the CephCluster
                is not Ready (status.message can lag behind operator errors).
        """
        patterns = tuple(message_patterns or ())
        log.info(
            "Waiting for CephCluster reconcile failure"
            + (f" matching {patterns}" if patterns else "")
            + (" (also checking rook-ceph-operator logs)" if operator_log_since else "")
        )

        def _message_matches(text):
            if not patterns:
                return True
            lower = (text or "").lower()
            return any(pattern.lower() in lower for pattern in patterns)

        def _reconcile_failed():
            self._reload()
            status = self.cephcluster_obj.data.get("status", {}) or {}
            phase = status.get("phase")
            message = status.get("message", "") or ""
            if phase and phase != constants.STATUS_READY and _message_matches(message):
                log.info(f"CephCluster phase={phase} message={message}")
                return True
            for condition in status.get("conditions", []) or []:
                if (
                    condition.get("type") == "Ready"
                    and condition.get("status") != "True"
                    and _message_matches(condition.get("message", "") or "")
                ):
                    log.info(f"CephCluster Ready condition false: {condition}")
                    return True
            if (
                patterns
                and operator_log_since
                and phase
                and phase != constants.STATUS_READY
            ):
                matches = self.verify_operator_logs_contain_any_pattern(
                    patterns,
                    since_time=operator_log_since,
                    require_match=False,
                )
                if matches:
                    log.info(
                        "CephCluster phase=%s message=%s; OSD rotate failure "
                        "confirmed via rook-ceph-operator logs",
                        phase,
                        message,
                    )
                    return True
            log.debug(
                "CephCluster reconcile failure not matched yet "
                f"(phase={phase}, message={message})"
            )
            return False

        for failed in TimeoutSampler(timeout, sleep, _reconcile_failed):
            if failed:
                return True

        raise UnexpectedBehaviour(
            "CephCluster did not report reconcile failure"
            + (f" matching {patterns}" if patterns else "")
            + f" within {timeout}s"
        )
