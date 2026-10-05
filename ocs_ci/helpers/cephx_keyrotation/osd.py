"""OSD CephX helpers: deployment cephx-status, lockbox, auth inject/restore."""

import json
import logging
import re
import shlex

from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed, UnexpectedBehaviour
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import (
    get_operator_pods,
    get_osd_deployments,
    get_osd_pods,
    get_osd_pods_having_ids,
    get_pod_logs,
)
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


class CephXOSDHelper:
    """OSD deployment, lockbox, and OSD auth failure/recovery helpers."""

    def wait_for_osd_rotation(self, expected_generation, timeout=900, sleep=15):
        """Wait until ``status.cephx.osd.keyGeneration`` reaches *expected_generation*."""
        return self._wait_for_status_entities(
            ["osd"],
            expected_generation,
            timeout,
            sleep,
            label="osd",
        )

    def _discover_osd_auth_entities(self, toolbox_pod=None):
        """Discover OSD auth entities, falling back to ``ceph osd dump``."""
        entities = self.list_auth_entities("osd.", toolbox_pod)
        if entities:
            return entities

        toolbox = toolbox_pod or self.get_ceph_cli_pod()
        osd_dump = toolbox.exec_ceph_cmd("ceph osd dump")
        discovered = []
        for osd in osd_dump.get("osds", []):
            osd_id = osd.get("osd")
            if osd_id is None:
                continue
            entity = f"osd.{osd_id}"
            if self._auth_entity_exists(entity, toolbox_pod):
                discovered.append(entity)
        return sorted(discovered, key=lambda name: int(name.split(".", 1)[1]))

    def discover_osd_auth_entities(self, toolbox_pod=None):
        """Return sorted OSD auth entity names (e.g. osd.0, osd.1)."""
        return self._discover_osd_auth_entities(toolbox_pod)

    def capture_osd_deployment_cephx_status(self):
        """
        Snapshot ``cephx-status`` deployment template annotations for OSDs.

        Returns:
            dict: deployment name to parsed CephxStatus JSON (may be empty).
        """
        statuses = {}
        for deployment in get_osd_deployments(namespace=self.namespace):
            deployment_data = deployment.get()
            annotation = (
                deployment_data.get("spec", {})
                .get("template", {})
                .get("metadata", {})
                .get("annotations", {})
                .get(constants.CEPHX_STATUS_ANNOTATION)
            )
            if annotation:
                statuses[deployment.name] = json.loads(annotation)
            else:
                statuses[deployment.name] = {}
        return statuses

    def clear_osd_deployment_cephx_status_annotations(self):
        """
        Remove ``cephx-status`` from OSD deployment templates.

        Simulates brownfield OSD deployments that pre-date cephx rotation support.
        """
        annotation_key = constants.CEPHX_STATUS_ANNOTATION
        cleared = []
        for deployment in get_osd_deployments(namespace=self.namespace):
            deployment_data = deployment.get()
            annotations = (
                deployment_data.get("spec", {})
                .get("template", {})
                .get("metadata", {})
                .get("annotations", {})
                or {}
            )
            if annotation_key not in annotations:
                continue
            patch_ops = [
                {
                    "op": "remove",
                    "path": (
                        "/spec/template/metadata/annotations/" f"{annotation_key}"
                    ),
                }
            ]
            deployment.ocp.patch(
                resource_name=deployment.name,
                params=json.dumps(patch_ops),
                format_type="json",
            )
            cleared.append(deployment.name)
        if cleared:
            log.info(
                "Cleared cephx-status annotation from OSD deployments: "
                f"{', '.join(cleared)}"
            )
        return cleared

    def restore_osd_deployment_cephx_status_annotations(self, statuses):
        """
        Restore OSD deployment ``cephx-status`` annotations from a snapshot.

        Args:
            statuses (dict): deployment name to parsed CephxStatus JSON (may be
                empty). Empty values remove the annotation.
        """
        annotation_key = constants.CEPHX_STATUS_ANNOTATION
        restored = []
        for deployment in get_osd_deployments(namespace=self.namespace):
            if deployment.name not in statuses:
                continue
            status = statuses[deployment.name] or {}
            deployment_data = deployment.get()
            annotations = (
                deployment_data.get("spec", {})
                .get("template", {})
                .get("metadata", {})
                .get("annotations", {})
                or {}
            )
            path = f"/spec/template/metadata/annotations/{annotation_key}"
            if not status:
                if annotation_key not in annotations:
                    continue
                patch_ops = [{"op": "remove", "path": path}]
            elif annotation_key in annotations:
                patch_ops = [
                    {"op": "replace", "path": path, "value": json.dumps(status)}
                ]
            else:
                patch_ops = [{"op": "add", "path": path, "value": json.dumps(status)}]
            deployment.ocp.patch(
                resource_name=deployment.name,
                params=json.dumps(patch_ops),
                format_type="json",
            )
            restored.append(deployment.name)
        if restored:
            log.info(
                "Restored cephx-status annotation on OSD deployments: "
                f"{', '.join(restored)}"
            )
        return restored

    def assert_osd_deployments_have_empty_cephx_status(self):
        """Assert all OSD deployments lack populated cephx-status annotations."""
        statuses = self.capture_osd_deployment_cephx_status()
        assert statuses, "No OSD deployments found for cephx-status verification"
        populated = {name: status for name, status in statuses.items() if status}
        if populated:
            raise UnexpectedBehaviour(
                "Expected empty cephx-status on brownfield OSD deployments; "
                f"populated: {populated}"
            )
        log.info("All OSD deployments have empty cephx-status annotations")

    def assert_all_osd_deployments_cephx_status_at_generation(
        self, expected_generation
    ):
        """Assert every OSD deployment cephx-status reached *expected_generation*."""
        statuses = self.capture_osd_deployment_cephx_status()
        assert statuses, "No OSD deployments found for cephx-status verification"
        behind = {
            name: int(status.get("keyGeneration", 0) or 0)
            for name, status in statuses.items()
            if int(status.get("keyGeneration", 0) or 0) < expected_generation
        }
        if behind:
            raise UnexpectedBehaviour(
                f"OSD deployments below cephx-status keyGeneration "
                f"{expected_generation}: {behind}"
            )
        log.info(
            f"All OSD deployments report cephx-status keyGeneration "
            f">= {expected_generation}"
        )

    def assert_osd_deployment_cephx_status_unchanged_for(
        self, deployment_names, baseline_status
    ):
        """Assert cephx-status for *deployment_names* matches *baseline_status*."""
        current = self.capture_osd_deployment_cephx_status()
        changed = {
            name: {
                "before": baseline_status.get(name),
                "after": current.get(name),
            }
            for name in deployment_names
            if baseline_status.get(name) != current.get(name)
        }
        if changed:
            raise UnexpectedBehaviour(
                "cephx-status changed for OSD deployments that should be "
                f"checkpoint-frozen: {changed}"
            )
        log.info(
            "cephx-status unchanged for checkpoint OSD deployments: "
            f"{', '.join(deployment_names)}"
        )

    def assert_auth_keys_unchanged_for(self, baseline_keys, entities=None):
        """Assert a subset of auth keys did not change."""
        entities = entities or list(baseline_keys.keys())
        self.assert_auth_keys_unchanged(
            baseline_keys,
            entities=entities,
            context="for checkpoint OSDs after operator restart",
        )

    def get_disk_based_encrypted_osd_deployments(self):
        """Return encrypted OSD deployments backed by host/disk store."""
        return {
            name: info
            for name, info in self.capture_encrypted_osd_deployments().items()
            if info.get("store_type") == "host"
        }

    def assert_lockbox_auth_keys_present(self, entities, toolbox_pod=None):
        """Assert lockbox auth entities still exist in the Ceph auth store."""
        missing = [
            entity
            for entity in entities
            if not self.get_auth_key(entity, toolbox_pod=toolbox_pod)
        ]
        if missing:
            raise UnexpectedBehaviour(
                f"Lockbox auth keys missing after rotation disruption: "
                f"{', '.join(missing)}"
            )
        log.info(f"Lockbox auth keys present for: {', '.join(entities)}")

    def get_osd_auth_entity_for_deployment(self, deployment_name):
        """Map an OSD deployment name to its ``osd.<id>`` auth entity."""
        deployment = OCP(
            kind=constants.DEPLOYMENT,
            namespace=self.namespace,
            resource_name=deployment_name,
        )
        osd_id = (
            deployment.get().get("metadata", {}).get("labels", {}).get("ceph-osd-id")
        )
        if osd_id is None:
            raise UnexpectedBehaviour(
                f"OSD deployment {deployment_name} missing ceph-osd-id label"
            )
        return f"osd.{osd_id}"

    def map_osd_deployments_to_auth_entities(self, deployment_names):
        """Return ``osd.<id>`` auth entities for OSD deployment names."""
        return [
            self.get_osd_auth_entity_for_deployment(name) for name in deployment_names
        ]

    def break_mon_quorum_during_lockbox_rotation(self, mons_to_stop=2, timeout=600):
        """
        Start daemon rotation and break mon quorum while lockbox rotation runs.

        Returns:
            tuple: ``(target_generation, scaled_mon_deployments)`` where
            ``scaled_mon_deployments`` is the list of mon deployment names
            scaled down for later restoration.
        """
        from ocs_ci.helpers.helpers import get_last_log_time_date

        operator_log_marker = get_last_log_time_date()
        target_generation = self.rotate_daemon_keys()

        def _lockbox_rotation_started():
            logs = self.get_operator_logs_since(operator_log_marker)
            return any(constants.OSD_LOCKBOX_OPERATOR_LOG in line for line in logs)

        for started in TimeoutSampler(timeout, 5, _lockbox_rotation_started):
            if started:
                log.info("Encrypted OSD lockbox rotation started; breaking mon quorum")
                scaled = self.break_mon_quorum(mons_to_stop=mons_to_stop)
                return target_generation, scaled

        raise UnexpectedBehaviour(f"Lockbox rotation did not start within {timeout}s")

    def verify_osd_lockbox_init_container_disruption_logs(self, osd_pods=None):
        """
        Verify encrypted OSD init containers logged failures during disruption.

        At least one encrypted OSD pod should report a failure pattern in an
        init container involved in lockbox key load.
        """
        osd_pods = osd_pods or self.get_encrypted_osd_pods()
        if not osd_pods:
            raise UnexpectedBehaviour(
                "No encrypted OSD pods found for lockbox disruption logs"
            )

        init_containers = list(constants.OSD_CEPHX_INIT_CONTAINER_NAMES) + [
            constants.OSD_ACTIVATE_INIT_CONTAINER
        ]
        failure_patterns = constants.CEPHX_LOCKBOX_ROTATION_FAILURE_LOG_PATTERNS
        pods_with_failures = []

        for osd_pod in osd_pods:
            for container_name in init_containers:
                try:
                    logs = get_pod_logs(
                        pod_name=osd_pod.name,
                        container=container_name,
                        namespace=self.namespace,
                    )
                except CommandFailed:
                    continue
                lower_logs = logs.lower()
                if any(pattern in lower_logs for pattern in failure_patterns):
                    pods_with_failures.append((osd_pod.name, container_name))
                    log.info(
                        f"OSD pod {osd_pod.name} init container {container_name} "
                        "logged lockbox rotation disruption"
                    )
                    break

        if not pods_with_failures:
            raise UnexpectedBehaviour(
                "No encrypted OSD init containers logged lockbox rotation failures"
            )

    def kill_operator_during_partial_osd_rotation(
        self,
        baseline_cephx_status,
        min_rotated,
        timeout=900,
        poll_interval=5,
    ):
        """
        Trigger OSD rotation and kill the operator after partial completion.

        Returns:
            tuple: (target_generation, list of deployment names rotated before kill)
        """
        target_generation = self.rotate_daemon_keys(wait_for_rotation=False)
        operator_pods = get_operator_pods(namespace=self.namespace)
        if not operator_pods:
            raise UnexpectedBehaviour("rook-ceph-operator pod not found")
        operator_pod = operator_pods[0]
        operator_ocp = OCP(kind=constants.POD, namespace=self.namespace)
        rotated_before_kill = []

        log.info(
            f"Waiting to kill operator after >= {min_rotated} OSD cephx-status "
            "updates and before all OSDs complete"
        )

        def _partial_rotation_ready():
            nonlocal rotated_before_kill
            current = self.capture_osd_deployment_cephx_status()
            rotated_before_kill = [
                deployment_name
                for deployment_name, prior in baseline_cephx_status.items()
                if current.get(deployment_name) != prior
                and int(current.get(deployment_name, {}).get("keyGeneration", 0) or 0)
                >= target_generation
            ]
            total = len(baseline_cephx_status)
            if (
                len(rotated_before_kill) >= min_rotated
                and len(rotated_before_kill) < total
            ):
                log.info(
                    f"Killing rook-ceph-operator after partial OSD rotation; "
                    f"rotated={rotated_before_kill}"
                )
                operator_ocp.delete(
                    resource_name=operator_pod.name, force=True, wait=False
                )
                return True
            return False

        for ready in TimeoutSampler(timeout, poll_interval, _partial_rotation_ready):
            if ready:
                return target_generation, list(rotated_before_kill)

        raise UnexpectedBehaviour(
            f"Partial OSD rotation checkpoint not reached within {timeout}s "
            f"(min_rotated={min_rotated})"
        )

    def capture_osd_store_types(self):
        """
        Classify OSD deployments by backing store (PVC-based vs host-based).

        Returns:
            dict: deployment name to ``pvc`` or ``host``.
        """
        store_types = {}
        for deployment in get_osd_deployments(namespace=self.namespace):
            deployment_data = deployment.get()
            labels = deployment_data.get("metadata", {}).get("labels", {})
            if labels.get(constants.OSD_STORE_LABEL):
                store_label = labels[constants.OSD_STORE_LABEL]
                store_types[deployment.name] = (
                    "pvc" if "pvc" in store_label.lower() else "host"
                )
                continue

            volumes = (
                deployment_data.get("spec", {})
                .get("template", {})
                .get("spec", {})
                .get("volumes", [])
            )
            if any(volume.get("persistentVolumeClaim") for volume in volumes):
                store_types[deployment.name] = "pvc"
            else:
                store_types[deployment.name] = "host"
        return store_types

    @staticmethod
    def get_osd_cephx_init_container_name(pod_data):
        """Return the CephX key init container name from an OSD pod spec."""
        init_containers = pod_data.get("spec", {}).get("initContainers", []) or []
        container_names = {container.get("name") for container in init_containers}
        for name in constants.OSD_CEPHX_INIT_CONTAINER_NAMES:
            if name in container_names:
                return name
        return None

    def verify_osd_cephx_init_container_logs(self, osd_pods=None):
        """
        Verify CephX init containers completed and loaded keys from the mon cluster.

        Args:
            osd_pods (list): Optional OSD pod objects; discovered when omitted.

        Raises:
            UnexpectedBehaviour: When init container is missing or logs indicate failure.
        """
        osd_pods = osd_pods or get_osd_pods(namespace=self.namespace)
        for osd_pod in osd_pods:
            pod_data = osd_pod.get()
            container_name = self.get_osd_cephx_init_container_name(pod_data)
            if not container_name:
                raise UnexpectedBehaviour(
                    f"OSD pod {osd_pod.name} is missing a CephX key init container"
                )
            logs = get_pod_logs(
                pod_name=osd_pod.name,
                container=container_name,
                namespace=self.namespace,
            )
            log.info(
                f"OSD pod {osd_pod.name} init container {container_name} logs:\n"
                f"{logs}"
            )
            if constants.OSD_CEPHX_INIT_SUCCESS_LOG not in logs:
                raise UnexpectedBehaviour(
                    f"OSD pod {osd_pod.name} init container {container_name} "
                    f"did not report successful CephX key load"
                )
            if constants.OSD_CEPHX_GET_OR_CREATE_LOG not in logs:
                raise UnexpectedBehaviour(
                    f"OSD pod {osd_pod.name} init container {container_name} "
                    f"did not use ceph auth get-or-create"
                )

    def assert_osd_deployment_cephx_status_updated(
        self, before_status, expected_generation
    ):
        """Assert OSD deployment cephx-status annotations reached *expected_generation*."""
        after_status = self.capture_osd_deployment_cephx_status()
        assert after_status, "No OSD deployments found for cephx-status verification"
        for deployment_name, prior in before_status.items():
            current = after_status.get(deployment_name, {})
            current_generation = int(current.get("keyGeneration", 0) or 0)
            assert current_generation >= expected_generation, (
                f"OSD deployment {deployment_name} cephx-status keyGeneration "
                f"{current_generation} did not reach {expected_generation}"
            )
            if prior.get("keyGeneration") is not None:
                assert current_generation >= int(prior.get("keyGeneration", 0) or 0), (
                    f"OSD deployment {deployment_name} cephx-status keyGeneration "
                    "did not increase after rotation"
                )
            if prior.get("keyCephVersion"):
                assert current.get("keyCephVersion"), (
                    f"OSD deployment {deployment_name} missing keyCephVersion "
                    "in cephx-status annotation"
                )

    def assert_osd_deployment_cephx_status_unchanged(self, baseline):
        """Assert OSD deployment ``cephx-status`` annotations did not change."""
        current = self.capture_osd_deployment_cephx_status()
        changed = {
            deployment_name: {
                "before": baseline[deployment_name],
                "after": current.get(deployment_name),
            }
            for deployment_name in baseline
            if baseline[deployment_name] != current.get(deployment_name)
        }
        if changed:
            raise UnexpectedBehaviour(
                "OSD deployment cephx-status annotations changed after "
                f"re-reconcile: {changed}"
            )
        log.info("OSD deployment cephx-status annotations unchanged")

    def set_osd_out(self, osd_id):
        """Mark an OSD out via ``ceph osd out``."""
        toolbox = self.get_ceph_cli_pod()
        toolbox.exec_cmd_on_pod(
            f"ceph osd out osd.{osd_id}",
            out_yaml_format=False,
        )
        log.info(f"Marked osd.{osd_id} out")

    def set_osd_in(self, osd_id):
        """Mark an OSD back in via ``ceph osd in``."""
        toolbox = self.get_ceph_cli_pod()
        toolbox.exec_cmd_on_pod(
            f"ceph osd in osd.{osd_id}",
            out_yaml_format=False,
        )
        log.info(f"Marked osd.{osd_id} in")

    def restore_osd_and_wait_for_recovery(self, osd_id, timeout=1500, sleep=15):
        """
        Mark an intentionally out OSD back in and wait for full recovery.

        Args:
            osd_id (int): OSD id previously marked out.
            timeout (int): Max seconds to wait for PGs and Ready state.
            sleep (int): Poll interval in seconds.
        """
        log.info(
            f"Restoring osd.{osd_id} and waiting for cluster recovery "
            f"(timeout={timeout}s)"
        )
        self.set_osd_in(osd_id)
        self.wait_for_cluster_fully_recovered(timeout=timeout, sleep=sleep)

    def wait_for_pgs_not_clean(self, timeout=300, sleep=15):
        """Wait until not all PGs are active+clean."""
        from ocs_ci.ocs.cluster import CephCluster

        ceph_cluster = CephCluster()
        log.info("Waiting for PGs to leave active+clean state")

        def _pgs_not_clean():
            return not ceph_cluster.get_rebalance_status()

        for ready in TimeoutSampler(timeout, sleep, _pgs_not_clean):
            if ready:
                log.info("PGs are not fully active+clean")
                return True

        raise UnexpectedBehaviour(
            f"PGs remained active+clean within {timeout}s after inducing degradation"
        )

    def delete_auth_entity(self, entity, toolbox_pod=None):
        """Delete a Ceph auth entity from the auth store."""
        toolbox = toolbox_pod or self.get_ceph_cli_pod()
        toolbox.exec_cmd_on_pod(
            f"ceph auth del {entity}",
            out_yaml_format=False,
        )
        log.info(f"Deleted Ceph auth entity {entity}")

    def get_osd_keyring_key_from_pod(self, osd_id):
        """
        Read the OSD CephX key from the running OSD pod keyring file.

        Args:
            osd_id (int): OSD id.

        Returns:
            str: Base64 CephX key for ``osd.<id>``.
        """
        # get_osd_pod_id returns string labels; keep public callers on int osd_id.
        osd_pods = get_osd_pods_having_ids([str(osd_id)])
        if not osd_pods:
            raise UnexpectedBehaviour(f"No running OSD pod found for osd.{osd_id}")
        osd_pod = osd_pods[0]
        keyring = osd_pod.exec_cmd_on_pod(
            f"cat /var/lib/ceph/osd/ceph-{osd_id}/keyring",
            out_yaml_format=False,
            container_name="osd",
        )
        match = re.search(r"key\s*=\s*(\S+)", keyring or "")
        if not match:
            raise UnexpectedBehaviour(
                f"Could not parse key from osd.{osd_id} pod keyring: {keyring!r}"
            )
        return match.group(1)

    def restore_osd_auth_entity_from_pod_keyring(self, entity, toolbox_pod=None):
        """
        Recreate a deleted OSD auth entity using the key from the OSD pod.

        Needed because Rook ``ceph auth rotate`` fails when the entity is missing
        and does not recreate it from the on-disk keyring.
        """
        if not entity.startswith("osd."):
            raise UnexpectedBehaviour(
                f"restore_osd_auth_entity_from_pod_keyring expects osd.* entity, "
                f"got {entity}"
            )
        osd_id = int(entity.split(".")[-1])
        key = self.get_osd_keyring_key_from_pod(osd_id)
        toolbox = toolbox_pod or self.get_ceph_cli_pod()
        # Pipe a minimal keyring into ceph auth add (entity must exist for rotate).
        # oc rsh does not invoke a shell, so compound printf|ceph needs bash -c.
        # Pass the CephX key via secrets so it is masked in logs.
        keyring_script = (
            f"printf '%s\\n' '[osd.{osd_id}]' ' key = {key}' | "
            f"ceph auth add osd.{osd_id} osd 'allow *' mon 'allow profile osd' "
            f"mgr 'allow profile osd' -i -"
        )
        toolbox.exec_cmd_on_pod(
            command=f"bash -c {shlex.quote(keyring_script)}",
            out_yaml_format=False,
            secrets=[key],
        )
        log.info(f"Restored Ceph auth entity {entity} from OSD pod keyring")

    def ensure_osd_auth_entity_restored(self, entity, toolbox_pod=None):
        """Restore *entity* from the OSD pod keyring if it is missing."""
        if self.auth_entity_exists(entity, toolbox_pod=toolbox_pod):
            log.info(f"Ceph auth entity {entity} already present; no restore needed")
            return False
        self.restore_osd_auth_entity_from_pod_keyring(entity, toolbox_pod=toolbox_pod)
        return True

    def wait_for_partial_osd_key_rotation(self, pre_keys, timeout=900, sleep=5):
        """
        Wait until at least one OSD auth key differs from *pre_keys*.

        Returns the key snapshot from the poll that first observed a change so
        callers can inject failure against still-pending OSDs without a second
        capture race on small/fast clusters.
        """
        entities = list(pre_keys.keys())

        def _partial_rotation():
            current_keys = self.capture_auth_keys(entities)
            changed = [
                entity
                for entity in entities
                if pre_keys.get(entity)
                and pre_keys.get(entity) != current_keys.get(entity)
            ]
            pending = [
                entity
                for entity in entities
                if pre_keys.get(entity)
                and pre_keys.get(entity) == current_keys.get(entity)
            ]
            # Prefer a true partial snapshot; skip all-rotated samples so the
            # next poll can catch mid-rotation on fast clusters.
            if changed and pending:
                return current_keys
            if changed and not pending:
                log.debug(
                    "OSD rotation already complete for all entities; "
                    "continuing to look for a mid-rotation window"
                )
                return False
            return False

        for current_keys in TimeoutSampler(timeout, sleep, _partial_rotation):
            if current_keys:
                log.info(
                    "Detected partial OSD key rotation "
                    f"(changed={[e for e in entities if pre_keys.get(e) != current_keys.get(e)]}, "
                    f"pending={[e for e in entities if pre_keys.get(e) == current_keys.get(e)]})"
                )
                return current_keys

        raise UnexpectedBehaviour(
            f"No partial OSD key rotation window within {timeout}s "
            "(rotation may have completed too quickly to inject failure)"
        )

    def inject_osd_auth_rotation_failure(self, pre_keys, timeout=900):
        """
        During OSD rotation, delete auth for an OSD that has not rotated yet.

        Returns:
            tuple: ``(failed_entity, operator_log_marker)`` where the marker is
            captured immediately before the auth delete so callers can search
            operator logs for the OSD rotate failure without matching earlier
            admin-rotate restart noise from the same rotation.
        """
        from ocs_ci.helpers.helpers import get_last_log_time_date

        current_keys = self.wait_for_partial_osd_key_rotation(pre_keys, timeout=timeout)
        pending_entities = [
            entity
            for entity in pre_keys
            if pre_keys.get(entity) and pre_keys.get(entity) == current_keys.get(entity)
        ]
        if not pending_entities:
            raise UnexpectedBehaviour(
                "All OSD auth keys rotated before failure could be injected"
            )
        # Delete the next OSD Rook is likely to rotate (first pending), not the
        # last. Deleting the last pending entity lets Rook spend a long time on
        # earlier OSDs/mons before the missing-auth failure surfaces.
        failed_entity = sorted(pending_entities)[0]
        # Record before delete so callers can restore if a later step raises.
        self.last_deleted_osd_auth_entity = failed_entity
        operator_log_marker = get_last_log_time_date()
        self.delete_auth_entity(failed_entity)
        return failed_entity, operator_log_marker

    def discover_lockbox_auth_entities(self, toolbox_pod=None):
        """Return sorted ``client.osd-lockbox.*`` auth entity names."""
        return self.list_auth_entities(constants.OSD_LOCKBOX_AUTH_PREFIX, toolbox_pod)

    def capture_encrypted_osd_deployments(self):
        """
        Return OSD deployments labeled as encrypted.

        Returns:
            dict: deployment name to osd_id and store_type metadata.
        """
        store_types = self.capture_osd_store_types()
        encrypted = {}
        for deployment in get_osd_deployments(namespace=self.namespace):
            deployment_data = deployment.get()
            labels = deployment_data.get("metadata", {}).get("labels", {})
            if labels.get(constants.OSD_ENCRYPTED_LABEL) != "true":
                continue
            encrypted[deployment.name] = {
                "osd_id": labels.get("ceph-osd-id"),
                "store_type": store_types.get(deployment.name, "unknown"),
            }
        return encrypted

    def assert_encrypted_osd_labels(self, encrypted_deployments):
        """Assert encrypted OSD deployments carry ``encrypted=true``."""
        for deployment_name in encrypted_deployments:
            deployment = OCP(
                kind=constants.DEPLOYMENT,
                namespace=self.namespace,
                resource_name=deployment_name,
            )
            labels = deployment.get().get("metadata", {}).get("labels", {})
            assert (
                labels.get(constants.OSD_ENCRYPTED_LABEL) == "true"
            ), f"OSD deployment {deployment_name} is not labeled encrypted=true"

    @staticmethod
    def _get_pod_env_value(pod_data, env_name):
        """Read an environment variable from pod init/main container specs."""
        for container_key in ("initContainers", "containers"):
            for container in pod_data.get("spec", {}).get(container_key, []) or []:
                for env in container.get("env", []) or []:
                    if env.get("name") == env_name:
                        return env.get("value")
        return None

    def get_encrypted_osd_pods(self):
        """Return Running OSD pod objects for encrypted deployments."""
        encrypted_ids = {
            info["osd_id"]
            for info in self.capture_encrypted_osd_deployments().values()
            if info.get("osd_id") is not None
        }
        if not encrypted_ids:
            return []

        encrypted_pods = []
        for osd_pod in get_osd_pods(namespace=self.namespace):
            osd_id = (
                osd_pod.get().get("metadata", {}).get("labels", {}).get("ceph-osd-id")
            )
            if osd_id in encrypted_ids:
                encrypted_pods.append(osd_pod)
        return encrypted_pods

    @staticmethod
    def lockbox_entity_for_uuid(osd_uuid):
        """Return the lockbox auth entity name for an OSD UUID."""
        return f"{constants.OSD_LOCKBOX_AUTH_PREFIX}{osd_uuid}"

    def map_lockbox_entities_to_osd_uuids(self, lockbox_entities):
        """
        Map lockbox auth entities to OSD UUIDs.

        Returns:
            dict: entity name to UUID string.
        """
        prefix = constants.OSD_LOCKBOX_AUTH_PREFIX
        mapping = {}
        for entity in lockbox_entities:
            if not entity.startswith(prefix):
                continue
            mapping[entity] = entity[len(prefix) :]
        return mapping

    def verify_osd_activate_lockbox_logs(self, osd_pods=None):
        """
        Verify the activate init container loaded lockbox keys for encrypted OSDs.

        Args:
            osd_pods (list): Encrypted OSD pods; discovered when omitted.
        """
        osd_pods = osd_pods or self.get_encrypted_osd_pods()
        if not osd_pods:
            raise UnexpectedBehaviour("No encrypted OSD pods found for activate logs")

        for osd_pod in osd_pods:
            pod_data = osd_pod.get()
            logs = get_pod_logs(
                pod_name=osd_pod.name,
                container=constants.OSD_ACTIVATE_INIT_CONTAINER,
                namespace=self.namespace,
            )
            osd_uuid = self._get_pod_env_value(pod_data, constants.ROOK_OSD_UUID_ENV)
            log.info(
                f"OSD pod {osd_pod.name} (uuid={osd_uuid}) activate container logs:\n"
                f"{logs}"
            )
            if constants.OSD_LOCKBOX_INIT_SUCCESS_LOG not in logs:
                raise UnexpectedBehaviour(
                    f"OSD pod {osd_pod.name} activate container did not report "
                    "successful lockbox key load"
                )
            if constants.OSD_LOCKBOX_GET_OR_CREATE_LOG not in logs:
                raise UnexpectedBehaviour(
                    f"OSD pod {osd_pod.name} activate container did not use "
                    "ceph auth get-or-create for lockbox key"
                )

    def verify_operator_lockbox_rotation_logs(self, expected_count, since_time=None):
        """
        Verify rook-ceph-operator logged lockbox key rotation for encrypted OSDs.

        Args:
            expected_count (int): Minimum number of lockbox rotation log lines.
            since_time: Optional marker from ``get_last_log_time_date``; when set,
                only logs newer than the marker are considered.
        """
        from ocs_ci.helpers.helpers import get_logs_rook_ceph_operator

        if since_time:
            log_lines = self.get_operator_logs_since(since_time)
        else:
            log_lines = get_logs_rook_ceph_operator().splitlines()
        matches = [
            line for line in log_lines if constants.OSD_LOCKBOX_OPERATOR_LOG in line
        ]
        log.info(
            f"Found {len(matches)} operator log lines for encrypted OSD lockbox "
            f"rotation (expected >= {expected_count})"
        )
        for line in matches:
            log.info(f"  operator: {line}")
        if len(matches) < expected_count:
            raise UnexpectedBehaviour(
                f"Expected at least {expected_count} operator log lines containing "
                f"'{constants.OSD_LOCKBOX_OPERATOR_LOG}', found {len(matches)}"
            )

    def verify_encrypted_osd_pods_running(self, osd_pods=None):
        """Assert encrypted OSD pods are Running with ready containers."""
        osd_pods = osd_pods or self.get_encrypted_osd_pods()
        if not osd_pods:
            raise UnexpectedBehaviour("No encrypted OSD pods found")

        for osd_pod in osd_pods:
            pod_data = osd_pod.get()
            phase = pod_data.get("status", {}).get("phase")
            if phase != constants.STATUS_RUNNING:
                raise UnexpectedBehaviour(
                    f"Encrypted OSD pod {osd_pod.name} is not Running (phase={phase})"
                )
            container_statuses = (
                pod_data.get("status", {}).get("containerStatuses", []) or []
            )
            not_ready = [
                status.get("name")
                for status in container_statuses
                if not status.get("ready")
            ]
            if not_ready:
                raise UnexpectedBehaviour(
                    f"Encrypted OSD pod {osd_pod.name} has containers not ready: "
                    f"{', '.join(not_ready)}"
                )
