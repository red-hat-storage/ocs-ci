"""Ceph auth entity/key/caps helpers for CephX rotation."""

import logging
import re

from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed, UnexpectedBehaviour
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import (
    get_pod_logs,
)
from ocs_ci.utility.retry import retry

log = logging.getLogger(__name__)


class CephXAuthHelper:
    """Auth store discovery, key snapshots, and capability checks."""

    @staticmethod
    def _extract_key_type_from_auth_entry(auth_entry):
        if not isinstance(auth_entry, dict):
            return None
        for field in ("key_type", "keyType", "type"):
            value = auth_entry.get(field)
            if value:
                return str(value).lower()
        return None

    def get_auth_entity_key_type(self, entity, toolbox_pod=None):
        """Return the CephX key type for *entity* when exposed by Ceph."""
        toolbox = toolbox_pod or self.get_ceph_cli_pod()
        try:
            result = toolbox.exec_cmd_on_pod(
                f"ceph auth get {entity} --format json",
                out_yaml_format=True,
            )
        except CommandFailed as exc:
            if "ENOENT" in str(exc):
                log.warning(f"Ceph auth entity {entity} not found")
                return None
            raise

        if isinstance(result, list) and result:
            result = result[0]
        key_type = self._extract_key_type_from_auth_entry(result)
        if key_type:
            return key_type

        text = toolbox.exec_cmd_on_pod(
            f"ceph auth get {entity}",
            out_yaml_format=False,
        )
        match = re.search(r"key[_\s-]*type\s*[=:]\s*(\S+)", text, re.IGNORECASE)
        if match:
            return match.group(1).lower()

        auth_listing = toolbox.exec_cmd_on_pod(
            "ceph auth ls",
            out_yaml_format=False,
        )
        entity_pattern = re.compile(
            rf"^{re.escape(entity)}\b.*?(?:key[_\s-]*type\s*[=:]\s*(\S+))",
            re.IGNORECASE | re.MULTILINE | re.DOTALL,
        )
        listing_match = entity_pattern.search(auth_listing)
        if listing_match:
            return listing_match.group(1).lower()
        return None

    def assert_auth_entities_key_type(
        self, entities, expected_key_type, toolbox_pod=None
    ):
        """Assert Ceph auth entities use the requested key type."""
        expected = expected_key_type.lower()
        mismatched = []
        unknown = []
        for entity in entities:
            actual = self.get_auth_entity_key_type(entity, toolbox_pod)
            if not actual:
                unknown.append(entity)
                continue
            if actual != expected:
                mismatched.append(f"{entity}={actual}")
            else:
                log.info(f"{entity} uses key type {actual}")

        if mismatched:
            raise UnexpectedBehaviour(
                f"Entities not using key type {expected}: {', '.join(mismatched)}"
            )

        insecure = self.get_insecure_service_key_type_entities(toolbox_pod)
        insecure_for_entities = [
            f"{entity}={key_type}"
            for entity, key_type in insecure
            if entity in entities
        ]
        if insecure_for_entities:
            raise UnexpectedBehaviour(
                "Entities still reported with insecure key types in health detail: "
                f"{', '.join(insecure_for_entities)}"
            )

        if unknown:
            log.warning(
                "Could not read key type from ceph auth for: "
                f"{', '.join(unknown)}; relying on health detail checks"
            )
        log.info(f"Verified key type {expected} for entities: {', '.join(entities)}")

    def verify_pods_no_auth_bad_key(self, pods, tail=500):
        """
        Assert pod logs do not contain AUTH_BAD_KEY authentication failures.

        Args:
            pods: Iterable of Pod objects, pod name strings, or pod dicts.
            tail (int): Number of log lines to scan.
        """
        for pod in pods:
            if isinstance(pod, str):
                pod_name = pod
                namespace = self.namespace
                container = None
            elif hasattr(pod, "name"):
                pod_name = pod.name
                namespace = pod.namespace
                containers = pod.data.get("spec", {}).get("containers", [])
                container = containers[0]["name"] if containers else None
            else:
                pod_name = pod["metadata"]["name"]
                namespace = pod["metadata"]["namespace"]
                containers = pod["spec"].get("containers", [])
                container = containers[0]["name"] if containers else None

            auth_errors = get_pod_logs(
                pod_name=pod_name,
                container=container,
                namespace=namespace,
                tail=str(tail),
                grep=constants.AUTH_BAD_KEY_LOG,
                return_empty_string=True,
            )
            if auth_errors and constants.AUTH_BAD_KEY_LOG in auth_errors:
                raise UnexpectedBehaviour(
                    f"AUTH_BAD_KEY errors found in {namespace}/{pod_name} logs: "
                    f"{auth_errors.strip()}"
                )
            log.info(f"No AUTH_BAD_KEY errors in {namespace}/{pod_name} logs")

    @retry(CommandFailed, tries=5, delay=10, backoff=1)
    def get_auth_key(self, entity, toolbox_pod=None):
        """
        Return the current CephX key for *entity* from the toolbox.

        Uses ``ceph auth get-key <entity>`` (not ``ceph auth <entity>``, which is
        invalid). Works for daemon entities (``osd.0``, ``mgr.a``, ``mds.*``,
        shared ``mon.`` / ``mon.a``) and client entities (``client.admin``, CSI
        users, etc.).

        Args:
            entity (str): Ceph auth entity name.
            toolbox_pod: Optional rook-ceph-tools pod object.

        Returns:
            str: Key string, or empty string if the entity does not exist.
        """
        toolbox = toolbox_pod or self.get_ceph_cli_pod()
        try:
            result = toolbox.exec_cmd_on_pod(
                f"ceph auth get-key {entity} --format json",
                out_yaml_format=True,
            )
        except CommandFailed as exc:
            if "ENOENT" in str(exc):
                log.warning(f"Ceph auth entity {entity} not found")
                return ""
            raise
        if isinstance(result, dict):
            return result.get("key", "")
        return str(result).strip()

    @staticmethod
    def log_auth_key_snapshot(label, keys):
        """Log CephX auth keys for a snapshot (before/after rotation)."""
        log.info(f"CephX auth keys {label}:")
        for entity in sorted(keys):
            key = keys[entity]
            log.info(f"  {entity}: {key if key else '<empty>'}")

    @staticmethod
    def log_auth_key_comparison(old_keys, new_keys):
        """Log per-entity CephX key comparison without exposing key values."""
        log.info("CephX auth key comparison (before vs after rotation):")
        for entity in sorted(set(old_keys) | set(new_keys)):
            old_key = old_keys.get(entity, "")
            new_key = new_keys.get(entity, "")
            if not old_key and not new_key:
                status = "MISSING"
            elif old_key == new_key:
                status = "UNCHANGED"
            else:
                status = "CHANGED"
            log.info(f"  {entity}: {status}")

    def capture_auth_keys(self, entities, toolbox_pod=None, label=None):
        """
        Snapshot CephX keys for a list of entities (for before/after comparison).

        Args:
            label (str): When set, log the captured keys under this label.

        Returns:
            dict: entity name to key string.
        """
        keys = {}
        for entity in entities:
            keys[entity] = self.get_auth_key(entity, toolbox_pod=toolbox_pod)
        if label:
            self.log_auth_key_snapshot(label, keys)
        return keys

    def discover_csi_auth_entities(self, toolbox_pod=None):
        """Return CSI-related ``client.csi*`` auth entities."""
        return [
            entity
            for entity in self.list_auth_entities(toolbox_pod=toolbox_pod)
            if entity.startswith("client.csi")
        ]

    def discover_rbd_mirror_auth_entities(self, toolbox_pod=None):
        """Return RBD mirror related client auth entities."""
        prefixes = (
            "client.rbd-mirror",
            "client.rbd_mirror",
            "client.rbd-mirror-peer",
        )
        entities = []
        for prefix in prefixes:
            entities.extend(self.list_auth_entities(prefix, toolbox_pod))
        return sorted(set(entities))

    def discover_cephclient_auth_entities(self, toolbox_pod=None):
        """Return auth entities associated with CephClient CRs when present."""
        cc_obj = OCP(kind=constants.CEPHCLIENT, namespace=self.namespace)
        try:
            resources = cc_obj.get()
        except CommandFailed as exc:
            log.info(
                "CephClient kind unavailable; skipping CephClient auth discovery "
                f"({exc})"
            )
            return []
        items = resources.get("items", [])
        if not items and resources.get("metadata"):
            items = [resources]

        entities = []
        for item in items:
            name = item.get("metadata", {}).get("name")
            if not name:
                continue
            for candidate in (f"client.{name}", f"client.ceph-{name}"):
                if self._auth_entity_exists(candidate, toolbox_pod):
                    entities.append(candidate)
        return sorted(set(entities))

    def discover_all_rotation_auth_entities(self, toolbox_pod=None):
        """
        Discover auth entities for daemons, CSI, RBD mirror, and CephClients.
        """
        entities = self.flatten_daemon_auth_entities(
            self.discover_rook_daemon_auth_entities(toolbox_pod)
        )
        entities.extend(self.discover_csi_auth_entities(toolbox_pod))
        entities.extend(self.discover_rbd_mirror_auth_entities(toolbox_pod))
        entities.extend(self.discover_cephclient_auth_entities(toolbox_pod))
        return sorted(set(entities))

    def assert_auth_keys_unchanged(
        self,
        old_keys,
        entities=None,
        toolbox_pod=None,
        context="while rotation is Disabled",
    ):
        """Assert CephX auth keys did not change."""
        entities = entities or list(old_keys.keys())
        new_keys = self.capture_auth_keys(entities, toolbox_pod=toolbox_pod)
        self.log_auth_key_comparison(old_keys, new_keys)
        changed = [
            entity
            for entity in entities
            if old_keys.get(entity) != new_keys.get(entity)
        ]
        if changed:
            raise UnexpectedBehaviour(
                f"CephX auth keys changed {context}: {', '.join(changed)}"
            )
        log.info(f"CephX auth keys unchanged for entities: {', '.join(entities)}")

    def list_auth_entities(self, prefix=None, toolbox_pod=None):
        """
        List Ceph auth entities, optionally filtered by prefix.

        Returns:
            list[str]: Sorted entity names.
        """
        auth_dump = self._get_auth_entities_dict(toolbox_pod)
        entities = sorted(auth_dump.keys())
        if prefix:
            entities = [entity for entity in entities if entity.startswith(prefix)]
        return entities

    def _auth_entity_exists(self, entity, toolbox_pod=None):
        """Return True when *entity* is present in the Ceph auth store."""
        toolbox = toolbox_pod or self.get_ceph_cli_pod()
        try:
            toolbox.exec_cmd_on_pod(
                f"ceph auth get-key {entity} --format json",
                out_yaml_format=True,
            )
            return True
        except CommandFailed:
            return False

    def auth_entity_exists(self, entity, toolbox_pod=None):
        """Return True if *entity* exists in the Ceph auth store."""
        return self._auth_entity_exists(entity, toolbox_pod=toolbox_pod)

    def get_auth_caps(self, entity, toolbox_pod=None):
        """
        Return capability map for a Ceph auth entity.

        Returns:
            dict: capability name to value (e.g. mon, mgr, osd).
        """
        toolbox = toolbox_pod or self.get_ceph_cli_pod()
        try:
            result = toolbox.exec_ceph_cmd(f"ceph auth get {entity}")
        except CommandFailed as exc:
            if "ENOENT" in str(exc):
                log.warning(f"Ceph auth entity {entity} not found")
                return {}
            raise
        if isinstance(result, dict):
            return result.get("caps", {}) or {}
        return {}

    def capture_auth_caps(self, entities, toolbox_pod=None):
        """Snapshot auth capabilities for *entities*."""
        return {
            entity: self.get_auth_caps(entity, toolbox_pod=toolbox_pod)
            for entity in entities
        }

    @retry(UnexpectedBehaviour, tries=5, delay=20)
    def verify_auth_caps_unchanged(self, old_caps, entities=None, toolbox_pod=None):
        """Assert capabilities are unchanged after rotation."""
        entities = entities or list(old_caps.keys())
        new_caps = self.capture_auth_caps(entities, toolbox_pod=toolbox_pod)
        changed = [
            entity
            for entity in entities
            if old_caps.get(entity) != new_caps.get(entity)
        ]
        if changed:
            raise UnexpectedBehaviour(
                f"CephX capabilities changed after rotation for: {', '.join(changed)}"
            )
        log.info(f"CephX capabilities unchanged for entities: {', '.join(entities)}")
        return new_caps

    def verify_auth_keys_changed(self, old_keys, entities=None, toolbox_pod=None):
        """
        Assert that keys for *entities* differ from *old_keys* after rotation.

        Args:
            old_keys (dict): Output of :meth:`capture_auth_keys`.
            entities (list): Subset to check (default: all keys in *old_keys*).

        Returns:
            dict: entity to new key mapping.
        """
        entities = entities or list(old_keys.keys())
        new_keys = self.capture_auth_keys(entities, toolbox_pod=toolbox_pod)
        self.log_auth_key_comparison(old_keys, new_keys)
        unchanged = [
            entity
            for entity in entities
            if old_keys.get(entity) and old_keys[entity] == new_keys.get(entity)
        ]
        if unchanged:
            raise UnexpectedBehaviour(
                f"CephX keys unchanged after rotation for: {', '.join(unchanged)}"
            )
        changed = [
            entity
            for entity in entities
            if old_keys.get(entity) != new_keys.get(entity)
        ]
        log.info(
            f"CephX keys rotated for entities: "
            f"{', '.join(changed) if changed else ', '.join(entities)}"
        )
        return new_keys

    @retry(CommandFailed, tries=5, delay=15, backoff=1)
    def _get_auth_entities_dict(self, toolbox_pod=None):
        """
        Return the parsed ``ceph auth ls`` dict from the toolbox.

        Retries on transient toolbox auth failures such as
        ``handle_auth_bad_method`` / RADOS permission denied while mons are
        hunting after key rotation or cluster settle.
        """
        toolbox = toolbox_pod or self.get_ceph_cli_pod()
        result = toolbox.exec_ceph_cmd("ceph auth ls")
        if isinstance(result, dict):
            return result
        raise UnexpectedBehaviour("Unexpected output from 'ceph auth ls'")
