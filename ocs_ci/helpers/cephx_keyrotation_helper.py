"""
Helper for CephX authentication key rotation on Rook-managed Ceph clusters.

Rotation is driven by the CephCluster CR ``spec.security.cephx`` fields (Rook
KeyGeneration policy). This is distinct from OSD LUKS / StorageCluster
encryption key rotation (see ``keyrotation_helper.KeyRotation``).

Reference: https://rook.io/docs/rook/latest/Storage-Configuration/Advanced/cephx-key-rotation/
"""

import json
import logging

from ocs_ci.framework import config
from ocs_ci.helpers import helpers
from ocs_ci.helpers.dr_helpers import check_rbd_mirror_running
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    UnexpectedBehaviour,
    UnexpectedDeploymentConfiguration,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod, get_pods_having_label
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)

CEPHX_KEY_IDENTIFIER_ANNOTATION = "cephx-key-identifier"
CEPH_RBD_MIRROR_KIND = "CephRBDMirror"
DEFAULT_CEPH_RBD_MIRROR_NAME = f"{constants.DEFAULT_CLUSTERNAME}-cephrbdmirror"

DAEMON_POD_LABELS = {
    "mon": "app=rook-ceph-mon",
    "mgr": "app=rook-ceph-mgr",
    "mds": constants.MDS_APP_LABEL,
    "rbd_mirror": constants.RBD_MIRROR_APP_LABEL,
}


class CephXKeyRotation:
    """
    Rotate CephX keys via the Rook CephCluster security.cephx configuration.

    Supported rotation targets:
      - ``daemon``: internal daemon keys (OSD, MGR, MDS, RGW, etc.). MON keys
        cannot be rotated (Ceph limitation).
      - ``csi``: CSI driver client keys (affects new PVCs; prior keys may be kept).
      - ``rbdMirrorPeer``: RBD mirror peer token keys.

    Example::

        rotator = CephXKeyRotation()
        rotator.rotate_daemon_keys()
        rotator.wait_for_daemon_rotation()
    """

    KEY_ROTATION_POLICY_KEY_GENERATION = "KeyGeneration"
    KEY_ROTATION_POLICY_DISABLED = "Disabled"

    COMPONENT_DAEMON = "daemon"
    COMPONENT_CSI = "csi"
    COMPONENT_RBD_MIRROR_PEER = "rbdMirrorPeer"
    ROTATION_COMPONENTS = (
        COMPONENT_DAEMON,
        COMPONENT_CSI,
        COMPONENT_RBD_MIRROR_PEER,
    )

    # status.cephx entities updated when daemon rotation completes (mon excluded)
    DAEMON_STATUS_ENTITIES = (
        "admin",
        "mgr",
        "osd",
        "crashCollector",
        "cephExporter",
    )

    def __init__(
        self,
        ceph_cluster_name=None,
        namespace=None,
        cephfilesystem_name=None,
        cephrbdmirror_name=None,
    ):
        """
        Args:
            ceph_cluster_name (str): CephCluster resource name.
            namespace (str): Cluster namespace (default: openshift-storage).
            cephfilesystem_name (str): CephFilesystem resource name.
            cephrbdmirror_name (str): CephRBDMirror resource name.
        """
        self.ceph_cluster_name = ceph_cluster_name or constants.CEPH_CLUSTER_NAME
        self.namespace = namespace or config.ENV_DATA["cluster_namespace"]
        self.cephfilesystem_name = cephfilesystem_name or defaults.CEPHFILESYSTEM_NAME
        self.cephrbdmirror_name = cephrbdmirror_name or DEFAULT_CEPH_RBD_MIRROR_NAME
        self.cephcluster_obj = OCP(
            kind=constants.CEPH_CLUSTER,
            resource_name=self.ceph_cluster_name,
            namespace=self.namespace,
        )
        self._cephfilesystem_obj = None
        self._cephrbdmirror_obj = None

    def _reload(self):
        self.cephcluster_obj.reload_data()

    def _get_cluster_dict(self):
        self._reload()
        return self.cephcluster_obj.data

    def get_spec_cephx(self):
        """Return ``spec.security.cephx`` from the CephCluster (may be empty)."""
        cluster = self._get_cluster_dict()
        return cluster.get("spec", {}).get("security", {}).get("cephx", {}) or {}

    def get_status_cephx(self):
        """Return ``status.cephx`` from the CephCluster (may be empty)."""
        cluster = self._get_cluster_dict()
        return cluster.get("status", {}).get("cephx", {}) or {}

    def get_spec_key_generation(self, component):
        """
        Read configured key generation for a rotation component from spec.

        Args:
            component (str): One of ``daemon``, ``csi``, ``rbdMirrorPeer``.

        Returns:
            int: Configured generation, or 0 if unset.
        """
        self._validate_component(component)
        value = self.get_spec_cephx().get(component, {}).get("keyGeneration", 0)
        return int(value or 0)

    def get_status_key_generation(self, entity):
        """
        Read reported key generation for a status.cephx entity.

        Args:
            entity (str): e.g. ``osd``, ``csi``, ``rbdMirrorPeer``, ``mgr``.

        Returns:
            int: Reported generation, or 0 if unset / unsupported (e.g. mon).
        """
        status_entry = self.get_status_cephx().get(entity) or {}
        if not status_entry:
            return 0
        return int(status_entry.get("keyGeneration", 0) or 0)

    def get_next_key_generation(self, component):
        """
        Return a generation value high enough to trigger rotation.

        Uses max(spec, relevant status) + 1.
        """
        self._validate_component(component)
        current = self.get_spec_key_generation(component)

        if component == self.COMPONENT_DAEMON:
            for entity in self.DAEMON_STATUS_ENTITIES:
                current = max(current, self.get_status_key_generation(entity))
        elif component == self.COMPONENT_CSI:
            current = max(current, self.get_status_key_generation("csi"))
        elif component == self.COMPONENT_RBD_MIRROR_PEER:
            current = max(current, self.get_status_key_generation("rbdMirrorPeer"))

        return current + 1

    def rotate_component_keys(
        self,
        component,
        key_generation=None,
        keep_prior_key_count_max=None,
    ):
        """
        Initiate a one-off CephX key rotation for a CephCluster cephx component.

        Args:
            component (str): ``daemon``, ``csi``, or ``rbdMirrorPeer``.
            key_generation (int): Desired generation (must be > current). Computed
                automatically when omitted.
            keep_prior_key_count_max (int): CSI only — number of prior CSI key
                generations to retain for existing PVC connections.

        Returns:
            int: The key generation written to the CephCluster spec.
        """
        self._validate_component(component)
        if key_generation is None:
            key_generation = self.get_next_key_generation(component)

        component_config = {
            "keyRotationPolicy": self.KEY_ROTATION_POLICY_KEY_GENERATION,
            "keyGeneration": int(key_generation),
        }
        if component == self.COMPONENT_CSI and keep_prior_key_count_max is not None:
            component_config["keepPriorKeyCountMax"] = int(keep_prior_key_count_max)

        patch_ops = self._build_cephx_component_patch_ops(component, component_config)
        log.info(
            "Initiating CephX key rotation for %s (generation=%s) on %s/%s",
            component,
            key_generation,
            self.namespace,
            self.ceph_cluster_name,
        )
        self.cephcluster_obj.patch(
            params=json.dumps(patch_ops),
            format_type="json",
        )
        self._reload()
        return int(key_generation)

    def rotate_daemon_keys(self, key_generation=None):
        """Rotate internal Ceph daemon CephX keys."""
        return self.rotate_component_keys(self.COMPONENT_DAEMON, key_generation)

    def rotate_csi_keys(self, key_generation=None, keep_prior_key_count_max=1):
        """Rotate CSI CephX keys."""
        return self.rotate_component_keys(
            self.COMPONENT_CSI,
            key_generation,
            keep_prior_key_count_max=keep_prior_key_count_max,
        )

    def rotate_rbd_mirror_peer_keys(self, key_generation=None):
        """Rotate RBD mirror peer CephX keys."""
        return self.rotate_component_keys(
            self.COMPONENT_RBD_MIRROR_PEER, key_generation
        )

    def rotate_all_keys(self, keep_prior_key_count_max=1):
        """
        Rotate daemon, CSI, and RBD mirror peer keys in one sequence.

        Returns:
            dict: Component name to generation applied.
        """
        generations = {}
        for component in self.ROTATION_COMPONENTS:
            kwargs = {}
            if component == self.COMPONENT_CSI:
                kwargs["keep_prior_key_count_max"] = keep_prior_key_count_max
            generations[component] = self.rotate_component_keys(component, **kwargs)
        return generations

    def wait_for_daemon_rotation(self, expected_generation, timeout=900, sleep=15):
        """
        Wait until daemon-related ``status.cephx`` entries reach *expected_generation*.

        MON key rotation is not supported and is not checked.
        """
        return self._wait_for_status_entities(
            self.DAEMON_STATUS_ENTITIES,
            expected_generation,
            timeout,
            sleep,
            label="daemon",
        )

    def wait_for_csi_rotation(self, expected_generation, timeout=900, sleep=15):
        """Wait until ``status.cephx.csi.keyGeneration`` matches *expected_generation*."""
        return self._wait_for_status_entities(
            ["csi"],
            expected_generation,
            timeout,
            sleep,
            label="csi",
        )

    def wait_for_rbd_mirror_peer_rotation(
        self, expected_generation, timeout=900, sleep=15
    ):
        """Wait until ``status.cephx.rbdMirrorPeer.keyGeneration`` matches."""
        return self._wait_for_status_entities(
            ["rbdMirrorPeer"],
            expected_generation,
            timeout,
            sleep,
            label="rbdMirrorPeer",
        )

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

    def wait_for_rbd_mirror_daemon_rotation(
        self, expected_generation, timeout=900, sleep=15
    ):
        """Wait until CephRBDMirror ``status.cephx.daemon.keyGeneration`` matches."""
        return self._wait_for_cr_daemon_rotation(
            self._get_cephrbdmirror_obj(),
            expected_generation,
            timeout,
            sleep,
            label=f"CephRBDMirror/{self.cephrbdmirror_name}",
        )

    def wait_for_rook_daemon_rotation(
        self, expected_generation, timeout=1200, sleep=15
    ):
        """
        Wait for TC-01 daemon rotation: CephCluster mgr (and mon when supported),
        CephFilesystem MDS, and CephRBDMirror daemon status.
        """
        self.wait_for_daemon_rotation(expected_generation, timeout, sleep)
        self.wait_for_mon_rotation(expected_generation, timeout, sleep)
        self.wait_for_filesystem_daemon_rotation(expected_generation, timeout, sleep)
        self.wait_for_rbd_mirror_daemon_rotation(expected_generation, timeout, sleep)

    def wait_for_rotation(self, component, expected_generation, timeout=900, sleep=15):
        """
        Wait for rotation completion for a cephx component.

        Args:
            component (str): ``daemon``, ``csi``, or ``rbdMirrorPeer``.
            expected_generation (int): Generation requested in spec.
        """
        self._validate_component(component)
        if component == self.COMPONENT_DAEMON:
            return self.wait_for_daemon_rotation(expected_generation, timeout, sleep)
        if component == self.COMPONENT_CSI:
            return self.wait_for_csi_rotation(expected_generation, timeout, sleep)
        return self.wait_for_rbd_mirror_peer_rotation(
            expected_generation, timeout, sleep
        )

    def get_auth_key(self, entity, toolbox_pod=None):
        """
        Return the current CephX key for *entity* from the toolbox.

        Args:
            entity (str): e.g. ``client.csi-rbd-node``, ``client.admin``.
            toolbox_pod: Optional rook-ceph-tools pod object.

        Returns:
            str: Key string, or empty string if the entity does not exist.
        """
        toolbox = toolbox_pod or get_ceph_tools_pod()
        try:
            result = toolbox.exec_ceph_cmd(f"ceph auth get-key {entity}")
        except CommandFailed as exc:
            if "ENOENT" in str(exc):
                log.warning("Ceph auth entity %s not found", entity)
                return ""
            raise
        if isinstance(result, dict):
            return result.get("key", "")
        return str(result).strip()

    def capture_auth_keys(self, entities, toolbox_pod=None):
        """
        Snapshot CephX keys for a list of entities (for before/after comparison).

        Returns:
            dict: entity name to key string.
        """
        keys = {}
        for entity in entities:
            keys[entity] = self.get_auth_key(entity, toolbox_pod=toolbox_pod)
        return keys

    @retry(UnexpectedBehaviour, tries=10, delay=30)
    def is_mon_key_rotation_supported(self):
        """Return True when CephCluster reports ``status.cephx.mon``."""
        mon_status = self.get_status_cephx().get("mon") or {}
        return bool(mon_status)

    def get_filesystem_status_cephx(self):
        """Return ``status.cephx`` from the CephFilesystem CR."""
        fs_obj = self._get_cephfilesystem_obj()
        fs_obj.reload_data()
        return fs_obj.data.get("status", {}).get("cephx", {}) or {}

    def get_filesystem_daemon_key_generation(self):
        """Return ``status.cephx.daemon.keyGeneration`` from CephFilesystem."""
        daemon_status = self.get_filesystem_status_cephx().get("daemon") or {}
        return int(daemon_status.get("keyGeneration", 0) or 0)

    def get_rbd_mirror_status_cephx(self):
        """Return ``status.cephx`` from the CephRBDMirror CR."""
        mirror_obj = self._get_cephrbdmirror_obj()
        mirror_obj.reload_data()
        return mirror_obj.data.get("status", {}).get("cephx", {}) or {}

    def get_rbd_mirror_daemon_key_generation(self):
        """Return ``status.cephx.daemon.keyGeneration`` from CephRBDMirror."""
        daemon_status = self.get_rbd_mirror_status_cephx().get("daemon") or {}
        return int(daemon_status.get("keyGeneration", 0) or 0)

    def ensure_daemon_key_rotation_enabled(self, key_generation=1):
        """
        Ensure ``spec.security.cephx.daemon`` uses KeyGeneration policy.

        Args:
            key_generation (int): Minimum desired generation in spec.

        Returns:
            int: Generation configured in spec.
        """
        current = self.get_spec_key_generation(self.COMPONENT_DAEMON)
        if (
            self.get_spec_cephx()
            .get(self.COMPONENT_DAEMON, {})
            .get("keyRotationPolicy")
            == self.KEY_ROTATION_POLICY_KEY_GENERATION
            and current >= key_generation
        ):
            log.info(
                "Daemon CephX key rotation already enabled at generation %s",
                current,
            )
            return current

        log.info(
            "Enabling daemon CephX KeyGeneration policy at generation %s",
            key_generation,
        )
        return self.rotate_component_keys(
            self.COMPONENT_DAEMON, key_generation=key_generation
        )

    def ensure_rbd_mirror(self, count=1, timeout=600):
        """
        Ensure an RBD mirror daemon is running (create CephRBDMirror CR if needed).

        Returns:
            str: CephRBDMirror resource name.
        """
        mirror_obj = OCP(
            kind=CEPH_RBD_MIRROR_KIND,
            namespace=self.namespace,
            resource_name=self.cephrbdmirror_name,
        )
        try:
            mirror_obj.get()
            log.info("CephRBDMirror %s already exists", self.cephrbdmirror_name)
        except CommandFailed:
            log.info("Creating CephRBDMirror %s", self.cephrbdmirror_name)
            helpers.create_resource(
                **{
                    "api_version": "ceph.rook.io/v1",
                    "kind": CEPH_RBD_MIRROR_KIND,
                    "namespace": self.namespace,
                    "name": self.cephrbdmirror_name,
                    "spec": {"count": int(count)},
                }
            )

        def _mirror_running():
            try:
                check_rbd_mirror_running(self.namespace)
                return True
            except UnexpectedDeploymentConfiguration:
                return False

        for _ in TimeoutSampler(timeout, 15, _mirror_running):
            log.info("RBD mirror daemon is running")
            break
        return self.cephrbdmirror_name

    def wait_for_cluster_ready(self, timeout=900):
        """Wait until CephCluster and StorageCluster reach Ready phase."""
        cephcluster = OCP(
            kind=constants.CEPH_CLUSTER,
            namespace=self.namespace,
            resource_name=self.ceph_cluster_name,
        )
        log.info("Waiting for CephCluster %s to be Ready", self.ceph_cluster_name)
        cephcluster.wait_for_phase(phase=constants.STATUS_READY, timeout=timeout)

        storage_cluster = OCP(
            kind=constants.STORAGECLUSTER,
            namespace=self.namespace,
            resource_name=constants.DEFAULT_CLUSTERNAME,
        )
        log.info("Waiting for StorageCluster to be Ready")
        storage_cluster.wait_for_phase(phase=constants.STATUS_READY, timeout=timeout)

    def wait_for_rook_daemon_pods_ready(self, timeout=600):
        """Wait until MON, MGR, MDS, and RBD mirror pods are Running."""
        for daemon, label in DAEMON_POD_LABELS.items():
            log.info("Waiting for %s pods (%s) to be Running", daemon, label)

            def _pods_running(lbl=label):
                pods = get_pods_having_label(
                    lbl, namespace=self.namespace, statuses=[constants.STATUS_RUNNING]
                )
                return bool(pods)

            for _ in TimeoutSampler(timeout, 15, _pods_running):
                break

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

    def discover_rook_daemon_auth_entities(self, toolbox_pod=None):
        """
        Discover MON, MGR, MDS, and RBD mirror auth entities for TC-01.

        Returns:
            dict: daemon type to list of entity names.
        """
        mds_prefix = f"mds.{self.cephfilesystem_name}"
        return {
            "mon": self.list_auth_entities("mon.", toolbox_pod=toolbox_pod),
            "mgr": self.list_auth_entities("mgr.", toolbox_pod=toolbox_pod),
            "mds": self.list_auth_entities(mds_prefix, toolbox_pod=toolbox_pod),
            "rbd_mirror": self.list_auth_entities(
                "client.rbd-mirror", toolbox_pod=toolbox_pod
            ),
        }

    def get_auth_caps(self, entity, toolbox_pod=None):
        """
        Return capability map for a Ceph auth entity.

        Returns:
            dict: capability name to value (e.g. mon, mgr, osd).
        """
        toolbox = toolbox_pod or get_ceph_tools_pod()
        try:
            result = toolbox.exec_ceph_cmd(f"ceph auth get {entity}")
        except CommandFailed as exc:
            if "ENOENT" in str(exc):
                log.warning("Ceph auth entity %s not found", entity)
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
        log.info("CephX capabilities unchanged for entities: %s", ", ".join(entities))
        return new_caps

    def capture_daemon_pod_state(self, label):
        """
        Record Running pod names and cephx-key-identifier annotations for *label*.

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
            state[name] = annotations.get(CEPHX_KEY_IDENTIFIER_ANNOTATION)
        return state

    def capture_all_daemon_pod_states(self):
        """Capture pod state for MON, MGR, MDS, and RBD mirror daemons."""
        return {
            daemon: self.capture_daemon_pod_state(label)
            for daemon, label in DAEMON_POD_LABELS.items()
        }

    def wait_for_pod_restarts(self, before_state, label, timeout=900, sleep=15):
        """
        Wait until all Running pods for *label* have new names or annotations.

        Args:
            before_state (dict): Output of :meth:`capture_daemon_pod_state`.
        """
        log.info(
            "Waiting for pod restarts (label=%s, prior pods=%s)",
            label,
            ", ".join(before_state) or "none",
        )

        def _pods_restarted():
            current = self.capture_daemon_pod_state(label)
            if not current:
                return False
            for pod_name, annotation in current.items():
                if pod_name not in before_state:
                    return True
                if before_state.get(pod_name) != annotation:
                    return True
            return False

        for restarted in TimeoutSampler(timeout, sleep, _pods_restarted):
            if restarted:
                log.info("Pods restarted for label %s", label)
                return self.capture_daemon_pod_state(label)

        raise UnexpectedBehaviour(
            f"Pods with label {label} did not restart within {timeout}s"
        )

    def wait_for_all_daemon_pod_restarts(self, before_states, timeout=900, sleep=15):
        """Wait for MON, MGR, MDS, and RBD mirror pod restarts."""
        after_states = {}
        for daemon, label in DAEMON_POD_LABELS.items():
            after_states[daemon] = self.wait_for_pod_restarts(
                before_states.get(daemon, {}),
                label,
                timeout=timeout,
                sleep=sleep,
            )
        return after_states

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
        unchanged = [
            entity
            for entity in entities
            if old_keys.get(entity) and old_keys[entity] == new_keys.get(entity)
        ]
        if unchanged:
            raise UnexpectedBehaviour(
                f"CephX keys unchanged after rotation for: {', '.join(unchanged)}"
            )
        log.info("CephX keys rotated for entities: %s", ", ".join(entities))
        return new_keys

    def _wait_for_status_entities(
        self, entities, expected_generation, timeout, sleep, label
    ):
        log.info(
            "Waiting for CephX %s rotation to reach generation %s (timeout=%ss)",
            label,
            expected_generation,
            timeout,
        )

        def _entities_ready():
            pending = []
            for entity in entities:
                generation = self.get_status_key_generation(entity)
                if generation < expected_generation:
                    pending.append(f"{entity}={generation}")
            if pending:
                log.debug(
                    "CephX %s rotation pending for: %s (want >= %s)",
                    label,
                    ", ".join(pending),
                    expected_generation,
                )
                return False
            return True

        for ready in TimeoutSampler(timeout, sleep, _entities_ready):
            if ready:
                log.info(
                    "CephX %s rotation reached generation %s",
                    label,
                    expected_generation,
                )
                return True

        raise UnexpectedBehaviour(
            f"CephX {label} rotation did not reach generation {expected_generation} "
            f"within {timeout}s"
        )

    def _build_cephx_component_patch_ops(self, component, component_config):
        cluster = self._get_cluster_dict()
        security = cluster.get("spec", {}).get("security")
        spec_cephx = (security or {}).get("cephx") or {}
        ops = []

        if security is None:
            ops.append(
                {
                    "op": "add",
                    "path": "/spec/security",
                    "value": {"cephx": {component: component_config}},
                }
            )
            return ops

        if not spec_cephx:
            ops.append(
                {
                    "op": "add",
                    "path": "/spec/security/cephx",
                    "value": {component: component_config},
                }
            )
            return ops

        if component in spec_cephx:
            ops.append(
                {
                    "op": "replace",
                    "path": f"/spec/security/cephx/{component}",
                    "value": component_config,
                }
            )
        else:
            ops.append(
                {
                    "op": "add",
                    "path": f"/spec/security/cephx/{component}",
                    "value": component_config,
                }
            )
        return ops

    def _get_cephfilesystem_obj(self):
        if self._cephfilesystem_obj is None:
            self._cephfilesystem_obj = OCP(
                kind=constants.CEPHFILESYSTEM,
                resource_name=self.cephfilesystem_name,
                namespace=self.namespace,
            )
        return self._cephfilesystem_obj

    def _get_cephrbdmirror_obj(self):
        if self._cephrbdmirror_obj is None:
            self._cephrbdmirror_obj = OCP(
                kind=CEPH_RBD_MIRROR_KIND,
                resource_name=self.cephrbdmirror_name,
                namespace=self.namespace,
            )
        return self._cephrbdmirror_obj

    def _get_auth_entities_dict(self, toolbox_pod=None):
        toolbox = toolbox_pod or get_ceph_tools_pod()
        result = toolbox.exec_ceph_cmd("ceph auth ls")
        if isinstance(result, dict):
            return result
        raise UnexpectedBehaviour("Unexpected output from 'ceph auth ls'")

    def _wait_for_cr_daemon_rotation(
        self, cr_obj, expected_generation, timeout, sleep, label
    ):
        log.info(
            "Waiting for CephX daemon rotation on %s to reach generation %s",
            label,
            expected_generation,
        )

        def _daemon_ready():
            cr_obj.reload_data()
            cephx = cr_obj.data.get("status", {}).get("cephx", {}) or {}
            generation = int((cephx.get("daemon") or {}).get("keyGeneration", 0) or 0)
            if generation < expected_generation:
                log.debug(
                    "%s daemon keyGeneration=%s (want >= %s)",
                    label,
                    generation,
                    expected_generation,
                )
                return False
            return True

        for ready in TimeoutSampler(timeout, sleep, _daemon_ready):
            if ready:
                log.info(
                    "CephX daemon rotation on %s reached generation %s",
                    label,
                    expected_generation,
                )
                return True

        raise UnexpectedBehaviour(
            f"CephX daemon rotation on {label} did not reach generation "
            f"{expected_generation} within {timeout}s"
        )

    @staticmethod
    def _validate_component(component):
        if component not in CephXKeyRotation.ROTATION_COMPONENTS:
            raise ValueError(
                f"Invalid cephx component '{component}'. "
                f"Expected one of: {', '.join(CephXKeyRotation.ROTATION_COMPONENTS)}"
            )
