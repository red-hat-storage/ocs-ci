"""CephX key rotation core: StorageCluster/CephCluster spec, status, and patch ops."""

import json
import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import (
    get_ceph_tools_pod,
)
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


class CephXKeyRotationCore:
    """Spec/status/patch and component rotation primitives for CephX."""

    KEY_ROTATION_POLICY_KEY_GENERATION = "KeyGeneration"

    KEY_ROTATION_POLICY_DISABLED = "Disabled"

    DEFAULT_DAEMON_KEY_GENERATION = 2

    COMPONENT_DAEMON = "daemon"

    COMPONENT_CSI = "csi"

    COMPONENT_RBD_MIRROR_PEER = "rbdMirrorPeer"

    CONFIG_KEY_ROOK_DAEMON = "rook_daemon"

    ROTATION_COMPONENTS = (
        COMPONENT_DAEMON,
        COMPONENT_CSI,
        COMPONENT_RBD_MIRROR_PEER,
    )

    STORAGECLUSTER_CEPHX_MANAGED_RESOURCES = {
        COMPONENT_DAEMON: "cephCluster",
        COMPONENT_CSI: "cephCluster",
        COMPONENT_RBD_MIRROR_PEER: "cephCluster",
    }

    CEPHX_KEY_CONFIG_ALIASES = {
        CONFIG_KEY_ROOK_DAEMON: COMPONENT_DAEMON,
        "daemon": COMPONENT_DAEMON,
    }

    CEPHX_KEY_CONFIG_NAMES = frozenset(
        (*ROTATION_COMPONENTS, *CEPHX_KEY_CONFIG_ALIASES)
    )

    ROOK_DAEMON_STATUS_ENTITIES = (
        constants.CEPHCLUSTER_CEPHX_KEYROTATION_STATUS_ENTITIES
    )

    DAEMON_STATUS_ENTITIES = (
        "admin",
        "mgr",
        "osd",
        "crashCollector",
        "cephExporter",
    )

    CEPHX_STATUS_GENERATION_ENTITIES = (
        "admin",
        "mgr",
        "osd",
        "mon",
        "csi",
        "rbdMirrorPeer",
        "crashCollector",
        "cephExporter",
    )

    def __init__(
        self,
        ceph_cluster_name=None,
        namespace=None,
        cephfilesystem_name=None,
    ):
        """
        Args:
            ceph_cluster_name (str): CephCluster resource name.
            namespace (str): Cluster namespace (default: openshift-storage).
            cephfilesystem_name (str): CephFilesystem resource name.
        """
        self.ceph_cluster_name = ceph_cluster_name or constants.CEPH_CLUSTER_NAME
        self.namespace = namespace or config.ENV_DATA["cluster_namespace"]
        self.cephfilesystem_name = cephfilesystem_name or defaults.CEPHFILESYSTEM_NAME
        self.cephcluster_obj = OCP(
            kind=constants.CEPH_CLUSTER,
            resource_name=self.ceph_cluster_name,
            namespace=self.namespace,
        )
        self._cephfilesystem_obj = None
        self._storagecluster_obj = None

    @classmethod
    def resolve_cephx_key_config(cls, key):
        """
        Map a ``cephx_keys`` config entry to a rotation component.

        ``rook_daemon`` (preferred) and legacy ``daemon`` both resolve to the
        daemon component (mon/mgr/osd/mds) driven via StorageCluster.
        """
        component = cls.CEPHX_KEY_CONFIG_ALIASES.get(key, key)
        cls._validate_component(component)
        return component

    def get_ceph_cli_pod(self):
        """Return the rook-ceph-tools pod used for Ceph CLI commands."""
        return get_ceph_tools_pod(namespace=self.namespace)

    def _reload(self):
        self.cephcluster_obj.reload_data()

    def _get_cluster_dict(self):
        self._reload()
        return self.cephcluster_obj.data

    def _get_storage_cluster_dict(self):
        if self._storagecluster_obj is None:
            self._storagecluster_obj = OCP(
                kind=constants.STORAGECLUSTER,
                resource_name=constants.DEFAULT_CLUSTERNAME,
                namespace=self.namespace,
            )
        self._storagecluster_obj.reload_data()
        return self._storagecluster_obj.data

    def get_storagecluster_managed_resource(self, managed_resource):
        """Return ``spec.managedResources.<managed_resource>`` from StorageCluster."""
        sc = self._get_storage_cluster_dict()
        managed = (sc.get("spec", {}) or {}).get("managedResources") or {}
        return managed.get(managed_resource) or {}

    def get_storagecluster_managed_cephcluster(self):
        """Return ``spec.managedResources.cephCluster`` from the StorageCluster."""
        return self.get_storagecluster_managed_resource("cephCluster")

    def get_storagecluster_managed_cephrbdmirror(self):
        """Return ``spec.managedResources.cephRBDMirror`` from the StorageCluster."""
        return self.get_storagecluster_managed_resource("cephRBDMirror")

    def get_cephcluster_reconcile_strategy(self):
        """Return StorageCluster ``managedResources.cephCluster.reconcileStrategy``."""
        return self.get_storagecluster_managed_cephcluster().get("reconcileStrategy")

    def patch_storagecluster_cephcluster_reconcile_strategy(self, strategy):
        """Patch ``managedResources.cephCluster.reconcileStrategy`` on StorageCluster."""
        cc_spec = self.get_storagecluster_managed_cephcluster()
        patch_ops = []
        strategy_path = "/spec/managedResources/cephCluster/reconcileStrategy"

        if not cc_spec:
            patch_ops.append(
                {
                    "op": "add",
                    "path": "/spec/managedResources/cephCluster",
                    "value": {"reconcileStrategy": strategy},
                }
            )
        elif "reconcileStrategy" in cc_spec:
            patch_ops.append(
                {"op": "replace", "path": strategy_path, "value": strategy}
            )
        else:
            patch_ops.append({"op": "add", "path": strategy_path, "value": strategy})

        log.info(
            "Patching StorageCluster managedResources.cephCluster.reconcileStrategy "
            f"to {strategy}"
        )
        sc_obj = OCP(
            kind=constants.STORAGECLUSTER,
            resource_name=constants.DEFAULT_CLUSTERNAME,
            namespace=self.namespace,
        )
        sc_obj.patch(params=json.dumps(patch_ops), format_type="json")
        self._storagecluster_obj = None

    @classmethod
    def uses_storagecluster_for_cephx(cls, component):
        """Return True when *component* CephX rotation is StorageCluster-driven."""
        return component in cls.STORAGECLUSTER_CEPHX_MANAGED_RESOURCES

    def get_storagecluster_cephx_managed_resource_name(self, component):
        """Return managedResources key used for *component* CephX rotation."""
        self._validate_component(component)
        managed = self.STORAGECLUSTER_CEPHX_MANAGED_RESOURCES.get(component)
        if not managed:
            raise UnexpectedBehaviour(
                f"Component {component} does not use StorageCluster for CephX rotation"
            )
        return managed

    def get_storagecluster_spec_cephx(self, component=None):
        """
        Return StorageCluster ``managedResources.<resource>.security.cephx``.

        Args:
            component (str): Rotation component; defaults to daemon (cephCluster).
        """
        component = component or self.COMPONENT_DAEMON
        managed_name = self.get_storagecluster_cephx_managed_resource_name(component)
        managed_spec = self.get_storagecluster_managed_resource(managed_name)
        security = managed_spec.get("security") or {}
        return security.get("cephx") or {}

    def get_storagecluster_component_spec(self, component):
        """Return StorageCluster security.cephx.<component> (may be empty)."""
        return self.get_storagecluster_spec_cephx(component).get(component) or {}

    def _get_storagecluster_ocp(self):
        """Return an OCP handle for the StorageCluster resource."""
        return OCP(
            kind=constants.STORAGECLUSTER,
            resource_name=constants.DEFAULT_CLUSTERNAME,
            namespace=self.namespace,
        )

    def patch_storagecluster_cephx_component(self, component, component_config):
        """
        Patch StorageCluster managedResources.cephCluster.security.cephx.<component>.

        Applies to daemon, csi, and rbdMirrorPeer. Preserves sibling fields such
        as allowedCiphers and other cephx components. Writes the provided
        component_config as-is (including an explicit lower keyGeneration for
        negative tests).
        """
        managed_name = self.get_storagecluster_cephx_managed_resource_name(component)
        patch_ops = self._build_storagecluster_cephx_component_patch_ops(
            component, component_config
        )
        log.info(
            "Patching StorageCluster managedResources."
            f"{managed_name}.security.cephx.{component} to {component_config}"
        )
        self._get_storagecluster_ocp().patch(
            params=json.dumps(patch_ops), format_type="json"
        )
        self._storagecluster_obj = None

    def get_status_cephx(self):
        """
        Return ``status.cephx`` from the CephCluster (may be empty).

        Status verification for daemon, CSI, and rbdMirrorPeer generations is
        still read from CephCluster even when rotation was triggered via
        StorageCluster.
        """
        cluster = self._get_cluster_dict()
        return cluster.get("status", {}).get("cephx", {}) or {}

    def get_spec_key_generation(self, component):
        """
        Read configured key generation for a rotation component from
        StorageCluster ``managedResources.cephCluster.security.cephx``.

        Args:
            component (str): One of ``daemon``, ``csi``, ``rbdMirrorPeer``.

        Returns:
            int: Configured generation, or 0 if unset.
        """
        self._validate_component(component)
        value = self.get_storagecluster_component_spec(component).get(
            "keyGeneration", 0
        )
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

        Uses max(spec, relevant status, DESIRED_CEPHX_KEY_GEN) + 1 so a bare
        ``rotate_*_keys()`` call advances past the operator desired baseline
        (writing only the baseline does not rotate CephCluster).
        """
        self._validate_component(component)
        current = self.get_spec_key_generation(component)

        if component == self.COMPONENT_DAEMON:
            for entity in self.DAEMON_STATUS_ENTITIES:
                current = max(current, self.get_status_key_generation(entity))
            # MDS (and other CR-backed daemon) generation lives on CephFilesystem,
            # not CephCluster.status.cephx; fold it in to avoid no-op rotations.
            current = max(current, self.get_filesystem_daemon_key_generation())
            current = max(current, self.get_desired_cephx_key_gen())
        elif component == self.COMPONENT_CSI:
            current = max(current, self.get_status_key_generation("csi"))
        elif component == self.COMPONENT_RBD_MIRROR_PEER:
            current = max(current, self.get_status_key_generation("rbdMirrorPeer"))

        return current + 1

    def get_component_status_key_generation(self, component):
        """
        Return the highest reported status keyGeneration for *component*.

        Used to decide whether a StorageCluster write will trigger an actual
        CephX rotation (Progressing) versus a no-op / policy-only update.
        """
        self._validate_component(component)
        if component == self.COMPONENT_DAEMON:
            current = 0
            for entity in self.ROOK_DAEMON_STATUS_ENTITIES:
                current = max(current, self.get_status_key_generation(entity))
            return max(current, self.get_filesystem_daemon_key_generation())
        if component == self.COMPONENT_CSI:
            return self.get_status_key_generation("csi")
        return self.get_status_key_generation("rbdMirrorPeer")

    def get_desired_cephx_key_gen(self):
        """
        Return ocs-operator ``DESIRED_CEPHX_KEY_GEN``, or the framework default.

        Greenfield clusters already reconcile to this baseline. Patching
        StorageCluster ``keyGeneration`` to the same value does not make
        CephCluster enter Progressing.
        """
        try:
            deploy = OCP(
                kind=constants.DEPLOYMENT,
                namespace=self.namespace,
                resource_name="ocs-operator",
            ).get()
            containers = (
                deploy.get("spec", {})
                .get("template", {})
                .get("spec", {})
                .get("containers")
                or []
            )
            for item in (containers[0].get("env") or []) if containers else []:
                if item.get("name") == constants.DESIRED_CEPHX_KEY_GEN_ENV:
                    return int(item.get("value") or 0) or (
                        constants.DEFAULT_DESIRED_CEPHX_KEY_GEN
                    )
        except Exception as exc:
            log.debug(
                "Could not read %s from ocs-operator: %s",
                constants.DESIRED_CEPHX_KEY_GEN_ENV,
                exc,
            )
        return constants.DEFAULT_DESIRED_CEPHX_KEY_GEN

    def will_storagecluster_key_generation_trigger_rotation(
        self, component, key_generation
    ):
        """
        Return True when writing *key_generation* should start a CephX rotation.

        CephCluster Progressing is expected only when the written generation is
        greater than the current StorageCluster spec keyGeneration, the current
        status keyGeneration, and the ocs-operator DESIRED_CEPHX_KEY_GEN baseline.

        Writing the desired baseline (commonly 2) onto StorageCluster is a no-op
        for CephCluster reconcile — keys are already at that desired level even
        when status.keyGeneration still reports 1.
        """
        self._validate_component(component)
        key_generation = int(key_generation)
        pre_spec = self.get_spec_key_generation(component)
        pre_status = self.get_component_status_key_generation(component)
        desired_baseline = self.get_desired_cephx_key_gen()
        threshold = max(pre_spec, pre_status, desired_baseline)
        will_rotate = key_generation > threshold
        log.info(
            "CephX rotation trigger check for %s: written=%s "
            "pre_spec=%s pre_status=%s desired_baseline=%s threshold=%s "
            "will_rotate=%s",
            component,
            key_generation,
            pre_spec,
            pre_status,
            desired_baseline,
            threshold,
            will_rotate,
        )
        return will_rotate

    def rotate_component_keys(
        self,
        component,
        key_generation=None,
        keep_prior_key_count_max=None,
        wait_for_rotation=True,
    ):
        """
        Initiate a one-off CephX key rotation for a cephx component.

        All components patch StorageCluster
        ``managedResources.cephCluster.security.cephx.<component>``.

        Waits for CephCluster Progressing→Ready only when *key_generation*
        will actually trigger rotation (above StorageCluster spec, status,
        and DESIRED_CEPHX_KEY_GEN). Policy enable / same-as-desired writes
        skip that wait so setup paths do not hang on Ready forever.

        Args:
            component (str): ``daemon``, ``csi``, or ``rbdMirrorPeer``.
            key_generation (int): Desired generation. When omitted, computed as
                max(spec, status) + 1. Explicit values (including lower than
                current) are written as-is for negative testing.
            keep_prior_key_count_max (int): CSI only — number of prior CSI key
                generations to retain for existing PVC connections.
            wait_for_rotation (bool): When False, patch only and return without
                waiting for CephCluster/StorageCluster Ready. Use for mid-
                rotation fault injection.

        Returns:
            int: The key generation written to the component spec.
        """
        self._validate_component(component)
        if key_generation is None:
            key_generation = self.get_next_key_generation(component)
        key_generation = int(key_generation)
        will_rotate = self.will_storagecluster_key_generation_trigger_rotation(
            component, key_generation
        )

        # Preserve sibling fields (e.g. keyType) already set on StorageCluster.
        component_config = dict(self.get_storagecluster_component_spec(component))
        component_config["keyRotationPolicy"] = self.KEY_ROTATION_POLICY_KEY_GENERATION
        component_config["keyGeneration"] = key_generation
        if component == self.COMPONENT_CSI and keep_prior_key_count_max is not None:
            component_config["keepPriorKeyCountMax"] = int(keep_prior_key_count_max)

        managed_name = self.get_storagecluster_cephx_managed_resource_name(component)
        log.info(
            f"Initiating CephX key rotation for {component} "
            f"(generation={key_generation}) via StorageCluster "
            f"managedResources.{managed_name} "
            f"{self.namespace}/{constants.DEFAULT_CLUSTERNAME}"
        )
        self.patch_storagecluster_cephx_component(component, component_config)
        if not wait_for_rotation:
            log.info(
                f"Triggered {component} rotation generation={key_generation} "
                "without waiting for Ready (async)"
            )
            return key_generation
        if will_rotate:
            log.info(
                f"Generation {key_generation} for {component} should trigger "
                "rotation; waiting for CephCluster Progressing→Ready"
            )
            # Daemon rotation restarts MON/MGR/OSD/MDS serially; encrypted
            # multi-OSD clusters commonly need >15m to reach Ready again.
            self.wait_for_cephcluster_rotation(timeout=1500, sleep=15)
        else:
            log.info(
                f"Generation {key_generation} for {component} does not exceed "
                "spec/status/DESIRED_CEPHX_KEY_GEN; skipping CephCluster "
                "Progressing wait"
            )
        self.wait_for_storagecluster_reconciliation(timeout=600, sleep=10)
        return key_generation

    def rotate_daemon_keys(self, key_generation=None, wait_for_rotation=True):
        """Rotate internal Ceph daemon CephX keys."""
        return self.rotate_component_keys(
            self.COMPONENT_DAEMON,
            key_generation,
            wait_for_rotation=wait_for_rotation,
        )

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

    def wait_for_rotation(self, component, expected_generation, timeout=900, sleep=15):
        """
        Wait for rotation completion for a cephx component.

        Args:
            component (str): ``daemon``, ``csi``, or ``rbdMirrorPeer``.
            expected_generation (int): Generation requested in spec.
        """
        self._validate_component(component)
        if component == self.COMPONENT_DAEMON:
            return self.wait_for_rook_daemon_rotation(
                expected_generation, timeout, sleep
            )
        if component == self.COMPONENT_CSI:
            return self.wait_for_csi_rotation(expected_generation, timeout, sleep)
        return self.wait_for_rbd_mirror_peer_rotation(
            expected_generation, timeout, sleep
        )

    def wait_for_all_key_rotations(self, generations, timeout=1500, sleep=15):
        """
        Wait for daemon, CSI, and RBD mirror peer key rotations to complete.

        Args:
            generations (dict): Component name to generation from
                :meth:`rotate_all_keys`.
            timeout (int): Timeout in seconds for each wait phase.
            sleep (int): Poll interval in seconds.
        """
        self.wait_for_rook_daemon_rotation(
            generations[self.COMPONENT_DAEMON], timeout, sleep
        )
        self.wait_for_csi_rotation(generations[self.COMPONENT_CSI], timeout, sleep)
        self.wait_for_rbd_mirror_peer_rotation(
            generations[self.COMPONENT_RBD_MIRROR_PEER], timeout, sleep
        )
        self.wait_for_pgs_active_clean(timeout=timeout, sleep=sleep)
        self.wait_for_cluster_ready(timeout=timeout)

    def get_spec_rotation_policy(self, component):
        """Return configured keyRotationPolicy for a cephx component."""
        self._validate_component(component)
        return (
            self.get_storagecluster_component_spec(component).get("keyRotationPolicy")
            or ""
        )

    def is_rotation_policy_disabled(self, component):
        """Return True when *component* rotation policy is Disabled or unset."""
        policy = self.get_spec_rotation_policy(component)
        return policy in ("", self.KEY_ROTATION_POLICY_DISABLED)

    def disable_component_key_rotation(self, component):
        """Set ``keyRotationPolicy: Disabled`` for a cephx component on StorageCluster."""
        self._validate_component(component)
        component_spec = dict(self.get_storagecluster_component_spec(component))

        if component_spec.get("keyRotationPolicy") == self.KEY_ROTATION_POLICY_DISABLED:
            log.info(f"CephX key rotation already Disabled for {component}")
            return

        component_spec["keyRotationPolicy"] = self.KEY_ROTATION_POLICY_DISABLED
        log.info(f"Disabling CephX key rotation policy for {component}")
        self.patch_storagecluster_cephx_component(component, component_spec)

    def ensure_key_rotation_disabled(self):
        """Ensure daemon, CSI, and RBD mirror peer rotation policies are Disabled."""
        for component in self.ROTATION_COMPONENTS:
            self.disable_component_key_rotation(component)

    def assert_key_rotation_disabled(self):
        """Assert all cephx rotation components use Disabled policy."""
        enabled = [
            component
            for component in self.ROTATION_COMPONENTS
            if not self.is_rotation_policy_disabled(component)
        ]
        if enabled:
            raise UnexpectedBehaviour(
                "CephX key rotation is not Disabled for: " f"{', '.join(enabled)}"
            )
        log.info(
            "CephX keyRotationPolicy is Disabled for daemon, csi, and rbdMirrorPeer"
        )

    def _wait_for_status_entities(
        self, entities, expected_generation, timeout, sleep, label
    ):
        log.info(
            f"Waiting for CephX {label} rotation to reach generation "
            f"{expected_generation} (timeout={timeout}s)"
        )

        def _entities_ready():
            pending = []
            for entity in entities:
                generation = self.get_status_key_generation(entity)
                if generation < expected_generation:
                    pending.append(f"{entity}={generation}")
            if pending:
                log.debug(
                    f"CephX {label} rotation pending for: {', '.join(pending)} "
                    f"(want >= {expected_generation})"
                )
                return False
            return True

        for ready in TimeoutSampler(timeout, sleep, _entities_ready):
            if ready:
                log.info(
                    f"CephX {label} rotation reached generation {expected_generation}"
                )
                return True

        raise UnexpectedBehaviour(
            f"CephX {label} rotation did not reach generation {expected_generation} "
            f"within {timeout}s"
        )

    def _build_storagecluster_cephx_component_patch_ops(
        self, component, component_config
    ):
        """
        Build JSON patch ops for StorageCluster
        managedResources.<resource>.security.cephx.<component>.
        """
        managed_name = self.get_storagecluster_cephx_managed_resource_name(component)
        managed_spec = self.get_storagecluster_managed_resource(managed_name)
        security = managed_spec.get("security") or {}
        cephx = security.get("cephx") or {}
        base = f"/spec/managedResources/{managed_name}"
        ops = []

        if not managed_spec:
            ops.append(
                {
                    "op": "add",
                    "path": base,
                    "value": {"security": {"cephx": {component: component_config}}},
                }
            )
            return ops

        if not security:
            ops.append(
                {
                    "op": "add",
                    "path": f"{base}/security",
                    "value": {"cephx": {component: component_config}},
                }
            )
            return ops

        if not cephx:
            ops.append(
                {
                    "op": "add",
                    "path": f"{base}/security/cephx",
                    "value": {component: component_config},
                }
            )
            return ops

        component_path = f"{base}/security/cephx/{component}"
        if component in cephx:
            ops.append(
                {
                    "op": "replace",
                    "path": component_path,
                    "value": component_config,
                }
            )
        else:
            ops.append(
                {
                    "op": "add",
                    "path": component_path,
                    "value": component_config,
                }
            )
        return ops

    @staticmethod
    def _validate_component(component):
        if component not in CephXKeyRotationCore.ROTATION_COMPONENTS:
            raise ValueError(
                f"Invalid cephx component '{component}'. "
                f"Expected one of: {', '.join(CephXKeyRotationCore.ROTATION_COMPONENTS)}"
            )
