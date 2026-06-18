"""
Helper for CephX authentication key rotation on Rook-managed Ceph clusters.

Rotation is driven by the CephCluster CR ``spec.security.cephx`` fields (Rook
KeyGeneration policy). This is distinct from OSD LUKS / StorageCluster
encryption key rotation (see ``keyrotation_helper.KeyRotation``).

Reference: https://rook.io/docs/rook/latest/Storage-Configuration/Advanced/cephx-key-rotation/
"""

import json
import logging
import re
import time
from threading import Thread

from ocs_ci.framework import config
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.exceptions import CommandFailed, UnexpectedBehaviour
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import (
    Pod,
    get_ceph_tools_pod,
    get_deployments_having_label,
    get_mon_pods,
    get_operator_pods,
    get_osd_deployments,
    get_osd_pods,
    get_pod_ip,
    get_pod_logs,
    get_pods_having_label,
    wait_for_matching_pattern_in_pod_logs,
)
from ocs_ci.ocs.resources.storage_cluster import StorageCluster
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


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
    CONFIG_KEY_ROOK_DAEMON = "rook_daemon"
    ROTATION_COMPONENTS = (
        COMPONENT_DAEMON,
        COMPONENT_CSI,
        COMPONENT_RBD_MIRROR_PEER,
    )
    CEPHX_KEY_CONFIG_ALIASES = {
        CONFIG_KEY_ROOK_DAEMON: COMPONENT_DAEMON,
        "daemon": COMPONENT_DAEMON,
    }
    CEPHX_KEY_CONFIG_NAMES = frozenset(
        (*ROTATION_COMPONENTS, *CEPHX_KEY_CONFIG_ALIASES)
    )

    # Rook-managed daemons tracked for daemon key rotation (TC-01)
    ROOK_DAEMON_STATUS_ENTITIES = (
        constants.CEPHCLUSTER_CEPHX_KEYROTATION_STATUS_ENTITIES
    )

    # status.cephx entities updated when daemon rotation completes (mon excluded)
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

    @classmethod
    def resolve_cephx_key_config(cls, key):
        """
        Map a ``cephx_keys`` config entry to a rotation component.

        ``rook_daemon`` (preferred) and legacy ``daemon`` both resolve to the
        Rook ``spec.security.cephx.daemon`` component (mon/mgr/osd/mds).
        """
        component = cls.CEPHX_KEY_CONFIG_ALIASES.get(key, key)
        cls._validate_component(component)
        return component

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

    def _reload(self):
        self.cephcluster_obj.reload_data()

    def _get_cluster_dict(self):
        self._reload()
        return self.cephcluster_obj.data

    def get_spec_cephx(self):
        """Return ``spec.security.cephx`` from the CephCluster (may be empty)."""
        cluster = self._get_cluster_dict()
        return cluster.get("spec", {}).get("security", {}).get("cephx", {}) or {}

    def get_spec_security(self):
        """Return ``spec.security`` from the CephCluster (may be empty)."""
        cluster = self._get_cluster_dict()
        return cluster.get("spec", {}).get("security", {}) or {}

    def get_allowed_ciphers(self):
        """Return ``spec.security.cephx.allowedCiphers`` from the CephCluster."""
        return self.get_spec_cephx().get("allowedCiphers")

    def _get_storage_cluster_dict(self):
        if self._storagecluster_obj is None:
            self._storagecluster_obj = OCP(
                kind=constants.STORAGECLUSTER,
                resource_name=constants.DEFAULT_CLUSTERNAME,
                namespace=self.namespace,
            )
        self._storagecluster_obj.reload_data()
        return self._storagecluster_obj.data

    def get_storagecluster_managed_cephcluster(self):
        """Return ``spec.managedResources.cephCluster`` from the StorageCluster."""
        sc = self._get_storage_cluster_dict()
        managed = (sc.get("spec", {}) or {}).get("managedResources") or {}
        return managed.get("cephCluster") or {}

    def get_storagecluster_allowed_ciphers(self):
        """Return allowedCiphers from StorageCluster managedResources.cephCluster."""
        cc_spec = self.get_storagecluster_managed_cephcluster()
        security = cc_spec.get("security") or {}
        cephx = security.get("cephx") or {}
        return cephx.get("allowedCiphers")

    def assert_allowed_ciphers(self, expected, source="cephcluster"):
        """
        Assert allowedCiphers match *expected* on CephCluster or StorageCluster.

        Args:
            expected (list|tuple): Expected cipher names.
            source (str): ``cephcluster`` or ``storagecluster``.
        """
        expected_list = list(expected)
        if source == "storagecluster":
            actual = self.get_storagecluster_allowed_ciphers()
            label = "StorageCluster"
        else:
            actual = self.get_allowed_ciphers()
            label = "CephCluster"
        if actual != expected_list:
            raise UnexpectedBehaviour(
                f"{label} allowedCiphers mismatch: expected {expected_list}, got {actual}"
            )
        log.info(f"{label} allowedCiphers matches expected: {expected_list}")

    def assert_cephcluster_security_populated(self):
        """Assert CephCluster ``spec.security.cephx`` includes allowedCiphers."""
        security = self.get_spec_security()
        if not security:
            raise UnexpectedBehaviour("CephCluster spec.security is empty or missing")
        cephx = security.get("cephx")
        if not cephx:
            raise UnexpectedBehaviour(
                "CephCluster spec.security.cephx is empty or missing"
            )
        if "allowedCiphers" not in cephx:
            raise UnexpectedBehaviour(
                "CephCluster spec.security.cephx.allowedCiphers is missing"
            )
        log.info(
            "CephCluster spec.security.cephx populated: "
            f"allowedCiphers={cephx.get('allowedCiphers')}"
        )

    def wait_for_allowed_ciphers(
        self, expected, timeout=600, sleep=10, source="cephcluster"
    ):
        """Wait until allowedCiphers on CephCluster or StorageCluster match *expected*."""
        expected_list = list(expected)
        log.info(f"Waiting for {source} allowedCiphers={expected_list}")

        def _matches():
            if source == "storagecluster":
                actual = self.get_storagecluster_allowed_ciphers()
            else:
                actual = self.get_allowed_ciphers()
            if actual == expected_list:
                return True
            log.debug(f"{source} allowedCiphers={actual}, want {expected_list}")
            return False

        for matched in TimeoutSampler(timeout, sleep, _matches):
            if matched:
                log.info(f"{source} allowedCiphers reached {expected_list}")
                return True

        raise UnexpectedBehaviour(
            f"Timed out waiting for {source} allowedCiphers={expected_list}"
        )

    def get_spec_key_type(self, component=None):
        """Return ``spec.security.cephx.<component>.keyType`` from the CephCluster."""
        component = component or self.COMPONENT_DAEMON
        return (self.get_spec_cephx().get(component) or {}).get("keyType")

    def patch_cephcluster_key_type(self, key_type, component=None):
        """Set ``spec.security.cephx.<component>.keyType`` on the CephCluster."""
        component = component or self.COMPONENT_DAEMON
        cluster = self._get_cluster_dict()
        security = cluster.get("spec", {}).get("security")
        cephx = (security or {}).get("cephx") or {}
        component_spec = cephx.get(component) or {}
        key_type_path = f"/spec/security/cephx/{component}/keyType"
        patch_ops = []

        if security is None:
            patch_ops.append(
                {
                    "op": "add",
                    "path": "/spec/security",
                    "value": {"cephx": {component: {"keyType": key_type}}},
                }
            )
        elif not cephx:
            patch_ops.append(
                {
                    "op": "add",
                    "path": "/spec/security/cephx",
                    "value": {component: {"keyType": key_type}},
                }
            )
        elif component not in cephx:
            patch_ops.append(
                {
                    "op": "add",
                    "path": f"/spec/security/cephx/{component}",
                    "value": {"keyType": key_type},
                }
            )
        elif "keyType" in component_spec:
            patch_ops.append(
                {
                    "op": "replace",
                    "path": key_type_path,
                    "value": key_type,
                }
            )
        else:
            patch_ops.append(
                {
                    "op": "add",
                    "path": key_type_path,
                    "value": key_type,
                }
            )

        log.info(
            f"Patching CephCluster spec.security.cephx.{component}.keyType to {key_type}"
        )
        self.cephcluster_obj.patch(
            params=json.dumps(patch_ops),
            format_type="json",
        )
        self._reload()

    def remove_cephcluster_key_type(self, component=None):
        """Remove ``spec.security.cephx.<component>.keyType`` from the CephCluster."""
        component = component or self.COMPONENT_DAEMON
        component_spec = self.get_spec_cephx().get(component) or {}
        if "keyType" not in component_spec:
            log.info(
                f"CephCluster spec.security.cephx.{component}.keyType not set; "
                "nothing to remove"
            )
            return

        log.info(f"Removing CephCluster spec.security.cephx.{component}.keyType")
        self.cephcluster_obj.patch(
            params=json.dumps(
                [{"op": "remove", "path": f"/spec/security/cephx/{component}/keyType"}]
            ),
            format_type="json",
        )
        self._reload()

    def wait_for_cephcluster_key_type(
        self, key_type, timeout=300, sleep=10, component=None
    ):
        """Wait until CephCluster ``spec.security.cephx.<component>.keyType`` matches."""
        component = component or self.COMPONENT_DAEMON
        log.info(
            f"Waiting for CephCluster spec.security.cephx.{component}.keyType={key_type}"
        )

        def _matches():
            actual = self.get_spec_key_type(component=component)
            if actual == key_type:
                return True
            log.debug(
                f"CephCluster spec.security.cephx.{component}.keyType={actual}, "
                f"want {key_type}"
            )
            return False

        for matched in TimeoutSampler(timeout, sleep, _matches):
            if matched:
                log.info(
                    f"CephCluster spec.security.cephx.{component}.keyType is {key_type}"
                )
                return True

        raise UnexpectedBehaviour(
            f"Timed out waiting for CephCluster spec.security.cephx.{component}.keyType="
            f"{key_type}"
        )

    def get_ceph_health_detail(self, toolbox_pod=None):
        """Return output of ``ceph health detail``."""
        toolbox = toolbox_pod or get_ceph_tools_pod()
        return toolbox.exec_cmd_on_pod(
            "ceph health detail",
            out_yaml_format=False,
        )

    def has_auth_insecure_service_key_type_warning(self, toolbox_pod=None):
        """Return True when AUTH_INSECURE_SERVICE_KEY_TYPE is present in health detail."""
        detail = self.get_ceph_health_detail(toolbox_pod)
        return constants.CEPHX_INSECURE_SERVICE_KEY_TYPE_WARN in detail

    def get_insecure_service_key_type_entities(self, toolbox_pod=None):
        """
        Parse entities still using insecure key types from health detail.

        Returns:
            list[tuple[str, str]]: (entity, key_type) pairs.
        """
        detail = self.get_ceph_health_detail(toolbox_pod)
        return re.findall(
            r"entity (\S+) using insecure key type: (\S+)",
            detail,
        )

    def wait_for_auth_insecure_service_key_type_cleared(
        self, timeout=1200, sleep=15, toolbox_pod=None
    ):
        """Wait until AUTH_INSECURE_SERVICE_KEY_TYPE is reconciled away."""
        log.info(
            "Waiting for AUTH_INSECURE_SERVICE_KEY_TYPE health warning to clear "
            f"(timeout={timeout}s)"
        )

        def _cleared():
            if not self.has_auth_insecure_service_key_type_warning(toolbox_pod):
                return True
            insecure = self.get_insecure_service_key_type_entities(toolbox_pod)
            log.debug(
                "AUTH_INSECURE_SERVICE_KEY_TYPE still present for: "
                f"{', '.join(f'{entity}={key_type}' for entity, key_type in insecure)}"
            )
            return False

        for cleared in TimeoutSampler(timeout, sleep, _cleared):
            if cleared:
                log.info("AUTH_INSECURE_SERVICE_KEY_TYPE health warning cleared")
                return True

        insecure = self.get_insecure_service_key_type_entities(toolbox_pod)
        raise UnexpectedBehaviour(
            "AUTH_INSECURE_SERVICE_KEY_TYPE not reconciled within "
            f"{timeout}s; remaining entities: {insecure}"
        )

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
        toolbox = toolbox_pod or get_ceph_tools_pod()
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

    def verify_operator_auth_rotate_key_type_logs(self, key_type):
        """Verify rook-ceph-operator invoked ceph auth rotate with --key-type."""
        from ocs_ci.helpers.helpers import get_logs_rook_ceph_operator

        operator_logs = get_logs_rook_ceph_operator()
        key_type_lower = key_type.lower()
        for line in operator_logs.splitlines():
            lower_line = line.lower()
            if "auth rotate" not in lower_line:
                continue
            if constants.CEPHX_AUTH_ROTATE_KEY_TYPE_OPERATOR_LOG not in lower_line:
                continue
            if key_type_lower in lower_line:
                log.info(
                    "Operator log confirms ceph auth rotate with key type "
                    f"{key_type}: {line.strip()}"
                )
                return

        raise UnexpectedBehaviour(
            "Operator logs missing ceph auth rotate invocation with "
            f"{constants.CEPHX_AUTH_ROTATE_KEY_TYPE_OPERATOR_LOG} {key_type}"
        )

    def patch_storagecluster_allowed_ciphers(self, ciphers):
        """Patch StorageCluster managedResources.cephCluster.security.cephx.allowedCiphers."""
        ciphers = list(ciphers)
        sc_obj = OCP(
            kind=constants.STORAGECLUSTER,
            resource_name=constants.DEFAULT_CLUSTERNAME,
            namespace=self.namespace,
        )
        cc_spec = self.get_storagecluster_managed_cephcluster()
        security = cc_spec.get("security") or {}
        cephx = security.get("cephx") or {}
        patch_ops = []

        if not cc_spec:
            patch_ops.append(
                {
                    "op": "add",
                    "path": "/spec/managedResources/cephCluster",
                    "value": {"security": {"cephx": {"allowedCiphers": ciphers}}},
                }
            )
        elif not security:
            patch_ops.append(
                {
                    "op": "add",
                    "path": "/spec/managedResources/cephCluster/security",
                    "value": {"cephx": {"allowedCiphers": ciphers}},
                }
            )
        elif not cephx:
            patch_ops.append(
                {
                    "op": "add",
                    "path": "/spec/managedResources/cephCluster/security/cephx",
                    "value": {"allowedCiphers": ciphers},
                }
            )
        elif "allowedCiphers" in cephx:
            patch_ops.append(
                {
                    "op": "replace",
                    "path": "/spec/managedResources/cephCluster/security/cephx/allowedCiphers",
                    "value": ciphers,
                }
            )
        else:
            patch_ops.append(
                {
                    "op": "add",
                    "path": "/spec/managedResources/cephCluster/security/cephx/allowedCiphers",
                    "value": ciphers,
                }
            )

        log.info(f"Patching StorageCluster allowedCiphers to {ciphers}")
        sc_obj.patch(params=json.dumps(patch_ops), format_type="json")
        self._storagecluster_obj = None

    def remove_storagecluster_cephcluster_security(self):
        """Remove security block from StorageCluster managedResources.cephCluster."""
        cc_spec = self.get_storagecluster_managed_cephcluster()
        if not cc_spec.get("security"):
            log.info(
                "StorageCluster managedResources.cephCluster.security not present; "
                "nothing to remove"
            )
            return

        sc_obj = OCP(
            kind=constants.STORAGECLUSTER,
            resource_name=constants.DEFAULT_CLUSTERNAME,
            namespace=self.namespace,
        )
        log.info("Removing StorageCluster managedResources.cephCluster.security block")
        sc_obj.patch(
            params=json.dumps(
                [
                    {
                        "op": "remove",
                        "path": "/spec/managedResources/cephCluster/security",
                    }
                ]
            ),
            format_type="json",
        )
        self._storagecluster_obj = None

    def get_metrics_exporter_pods(self):
        """Return Running ocs-metrics-exporter pod resource dicts."""
        return get_pods_having_label(
            constants.OCS_METRICS_EXPORTER,
            namespace=self.namespace,
            statuses=[constants.STATUS_RUNNING],
        )

    def assert_metrics_exporter_running(self):
        """Assert a Running ocs-metrics-exporter pod exists."""
        pods = self.get_metrics_exporter_pods()
        if not pods:
            raise UnexpectedBehaviour("No Running ocs-metrics-exporter pods found")
        pod_name = pods[0]["metadata"]["name"]
        log.info(f"ocs-metrics-exporter pod is Running: {pod_name}")
        return pods[0]

    def fetch_metrics_exporter_metrics(self, metrics_pod=None):
        """
        Fetch the ocs-metrics-exporter /metrics payload via the toolbox pod.

        The exporter serves HTTPS on the exporter port (default 9443).
        """
        metrics_pod = metrics_pod or self.assert_metrics_exporter_running()
        pod_ip = get_pod_ip(
            OCP(
                kind=constants.POD,
                namespace=self.namespace,
                resource_name=metrics_pod["metadata"]["name"],
            )
        )
        if not pod_ip:
            raise UnexpectedBehaviour("ocs-metrics-exporter pod IP is not assigned")

        metrics_url = (
            f"https://{pod_ip}:{constants.OCS_METRICS_EXPORTER_PORT}"
            f"{constants.OCS_METRICS_EXPORTER_METRICS_PATH}"
        )
        log.info(f"Fetching ocs-metrics-exporter metrics from {metrics_url}")
        toolbox = get_ceph_tools_pod()
        return toolbox.exec_cmd_on_pod(
            f"curl -sk --connect-timeout 15 {metrics_url}",
            out_yaml_format=False,
        )

    def assert_metrics_exporter_metrics(self, metrics_output=None, metrics_pod=None):
        """
        Assert ocs-metrics-exporter exposes Prometheus metrics from Ceph/OCS.

        Args:
            metrics_output (str): Pre-fetched /metrics payload.
            metrics_pod (dict): Optional exporter pod resource dict.
        """
        metrics_output = metrics_output or self.fetch_metrics_exporter_metrics(
            metrics_pod
        )
        if not metrics_output or "# TYPE" not in metrics_output:
            raise UnexpectedBehaviour(
                "ocs-metrics-exporter /metrics response is empty or invalid"
            )
        if not any(
            prefix in metrics_output
            for prefix in constants.OCS_METRICS_EXPORTER_METRIC_PREFIXES
        ):
            raise UnexpectedBehaviour(
                "ocs-metrics-exporter /metrics missing expected OCS/Ceph metric names"
            )
        log.info("ocs-metrics-exporter metrics export verified successfully")

    def verify_metrics_exporter_no_auth_bad_key(self, metrics_pod=None, tail=500):
        """Assert ocs-metrics-exporter logs do not contain AUTH_BAD_KEY errors."""
        metrics_pod = metrics_pod or self.assert_metrics_exporter_running()
        pod_name = metrics_pod["metadata"]["name"]
        auth_errors = get_pod_logs(
            pod_name=pod_name,
            container=constants.OCS_METRICS_EXPORTER_CONTAINER,
            namespace=self.namespace,
            tail=str(tail),
            grep=constants.AUTH_BAD_KEY_LOG,
            return_empty_string=True,
        )
        if auth_errors and constants.AUTH_BAD_KEY_LOG in auth_errors:
            raise UnexpectedBehaviour(
                f"AUTH_BAD_KEY errors found in ocs-metrics-exporter logs: "
                f"{auth_errors.strip()}"
            )
        log.info("No AUTH_BAD_KEY errors in ocs-metrics-exporter logs")

    def wait_for_metrics_exporter_metrics(
        self, timeout=600, sleep=15, metrics_pod=None
    ):
        """Wait until ocs-metrics-exporter exports valid metrics."""
        log.info(
            f"Waiting for ocs-metrics-exporter metrics export (timeout={timeout}s)"
        )

        def _metrics_ready():
            try:
                self.assert_metrics_exporter_metrics(metrics_pod=metrics_pod)
                self.verify_metrics_exporter_no_auth_bad_key(metrics_pod=metrics_pod)
                return True
            except UnexpectedBehaviour as exc:
                log.debug(f"ocs-metrics-exporter metrics not ready yet: {exc}")
                return False

        for ready in TimeoutSampler(timeout, sleep, _metrics_ready):
            if ready:
                log.info("ocs-metrics-exporter metrics export is healthy")
                return True

        raise UnexpectedBehaviour(
            f"ocs-metrics-exporter did not export metrics within {timeout}s"
        )

    def wait_for_metrics_exporter_after_rotation(
        self, previous_pod_name=None, timeout=900, sleep=15
    ):
        """
        Wait for ocs-metrics-exporter to recover after CephX key rotation.

        The exporter may restart or reload its Ceph keyring; metrics export and
        logs are polled until healthy.
        """
        log.info(
            "Waiting for ocs-metrics-exporter to use rotated CephX key "
            f"(previous pod={previous_pod_name or 'unknown'})"
        )

        def _exporter_ready():
            pods = self.get_metrics_exporter_pods()
            if not pods:
                log.debug("ocs-metrics-exporter pod is not Running yet")
                return False
            pod_name = pods[0]["metadata"]["name"]
            if previous_pod_name and pod_name != previous_pod_name:
                log.info(
                    f"ocs-metrics-exporter pod restarted: "
                    f"{previous_pod_name} -> {pod_name}"
                )
            try:
                self.assert_metrics_exporter_metrics(pods[0])
                self.verify_metrics_exporter_no_auth_bad_key(pods[0])
                return True
            except UnexpectedBehaviour as exc:
                log.debug(f"ocs-metrics-exporter not ready after rotation: {exc}")
                return False

        for ready in TimeoutSampler(timeout, sleep, _exporter_ready):
            if ready:
                log.info("ocs-metrics-exporter is healthy after CephX key rotation")
                return True

        raise UnexpectedBehaviour(
            f"ocs-metrics-exporter did not recover within {timeout}s after rotation"
        )

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

    def get_next_rook_daemon_key_generation(self):
        """
        Return a generation value to trigger rotation for Rook daemons only.

        Considers MON, MGR, and OSD on CephCluster plus MDS on CephFilesystem.
        """
        current = self.get_spec_key_generation(self.COMPONENT_DAEMON)
        for entity in self.ROOK_DAEMON_STATUS_ENTITIES:
            current = max(current, self.get_status_key_generation(entity))
        current = max(current, self.get_filesystem_daemon_key_generation())
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
            f"Initiating CephX key rotation for {component} "
            f"(generation={key_generation}) on "
            f"{self.namespace}/{self.ceph_cluster_name}"
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

    def rotate_rook_daemon_keys(self, key_generation=None):
        """
        Rotate CephX keys for Rook-managed daemons: MON, MGR, OSD, and MDS.

        Triggers ``spec.security.cephx.daemon`` reconciliation; generation
        calculation and completion checks use only those four daemon types
        (not admin, crashCollector, or cephExporter).
        """
        if key_generation is None:
            key_generation = self.get_next_rook_daemon_key_generation()
        return self.rotate_component_keys(
            self.COMPONENT_DAEMON, key_generation=key_generation
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

    def wait_for_osd_rotation(self, expected_generation, timeout=900, sleep=15):
        """Wait until ``status.cephx.osd.keyGeneration`` reaches *expected_generation*."""
        return self._wait_for_status_entities(
            ["osd"],
            expected_generation,
            timeout,
            sleep,
            label="osd",
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

    def start_dd_io_in_background(
        self, pod_obj, file_path, bs="4k", count=10000, loop=True
    ):
        """
        Start continuous ``dd`` I/O on a pod mount path in a background thread.

        Args:
            pod_obj: Pod object with ``exec_cmd_on_pod``.
            file_path (str): Destination file path on the mounted volume.
            bs (str): Block size for ``dd``.
            count (int): Block count per ``dd`` invocation.
            loop (bool): When True, repeat ``dd`` until stopped.

        Returns:
            Thread: Background I/O thread.
        """
        mount_dir = file_path.rsplit("/", 1)[0]
        pod_obj.exec_cmd_on_pod(f"mkdir -p {mount_dir}", out_yaml_format=False)

        if loop:
            dd_cmd = (
                f"while true; do "
                f"dd if=/dev/urandom of={file_path} bs={bs} count={count} "
                f"status=none conv=notrunc; "
                f"done"
            )
        else:
            dd_cmd = (
                f"dd if=/dev/urandom of={file_path} bs={bs} count={count} "
                f"status=none"
            )

        def _run_dd():
            pod_obj.exec_cmd_on_pod(
                command=f"bash -c '{dd_cmd}'",
                timeout=7200,
                out_yaml_format=False,
            )

        thread = Thread(target=_run_dd, name=f"dd-io-{pod_obj.name}")
        thread.daemon = True
        thread.start()
        time.sleep(2)
        log.info(f"Started background dd I/O on {pod_obj.name}:{file_path}")
        return thread

    def stop_dd_io(self, pod_obj, file_path):
        """Stop background ``dd`` I/O started by :meth:`start_dd_io_in_background`."""
        pod_obj.exec_cmd_on_pod(
            command=(
                f"pkill -f 'dd if=/dev/urandom of={file_path}' || "
                f"pkill -f 'bash -c while true' || true"
            ),
            out_yaml_format=False,
            timeout=60,
        )
        log.info(f"Stopped background dd I/O on {pod_obj.name}:{file_path}")

    def verify_io_file_readable(self, pod_obj, file_path):
        """Assert *file_path* exists on the pod volume and is readable."""
        pod_obj.exec_cmd_on_pod(
            command=(
                f"test -s {file_path} && "
                f"dd if={file_path} of=/dev/null bs=4k count=1 status=none"
            ),
            out_yaml_format=False,
        )
        log.info(f"Verified I/O file is readable on {pod_obj.name}:{file_path}")

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

    def get_auth_key(self, entity, toolbox_pod=None):
        """
        Return the current CephX key for *entity* from the toolbox.

        Uses ``ceph auth get-key <entity>`` (not ``ceph auth <entity>``, which is
        invalid). Works for daemon entities (``osd.0``, ``mgr.a``, ``mds.*``,
        ``mon.a``) and client entities (``client.admin``, CSI users, etc.).

        Args:
            entity (str): Ceph auth entity name.
            toolbox_pod: Optional rook-ceph-tools pod object.

        Returns:
            str: Key string, or empty string if the entity does not exist.
        """
        toolbox = toolbox_pod or get_ceph_tools_pod()
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
        """Log per-entity CephX key comparison between two snapshots."""
        log.info("CephX auth key comparison (before vs after rotation):")
        for entity in sorted(old_keys):
            old_key = old_keys.get(entity, "")
            new_key = new_keys.get(entity, "")
            if not old_key and not new_key:
                status = "MISSING"
            elif old_key == new_key:
                status = "UNCHANGED"
            else:
                status = "CHANGED"
            log.info(
                f"  {entity} [{status}]: "
                f"before={old_key if old_key else '<empty>'} "
                f"after={new_key if new_key else '<empty>'}"
            )

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

    def is_mon_key_rotation_supported(self):
        """
        Return True when CephCluster reports MON ``status.cephx.mon.keyGeneration``.

        Note: Rook may report MON rotation status even when ``mon.*`` entities are
        not present in ``ceph auth ls`` (e.g. Ceph Tentacle). Use
        :meth:`is_mon_auth_verifiable` before asserting on MON auth keys.
        """
        mon_status = self.get_status_cephx().get("mon") or {}
        return bool(mon_status.get("keyGeneration"))

    def is_mon_auth_verifiable(self, toolbox_pod=None):
        """Return True when MON auth entities are readable from the auth store."""
        return bool(self._discover_mon_auth_entities(toolbox_pod))

    def get_filesystem_status_cephx(self):
        """Return ``status.cephx`` from the CephFilesystem CR."""
        fs_obj = self._get_cephfilesystem_obj()
        fs_obj.reload_data()
        return fs_obj.data.get("status", {}).get("cephx", {}) or {}

    def get_filesystem_daemon_key_generation(self):
        """Return ``status.cephx.daemon.keyGeneration`` from CephFilesystem."""
        daemon_status = self.get_filesystem_status_cephx().get("daemon") or {}
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
                f"Daemon CephX key rotation already enabled at generation {current}"
            )
            return current

        log.info(
            f"Enabling daemon CephX KeyGeneration policy at generation {key_generation}"
        )
        return self.rotate_component_keys(
            self.COMPONENT_DAEMON, key_generation=key_generation
        )

    def get_spec_rotation_policy(self, component):
        """Return configured keyRotationPolicy for a cephx component."""
        self._validate_component(component)
        return self.get_spec_cephx().get(component, {}).get("keyRotationPolicy") or ""

    def is_rotation_policy_disabled(self, component):
        """Return True when *component* rotation policy is Disabled or unset."""
        policy = self.get_spec_rotation_policy(component)
        return policy in ("", self.KEY_ROTATION_POLICY_DISABLED)

    def disable_component_key_rotation(self, component):
        """Set ``keyRotationPolicy: Disabled`` for a cephx component."""
        self._validate_component(component)
        component_spec = dict(self.get_spec_cephx().get(component) or {})
        if component_spec.get("keyRotationPolicy") == self.KEY_ROTATION_POLICY_DISABLED:
            log.info(f"CephX key rotation already Disabled for {component}")
            return

        component_spec["keyRotationPolicy"] = self.KEY_ROTATION_POLICY_DISABLED
        patch_ops = self._build_cephx_component_patch_ops(component, component_spec)
        log.info(f"Disabling CephX key rotation policy for {component}")
        self.cephcluster_obj.patch(
            params=json.dumps(patch_ops),
            format_type="json",
        )
        self._reload()

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
        cc_obj = OCP(kind="CephClient", namespace=self.namespace)
        resources = cc_obj.get()
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
        toolbox = toolbox_pod or get_ceph_tools_pod()
        try:
            toolbox.exec_cmd_on_pod(
                f"ceph auth get-key {entity} --format json",
                out_yaml_format=True,
            )
            return True
        except CommandFailed:
            return False

    def _discover_mon_auth_entities(self, toolbox_pod=None):
        """
        Discover MON auth entities (e.g. mon.a).

        MON keys are not always listed in ``ceph auth ls`` on newer Ceph builds,
        so fall back to ``ceph mon dump`` names.
        """
        entities = self.list_auth_entities("mon.", toolbox_pod)
        if entities:
            return entities

        toolbox = toolbox_pod or get_ceph_tools_pod()
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

    def _discover_mgr_auth_entities(self, toolbox_pod=None):
        """Discover MGR auth entities, falling back to ``ceph mgr dump``."""
        entities = self.list_auth_entities("mgr.", toolbox_pod)
        if entities:
            return entities

        toolbox = toolbox_pod or get_ceph_tools_pod()
        mgr_dump = toolbox.exec_ceph_cmd("ceph mgr dump")
        discovered = []
        active = mgr_dump.get("active_name")
        if active:
            entity = f"mgr.{active}"
            if self._auth_entity_exists(entity, toolbox_pod):
                discovered.append(entity)
        for standby in mgr_dump.get("standbys", []) or []:
            entity = f"mgr.{standby}"
            if self._auth_entity_exists(entity, toolbox_pod):
                discovered.append(entity)
        return sorted(set(discovered))

    def _discover_osd_auth_entities(self, toolbox_pod=None):
        """Discover OSD auth entities, falling back to ``ceph osd dump``."""
        entities = self.list_auth_entities("osd.", toolbox_pod)
        if entities:
            return entities

        toolbox = toolbox_pod or get_ceph_tools_pod()
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
        log.info(
            "Cleared cephx-status annotation from OSD deployments: "
            f"{', '.join(cleared) or 'none'}"
        )
        return cleared

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
            list: Mon deployment names scaled down for later restoration.
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

    def verify_csi_node_plugin_logs_for_auth_errors(self, since_time=None):
        """
        Collect AUTH_BAD_KEY lines from CSI RBD node plugin logs.

        Returns:
            list: Matching log lines (may be non-empty when old CSI keys are deleted).
        """
        matches = []
        for csi_pod in self.get_csi_node_plugin_pods():
            logs = get_pod_logs(
                pod_name=csi_pod.name,
                namespace=self.namespace,
            )
            if since_time:
                logs = "\n".join(
                    line for line in logs.splitlines() if line[:19] >= since_time[:19]
                )
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
        target_generation = self.rotate_daemon_keys()
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
            pod_states (dict): OSD pod name to cephx-key-identifier from
                :meth:`capture_daemon_pod_state`.
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

    # TODO(cephx-keyrotation): Remove restart_ceph_tools_pod_after_keyrotation and its
    # call site in background_cluster_operations._cephx_keyrotation_operation once
    # rook-ceph-tools reloads CephX keys after rotation without a pod restart.
    def restart_ceph_tools_pod_after_keyrotation(self, timeout=300):
        """
        Temporary workaround: restart rook-ceph-tools after CephX key rotation.

        Deletes the toolbox pod so its deployment recreates it with updated
        CephX credentials.

        .. warning::
            This is a short-term workaround only. Remove this method and its
            caller when the upstream toolbox key-reload issue is fixed.
        """
        tools_pod = get_ceph_tools_pod(namespace=self.namespace)
        pod_name = tools_pod.name
        log.warning(
            "TEMPORARY WORKAROUND (remove when cephx toolbox key-reload is fixed): "
            "restarting rook-ceph-tools pod %s after CephX key rotation",
            pod_name,
        )
        tools_pod.delete()
        tools_pod.ocp.wait_for_delete(resource_name=pod_name, timeout=timeout)
        new_tools_pod = get_ceph_tools_pod(wait=True, namespace=self.namespace)
        log.warning(
            "TEMPORARY WORKAROUND complete: rook-ceph-tools pod %s is Running; "
            "remove restart_ceph_tools_pod_after_keyrotation once toolbox reloads "
            "rotated keys without a restart",
            new_tools_pod.name,
        )
        return new_tools_pod

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

    def wait_for_mon_quorum_count_at_most(self, max_count, timeout=300, sleep=15):
        """Wait until monitors in quorum are at most *max_count*."""
        from ocs_ci.helpers.ceph_helpers import get_mon_quorum_count

        log.info(f"Waiting for mon quorum count <= {max_count}")

        def _quorum_reduced():
            count = get_mon_quorum_count()
            log.info(f"Current mon quorum count: {count}")
            return count <= max_count

        for ready in TimeoutSampler(timeout, sleep, _quorum_reduced):
            if ready:
                return get_mon_quorum_count()

        raise UnexpectedBehaviour(
            f"Mon quorum count did not drop to {max_count} within {timeout}s"
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
            section_match = re.match(r"\[(mon\.[^\]]+)\]", line.strip())
            if section_match:
                current_entity = section_match.group(1)
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

    def get_mon_keys_from_ceph_auth(self, toolbox_pod=None):
        """Return mon entity to key mapping from the Ceph auth store."""
        toolbox = toolbox_pod or get_ceph_tools_pod()
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

    def kill_operator_during_mon_rotation(self, timeout=900, poll_interval=2):
        """
        Trigger daemon rotation and kill the operator after mon auth rotation
        starts but before mon secrets are updated.

        Returns:
            int: Requested daemon key generation.
        """
        from ocs_ci.helpers.helpers import get_logs_rook_ceph_operator

        target_generation = self.rotate_daemon_keys()
        operator_pods = get_operator_pods(namespace=self.namespace)
        if not operator_pods:
            raise UnexpectedBehaviour("rook-ceph-operator pod not found")
        operator_pod = operator_pods[0]
        operator_ocp = OCP(kind=constants.POD, namespace=self.namespace)

        log.info(
            "Waiting to kill rook-ceph-operator between mon auth rotation and "
            "mon secret update"
        )

        def _kill_when_mon_rotating():
            logs = get_logs_rook_ceph_operator()
            lower_logs = logs.lower()
            mon_rotating = bool(
                re.search(constants.CEPHX_MON_AUTH_ROTATION_LOG_PATTERN, logs, re.I)
            )
            secret_updating = (
                constants.CEPHX_MON_SECRET_UPDATE_LOG.lower() in lower_logs
            )
            if secret_updating:
                raise UnexpectedBehaviour(
                    "Mon secret update began before operator could be killed"
                )
            if mon_rotating:
                log.info(
                    f"Killing rook-ceph-operator pod {operator_pod.name} during "
                    "mon key rotation"
                )
                operator_ocp.delete(
                    resource_name=operator_pod.name, force=True, wait=False
                )
                return True
            return False

        killed = False
        for done in TimeoutSampler(timeout, poll_interval, _kill_when_mon_rotating):
            if done:
                killed = True
                break

        if not killed:
            raise UnexpectedBehaviour(
                "Timed out waiting for mon auth rotation log before killing operator"
            )
        return target_generation

    def recover_after_operator_crash_during_mon_rotation(self, timeout=1500):
        """Wait for operator recovery and mon secret reconciliation after crash."""
        self.wait_for_rook_ceph_operator_ready()
        self.wait_for_operator_log_pattern(
            constants.CEPHX_MON_AUTH_GET_LOG_PATTERN,
            timeout=timeout,
            sleep=10,
        )
        self.verify_mon_secrets_match_ceph_auth()
        self.assert_mon_pods_not_crashlooping()
        from ocs_ci.helpers.ceph_helpers import wait_for_mons_in_quorum

        wait_for_mons_in_quorum(len(self.get_mon_deployment_names()), timeout=timeout)
        self.wait_for_cluster_ready(timeout=timeout)
        self.wait_for_pgs_active_clean(timeout=timeout)

    def set_osd_out(self, osd_id):
        """Mark an OSD out via ``ceph osd out``."""
        toolbox = get_ceph_tools_pod()
        toolbox.exec_cmd_on_pod(
            f"ceph osd out osd.{osd_id}",
            out_yaml_format=False,
        )
        log.info(f"Marked osd.{osd_id} out")

    def set_osd_in(self, osd_id):
        """Mark an OSD back in via ``ceph osd in``."""
        toolbox = get_ceph_tools_pod()
        toolbox.exec_cmd_on_pod(
            f"ceph osd in osd.{osd_id}",
            out_yaml_format=False,
        )
        log.info(f"Marked osd.{osd_id} in")

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
        toolbox = toolbox_pod or get_ceph_tools_pod()
        toolbox.exec_cmd_on_pod(
            f"ceph auth del {entity}",
            out_yaml_format=False,
        )
        log.info(f"Deleted Ceph auth entity {entity}")

    def wait_for_partial_osd_key_rotation(self, pre_keys, timeout=900, sleep=15):
        """Wait until at least one OSD auth key differs from *pre_keys*."""
        entities = list(pre_keys.keys())

        def _partial_rotation():
            current_keys = self.capture_auth_keys(entities)
            return any(
                pre_keys.get(entity)
                and pre_keys.get(entity) != current_keys.get(entity)
                for entity in entities
            )

        for rotated in TimeoutSampler(timeout, sleep, _partial_rotation):
            if rotated:
                log.info("Detected partial OSD key rotation")
                return self.capture_auth_keys(entities)

        raise UnexpectedBehaviour(
            f"No OSD keys rotated within {timeout}s while waiting for partial rotation"
        )

    def wait_for_cephcluster_reconcile_failure(self, timeout=600, sleep=15):
        """Wait until CephCluster reports a non-Ready reconcile state."""
        log.info("Waiting for CephCluster reconcile failure")

        def _reconcile_failed():
            self._reload()
            status = self.cephcluster_obj.data.get("status", {}) or {}
            phase = status.get("phase")
            message = status.get("message", "")
            if phase and phase != constants.STATUS_READY:
                log.info(f"CephCluster phase={phase} message={message}")
                return True
            for condition in status.get("conditions", []) or []:
                if (
                    condition.get("type") == "Ready"
                    and condition.get("status") != "True"
                ):
                    log.info(f"CephCluster Ready condition false: {condition}")
                    return True
            return False

        for failed in TimeoutSampler(timeout, sleep, _reconcile_failed):
            if failed:
                return True

        raise UnexpectedBehaviour(
            f"CephCluster did not report reconcile failure within {timeout}s"
        )

    def inject_osd_auth_rotation_failure(self, pre_keys, timeout=900):
        """
        During OSD rotation, delete auth for an OSD that has not rotated yet.

        Returns:
            str: OSD auth entity whose deletion should block rotation.
        """
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
        failed_entity = sorted(pending_entities)[-1]
        self.delete_auth_entity(failed_entity)
        return failed_entity

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

    def verify_operator_lockbox_rotation_logs(self, expected_count):
        """
        Verify rook-ceph-operator logged lockbox key rotation for encrypted OSDs.

        Args:
            expected_count (int): Minimum number of lockbox rotation log lines.
        """
        from ocs_ci.helpers.helpers import get_logs_rook_ceph_operator

        operator_logs = get_logs_rook_ceph_operator()
        matches = [
            line
            for line in operator_logs.splitlines()
            if constants.OSD_LOCKBOX_OPERATOR_LOG in line
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
            for pod_name, annotation in current.items():
                if pod_name not in before_state:
                    return True
                if before_state.get(pod_name) != annotation:
                    return True
            return False

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

    @staticmethod
    def _validate_component(component):
        if component not in CephXKeyRotation.ROTATION_COMPONENTS:
            raise ValueError(
                f"Invalid cephx component '{component}'. "
                f"Expected one of: {', '.join(CephXKeyRotation.ROTATION_COMPONENTS)}"
            )
