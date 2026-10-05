"""CephX security helpers: allowed ciphers, key types, AUTH_INSECURE checks."""

import json
import logging
import re

from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


class CephXSecurityHelper:
    """Allowed ciphers, keyType, and AUTH_INSECURE health helpers."""

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
        """
        Return ``keyType`` for *component* from StorageCluster cephx config.

        Path: managedResources.cephCluster.security.cephx.<component>.keyType
        """
        component = component or self.COMPONENT_DAEMON
        return self.get_storagecluster_component_spec(component).get("keyType")

    def get_cephcluster_key_type(self, component=None):
        """Return ``spec.security.cephx.<component>.keyType`` from the CephCluster."""
        component = component or self.COMPONENT_DAEMON
        return (self.get_spec_cephx().get(component) or {}).get("keyType")

    def patch_cephcluster_key_type(self, key_type, component=None):
        """
        Set ``keyType`` for *component* on StorageCluster (ODF passthrough to Rook).

        Patches managedResources.cephCluster.security.cephx.<component>.keyType
        while preserving sibling fields such as keyGeneration / keyRotationPolicy.
        """
        component = component or self.COMPONENT_DAEMON
        component_spec = dict(self.get_storagecluster_component_spec(component))
        component_spec["keyType"] = key_type
        log.info(
            "Patching StorageCluster managedResources.cephCluster.security.cephx."
            f"{component}.keyType to {key_type}"
        )
        self.patch_storagecluster_cephx_component(component, component_spec)
        self.wait_for_storagecluster_reconciliation(timeout=600, sleep=10)

    def remove_cephcluster_key_type(self, component=None):
        """
        Remove ``keyType`` for *component* from StorageCluster cephx config.

        Preserves other fields on the component (keyGeneration, keyRotationPolicy).
        """
        component = component or self.COMPONENT_DAEMON
        component_spec = dict(self.get_storagecluster_component_spec(component))
        if "keyType" not in component_spec:
            log.info(
                "StorageCluster managedResources.cephCluster.security.cephx."
                f"{component}.keyType not set; nothing to remove"
            )
            return

        component_spec.pop("keyType", None)
        log.info(
            "Removing StorageCluster managedResources.cephCluster.security.cephx."
            f"{component}.keyType"
        )
        if component_spec:
            self.patch_storagecluster_cephx_component(component, component_spec)
        else:
            # No remaining fields — replace with empty object via full component patch
            self.patch_storagecluster_cephx_component(component, {})
        self.wait_for_storagecluster_reconciliation(timeout=600, sleep=10)

    def wait_for_cephcluster_key_type(
        self, key_type, timeout=300, sleep=10, component=None
    ):
        """
        Wait until StorageCluster and CephCluster report *key_type* for *component*.

        StorageCluster is the write target; CephCluster confirms ODF passthrough.
        """
        component = component or self.COMPONENT_DAEMON
        log.info(
            f"Waiting for StorageCluster/CephCluster security.cephx.{component}."
            f"keyType={key_type}"
        )

        def _matches():
            sc_actual = self.get_spec_key_type(component=component)
            cc_actual = self.get_cephcluster_key_type(component=component)
            if sc_actual == key_type and cc_actual == key_type:
                return True
            log.debug(
                f"keyType pending for {component}: StorageCluster={sc_actual}, "
                f"CephCluster={cc_actual}, want {key_type}"
            )
            return False

        for matched in TimeoutSampler(timeout, sleep, _matches):
            if matched:
                log.info(
                    f"StorageCluster and CephCluster security.cephx.{component}."
                    f"keyType is {key_type}"
                )
                return True

        raise UnexpectedBehaviour(
            f"Timed out waiting for security.cephx.{component}.keyType={key_type} "
            f"(StorageCluster={self.get_spec_key_type(component=component)}, "
            f"CephCluster={self.get_cephcluster_key_type(component=component)})"
        )

    def get_ceph_health_detail(self, toolbox_pod=None):
        """Return output of ``ceph health detail``."""
        toolbox = toolbox_pod or self.get_ceph_cli_pod()
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

    def restore_storagecluster_cephcluster_security(self, security):
        """
        Restore ``managedResources.cephCluster.security`` on StorageCluster.

        Args:
            security (dict | None): Pre-test security block. When None, removes
                the security path if present (pre-test absent state).
        """
        if security is None:
            self.remove_storagecluster_cephcluster_security()
            return

        cc_spec = self.get_storagecluster_managed_cephcluster()
        sc_obj = OCP(
            kind=constants.STORAGECLUSTER,
            resource_name=constants.DEFAULT_CLUSTERNAME,
            namespace=self.namespace,
        )
        path = "/spec/managedResources/cephCluster/security"
        if not cc_spec:
            patch_ops = [
                {
                    "op": "add",
                    "path": "/spec/managedResources/cephCluster",
                    "value": {"security": security},
                }
            ]
        elif "security" in cc_spec:
            patch_ops = [{"op": "replace", "path": path, "value": security}]
        else:
            patch_ops = [{"op": "add", "path": path, "value": security}]

        log.info(
            "Restoring StorageCluster managedResources.cephCluster.security "
            f"to pre-test state: {security}"
        )
        sc_obj.patch(params=json.dumps(patch_ops), format_type="json")
        self._storagecluster_obj = None
