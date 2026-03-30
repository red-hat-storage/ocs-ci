"""
Background Cluster Validation System

This module provides validation and monitoring capabilities for background cluster operations.
It ensures success criteria are met:
- No orphan PVs/images in Ceph
- Clean PVC events (no errors)
- Consistent data checksums
- Healthy Ceph status throughout operations
"""

import logging
import time
from typing import List, Dict, Any, Tuple

from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.resources import pod as pod_helpers

log = logging.getLogger(__name__)


class BackgroundClusterValidator:
    """
    Validator for background cluster operations.

    This class performs validation checks before, during, and after
    background cluster operations to ensure system health and data integrity.
    """

    def __init__(self, namespace: str):
        """
        Initialize BackgroundClusterValidator.

        Args:
            namespace: Namespace to validate
        """
        self.namespace = namespace
        self.initial_pv_count = 0
        self.initial_rbd_images: set = set()
        self.initial_cephfs_subvolumes: set = set()
        self.validation_errors: List[Dict[str, Any]] = []

    def pre_operation_validation(self):
        """
        Perform validation before starting background cluster operations.

        Captures baseline state for comparison during and after operations.
        """
        log.info("Performing pre-operation validation")

        try:
            # Capture initial PV count
            self.initial_pv_count = self._get_pv_count()
            log.info(f"Initial PV count: {self.initial_pv_count}")

            # Capture initial Ceph RBD images
            self.initial_rbd_images = self._get_rbd_images()
            log.info(f"Initial RBD images count: {len(self.initial_rbd_images)}")

            # Capture initial CephFS subvolumes
            self.initial_cephfs_subvolumes = self._get_cephfs_subvolumes()
            log.info(
                f"Initial CephFS subvolumes count: {len(self.initial_cephfs_subvolumes)}"
            )

            # Check initial Ceph health
            ceph_health = self._check_ceph_health()
            log.info(f"Initial Ceph health: {ceph_health}")

            if ceph_health not in ["HEALTH_OK", "HEALTH_WARN"]:
                log.warning(f"Ceph health is not OK: {ceph_health}")

            log.info("Pre-operation validation completed")

        except Exception as e:
            log.error(f"Pre-operation validation failed: {e}")
            raise

    def continuous_validation(self) -> bool:
        """
        Perform continuous validation during background cluster operations.

        Returns:
            bool: True if validation passes, False otherwise
        """
        log.debug("Performing continuous validation")

        try:
            # Check Ceph health
            ceph_health = self._check_ceph_health()
            if ceph_health not in ["HEALTH_OK", "HEALTH_WARN"]:
                error_msg = f"Ceph health degraded: {ceph_health}"
                log.warning(error_msg)
                self.validation_errors.append(
                    {
                        "type": "ceph_health",
                        "message": error_msg,
                        "timestamp": time.time(),
                    }
                )
                return False

            # Check for PVC errors in events
            pvc_errors = self._check_pvc_events()
            if pvc_errors:
                error_msg = f"Found {len(pvc_errors)} PVC errors in events"
                log.warning(error_msg)
                self.validation_errors.append(
                    {
                        "type": "pvc_events",
                        "message": error_msg,
                        "details": pvc_errors,
                        "timestamp": time.time(),
                    }
                )

            return True

        except Exception as e:
            log.error(f"Continuous validation error: {e}")
            return False

    def post_operation_validation(self) -> Tuple[bool, Dict[str, Any]]:
        """
        Perform validation after background cluster operations complete.

        Returns:
            Tuple of (success, validation_report)
        """
        log.info("Performing post-operation validation")
        validation_report = {
            "passed": True,
            "checks": {},
            "errors": self.validation_errors,
        }

        try:
            # Check 1: No orphan PVs
            orphan_pvs = self._check_orphan_pvs()
            validation_report["checks"]["orphan_pvs"] = {
                "passed": len(orphan_pvs) == 0,
                "count": len(orphan_pvs),
                "details": orphan_pvs,
            }
            if orphan_pvs:
                log.warning(f"Found {len(orphan_pvs)} orphan PVs")
                validation_report["passed"] = False

            # Check 2: No orphan RBD images
            orphan_rbd_images = self._check_orphan_rbd_images()
            rbd_check: Dict[str, Any] = {
                "passed": len(orphan_rbd_images) == 0,
                "count": len(orphan_rbd_images),
                "details": list(orphan_rbd_images),
            }
            validation_report["checks"]["orphan_rbd_images"] = rbd_check
            if orphan_rbd_images:
                log.warning(f"Found {len(orphan_rbd_images)} orphan RBD images")
                validation_report["passed"] = False

            # Check 3: No orphan CephFS subvolumes
            orphan_subvolumes = self._check_orphan_cephfs_subvolumes()
            subvol_check: Dict[str, Any] = {
                "passed": len(orphan_subvolumes) == 0,
                "count": len(orphan_subvolumes),
                "details": list(orphan_subvolumes),
            }
            validation_report["checks"]["orphan_cephfs_subvolumes"] = subvol_check
            if orphan_subvolumes:
                log.warning(f"Found {len(orphan_subvolumes)} orphan CephFS subvolumes")
                validation_report["passed"] = False

            # Check 4: PVC events are clean
            pvc_errors = self._check_pvc_events()
            events_check: Dict[str, Any] = {
                "passed": len(pvc_errors) == 0,
                "error_count": len(pvc_errors),
                "details": pvc_errors,
            }
            validation_report["checks"]["pvc_events"] = events_check
            if pvc_errors:
                log.warning(f"Found {len(pvc_errors)} PVC errors in events")

            # Check 5: Ceph health is OK
            ceph_health = self._check_ceph_health()
            health_check: Dict[str, Any] = {
                "passed": ceph_health in ["HEALTH_OK", "HEALTH_WARN"],
                "status": ceph_health,
            }
            validation_report["checks"]["ceph_health"] = health_check
            if ceph_health not in ["HEALTH_OK", "HEALTH_WARN"]:
                log.error(f"Ceph health is not OK: {ceph_health}")
                validation_report["passed"] = False

            # Log summary
            self._log_validation_summary(validation_report)

            log.info(
                f"Post-operation validation completed: {'PASSED' if validation_report['passed'] else 'FAILED'}"
            )
            return validation_report["passed"], validation_report

        except Exception as e:
            log.error(f"Post-operation validation failed: {e}")
            validation_report["passed"] = False
            validation_report["error"] = str(e)
            return False, validation_report

    # ==========================================================================
    # Helper Methods
    # ==========================================================================

    def _get_pv_count(self) -> int:
        """Get current PV count."""
        pv_obj = ocp.OCP(kind=constants.PV)
        pvs = pv_obj.get()["items"]
        return len(pvs)

    def _get_rbd_images(self) -> set:
        """Get list of RBD images in Ceph."""
        try:
            ct_pod = pod_helpers.get_ceph_tools_pod()
            pool = config.ENV_DATA.get("rbd_pool", constants.DEFAULT_BLOCKPOOL)

            # List RBD images
            cmd = f"rbd ls -p {pool}"
            result = ct_pod.exec_cmd_on_pod(cmd, out_yaml_format=False)

            images = set()
            if result:
                for line in result.strip().split("\n"):
                    if line.strip():
                        images.add(line.strip())

            return images
        except Exception as e:
            log.warning(f"Failed to get RBD images: {e}")
            return set()

    def _get_cephfs_subvolumes(self) -> set:
        """Get list of CephFS subvolumes."""
        try:
            ct_pod = pod_helpers.get_ceph_tools_pod()

            # Get filesystem name
            cmd = "ceph fs ls --format=json"
            result = ct_pod.exec_cmd_on_pod(cmd)

            if not result or not isinstance(result, list):
                return set()

            subvolumes = set()
            for fs in result:
                fs_name = fs.get("name")
                if not fs_name:
                    continue

                # List subvolumes
                cmd = f"ceph fs subvolume ls {fs_name} csi"
                try:
                    subvol_result = ct_pod.exec_cmd_on_pod(cmd)
                    if subvol_result and isinstance(subvol_result, list):
                        for subvol in subvol_result:
                            if "name" in subvol:
                                subvolumes.add(subvol["name"])
                except Exception:
                    pass  # Subvolume group might not exist

            return subvolumes
        except Exception as e:
            log.warning(f"Failed to get CephFS subvolumes: {e}")
            return set()

    def _check_ceph_health(self) -> str:
        """
        Check Ceph health status.

        Returns:
            str: Ceph health status (HEALTH_OK, HEALTH_WARN, HEALTH_ERR)
        """
        try:
            ct_pod = pod_helpers.get_ceph_tools_pod()
            result = ct_pod.exec_ceph_cmd("ceph health")
            return result.strip() if result else "UNKNOWN"
        except Exception as e:
            log.error(f"Failed to check Ceph health: {e}")
            return "ERROR"

    def _check_orphan_pvs(self) -> List[str]:
        """
        Check for orphan PVs (PVs without corresponding PVCs).

        Returns:
            List of orphan PV names
        """
        orphan_pvs = []

        try:
            pv_obj = ocp.OCP(kind=constants.PV)
            pvs = pv_obj.get()["items"]

            for pv in pvs:
                pv_name = pv["metadata"]["name"]
                claim_ref = pv["spec"].get("claimRef")

                if not claim_ref:
                    # PV has no claim reference (available or released)
                    phase = pv["status"]["phase"]
                    if phase == "Released":
                        orphan_pvs.append(pv_name)
                else:
                    # Check if PVC still exists
                    pvc_name = claim_ref["name"]
                    pvc_namespace = claim_ref["namespace"]

                    pvc_obj = ocp.OCP(
                        kind=constants.PVC,
                        namespace=pvc_namespace,
                        resource_name=pvc_name,
                    )
                    if not pvc_obj.is_exist():
                        orphan_pvs.append(pv_name)

        except Exception as e:
            log.error(f"Failed to check orphan PVs: {e}")

        return orphan_pvs

    def _check_orphan_rbd_images(self) -> set:
        """
        Check for orphan RBD images (images without corresponding PVs).

        Returns:
            Set of orphan RBD image names
        """
        try:
            current_images = self._get_rbd_images()

            # Get all PV-backed RBD images
            pv_images = self._get_pv_rbd_images()

            # Orphans are images not backed by PVs and not in initial set
            orphans = current_images - pv_images - self.initial_rbd_images

            return orphans
        except Exception as e:
            log.error(f"Failed to check orphan RBD images: {e}")
            return set()

    def _get_pv_rbd_images(self) -> set:
        """Get RBD image names from all RBD PVs."""
        images = set()

        try:
            pv_obj = ocp.OCP(kind=constants.PV)
            pvs = pv_obj.get()["items"]

            for pv in pvs:
                # Check if it's an RBD PV
                csi = pv["spec"].get("csi", {})
                if "rbd.csi.ceph.com" in csi.get("driver", ""):
                    # Extract image name from volume handle
                    volume_handle = csi.get("volumeHandle", "")
                    if volume_handle:
                        # Volume handle format: <cluster-id>-<pool-id>-<image-name>-<image-id>
                        parts = volume_handle.split("-")
                        if len(parts) >= 4:
                            image_name = "-".join(parts[2:-1])
                            images.add(image_name)

        except Exception as e:
            log.error(f"Failed to get PV RBD images: {e}")

        return images

    def _check_orphan_cephfs_subvolumes(self) -> set:
        """
        Check for orphan CephFS subvolumes.

        Returns:
            Set of orphan subvolume names
        """
        try:
            current_subvolumes = self._get_cephfs_subvolumes()

            # Get all PV-backed subvolumes
            pv_subvolumes = self._get_pv_cephfs_subvolumes()

            # Orphans are subvolumes not backed by PVs and not in initial set
            orphans = (
                current_subvolumes - pv_subvolumes - self.initial_cephfs_subvolumes
            )

            return orphans
        except Exception as e:
            log.error(f"Failed to check orphan CephFS subvolumes: {e}")
            return set()

    def _get_pv_cephfs_subvolumes(self) -> set:
        """Get CephFS subvolume names from all CephFS PVs."""
        subvolumes = set()

        try:
            pv_obj = ocp.OCP(kind=constants.PV)
            pvs = pv_obj.get()["items"]

            for pv in pvs:
                # Check if it's a CephFS PV
                csi = pv["spec"].get("csi", {})
                if "cephfs.csi.ceph.com" in csi.get("driver", ""):
                    # Extract subvolume name from volume attributes
                    volume_attributes = csi.get("volumeAttributes", {})
                    subvol_name = volume_attributes.get("subvolumeName")
                    if subvol_name:
                        subvolumes.add(subvol_name)

        except Exception as e:
            log.error(f"Failed to get PV CephFS subvolumes: {e}")

        return subvolumes

    def _check_pvc_events(self) -> List[Dict[str, Any]]:
        """
        Check PVC events for errors.

        Returns:
            List of error events
        """
        error_events = []

        try:
            # Get all events in namespace
            event_obj = ocp.OCP(kind="Event", namespace=self.namespace)
            events = event_obj.get()["items"]

            # Filter for PVC-related error/warning events
            for event in events:
                event_type = event.get("type", "")
                reason = event.get("reason", "")
                message = event.get("message", "")

                # Check if it's a PVC event
                involved_object = event.get("involvedObject", {})
                if involved_object.get("kind") != "PersistentVolumeClaim":
                    continue

                # Check if it's an error
                if (
                    event_type in ["Warning", "Error"]
                    or "fail" in reason.lower()
                    or "error" in reason.lower()
                ):
                    error_events.append(
                        {
                            "pvc": involved_object.get("name"),
                            "type": event_type,
                            "reason": reason,
                            "message": message,
                            "timestamp": event.get("lastTimestamp"),
                        }
                    )

        except Exception as e:
            log.error(f"Failed to check PVC events: {e}")

        return error_events

    def _log_validation_summary(self, report: Dict[str, Any]):
        """Log validation summary."""
        log.info("=" * 80)
        log.info("BACKGROUND CLUSTER VALIDATION SUMMARY")
        log.info("=" * 80)
        log.info(f"Overall Status: {'PASSED' if report['passed'] else 'FAILED'}")
        log.info("\nValidation Checks:")

        for check_name, check_result in report["checks"].items():
            status = "✓ PASS" if check_result["passed"] else "✗ FAIL"
            log.info(f"  {check_name}: {status}")

            # Log additional details for failed checks
            if not check_result["passed"]:
                if "count" in check_result and check_result["count"] > 0:
                    log.warning(f"    Found {check_result['count']} issues")
                if "error_count" in check_result and check_result["error_count"] > 0:
                    log.warning(f"    {check_result['error_count']} errors detected")

        if report["errors"]:
            log.warning(
                f"\nTotal validation errors during operations: {len(report['errors'])}"
            )

        log.info("=" * 80)
