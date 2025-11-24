"""
Test case for validating CSI addons log format, content, and debugging information
"""

import logging
import re

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    tier2,
    green_squad,
    skipif_ocs_version,
    polarion_id,
)
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import (
    get_pods_having_label,
    get_pod_logs,
    get_rbdfsplugin_provisioner_pods,
    get_cephfsplugin_provisioner_pods,
    Pod,
)

log = logging.getLogger(__name__)


@skipif_ocs_version("<4.20")
@tier2
@green_squad
class TestCSIAddonsLogsValidation(ManageTest):
    """
    Test class for validating CSI addons log format, content, and debugging information
    """

    def _validate_log_format(self, logs, pod_name, container_name):
        """
        Validate log format and structure

        Args:
            logs (str): Log content from the pod
            pod_name (str): Name of the pod
            container_name (str): Name of the container

        Returns:
            dict: Validation results with details
        """
        validation_results = {
            "has_timestamp": False,
            "has_log_level": False,
            "has_message": False,
            "log_lines": [],
            "errors": [],
        }

        if not logs or not logs.strip():
            validation_results["errors"].append(
                f"Empty logs for pod {pod_name}, container {container_name}"
            )
            return validation_results

        log_lines = logs.strip().split("\n")
        validation_results["log_lines"] = log_lines

        # Common log patterns to check
        timestamp_patterns = [
            r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}",  # ISO 8601 format
            r"\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2}",  # Alternative format
            r"time=",  # Kubernetes log format with time=
        ]

        log_level_patterns = [
            r"(?i)\b(ERROR|FATAL|PANIC)\b",
            r"(?i)\b(WARN|WARNING)\b",
            r"(?i)\b(INFO|DEBUG|TRACE)\b",
            r'"level":"(error|warn|info|debug)"',  # JSON log format
        ]

        # Check first few lines for format validation
        sample_lines = log_lines[:10] if len(log_lines) >= 10 else log_lines

        for line in sample_lines:
            # Check for timestamp
            for pattern in timestamp_patterns:
                if re.search(pattern, line):
                    validation_results["has_timestamp"] = True
                    break

            # Check for log level
            for pattern in log_level_patterns:
                if re.search(pattern, line):
                    validation_results["has_log_level"] = True
                    break

            # Check if line has meaningful content (not just whitespace)
            if line.strip() and len(line.strip()) > 5:
                validation_results["has_message"] = True

        return validation_results

    def _validate_debugging_information(self, logs, pod_name, container_name):
        """
        Validate that debugging information exists in logs

        Args:
            logs (str): Log content from the pod
            pod_name (str): Name of the pod
            container_name (str): Name of the container

        Returns:
            dict: Debugging information validation results
        """
        debug_info = {
            "has_startup_info": False,
            "has_health_check": False,
            "has_error_messages": False,
            "has_warning_messages": False,
            "has_info_messages": False,
            "has_stack_trace": False,
            "error_count": 0,
            "warning_count": 0,
            "info_count": 0,
            "errors": [],
            "warnings": [],
        }

        if not logs or not logs.strip():
            return debug_info

        log_content = logs.lower()

        # Check for startup information
        startup_keywords = [
            "starting",
            "initialized",
            "listening",
            "ready",
            "server started",
            "csi-addons",
            "version",
        ]
        for keyword in startup_keywords:
            if keyword in log_content:
                debug_info["has_startup_info"] = True
                break

        # Check for health check information
        health_keywords = [
            "health",
            "healthz",
            "liveness",
            "readiness",
            "probe",
            "status",
        ]
        for keyword in health_keywords:
            if keyword in log_content:
                debug_info["has_health_check"] = True
                break

        # Count and collect error messages
        error_patterns = [
            r"(?i)\berror\b",
            r"(?i)\bfatal\b",
            r"(?i)\bpanic\b",
            r'"level":"error"',
            r'"severity":"error"',
        ]
        for pattern in error_patterns:
            matches = re.findall(pattern, logs)
            if matches:
                debug_info["has_error_messages"] = True
                debug_info["error_count"] += len(matches)

        # Count and collect warning messages
        warning_patterns = [
            r"(?i)\bwarn\b",
            r"(?i)\bwarning\b",
            r'"level":"warn"',
            r'"severity":"warning"',
        ]
        for pattern in warning_patterns:
            matches = re.findall(pattern, logs)
            if matches:
                debug_info["has_warning_messages"] = True
                debug_info["warning_count"] += len(matches)

        # Count info messages
        info_patterns = [
            r"(?i)\binfo\b",
            r'"level":"info"',
            r'"severity":"info"',
        ]
        for pattern in info_patterns:
            matches = re.findall(pattern, logs)
            if matches:
                debug_info["has_info_messages"] = True
                debug_info["info_count"] += len(matches)

        # Check for stack traces
        stack_trace_patterns = [
            r"stack trace",
            r"goroutine",
            r"panic:",
            r"at .*\(.*\)",
            r"runtime\.",
        ]
        for pattern in stack_trace_patterns:
            if re.search(pattern, logs, re.IGNORECASE):
                debug_info["has_stack_trace"] = True
                break

        # Extract actual error and warning lines for debugging
        log_lines = logs.split("\n")
        for line in log_lines:
            if re.search(r"(?i)\b(error|fatal|panic)\b", line):
                debug_info["errors"].append(line.strip()[:200])  # Limit length
            if re.search(r"(?i)\b(warn|warning)\b", line):
                debug_info["warnings"].append(line.strip()[:200])  # Limit length

        return debug_info

    @polarion_id("OCS-xxx")
    def test_csi_addons_daemonset_logs_validation(self):
        """
        Validate log format, content, and debugging information for CSI addons daemonset pods

        Steps:
        1. Get all CSI addons daemonset pods
        2. For each pod, get logs from the csi-addons container
        3. Validate log format (timestamp, log level, message structure)
        4. Validate debugging information exists (startup info, health checks, error/warning messages)
        5. Verify logs contain expected fields for troubleshooting

        Expected Results:
        - Logs should have proper format with timestamps and log levels
        - Logs should contain debugging information like startup messages, health checks
        - Error and warning messages should be properly logged
        """
        namespace = config.ENV_DATA["cluster_namespace"]
        log.info("Starting validation of CSI addons daemonset logs")

        # Get all CSI addons daemonset pods
        csi_addon_pods = get_pods_having_label(
            constants.CSI_RBD_ADDON_NODEPLUGIN_LABEL_420, namespace
        )

        assert len(csi_addon_pods) > 0, "No CSI addons daemonset pods found"

        log.info(f"Found {len(csi_addon_pods)} CSI addons daemonset pods")

        validation_failures = []

        # Validate logs for each pod
        for pod_data in csi_addon_pods:
            pod_obj = Pod(**pod_data)
            pod_name = pod_obj.name

            log.info(f"Validating logs for pod: {pod_name}")

            # Verify pod has csi-addons container
            csi_addon_container = pod_obj.get_container_data("csi-addons")
            assert (
                csi_addon_container
            ), f"Pod {pod_name} does not contain 'csi-addons' container"

            try:
                # Get logs from csi-addons container
                logs = get_pod_logs(
                    pod_name=pod_name,
                    container="csi-addons",
                    namespace=namespace,
                    tail=500,  # Get last 500 lines
                )

                log.info(
                    f"Retrieved {len(logs.split(chr(10)))} log lines from pod {pod_name}"
                )

                # Validate log format
                format_validation = self._validate_log_format(
                    logs, pod_name, "csi-addons"
                )

                # Validate debugging information
                debug_validation = self._validate_debugging_information(
                    logs, pod_name, "csi-addons"
                )

                # Compile validation results
                log.info(f"Log format validation for {pod_name}:")
                log.info(f"  - Has timestamp: {format_validation['has_timestamp']}")
                log.info(f"  - Has log level: {format_validation['has_log_level']}")
                log.info(f"  - Has message: {format_validation['has_message']}")

                log.info(f"Debugging information validation for {pod_name}:")
                log.info(
                    f"  - Has startup info: {debug_validation['has_startup_info']}"
                )
                log.info(
                    f"  - Has health check: {debug_validation['has_health_check']}"
                )
                log.info(f"  - Error count: {debug_validation['error_count']}")
                log.info(f"  - Warning count: {debug_validation['warning_count']}")
                log.info(f"  - Info count: {debug_validation['info_count']}")

                # Assertions for log format
                if not format_validation["has_message"]:
                    validation_failures.append(
                        f"Pod {pod_name}: Logs do not contain meaningful messages"
                    )

                # Assertions for debugging information
                if not debug_validation["has_startup_info"]:
                    validation_failures.append(
                        f"Pod {pod_name}: Logs do not contain startup information"
                    )

                # Log errors and warnings found (for debugging purposes)
                if debug_validation["errors"]:
                    log.warning(
                        f"Found {len(debug_validation['errors'])} error messages in {pod_name}"
                    )
                    for error in debug_validation["errors"][:5]:  # Log first 5
                        log.warning(f"  Error: {error}")

                if debug_validation["warnings"]:
                    log.info(
                        f"Found {len(debug_validation['warnings'])} warning messages in {pod_name}"
                    )
                    for warning in debug_validation["warnings"][:5]:  # Log first 5
                        log.info(f"  Warning: {warning}")

            except Exception as e:
                validation_failures.append(
                    f"Pod {pod_name}: Failed to retrieve or validate logs - {str(e)}"
                )
                log.error(f"Error validating logs for pod {pod_name}: {str(e)}")

        # Fail test if any validation failed
        if validation_failures:
            error_msg = "Log validation failures:\n" + "\n".join(validation_failures)
            log.error(error_msg)
            pytest.fail(error_msg)

        log.info("CSI addons daemonset logs validation completed successfully")

    @polarion_id("OCS-xxx")
    def test_csi_addons_provisioner_pod_logs_validation(self):
        """
        Validate log format, content, and debugging information for CSI addons provisioner pods

        Steps:
        1. Get all CSI provisioner pods (RBD and CephFS)
        2. For each pod, check if it has csi-addons container
        3. Get logs from the csi-addons container
        4. Validate log format and debugging information
        5. Verify logs contain expected fields for troubleshooting

        Expected Results:
        - Provisioner pods with csi-addons container should have proper log format
        - Logs should contain debugging information
        - Error and warning messages should be properly logged
        """
        namespace = config.ENV_DATA["cluster_namespace"]
        log.info("Starting validation of CSI addons provisioner pod logs")

        # Get provisioner pods for both RBD and CephFS
        rbd_provisioner_pods = get_rbdfsplugin_provisioner_pods(namespace=namespace)
        cephfs_provisioner_pods = get_cephfsplugin_provisioner_pods(namespace=namespace)

        all_provisioner_pods = rbd_provisioner_pods + cephfs_provisioner_pods

        assert len(all_provisioner_pods) > 0, "No CSI provisioner pods found"

        log.info(
            f"Found {len(all_provisioner_pods)} provisioner pods "
            f"({len(rbd_provisioner_pods)} RBD, {len(cephfs_provisioner_pods)} CephFS)"
        )

        validation_failures = []
        pods_with_csi_addons = []

        # Check each provisioner pod for csi-addons container
        for pod_obj in all_provisioner_pods:
            pod_name = pod_obj.name
            csi_addon_container = pod_obj.get_container_data("csi-addons")

            if csi_addon_container:
                pods_with_csi_addons.append(pod_obj)
                log.info(
                    f"Pod {pod_name} contains csi-addons container, will validate logs"
                )

        if not pods_with_csi_addons:
            log.warning(
                "No provisioner pods found with csi-addons container. "
                "This might be expected if csi-addons runs only in daemonset."
            )
            pytest.skip(
                "No provisioner pods with csi-addons container found. "
                "Skipping provisioner pod log validation."
            )

        # Validate logs for each pod with csi-addons container
        for pod_obj in pods_with_csi_addons:
            pod_name = pod_obj.name

            log.info(f"Validating logs for csi-addons provisioner pod: {pod_name}")

            try:
                # Get logs from csi-addons container
                logs = get_pod_logs(
                    pod_name=pod_name,
                    container="csi-addons",
                    namespace=namespace,
                    tail=500,  # Get last 500 lines
                )

                log.info(
                    f"Retrieved {len(logs.split(chr(10)))} log lines from pod {pod_name}"
                )

                # Validate log format
                format_validation = self._validate_log_format(
                    logs, pod_name, "csi-addons"
                )

                # Validate debugging information
                debug_validation = self._validate_debugging_information(
                    logs, pod_name, "csi-addons"
                )

                # Compile validation results
                log.info(f"Log format validation for {pod_name}:")
                log.info(f"  - Has timestamp: {format_validation['has_timestamp']}")
                log.info(f"  - Has log level: {format_validation['has_log_level']}")
                log.info(f"  - Has message: {format_validation['has_message']}")

                log.info(f"Debugging information validation for {pod_name}:")
                log.info(
                    f"  - Has startup info: {debug_validation['has_startup_info']}"
                )
                log.info(
                    f"  - Has health check: {debug_validation['has_health_check']}"
                )
                log.info(f"  - Error count: {debug_validation['error_count']}")
                log.info(f"  - Warning count: {debug_validation['warning_count']}")
                log.info(f"  - Info count: {debug_validation['info_count']}")

                # Assertions for log format
                if not format_validation["has_message"]:
                    validation_failures.append(
                        f"Pod {pod_name}: Logs do not contain meaningful messages"
                    )

                # Assertions for debugging information
                if not debug_validation["has_startup_info"]:
                    validation_failures.append(
                        f"Pod {pod_name}: Logs do not contain startup information"
                    )

                # Log errors and warnings found
                if debug_validation["errors"]:
                    log.warning(
                        f"Found {len(debug_validation['errors'])} error messages in {pod_name}"
                    )
                    for error in debug_validation["errors"][:5]:
                        log.warning(f"  Error: {error}")

                if debug_validation["warnings"]:
                    log.info(
                        f"Found {len(debug_validation['warnings'])} warning messages in {pod_name}"
                    )
                    for warning in debug_validation["warnings"][:5]:
                        log.info(f"  Warning: {warning}")

            except Exception as e:
                validation_failures.append(
                    f"Pod {pod_name}: Failed to retrieve or validate logs - {str(e)}"
                )
                log.error(f"Error validating logs for pod {pod_name}: {str(e)}")

        # Fail test if any validation failed
        if validation_failures:
            error_msg = "Log validation failures:\n" + "\n".join(validation_failures)
            log.error(error_msg)
            pytest.fail(error_msg)

        log.info("CSI addons provisioner pod logs validation completed successfully")
