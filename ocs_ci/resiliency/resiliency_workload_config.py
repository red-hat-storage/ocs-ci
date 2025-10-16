"""
Configuration loader for resiliency workload testing.

This module provides a configuration management system for resiliency tests,
similar to the KrknWorkloadConfig used in krkn chaos tests.
"""

import logging
from typing import Dict, Any, List

from ocs_ci.framework import config

log = logging.getLogger(__name__)


class ResiliencyWorkloadConfig:
    """
    Configuration loader for resiliency workload testing.

    This class accesses configuration from the framework's config system
    and provides methods to access workload-specific settings.

    Configuration should be passed via --ocsci-conf parameter:
        pytest --ocsci-conf conf/ocsci/resiliency_tests_config.yaml ...

    The framework loads and merges all configs during initialization,
    so by the time this class is instantiated, config.ENV_DATA already
    contains the merged configuration.
    """

    def __init__(self):
        """
        Initialize ResiliencyWorkloadConfig.
        Uses the framework's global config object.

        Note: Config should already be loaded via --ocsci-conf parameter.
        This class just provides convenience methods to access resiliency_config.
        """
        self.config = config

    def get_workloads(self) -> List[str]:
        """
        Get list of enabled workload types.

        Returns:
            list: List of workload type strings
        """
        resiliency_config = self.config.ENV_DATA.get("resiliency_config", {})
        workloads = resiliency_config.get("workloads", ["VDBENCH"])

        if isinstance(workloads, str):
            return [workloads]
        return workloads

    def should_run_workload(self) -> bool:
        """
        Check if workloads should be run during resiliency testing.

        Returns:
            bool: True if workloads should be run
        """
        resiliency_config = self.config.ENV_DATA.get("resiliency_config", {})
        return resiliency_config.get("run_workload", True)

    def should_run_verification(self) -> bool:
        """
        Check if verification should be run after failure injection.

        Returns:
            bool: True if verification should be run
        """
        resiliency_config = self.config.ENV_DATA.get("resiliency_config", {})
        return resiliency_config.get("enable_verification", True)

    def get_vdbench_config(self) -> Dict[str, Any]:
        """
        Get VDBENCH configuration.

        Returns:
            dict: VDBENCH configuration dictionary
        """
        resiliency_config = self.config.ENV_DATA.get("resiliency_config", {})
        return resiliency_config.get("vdbench_config", {})

    def get_gosbench_config(self) -> Dict[str, Any]:
        """
        Get GOSBENCH configuration.

        Returns:
            dict: GOSBENCH configuration dictionary
        """
        resiliency_config = self.config.ENV_DATA.get("resiliency_config", {})
        return resiliency_config.get("gosbench_config", {})

    def get_background_operations_config(self) -> Dict[str, Any]:
        """
        Get background cluster operations configuration.

        Returns:
            dict: Background operations configuration
        """
        resiliency_config = self.config.ENV_DATA.get("resiliency_config", {})
        return resiliency_config.get("background_cluster_operations", {})

    def get_scaling_config(self) -> Dict[str, Any]:
        """
        Get workload scaling configuration.

        Returns:
            dict: Scaling configuration
        """
        resiliency_config = self.config.ENV_DATA.get("resiliency_config", {})
        return resiliency_config.get("scaling_config", {})

    def is_scaling_enabled(self) -> bool:
        """
        Check if workload scaling is enabled.

        Returns:
            bool: True if scaling is enabled
        """
        scaling_config = self.get_scaling_config()
        return scaling_config.get("enabled", False)

    def get_scaling_min_replicas(self) -> int:
        """
        Get minimum replica count for scaling.

        Returns:
            int: Minimum replicas
        """
        scaling_config = self.get_scaling_config()
        return scaling_config.get("min_replicas", 1)

    def get_scaling_max_replicas(self) -> int:
        """
        Get maximum replica count for scaling.

        Returns:
            int: Maximum replicas
        """
        scaling_config = self.get_scaling_config()
        return scaling_config.get("max_replicas", 5)

    def get_scaling_delay(self) -> int:
        """
        Get scaling delay in seconds.

        Returns:
            int: Delay in seconds before starting scaling operations
        """
        scaling_config = self.get_scaling_config()
        return scaling_config.get("delay", 30)

    def get_vdbench_threads(self) -> int:
        """
        Get default thread count for VDBENCH.

        Returns:
            int: Thread count
        """
        vdbench_config = self.get_vdbench_config()
        return vdbench_config.get("threads", 10)

    def get_vdbench_elapsed(self) -> int:
        """
        Get elapsed time for VDBENCH workloads.

        Returns:
            int: Elapsed time in seconds
        """
        vdbench_config = self.get_vdbench_config()
        return vdbench_config.get("elapsed", 600)

    def get_vdbench_interval(self) -> int:
        """
        Get reporting interval for VDBENCH.

        Returns:
            int: Interval in seconds
        """
        vdbench_config = self.get_vdbench_config()
        return vdbench_config.get("interval", 60)

    def get_vdbench_block_config(self) -> Dict[str, Any]:
        """
        Get VDBENCH block configuration.

        Returns:
            dict: Block configuration
        """
        vdbench_config = self.get_vdbench_config()
        return vdbench_config.get("block", {})

    def get_vdbench_filesystem_config(self) -> Dict[str, Any]:
        """
        Get VDBENCH filesystem configuration.

        Returns:
            dict: Filesystem configuration
        """
        vdbench_config = self.get_vdbench_config()
        return vdbench_config.get("filesystem", {})

    def get_vdbench_block_patterns(self) -> List[Dict[str, Any]]:
        """
        Get VDBENCH block I/O patterns.

        Returns:
            list: List of pattern configurations
        """
        block_config = self.get_vdbench_block_config()
        return block_config.get("patterns", [])

    def get_vdbench_filesystem_patterns(self) -> List[Dict[str, Any]]:
        """
        Get VDBENCH filesystem I/O patterns.

        Returns:
            list: List of pattern configurations
        """
        filesystem_config = self.get_vdbench_filesystem_config()
        return filesystem_config.get("patterns", [])

    def get_config_summary(self) -> Dict[str, Any]:
        """
        Get a summary of the current configuration.

        Returns:
            dict: Configuration summary
        """
        return {
            "workloads": self.get_workloads(),
            "run_workload": self.should_run_workload(),
            "enable_verification": self.should_run_verification(),
            "scaling_enabled": self.is_scaling_enabled(),
            "vdbench_threads": self.get_vdbench_threads(),
            "vdbench_elapsed": self.get_vdbench_elapsed(),
            "background_operations_enabled": self.get_background_operations_config().get(
                "enabled", False
            ),
        }

    def validate_config(self) -> bool:
        """
        Validate the current configuration.

        Returns:
            bool: True if configuration is valid

        Raises:
            ValueError: If configuration is invalid
        """
        try:
            # Check if resiliency_config exists
            resiliency_config = self.config.ENV_DATA.get("resiliency_config", {})
            if not resiliency_config:
                log.warning(
                    "Missing resiliency_config section in ENV_DATA - using defaults"
                )
                return True

            # Check required fields
            if not self.get_workloads():
                raise ValueError("No workloads configured")

            # Validate VDBENCH config if enabled
            if "VDBENCH" in self.get_workloads():
                vdbench_config = self.get_vdbench_config()
                if not vdbench_config:
                    log.warning("VDBENCH enabled but no vdbench_config found")
                    return True

                # Check block and filesystem configs
                block_config = vdbench_config.get("block", {})
                filesystem_config = vdbench_config.get("filesystem", {})

                if not block_config and not filesystem_config:
                    log.warning("No block or filesystem workload configuration found")

                # Validate patterns
                for workload_type in ["block", "filesystem"]:
                    config_section = vdbench_config.get(workload_type, {})
                    if config_section:
                        patterns = config_section.get("patterns", [])
                        if not patterns:
                            log.warning(
                                f"No patterns configured for {workload_type} workloads"
                            )

            return True

        except Exception as e:
            log.error(f"Configuration validation failed: {e}")
            raise

    # Workload type constants
    VDBENCH = "VDBENCH"
    GOSBENCH = "GOSBENCH"
    CNV_WORKLOAD = "CNV_WORKLOAD"
    FIO = "FIO"
