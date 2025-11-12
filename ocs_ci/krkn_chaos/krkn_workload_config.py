import logging
from typing import Dict, Any, List

from ocs_ci.framework import config

log = logging.getLogger(__name__)


class KrknWorkloadConfig:
    """
    Configuration loader for Krkn workload testing.

    This class accesses configuration from the framework's config system
    and provides methods to access workload-specific settings.

    Configuration should be passed via --ocsci-conf parameter:
        pytest --ocsci-conf conf/ocsci/krkn_chaos_config.yaml ...

    The framework loads and merges all configs during initialization,
    so by the time this class is instantiated, config.ENV_DATA already
    contains the merged configuration.
    """

    def __init__(self):
        """
        Initialize KrknWorkloadConfig.
        Uses the framework's global config object.

        Note: Config should already be loaded via --ocsci-conf parameter.
        This class just provides convenience methods to access krkn_config.
        """
        self.config = config

    def get_workloads(self) -> List[str]:
        """
        Get list of enabled workload types.

        Returns:
            list: List of workload type strings
        """
        krkn_config = self.config.ENV_DATA.get("krkn_config", {})
        workloads = krkn_config.get("workloads", ["VDBENCH"])

        if isinstance(workloads, str):
            return [workloads]
        return workloads

    def should_run_workload(self) -> bool:
        """
        Check if workloads should be run during chaos testing.

        Returns:
            bool: True if workloads should be run
        """
        krkn_config = self.config.ENV_DATA.get("krkn_config", {})
        return krkn_config.get("run_workload", True)

    def should_run_verification(self) -> bool:
        """
        Check if verification should be run after chaos testing.

        Returns:
            bool: True if verification should be run
        """
        krkn_config = self.config.ENV_DATA.get("krkn_config", {})
        return krkn_config.get("enable_verification", True)

    def get_vdbench_config(self) -> Dict[str, Any]:
        """
        Get VDBENCH configuration.

        Returns:
            dict: VDBENCH configuration
        """
        krkn_config = self.config.ENV_DATA.get("krkn_config", {})
        return krkn_config.get("vdbench_config", {})

    def get_rgw_config(self) -> Dict[str, Any]:
        """
        Get RGW configuration.

        Returns:
            dict: RGW configuration
        """
        krkn_config = self.config.ENV_DATA.get("krkn_config", {})
        return krkn_config.get("rgw_config", {})

    def get_cnv_config(self) -> Dict[str, Any]:
        """
        Get CNV configuration.

        Returns:
            dict: CNV configuration
        """
        krkn_config = self.config.ENV_DATA.get("krkn_config", {})
        return krkn_config.get("cnv_config", {})

    def get_background_cluster_operations_config(self) -> Dict[str, Any]:
        """
        Get background cluster operations configuration.

        Returns:
            dict: Background cluster operations configuration with keys:
                - enabled (bool): Whether background operations are enabled
                - operation_interval (int): Interval between operations in seconds
                - max_concurrent_operations (int): Maximum concurrent operations
                - enabled_operations (list): List of enabled operation types
        """
        krkn_config = self.config.ENV_DATA.get("krkn_config", {})
        return krkn_config.get("background_cluster_operations", {})

    def is_background_cluster_operations_enabled(self) -> bool:
        """
        Check if background cluster operations are enabled.

        Returns:
            bool: True if background cluster operations are enabled
        """
        bg_ops_config = self.get_background_cluster_operations_config()
        return bg_ops_config.get("enabled", False)

    def get_background_operations_interval(self) -> int:
        """
        Get background operations interval in seconds.

        Returns:
            int: Operation interval in seconds (default: 60)
        """
        bg_ops_config = self.get_background_cluster_operations_config()
        return bg_ops_config.get("operation_interval", 60)

    def get_background_operations_max_concurrent(self) -> int:
        """
        Get maximum concurrent background operations.

        Returns:
            int: Maximum concurrent operations (default: 3)
        """
        bg_ops_config = self.get_background_cluster_operations_config()
        return bg_ops_config.get("max_concurrent_operations", 3)

    def get_enabled_background_operations(self) -> List[str]:
        """
        Get list of enabled background operation types.

        Returns:
            list: List of enabled operation type names
        """
        bg_ops_config = self.get_background_cluster_operations_config()
        return bg_ops_config.get("enabled_operations", [])

    def is_parallel_verification_enabled(self) -> bool:
        """
        Check if parallel verification is enabled.

        Returns:
            bool: True if parallel verification is enabled
        """
        vdbench_config = self.get_vdbench_config()
        return vdbench_config.get("enable_parallel_verification", True)

    def get_max_verification_threads(self) -> int:
        """
        Get maximum number of verification threads.

        Returns:
            int: Maximum verification threads
        """
        vdbench_config = self.get_vdbench_config()
        return vdbench_config.get("max_verification_threads", 16)

    def get_max_workload_restarts(self) -> int:
        """
        Get maximum number of workload restarts.

        Returns:
            int: Maximum restarts
        """
        vdbench_config = self.get_vdbench_config()
        return vdbench_config.get("max_workload_restarts", 10)

    def get_workload_monitor_interval(self) -> int:
        """
        Get workload monitoring interval in seconds.

        Returns:
            int: Monitor interval in seconds
        """
        vdbench_config = self.get_vdbench_config()
        return vdbench_config.get("workload_monitor_interval", 30)

    def get_workload_loop(self) -> int:
        """
        Get number of times to loop/restart the workload.

        Returns:
            int: Number of loops (default: 1 for single run)
        """
        vdbench_config = self.get_vdbench_config()
        return vdbench_config.get("workload_loop", 1)

    def get_num_pvcs_per_interface(self) -> int:
        """
        Get number of PVCs to create per storage interface.

        Returns:
            int: Number of PVCs per interface (default: 4)
        """
        vdbench_config = self.get_vdbench_config()
        return vdbench_config.get("num_pvcs_per_interface", 4)

    def get_pvc_size(self) -> int:
        """
        Get PVC size in GiB.

        Returns:
            int: PVC size in GiB (default: 50)
        """
        vdbench_config = self.get_vdbench_config()
        return vdbench_config.get("pvc_size", 50)

    def get_block_workload_config(self) -> Dict[str, Any]:
        """
        Get block workload configuration.

        Returns:
            dict: Block workload configuration
        """
        vdbench_config = self.get_vdbench_config()
        return vdbench_config.get("block", {})

    def get_filesystem_workload_config(self) -> Dict[str, Any]:
        """
        Get filesystem workload configuration.

        Returns:
            dict: Filesystem workload configuration
        """
        vdbench_config = self.get_vdbench_config()
        return vdbench_config.get("filesystem", {})

    def get_workload_patterns(self, workload_type: str) -> List[Dict[str, Any]]:
        """
        Get workload patterns for a specific workload type.

        Args:
            workload_type: Type of workload ("block" or "filesystem")

        Returns:
            list: List of workload patterns
        """
        if workload_type == "block":
            config = self.get_block_workload_config()
        elif workload_type == "filesystem":
            config = self.get_filesystem_workload_config()
        else:
            raise ValueError(f"Unknown workload type: {workload_type}")

        return config.get("patterns", [])

    def get_workload_config_for_pattern(
        self, workload_type: str, pattern_name: str
    ) -> Dict[str, Any]:
        """
        Get configuration for a specific workload pattern.

        Args:
            workload_type: Type of workload ("block" or "filesystem")
            pattern_name: Name of the pattern

        Returns:
            dict: Pattern configuration

        Raises:
            ValueError: If pattern not found
        """
        patterns = self.get_workload_patterns(workload_type)
        for pattern in patterns:
            if pattern.get("name") == pattern_name:
                return pattern

        raise ValueError(
            f"Pattern '{pattern_name}' not found in {workload_type} workloads"
        )

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
            "parallel_verification": self.is_parallel_verification_enabled(),
            "max_restarts": self.get_max_workload_restarts(),
            "block_patterns": len(self.get_workload_patterns("block")),
            "filesystem_patterns": len(self.get_workload_patterns("filesystem")),
            "background_operations_enabled": self.is_background_cluster_operations_enabled(),
            "background_operations_count": len(
                self.get_enabled_background_operations()
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
            # Check if krkn_config exists
            krkn_config = self.config.ENV_DATA.get("krkn_config", {})
            if not krkn_config:
                raise ValueError("Missing krkn_config section in ENV_DATA")

            # Check required fields
            if not self.get_workloads():
                raise ValueError("No workloads configured")

            # Validate VDBENCH config if enabled
            if "VDBENCH" in self.get_workloads():
                vdbench_config = self.get_vdbench_config()
                if not vdbench_config:
                    raise ValueError("VDBENCH enabled but no vdbench_config found")

                # Check block and filesystem configs
                block_config = vdbench_config.get("block", {})
                filesystem_config = vdbench_config.get("filesystem", {})

                if not block_config and not filesystem_config:
                    raise ValueError(
                        "No block or filesystem workload configuration found"
                    )

                # Validate patterns
                for workload_type in ["block", "filesystem"]:
                    config_section = vdbench_config.get(workload_type, {})
                    if config_section:
                        patterns = config_section.get("patterns", [])
                        if not patterns:
                            raise ValueError(
                                f"No patterns configured for {workload_type} workloads"
                            )

            return True

        except Exception as e:
            log.error(f"Configuration validation failed: {e}")
            raise

    # Workload type constants
    VDBENCH = "VDBENCH"
    CNV_WORKLOAD = "CNV_WORKLOAD"
    RGW_WORKLOAD = "RGW_WORKLOAD"
