import os
import logging
import yaml
from ocs_ci.framework import config

log = logging.getLogger(__name__)


class KrknWorkloadConfig:
    """
    Utility class to read and manage Krkn chaos workload configuration.

    This class reads the krkn_chaos_config.yaml file and provides methods
    to determine which workload type should be used for chaos testing.
    """

    # Supported workload types
    VDBENCH = "VDBENCH"
    CNV_WORKLOAD = "CNV_WORKLOAD"
    GOSBENCH = "GOSBENCH"
    FIO = "FIO"

    # Default workload type
    DEFAULT_WORKLOAD = VDBENCH

    def __init__(self, config_file_path=None):
        """
        Initialize the workload configuration reader.

        Args:
            config_file_path (str, optional): Path to krkn_chaos_config.yaml.
                                            If None, uses default location.
        """
        self.config_file_path = config_file_path or self._get_default_config_path()
        self._ensure_krkn_config_loaded()

    def _ensure_krkn_config_loaded(self):
        """
        Ensure krkn_config is loaded into config.ENV_DATA.

        Following the same pattern as sc_encryption.yaml:
        1. First check if krkn_config already exists in config.ENV_DATA (from Jenkins exports)
        2. If not, load the config file and merge it into config.ENV_DATA
        3. If config file doesn't exist, set defaults
        """
        # Check if krkn_config is already set in ENV_DATA (from Jenkins exports)
        if config.ENV_DATA.get("krkn_config"):
            log.info(
                "Using krkn_config from runtime ENV_DATA (Jenkins exports or test configuration)"
            )
            return

        # Load from config file if it exists
        if os.path.exists(self.config_file_path):
            try:
                log.info(f"Loading Krkn chaos config from {self.config_file_path}")
                with open(self.config_file_path, "r") as f:
                    config_data = yaml.safe_load(f)
                    if config_data and config_data.get("ENV_DATA", {}).get(
                        "krkn_config"
                    ):
                        # Merge the krkn_config into the global config.ENV_DATA
                        config.ENV_DATA["krkn_config"] = config_data["ENV_DATA"][
                            "krkn_config"
                        ]
                        log.info("Merged krkn_config from config file into ENV_DATA")
                    else:
                        log.warning(
                            "Config file exists but no krkn_config found, using defaults"
                        )
                        self._set_default_krkn_config()
            except Exception as e:
                log.error(f"Error loading Krkn chaos config: {e}")
                log.warning("Using default krkn_config")
                self._set_default_krkn_config()
        else:
            log.warning(
                f"Krkn chaos config file not found at {self.config_file_path}. "
                "Using default krkn_config"
            )
            self._set_default_krkn_config()

    def _set_default_krkn_config(self):
        """Set default krkn_config in ENV_DATA."""
        config.ENV_DATA["krkn_config"] = {
            "workloads": [self.DEFAULT_WORKLOAD],  # Support both old and new format
            "enable_verification": True,
            "run_workload": True,  # Default to running workloads
        }

    def _get_default_config_path(self):
        """Get the default path to krkn_chaos_config.yaml."""
        return os.path.join(
            config.ENV_DATA.get("cluster_path", ""), "conf/ocsci/krkn_chaos_config.yaml"
        )

    def get_workload_types(self):
        """
        Get the configured workload types (supports both single and multiple workloads).

        Returns:
            list: List of workload types (VDBENCH, CNV_WORKLOAD, GOSBENCH, FIO, etc.)
        """
        krkn_config = config.ENV_DATA.get("krkn_config", {})

        # Support both old format (workload) and new format (workloads)
        if "workloads" in krkn_config:
            workloads = krkn_config["workloads"]
            # Handle both single string and list formats
            if isinstance(workloads, str):
                workload_types = [workloads]
            elif isinstance(workloads, list):
                workload_types = workloads
            else:
                log.warning(f"Invalid workloads format: {workloads}, using default")
                workload_types = [self.DEFAULT_WORKLOAD]
        elif "workload" in krkn_config:
            # Backward compatibility with old format
            workload_types = [krkn_config["workload"]]
        else:
            workload_types = [self.DEFAULT_WORKLOAD]

        log.info(f"Using workload types: {workload_types}")
        return workload_types

    def get_workload_type(self):
        """
        Get the first configured workload type (for backward compatibility).

        Returns:
            str: First workload type (VDBENCH, CNV_WORKLOAD, GOSBENCH, FIO, etc.)
        """
        workload_types = self.get_workload_types()
        return workload_types[0] if workload_types else self.DEFAULT_WORKLOAD

    def is_vdbench_workload(self):
        """Check if VDBENCH workload is configured."""
        return self.VDBENCH in self.get_workload_types()

    def is_cnv_workload(self):
        """Check if CNV_WORKLOAD is configured."""
        return self.CNV_WORKLOAD in self.get_workload_types()

    def is_gosbench_workload(self):
        """Check if GOSBENCH workload is configured."""
        return self.GOSBENCH in self.get_workload_types()

    def is_fio_workload(self):
        """Check if FIO workload is configured."""
        return self.FIO in self.get_workload_types()

    def get_workload_config(self):
        """
        Get additional workload-specific configuration.

        Returns:
            dict: Workload-specific configuration parameters.
        """
        return config.ENV_DATA.get("krkn_config", {})

    def is_verification_enabled(self):
        """
        Check if post-chaos verification is enabled.

        Returns:
            bool: True if verification is enabled, False otherwise
        """
        krkn_config = config.ENV_DATA.get("krkn_config", {})
        return krkn_config.get("enable_verification", True)  # Default to True

    def should_run_workload(self):
        """
        Check if workloads should be executed during chaos testing.

        Returns:
            bool: True if workloads should run, False for pure chaos testing
        """
        krkn_config = config.ENV_DATA.get("krkn_config", {})
        return krkn_config.get("run_workload", True)  # Default to True

    def should_run_verification(self):
        """
        Determine if verification should run based on workload types and config.

        Verification is only supported for VDBENCH workloads and must be enabled.
        GOSBENCH and other workloads skip verification.

        Returns:
            bool: True if verification should run, False otherwise
        """
        if not self.is_verification_enabled():
            log.info("Post-chaos verification disabled in configuration")
            return False

        workload_types = self.get_workload_types()
        supported_verification_types = [
            self.VDBENCH
        ]  # Only VDBENCH supports verification

        # Check if any workload supports verification
        verification_supported = any(
            wl_type in supported_verification_types for wl_type in workload_types
        )

        if not verification_supported:
            log.info(
                f"Post-chaos verification not supported for workload types: {workload_types}"
            )
            return False

        log.info(
            f"Post-chaos verification enabled for workload types: {workload_types}"
        )
        return True

    def get_verification_workloads(self):
        """
        Get workload types that support verification.

        Returns:
            list: List of workload types that support verification
        """
        workload_types = self.get_workload_types()
        supported_verification_types = [
            self.VDBENCH
        ]  # Only VDBENCH supports verification

        return [
            wl_type
            for wl_type in workload_types
            if wl_type in supported_verification_types
        ]

    @classmethod
    def get_supported_workloads(cls):
        """Get list of supported workload types."""
        return [cls.VDBENCH, cls.CNV_WORKLOAD, cls.GOSBENCH, cls.FIO]

    def validate_workload_types(self):
        """
        Validate that all configured workload types are supported.

        Returns:
            bool: True if all workload types are supported, False otherwise.
        """
        workload_types = self.get_workload_types()
        supported_workloads = self.get_supported_workloads()

        unsupported = [wl for wl in workload_types if wl not in supported_workloads]

        if unsupported:
            log.warning(
                f"Unsupported workload types: {unsupported}. "
                f"Supported types: {supported_workloads}"
            )
            return False

        return True

    def validate_workload_type(self):
        """
        Validate that the configured workload type is supported (backward compatibility).

        Returns:
            bool: True if workload type is supported, False otherwise.
        """
        return self.validate_workload_types()
