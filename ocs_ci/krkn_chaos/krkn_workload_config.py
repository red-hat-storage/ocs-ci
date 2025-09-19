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
            "workload": self.DEFAULT_WORKLOAD,
            "enable_verification": True,
        }

    def _get_default_config_path(self):
        """Get the default path to krkn_chaos_config.yaml."""
        return os.path.join(
            config.ENV_DATA.get("cluster_path", ""), "conf/ocsci/krkn_chaos_config.yaml"
        )

    def get_workload_type(self):
        """
        Get the configured workload type.

        Returns:
            str: Workload type (VDBENCH, CNV_WORKLOAD, FIO, etc.)
        """
        krkn_config = config.ENV_DATA.get("krkn_config", {})
        workload_type = krkn_config.get("workload", self.DEFAULT_WORKLOAD)
        log.info(f"Using workload type: {workload_type}")
        return workload_type

    def is_vdbench_workload(self):
        """Check if VDBENCH workload is configured."""
        return self.get_workload_type() == self.VDBENCH

    def is_cnv_workload(self):
        """Check if CNV_WORKLOAD is configured."""
        return self.get_workload_type() == self.CNV_WORKLOAD

    def is_fio_workload(self):
        """Check if FIO workload is configured."""
        return self.get_workload_type() == self.FIO

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

    def should_run_verification(self):
        """
        Determine if verification should run based on workload type and config.

        Verification is only supported for VDBENCH workloads and must be enabled.

        Returns:
            bool: True if verification should run, False otherwise
        """
        if not self.is_verification_enabled():
            log.info("Post-chaos verification disabled in configuration")
            return False

        if self.get_workload_type() != self.VDBENCH:
            log.info(
                f"Post-chaos verification not supported for {self.get_workload_type()} workloads"
            )
            return False

        log.info("Post-chaos verification enabled for VDBENCH workloads")
        return True

    @classmethod
    def get_supported_workloads(cls):
        """Get list of supported workload types."""
        return [cls.VDBENCH, cls.CNV_WORKLOAD, cls.FIO]

    def validate_workload_type(self):
        """
        Validate that the configured workload type is supported.

        Returns:
            bool: True if workload type is supported, False otherwise.
        """
        workload_type = self.get_workload_type()
        supported = workload_type in self.get_supported_workloads()

        if not supported:
            log.warning(
                f"Unsupported workload type '{workload_type}'. "
                f"Supported types: {self.get_supported_workloads()}"
            )

        return supported
