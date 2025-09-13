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
        self.config_data = self._load_config()

    def _get_default_config_path(self):
        """Get the default path to krkn_chaos_config.yaml."""
        return os.path.join(
            config.ENV_DATA.get("cluster_path", ""), "conf/ocsci/krkn_chaos_config.yaml"
        )

    def _load_config(self):
        """
        Load the krkn_chaos_config.yaml file.

        Returns:
            dict: Configuration data or empty dict if file not found.
        """
        if not os.path.exists(self.config_file_path):
            log.warning(
                f"Krkn chaos config file not found at {self.config_file_path}. "
                f"Using default workload: {self.DEFAULT_WORKLOAD}"
            )
            return {"ENV_DATA": {"workload": self.DEFAULT_WORKLOAD}}

        try:
            with open(self.config_file_path, "r") as f:
                config_data = yaml.safe_load(f)
                log.info(f"Loaded Krkn chaos config from {self.config_file_path}")
                return config_data or {}
        except Exception as e:
            log.error(f"Error loading Krkn chaos config: {e}")
            log.warning(f"Using default workload: {self.DEFAULT_WORKLOAD}")
            return {"ENV_DATA": {"workload": self.DEFAULT_WORKLOAD}}

    def get_workload_type(self):
        """
        Get the configured workload type.

        Returns:
            str: Workload type (VDBENCH, CNV_WORKLOAD, FIO, etc.)
        """
        workload_type = self.config_data.get("ENV_DATA", {}).get(
            "workload", self.DEFAULT_WORKLOAD
        )

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
        return self.config_data.get("ENV_DATA", {})

    def is_verification_enabled(self):
        """
        Check if post-chaos verification is enabled.

        Returns:
            bool: True if verification is enabled, False otherwise
        """
        env_data = self.config_data.get("ENV_DATA", {})
        return env_data.get("enable_verification", True)  # Default to True

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
