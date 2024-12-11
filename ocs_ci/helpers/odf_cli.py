import os
from stat import S_IEXEC
from logging import getLogger
from typing import Union

from ocs_ci.ocs.exceptions import NotSupportedException
from ocs_ci.utility.version import get_semantic_ocs_version_from_config, VERSION_4_15
from ocs_ci.utility.utils import exec_cmd
from ocs_ci.framework import config
from ocs_ci.deployment.ocp import download_pull_secret
from ocs_ci.ocs.constants import ODF_CLI_DEV_IMAGE


log = getLogger(__name__)


class ODFCLIRetriever:
    def __init__(self):
        self.semantic_version = get_semantic_ocs_version_from_config()
        self.local_cli_path = os.path.join(config.RUN["bin_dir"], "odf")

    def check_odf_cli_binary(self):
        """
        Check if the ODF CLI binary exists and is executable.

        Returns:
            bool: True if the binary exists and is executable, False otherwise.
        """
        return os.path.isfile(self.local_cli_path) and os.access(
            self.local_cli_path, os.X_OK
        )

    def retrieve_odf_cli_binary(self):
        """
        Download and set up the ODF-CLI binary.

        Raises:
            NotSupportedException: If ODF CLI is not supported on the current version or deployment.
        """
        self._validate_odf_cli_support()

        if not self.check_odf_cli_binary():
            image = self._get_odf_cli_image()
            self._extract_cli_binary(image)
            self._set_executable_permissions()
            self.add_cli_to_path()

        if not self.check_odf_cli_binary():
            raise RuntimeError(
                f"Failed to retrieve and set up ODF CLI binary at {self.local_cli_path}"
            )

        log.info(f"ODF CLI binary is ready at {self.local_cli_path}")

    def _validate_odf_cli_support(self):
        if self.semantic_version < VERSION_4_15:
            raise NotSupportedException(
                f"ODF CLI tool not supported on ODF {self.semantic_version}"
            )

    def _get_odf_cli_image(self, build_no: str = None):
        if build_no:
            return f"{ODF_CLI_DEV_IMAGE}:{build_no}"
        else:
            return f"{ODF_CLI_DEV_IMAGE}:latest-{self.semantic_version}"

    def _extract_cli_binary(self, image):
        pull_secret_path = download_pull_secret()
        local_cli_dir = os.path.dirname(self.local_cli_path)

        # Ensure the directory exists
        os.makedirs(local_cli_dir, exist_ok=True)

        exec_cmd(
            f"oc image extract --registry-config {pull_secret_path} "
            f"{image} --confirm "
            f"--path /usr/bin/odf:{local_cli_dir}"
        )

        if not os.path.exists(self.local_cli_path):
            raise FileNotFoundError(
                f"ODF CLI binary not found at {self.local_cli_path}"
            )

        log.info(f"Extracted ODF CLI binary to {self.local_cli_path}")

    def _set_executable_permissions(self):
        if not os.path.exists(self.local_cli_path):
            raise FileNotFoundError(
                f"ODF CLI binary not found at {self.local_cli_path}"
            )
        current_permissions = os.stat(self.local_cli_path).st_mode
        os.chmod(self.local_cli_path, current_permissions | S_IEXEC)
        log.info(f"Set executable permissions for {self.local_cli_path}")

    def _verify_cli_binary(self):
        if not self.check_odf_cli_binary():
            raise AssertionError(
                f"ODF CLI binary not found or not executable at {self.local_cli_path}"
            )

    def add_cli_to_path(self):
        """
        Add the directory containing the ODF CLI binary to the system PATH.
        """
        cli_dir = os.path.dirname(os.path.abspath(self.local_cli_path))
        current_path = os.environ.get("PATH", "")
        if cli_dir not in current_path:
            os.environ["PATH"] = f"{cli_dir}:{current_path}"
        log.info(f"Added {cli_dir} to PATH")
        log.info(f"Current PATH: {os.environ['PATH']}")


class ODFCliRunner:
    def __init__(self) -> None:
        self.binary_name = "odf"

    def run_command(self, command_args: Union[str, list]) -> str:
        # by default Operator namespace is set to 'openshift-storage' in ODF CLI,
        # when -n <storage_ns> is not passed the command will fail if the namespace is not 'openshift-storage'
        if isinstance(command_args, str):
            full_command = str(
                self.binary_name
                + f' -n {config.ENV_DATA["cluster_namespace"]} '
                + command_args
            )
        elif isinstance(command_args, list):
            full_command = " ".join(
                [self.binary_name, "-n", config.ENV_DATA["cluster_namespace"]]
                + command_args
            )

        output = exec_cmd(full_command)
        log.info(f"output type: {type(output)}")
        log.info(f"*Command output*: {output}")
        return output

    def run_help(self):
        return self.run_command(" help")

    def run_get_health(self):
        return self.run_command(" get health")

    def run_get_recovery_profile(self):
        return self.run_command(" get recovery-profile")

    def run_get_mon_endpoint(self):
        return self.run_command(" get mon-endpoints")

    def run_rook_restart(self):
        return self.run_command(" operator rook restart")

    def run_rook_set_log_level(self, log_level: str):
        assert log_level in (
            "DEBUG",
            "INFO",
            "WARNING",
            "ERROR",
        ), f"log level {log_level} is not supported"
        return self.run_command(f" operator rook set ROOK_LOG_LEVEL {log_level}")

    def run_set_ceph_log_level(self, service: str, log_level: str, subsystem: str):
        return self.run_command(
            f" set ceph log-level {service} {subsystem} {log_level}"
        )
