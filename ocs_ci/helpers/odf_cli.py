import os
import platform
from stat import S_IEXEC
from logging import getLogger
from typing import Union

from ocs_ci.ocs.exceptions import NotSupportedException
from ocs_ci.utility.version import get_semantic_ocs_version_from_config, VERSION_4_15
from ocs_ci.utility.utils import exec_cmd
from ocs_ci.framework import config
from ocs_ci.deployment.ocp import download_pull_secret
from ocs_ci.ocs.constants import (
    ODF_CLI_DEV_IMAGE,
    LOW_RECOVERY_OPS,
    BALANCED,
    HIGH_RECOVERY_OPS,
)


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

    def _get_odf_cli_image(self):
        return f"{ODF_CLI_DEV_IMAGE}:v{self.semantic_version}"

    def _get_architecture_path(self):
        """Get architecture-specific path for ODF CLI binary in the container image."""
        system = platform.system()
        machine = platform.machine()
        path = "/usr/share/odf/"

        if system == "Linux":
            path = os.path.join(path, "linux")
            if machine == "x86_64":
                path = os.path.join(path, "odf-amd64")
            elif machine == "ppc64le":
                path = os.path.join(path, "odf-ppc64le")
            elif machine == "s390x":
                path = os.path.join(path, "odf-s390x")
        elif system == "Darwin":  # Mac
            # For ODF CLI 4.20+, Mac has architecture-specific binaries
            path = os.path.join(path, "macosx")
            if machine == "arm64" or machine == "aarch64":  # Apple Silicon
                path = os.path.join(path, "odf-arm64")
            else:  # Intel Mac (x86_64/amd64)
                path = os.path.join(path, "odf-amd64")

        return path

    def _extract_cli_binary(self, image):
        pull_secret_path = download_pull_secret()
        local_cli_dir = os.path.dirname(self.local_cli_path)

        # Get architecture-specific path for odf-cli
        remote_path = self._get_architecture_path()
        remote_cli_basename = os.path.basename(remote_path)

        # Ensure the directory exists
        os.makedirs(local_cli_dir, exist_ok=True)

        exec_cmd(
            f"oc image extract --registry-config {pull_secret_path} "
            f"{image} --confirm "
            f"--path {remote_path}:{local_cli_dir}"
        )

        # For ODF CLI 4.20+, Mac binaries need to be renamed from odf-{arch} to odf
        extracted_path = os.path.join(local_cli_dir, remote_cli_basename)
        if os.path.exists(extracted_path) and extracted_path != self.local_cli_path:
            log.info(f"Renaming {extracted_path} to {self.local_cli_path}")
            os.rename(extracted_path, self.local_cli_path)

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

    def get_recovery_profile(self):
        """
        Retrieve the current recovery profile using the ODF CLI.

        Returns:
            str: The name of the current recovery profile (e.g., 'low_recovery_ops',
            'balanced', 'high_recovery_ops').

        Notes:
            If the CLI returns no output, the method logs a warning and returns the
            default profile 'balanced' as a fallback.

        """
        output = self.run_get_recovery_profile()
        str_output = output.stdout.decode().strip()
        if not str_output:
            log.warning(
                f"ODF CLI returned no recovery profile; Fall back to the default value {BALANCED}"
            )
            return BALANCED
        return str_output.split()[-1]

    def run_set_recovery_profile(self, profile_name):
        """
        Set the recovery profile using the ODF CLI.

        Args:
            profile_name (str): The name of the recovery profile to apply
                (e.g., 'low_recovery_ops', 'balanced', 'high_recovery_ops').

        Raises:
            CommandFailed: If the CLI command fails.

        """
        self.run_command(f" set recovery-profile {profile_name}")

    def run_set_recovery_profile_low(self):
        """
        Set the recovery profile to 'low_recovery_ops'.

        """
        return self.run_set_recovery_profile(LOW_RECOVERY_OPS)

    def run_set_recovery_profile_balanced(self):
        """
        Set the recovery profile to 'balanced'.

        """
        return self.run_set_recovery_profile(BALANCED)

    def run_set_recovery_profile_high(self):
        """
        Set the recovery profile to 'high_recovery_ops'.

        """
        return self.run_set_recovery_profile(HIGH_RECOVERY_OPS)

    def run_noobaa(
        self,
        command_args: Union[str, list],
        namespace: str = None,
        use_yes: bool = False,
        ignore_error: bool = False,
        **kwargs,
    ) -> str:
        """
        Run noobaa subcommand via odf-cli.

        Args:
            command_args: NooBaa command arguments (without 'noobaa' prefix)
                         Can be string or list
            namespace: Override default namespace (if provided)
            use_yes: If True, pipe 'yes' to the command for auto-confirmation
            ignore_error: If True, don't raise exception on non-zero exit
            **kwargs: Additional arguments to pass to exec_cmd

        Returns:
            Command output from exec_cmd

        Examples:
            run_noobaa("status")
            run_noobaa("obc list")
            run_noobaa(["bucket", "list"], namespace="my-namespace")
        """
        # Build the noobaa command
        if isinstance(command_args, str):
            noobaa_cmd = f"noobaa {command_args}"
        else:
            noobaa_cmd = " ".join(["noobaa"] + command_args)

        # Add namespace if provided (will override the default one in run_command)
        if namespace:
            noobaa_cmd += f" -n {namespace}"
            # Don't let run_command add its default namespace
            full_command = f"{self.binary_name} {noobaa_cmd}"
        else:
            # Let run_command add the default namespace
            full_command = f"{self.binary_name} -n {config.ENV_DATA['cluster_namespace']} {noobaa_cmd}"

        # Execute with appropriate method based on use_yes
        if use_yes:
            output = exec_cmd(
                [f"yes | {full_command}"],
                shell=True,
                ignore_error=ignore_error,
                **kwargs,
            )
        else:
            output = exec_cmd(full_command, ignore_error=ignore_error, **kwargs)

        log.info(f"output type: {type(output)}")
        log.info(f"*Command output*: {output}")
        return output


def odf_cli_setup_helper():
    """
    Initializes and returns an instance of ODFCliRunner.
    Downloads the ODF CLI binary if it does not exist.

    Returns:
        ODFCliRunner: The initialized runner.

    Raises:
        NotSupportedException: If ODF CLI is not supported on the current version or deployment.
        RuntimeError: If CLI binary download or ODFCliRunner initialization fails.

    """
    odf_cli_retriever = ODFCLIRetriever()

    # Check and download ODF CLI binary if needed
    if not odf_cli_retriever.check_odf_cli_binary():
        log.warning("ODF CLI binary not found. Attempting to download...")
        odf_cli_retriever.retrieve_odf_cli_binary()
        if not odf_cli_retriever.check_odf_cli_binary():
            raise RuntimeError("Failed to download ODF CLI binary")

    # Check and initialize ODFCliRunner
    odf_cli_runner = ODFCliRunner()
    if not odf_cli_runner:
        log.warning("ODFCliRunner not initialized. Attempting to initialize again...")
        odf_cli_runner = ODFCliRunner()
        if not odf_cli_runner:
            raise RuntimeError("Failed to initialize ODFCliRunner after retry")

    log.info("ODF CLI binary downloaded and ODFCliRunner initialized successfully")
    return odf_cli_runner
