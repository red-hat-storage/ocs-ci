import logging
import os
import stat
from pathlib import Path

from tempfile import NamedTemporaryFile

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed, ConfigurationError
from ocs_ci.utility.utils import download_file, exec_cmd

logger = logging.getLogger(__name__)


def generate_onboarding_token(
    private_key: str = None,
    use_ticketgen_with_quota: bool = False,
    subject_role: str = None,
    storage_quota: int = None,
):
    """
    Generate Onboarding token for consumer cluster via following steps:

    1. Download ticketgen.sh script from:
        https://raw.githubusercontent.com/red-hat-storage/ocs-operator/main/hack/ticketgen/ticketgen.sh
    2. Save private key from AUTH["managed_service"]["private_key"] to
        temporary file.
    3. Run ticketgen.sh script to generate Onboarding token.

    Important! The header and footer of the private key are rewritten to make the ticketgen.sh script work on older
     openssl version OpenSSL 1.1.1k, which is the last version that was supported by CentOS 8 in the day of this comment

    Args:
        private_key (str): private key for Managed Service
        use_ticketgen_with_quota (bool): If True, the ticketgen.sh script will be run with -q flag
        subject_role (str): Role of the subject, this role has a default in ticketgen-with-quota.sh - ocs-client
        storage_quota (int): Quota for the storage cluster

    Raises:
        CommandFailed: In case the script ticketgen.sh fails.
        ConfigurationError: when AUTH["managed_service"]["private_key"] not is not defined

    Returns:
        string: Onboarding token

    """
    logger.debug("Generate onboarding token for ODF to ODF deployment")
    ticketgen_script_path = os.path.join(constants.DATA_DIR, "ticketgen.sh")

    if not use_ticketgen_with_quota:
        logger.debug("Download and prepare ticketgen.sh script")
        download_file(
            "https://raw.githubusercontent.com/red-hat-storage/ocs-operator/main/hack/ticketgen/ticketgen.sh",
            ticketgen_script_path,
        )
        if not Path(ticketgen_script_path).exists():
            raise FileNotFoundError(
                f"Failed to download ticketgen.sh script to {ticketgen_script_path}"
            )
    else:
        logger.info("using ticketgen.sh script with --quota flag")
        script_path = (
            Path(__file__).resolve().parents[2]
            / "scripts"
            / "bash"
            / "ticketgen-with-quota.sh"
        )
        if not script_path.exists():
            raise FileNotFoundError(
                f"Failed to find ticketgen-with-quota.sh script in {script_path}"
            )
        ticketgen_script_path = str(script_path)

    # add execute permission to the ticketgen.sh script
    current_file_permissions = os.stat(ticketgen_script_path)
    os.chmod(
        ticketgen_script_path,
        current_file_permissions.st_mode | stat.S_IEXEC,
    )
    # save private key to temp file
    logger.debug("Prepare temporary file with private key")
    if not private_key:
        private_key = config.AUTH.get("managed_service", {}).get("private_key", "")

    # rewrite header and footer of private key to make it work on older openssl version
    key_tmp = remove_header_footer_from_key(private_key)
    private_key = add_header_footer_to_key(key_tmp)

    if not private_key:
        raise ConfigurationError(
            "Private key for Managed Service not defined.\n"
            "Expected following configuration in auth.yaml file:\n"
            "managed_service:\n"
            '  private_key: "..."\n'
            '  public_key: "..."'
        )

    script_cmd = f"{ticketgen_script_path}"

    if use_ticketgen_with_quota:

        from ocs_ci.ocs.ocp import OCP

        sc_obj = OCP(
            kind=constants.STORAGECLUSTER,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        uid = sc_obj.get()["items"][0]["metadata"]["uid"]
        script_cmd += f" -c {uid} "

        if subject_role:
            script_cmd += f" -r {subject_role}"
        if storage_quota:
            script_cmd += f" -q {storage_quota}"

    with NamedTemporaryFile(
        mode="w", prefix="private", suffix=".pem", delete=True
    ) as key_file:
        key_file.write(private_key)
        key_file.flush()
        logger.debug("Generate Onboarding token")
        script_cmd += f" {key_file.name}"
        ticketgen_result = exec_cmd(script_cmd)

    ticketgen_output = ticketgen_result.stdout.decode()
    if ticketgen_result.stderr:
        raise CommandFailed(
            f"Script ticketgen.sh failed to generate Onboarding token:\n"
            f"command: '{' '.join(ticketgen_result.args)}'\n"
            f"stderr: {ticketgen_result.stderr.decode()}\n"
            f"stdout: {ticketgen_output}"
        )
    return str(ticketgen_output).strip()


def remove_header_footer_from_key(key):
    """
    This function will remove header and footer from key (like:
    -----BEGIN RSA PRIVATE KEY-----
    -----END RSA PRIVATE KEY-----
    ) and return the key on one line.

    Returns:
        string: one line key string without header and footer

    """
    key_lines = key.strip().split("\n")
    if "-----BEGIN" in key_lines[0]:
        key_lines = key_lines[1:]
    if "-----END" in key_lines[-1]:
        key_lines = key_lines[:-1]
    return "".join(key_lines)


def add_header_footer_to_key(key):
    """
    This function will add header and footer to key (like:
    -----BEGIN RSA PRIVATE KEY-----
    -----END RSA PRIVATE KEY-----
    ) and return the key with header and footer.

    Returns:
        string: key string with header and footer

    """

    key = key.strip()
    key = f"-----BEGIN RSA PRIVATE KEY-----\n{key}\n-----END RSA PRIVATE KEY-----"
    return key


def get_storage_provider_endpoint(cluster):
    """
    Get get_storage_provider_endpoint

    Args:
        cluster (str): cluster name

    Returns:
        str: value of storage provider endpoint

    """

    # TODO: p2 task to implement below functionality
    #  Use multicluster implementation to use
    #  kubeconfig as per cluster name and
    #  extract value of storage_provider_endpoint
    #  handle invalid cluster name in implementation
    #  validate Return String storage provider endpoint:
    #     1. raise Error if storage_provider_endpoint is
    #        not found in cluster yaml
    #     2. warning if storage cluster is not ready
    #     and storage_provider_endpoint is available in
    #     storagecluster yaml .
    #  For now use hardcoded value from config with key
    #  storage_provider_endpoint:
    return config.DEPLOYMENT.get("storage_provider_endpoint", "")
