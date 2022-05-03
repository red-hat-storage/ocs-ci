import logging
import os
import stat

from tempfile import NamedTemporaryFile

from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import CommandFailed, ConfigurationError
from ocs_ci.utility.utils import download_file, exec_cmd, TimeoutSampler

logger = logging.getLogger(__name__)


def generate_onboarding_token():
    """
    Generate Onboarding token for consumer cluster via following steps:

    1. Download ticketgen.sh script from:
        https://raw.githubusercontent.com/jarrpa/ocs-operator/ticketgen/hack/ticketgen/ticketgen.sh
    2. Save private key from AUTH["managed_service"]["private_key"] to
        temporary file.
    3. Run ticketgen.sh script to generate Onboarding token.

    Raises:
        CommandFailed: In case the script ticketgen.sh fails.
        ConfigurationError: when AUTH["managed_service"]["private_key"] not is not defined

    Returns:
        string: Onboarding token

    """
    logger.debug("Generate onboarding token for ODF to ODF deployment")
    ticketgen_script_path = os.path.join(constants.DATA_DIR, "ticketgen.sh")
    # download ticketgen.sh script
    logger.debug("Download and prepare ticketgen.sh script")
    download_file(
        "https://raw.githubusercontent.com/jarrpa/ocs-operator/ticketgen/hack/ticketgen/ticketgen.sh",
        ticketgen_script_path,
    )
    # add execute permission to the ticketgen.sh script
    current_file_permissions = os.stat(ticketgen_script_path)
    os.chmod(
        ticketgen_script_path,
        current_file_permissions.st_mode | stat.S_IEXEC,
    )
    # save private key to temp file
    logger.debug("Prepare temporary file with private key")
    private_key = config.AUTH.get("managed_service", {}).get("private_key", "")
    if not private_key:
        raise ConfigurationError(
            "Private key for Managed Service not defined.\n"
            "Expected following configuration in auth.yaml file:\n"
            "managed_service:\n"
            '  private_key: "..."\n'
            '  public_key: "..."'
        )
    with NamedTemporaryFile(
        mode="w", prefix="private", suffix=".pem", delete=True
    ) as key_file:
        key_file.write(private_key)
        key_file.flush()
        logger.debug("Generate Onboarding token")
        ticketgen_result = exec_cmd(f"{ticketgen_script_path} {key_file.name}")
    ticketgen_output = ticketgen_result.stdout.decode()
    if ticketgen_result.stderr:
        raise CommandFailed(
            f"Script ticketgen.sh failed to generate Onboarding token:\n"
            f"command: '{' '.join(ticketgen_result.args)}'\n"
            f"stderr: {ticketgen_result.stderr.decode()}\n"
            f"stdout: {ticketgen_output}"
        )
    return ticketgen_output


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


def get_storage_provider_endpoint(wait=False, timeout=1080):
    """
    Get storage provider endpoint from Provider storage cluster or from
    configuration if configured.

    Args:
        wait (bool): If true then wait for the value to be available for
            number of seconds defined in timeout parameter. If false then
            try to return value only once.
        timeout (int): Number of seconds to wait for the value

    Returns:
        str: value of storage provider endpoint

    """
    provider_endpoint = config.DEPLOYMENT.get("storage_provider_endpoint")
    if provider_endpoint:
        logger.info(
            f"Provider endpoint was loaded from configuration: {provider_endpoint}"
        )
        return provider_endpoint

    config.switch_to_provider()

    def _get_provider_endpoint():
        oc = ocp.OCP(namespace="openshift-config")
        oc.exec_oc_cmd(
            "get storagecluster -n openshift-storage -o=jsonpath="
            "'{.items[0].status.storageProviderEndpoint}'"
        )

    try:
        if wait:
            for result in TimeoutSampler(
                timeout=timeout, sleep=5, func=_get_provider_endpoint
            ):
                if result:
                    provider_endpoint = result
                    break
        else:
            provider_endpoint = _get_provider_endpoint()
    finally:
        config.reset_ctx()

    return provider_endpoint
