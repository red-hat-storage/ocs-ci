import logging
import os
import stat

from tempfile import NamedTemporaryFile

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed, ConfigurationError
from ocs_ci.utility.utils import download_file, exec_cmd

logger = logging.getLogger(__name__)


def generate_onboarding_token():
    """
    Generate Onboarding token for consumer cluster via following steps:

    1. Download ticketgen.sh script from:
        https://raw.githubusercontent.com/jarrpa/ocs-operator/ticketgen/hack/ticketgen/ticketgen.sh
    2. Save private key from AUTH["managed_service"]["private_key"] to
        temporary file.
    3. Run ticketgen.sh script to generate Onboarding token.

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
