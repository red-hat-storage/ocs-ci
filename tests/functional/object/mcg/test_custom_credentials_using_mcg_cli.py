import logging
import string
import random

from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    MCGTest,
    tier1,
    red_squad,
    mcg,
)
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.helpers.helpers import retrieve_cli_binary
from ocs_ci.utility.utils import run_cmd, get_random_str
from ocs_ci.helpers.helpers import get_s3_credentials_from_secret
from pathlib import Path

logger = logging.getLogger(__name__)
mcg_cli = constants.NOOBAA_OPERATOR_LOCAL_CLI_PATH


@tier1
@red_squad
@mcg
class TestCustomCredsUsingMcgCli(MCGTest):
    def update_nb_account(self, account_name, access_key, secret_key):
        """
        Update noobaa account with custom credential values
        """
        namespace = config.ENV_DATA["cluster_namespace"]
        output = run_cmd(
            cmd=f"{mcg_cli} account credentials {account_name} "
            + f"--access-key={access_key} "
            + f"--secret-key={secret_key} "
            + f"-n {namespace}",
            ignore_error=True,
        )
        logger.info(output)

    @skipif_ocs_version("<4.17")
    def test_account_update_with_custom_creds_using_cli(self, mcg_account_factory):
        """
        1. Create a new account using MCG CLI
        2. Retrive creds of the account
        3. Change creds of the with custom value suing CLI
        4. Verify creds are updated using CLI
        5. Validate custom credentials against against length and valid characters
        """
        if not Path(mcg_cli).exists():
            retrieve_cli_binary(cli_type="mcg")

        output = run_cmd(cmd=f"{mcg_cli} version")
        logger.info(output)
        account_name = get_random_str(5)
        original_acc_credentials = mcg_account_factory(name=account_name)
        original_secret_key = original_acc_credentials["access_key"]
        original_access_key = original_acc_credentials["access_key_id"]

        logger.info(f"Original access key: {original_access_key}")
        logger.info(f"Original secret key: {original_secret_key}")
        logger.info("Updating noobaa account credentials with custom values")
        new_access_key = get_random_str(20)
        new_secret_key = get_random_str(40)
        self.update_nb_account(account_name, new_access_key, new_secret_key)
        retrived_access_key, retrived_secret_key = get_s3_credentials_from_secret(
            f"noobaa-account-{account_name}"
        )
        logger.info(f"retrived access key: {retrived_access_key}")
        logger.info(f"retrived secret key: {retrived_secret_key}")
        assert (
            retrived_access_key == new_access_key
        ), "Mismatch in updated and new access key"
        assert (
            retrived_secret_key == new_secret_key
        ), "Mismatch in updated and new secret key"
        logger.info("Custom credentials updated successfully")
        logger.info("Updating credentials with incorrect length")
        bad_length_access_key = get_random_str(19)
        bad_length_secret_key = get_random_str(39)
        self.update_nb_account(
            account_name, bad_length_access_key, bad_length_secret_key
        )
        retrived_access_key, retrived_secret_key = get_s3_credentials_from_secret(
            f"noobaa-account-{account_name}"
        )
        logger.info(f"retrived access key after bad value: {retrived_access_key}")
        logger.info(f"retrived secret key after bad value: {retrived_secret_key}")
        assert (
            retrived_access_key != bad_length_access_key
        ), "CLI is accepting access key with invalid lenght"
        assert (
            retrived_secret_key != bad_length_secret_key
        ), "CLI is accepting secret key with invalid lenght"
        logger.info("Credentials with incorrect length didnt updated as expected")
        logger.info("Updating credentials with invalid characters")
        bad_access_key = bad_length_access_key + "".join(
            random.choices(string.punctuation, k=1)
        )
        bad_secret_key = bad_length_secret_key + "".join(
            random.choices(string.punctuation.replace("+", "").replace("/", ""), k=1)
        )
        self.update_nb_account(account_name, bad_access_key, bad_secret_key)
        retrived_access_key, retrived_secret_key = get_s3_credentials_from_secret(
            f"noobaa-account-{account_name}"
        )
        logger.info(f"retrived access key after bad value: {retrived_access_key}")
        logger.info(f"retrived secret key after bad value: {retrived_secret_key}")
        assert (
            retrived_access_key != bad_access_key
        ), "CLI is accepting invalid special characters in access key"
        assert (
            retrived_secret_key != bad_secret_key
        ), "CLI is accepting invalid special characters in secret key"
        logger.info("Credentials with bad keywords didnt updated as expected")
