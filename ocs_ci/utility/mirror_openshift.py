import logging
import os

from ocs_ci.framework import config
from ocs_ci.ocs import constants, exceptions


logger = logging.getLogger(__name__)


def prepare_mirror_openshift_credential_files():
    """
    Prepare local files containing username and password for mirror.openshift.com:
        data/mirror_openshift_user,
        data/mirror_openshift_password.
    Those files are used as variable files for repo files (stored in /etc/yum/vars/
    on the target RHEL nodes or pods).

    Returns:
        (tuple): tuple containing two strings - file names for username file and password file

    Raises
        ConfigurationError: if mirror_openshift credentials are not provided in auth.yaml file

    """
    if (
        config.AUTH.get("mirror_openshift")
        and config.AUTH["mirror_openshift"].get("user")
        and config.AUTH["mirror_openshift"].get("password")
    ):
        user_file = os.path.join(
            constants.DATA_DIR, constants.MIRROR_OPENSHIFT_USER_FILE
        )
        with open(user_file, "w") as u_file:
            u_file.writelines(config.AUTH["mirror_openshift"]["user"])

        password_file = os.path.join(
            constants.DATA_DIR, constants.MIRROR_OPENSHIFT_PASSWORD_FILE
        )
        with open(password_file, "w") as p_file:
            p_file.writelines(config.AUTH["mirror_openshift"]["password"])

        logger.debug(
            f"Credentials for mirror.openshift.com saved to files '{user_file}' and '{password_file}'"
        )
        return user_file, password_file
    else:
        raise exceptions.ConfigurationError(
            "Credentials for mirror.openshift.com are not provided in data/auth.yaml file."
        )
