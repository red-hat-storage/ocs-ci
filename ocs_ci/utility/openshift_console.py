import os
import logging
from datetime import datetime

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    OpenshiftConsoleSuiteNotDefined,
    UnsupportedBrowser,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility.utils import get_kubeadmin_password, run_cmd

logger = logging.getLogger(__name__)


class OpenshiftConsole:

    def __init__(self, browser=constants.CHROME_BROWSER):
        self.browser = browser
        self.console_path = config.RUN['openshift_console_path']
        self.env_vars = {}
        self.setup_console_prereq()

    def setup_console_prereq(self):
        """
        Setup openshift console prerequisites

        Raises:
            UnsupportedBrowser: in case the browser is not supported

        """
        # TODO: add support for other browsers
        if self.browser not in constants.SUPPORTED_BROWSERS:
            raise UnsupportedBrowser(
                f"Support for {self.browser} hasn't been implemented yet!"
            )
        if self.browser == constants.CHROME_BROWSER:
            chrome_branch_base = config.RUN.get("force_chrome_branch_base")
            chrome_branch_sha = config.RUN.get("force_chrome_branch_sha256sum")
            self.env_vars["FORCE_CHROME_BRANCH_BASE"] = chrome_branch_base
            self.env_vars["FORCE_CHROME_BRANCH_SHA256SUM"] = chrome_branch_sha

        htpasswd_secret = OCP(
            kind="Secret", resource_name=constants.HTPASSWD_SECRET_NAME
        )
        try:
            htpasswd_secret.get()
            logger.info("Htpasswd secret is already set! Skipping secret setup")
        except CommandFailed:
            logger.info("Setting up htpasswd secret file for openshift console")
            password_secret_yaml = os.path.join(
                self.console_path, constants.HTPASSWD_SECRET_YAML
            )
            patch_htpasswd_yaml = os.path.join(
                self.console_path, constants.HTPASSWD_PATCH_YAML
            )
            with open(patch_htpasswd_yaml) as fd_patch_htpasswd:
                content_patch_htpasswd_yaml = fd_patch_htpasswd.read()
            run_cmd(
                f"oc apply -f {password_secret_yaml}", cwd=self.console_path
            )
            run_cmd(
                f"oc patch oauths cluster --patch "
                f"\"{content_patch_htpasswd_yaml}\" --type=merge",
                cwd=self.console_path
            )
        self.bridge_base_address = run_cmd(
            "oc get consoles.config.openshift.io cluster -o"
            "jsonpath='{.status.consoleURL}'"
        )
        logger.info(f"Bridge base address: {self.bridge_base_address}")
        self.env_vars["BRIDGE_KUBEADMIN_PASSWORD"] = get_kubeadmin_password()
        self.env_vars["BRIDGE_BASE_ADDRESS"] = self.bridge_base_address
        self.env_vars.update(os.environ)

    def run_openshift_console(
        self, suite, env_vars=None, timeout=1500, log_suffix=""
    ):
        """
        Run openshift console suite

        Args:
            suite (str): openshift console suite to execute
            env_vars (dict): env variables to expose for openshift console
            timeout (int): timeout for test-gui.sh script

        Raises:
            OpenshiftConsoleSuiteNotDefined: if suite is not defined

        """
        if not suite:
            raise OpenshiftConsoleSuiteNotDefined(
                "Please specify suite to run!"
            )
        env_vars = env_vars if env_vars else {}
        combined_env_vars = {**self.env_vars, **env_vars}

        ui_deploy_output = run_cmd(
            f"./test-gui.sh {suite}", cwd=self.console_path,
            env=combined_env_vars, timeout=timeout,
        )
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        ui_deploy_log_file = os.path.expanduser(
            os.path.join(
                config.RUN['log_dir'],
                f"openshift-console-{log_suffix}{timestamp}.log"
            )
        )
        logger.info(
            f"Log from test-gui.sh will be located here: "
            f"{ui_deploy_log_file}"
        )
        with open(ui_deploy_log_file, "w+") as log_fd:
            log_fd.write(ui_deploy_output)
