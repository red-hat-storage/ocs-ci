import logging

from ocs_ci.framework import config
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility.utils import exec_cmd

logger = logging.getLogger(__name__)

KEDACORE_REPO_URL = "https://kedacore.github.io/charts"


class KEDA:

    def __init__(self, namespace="keda"):
        self.namespace = namespace

    def install(self):
        """
        Install KEDA via the Helm CLI
        """
        # Check if Keda is already installed
        if self.is_installed():
            logger.info(f"KEDA is already installed in namespace {self.namespace}")
            return

        # Check if helm is available
        logger.info(f"Installing KEDA in namespace {self.namespace} via Helm CLI")

        try:
            exec_cmd("helm version")
        except FileNotFoundError:
            raise FileNotFoundError("Helm is not installed")

        # Install KEDA via the Helm CLI
        try:
            exec_cmd(f"helm repo add kedacore {KEDACORE_REPO_URL}")
            exec_cmd("helm repo update")
            exec_cmd(
                f"helm install keda kedacore/keda --namespace {self.namespace} --create-namespace"
                " --wait --timeout 5m"  # Waits for all of Keda's pods to be in running state
            )
        except CommandFailed as e:
            raise CommandFailed(
                f"Failed to install KEDA in namespace {self.namespace}: {e}"
            )

        # Verify that KEDA is installed
        if not self.is_installed():
            raise AssertionError(f"KEDA is not installed in namespace {self.namespace}")

        logger.info(f"KEDA installed in namespace {self.namespace}")

    def is_installed(self):
        """
        Check if KEDA is installed
        """
        return (
            exec_cmd(
                f"helm status keda --namespace {self.namespace}",
                silent=True,
                ignore_error=True,
            ).returncode
            == 0
        )

    def cleanup(self):
        """
        Cleanup KEDA via the Helm CLI
        """
        # Skip the cleanup if we're in dev-mode for easier re-runs
        if config.RUN["cli_params"].get("dev_mode"):
            logger.info("Skipping KEDA cleanup in dev-mode")
            return

        logger.info(f"Cleaning up KEDA in namespace {self.namespace} via Helm CLI")

        # Uninstall KEDA via the Helm CLI
        try:
            exec_cmd(f"helm uninstall keda --namespace {self.namespace}")
            exec_cmd(f"oc delete namespace {self.namespace}")
        except CommandFailed:
            raise CommandFailed("Failed to uninstall KEDA")

        # Verify that KEDA is uninstalled
        if self.is_installed():
            raise AssertionError(
                f"KEDA is still installed in namespace {self.namespace}"
            )

        logger.info(f"KEDA uninstalled in namespace {self.namespace}")
