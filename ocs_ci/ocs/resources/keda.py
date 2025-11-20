import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.helpers.helpers import (
    create_unique_resource_name,
    create_resource,
)
from ocs_ci.utility import templating
from ocs_ci.utility.utils import exec_cmd
from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(__name__)

KEDACORE_REPO_URL = "https://kedacore.github.io/charts"


class KEDA:

    def __init__(
        self,
        keda_namespace,
        workload_namespace,
    ):
        self.keda_namespace = keda_namespace
        self.workload_namespace = workload_namespace
        self.sa_name = None
        self.secret_name = None
        self.ta_name = None

    def install(self):
        """
        Install KEDA via the Helm CLI
        """
        # Check if Keda is already installed
        if self.is_installed():
            logger.info(f"KEDA is already installed in namespace {self.keda_namespace}")
            return

        # Check if helm is available
        logger.info(f"Installing KEDA in namespace {self.keda_namespace} via Helm CLI")

        try:
            exec_cmd("helm version")
        except FileNotFoundError:
            raise FileNotFoundError("Helm is not installed")

        # Install KEDA via the Helm CLI
        try:
            exec_cmd(f"helm repo add kedacore {KEDACORE_REPO_URL}")
            exec_cmd("helm repo update")
            exec_cmd(
                f"helm install keda kedacore/keda --namespace {self.keda_namespace} --create-namespace"
                " --wait --timeout 5m"  # Waits for all of Keda's pods to be in running state
            )
        except CommandFailed as e:
            raise CommandFailed(
                f"Failed to install KEDA in namespace {self.keda_namespace}: {e}"
            )

        # Verify that KEDA is installed
        if not self.is_installed():
            raise AssertionError(
                f"KEDA is not installed in namespace {self.keda_namespace}"
            )

        logger.info(f"KEDA installed in namespace {self.keda_namespace}")

    def is_installed(self):
        """
        Check if KEDA is installed
        """
        return (
            exec_cmd(
                f"helm status keda --namespace {self.keda_namespace}",
                silent=True,
                ignore_error=True,
            ).returncode
            == 0
        )

    def allow_keda_to_read_thanos_metrics(self):
        """
        Allow KEDA to read Thanos metrics
        """
        logger.info("Configuring KEDA to read Thanos metrics")

        ocp_obj = OCP(namespace=self.workload_namespace)
        # Create a Service Account for KEDA and give it the necessary permissions
        self.sa_name = create_unique_resource_name("keda-prom", "serviceaccount")
        ocp_obj.exec_oc_cmd(f"create sa {self.sa_name}")
        ocp_obj.exec_oc_cmd(
            f"adm policy add-cluster-role-to-user view -z {self.sa_name}",
        )

        # Mint a short-lived token and store in a secret
        self.secret_name = create_unique_resource_name("keda-prom-token", "secret")
        token = ocp_obj.exec_oc_cmd(
            f"create token {self.sa_name}",
        )
        ocp_obj.exec_oc_cmd(
            f"create secret generic {self.secret_name} --from-literal=token={token}"
        )

        # Connect KEDA to the secret
        trigger_auth_data = templating.load_yaml(
            constants.KEDA_TRIGGER_AUTHENTICATION_YAML
        )
        self.ta_name = create_unique_resource_name(
            "keda-prom-auth", "triggerauthentication"
        )
        trigger_auth_data["metadata"]["name"] = self.ta_name
        trigger_auth_data["metadata"]["namespace"] = self.keda_namespace
        trigger_auth_data["spec"]["secretTargetRef"][0]["name"] = self.secret_name
        create_resource(**trigger_auth_data)

        logger.info("KEDA configured to read Thanos metrics")

    def cleanup(self):
        """
        Cleanup KEDA
        """
        for resource_name, kind, namespace in [
            (self.ta_name, constants.TRIGGER_AUTHENTICATION, self.keda_namespace),
            (self.secret_name, constants.SECRET, self.workload_namespace),
            (self.sa_name, constants.SERVICE_ACCOUNT, self.workload_namespace),
        ]:
            try:
                ocp_obj = OCP(namespace=namespace, kind=kind)
                ocp_obj.delete(resource_name=resource_name, wait=True)
            except CommandFailed:
                logger.warning(f"Failed to delete {resource_name} of kind {kind}")

        self.uninstall()

    def uninstall(self):
        """
        Cleanup KEDA via the Helm CLI
        """
        # Skip the uninstall if we're in dev-mode for easier re-runs
        if config.RUN["cli_params"].get("dev_mode"):
            logger.info("Skipping KEDA uninstall in dev-mode")
            return

        logger.info(
            f"Uninstalling KEDA in namespace {self.keda_namespace} via Helm CLI"
        )

        # Uninstall KEDA via the Helm CLI
        try:
            exec_cmd(f"helm uninstall keda --namespace {self.keda_namespace}")
        except CommandFailed:
            raise CommandFailed("Failed to uninstall KEDA")

        # Verify that KEDA is uninstalled
        if self.is_installed():
            raise AssertionError(
                f"KEDA is still installed in namespace {self.keda_namespace}"
            )

        logger.info(f"KEDA uninstalled in namespace {self.keda_namespace}")
