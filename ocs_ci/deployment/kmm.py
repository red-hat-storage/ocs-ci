"""
This module contains functionality required for KMM (Kernel Module Management) operator installation.
"""

import logging
import tempfile

from ocs_ci.framework import config
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import constants
from ocs_ci.utility import templating
from ocs_ci.utility.utils import run_cmd
from ocs_ci.ocs import exceptions
from ocs_ci.ocs.resources.csv import CSV, get_csvs_start_with_prefix
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.ocs.resources.packagemanifest import PackageManifest

logger = logging.getLogger(__name__)


class KMMInstaller(object):
    """
    KMM Installer class for KMM operator deployment
    """

    def __init__(self):
        self.namespace = "openshift-kmm"
        self.operator_name = "kernel-module-management"
        self.subscription_name = "kernel-module-management"

    def create_kmm_namespace(self):
        """
        Creates the namespace for KMM operator resources

        Raises:
            CommandFailed: If the 'oc create' command fails.
        """
        try:
            logger.info(f"Creating namespace {self.namespace} for KMM operator")
            namespace_data = {
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {
                    "name": self.namespace,
                    "labels": {"openshift.io/cluster-monitoring": "true"},
                },
            }
            namespace_yaml = OCS(**namespace_data)
            namespace_yaml.create()
            logger.info(f"KMM namespace {self.namespace} was created successfully")
        except exceptions.CommandFailed as ef:
            if f'namespaces "{self.namespace}" already exists' in str(ef):
                logger.info(f"Namespace {self.namespace} already present")
            else:
                raise ef

    def create_kmm_operatorgroup(self):
        """
        Creates an OperatorGroup for KMM operator
        KMM operator requires AllNamespaces install mode, so we don't specify targetNamespaces

        """
        logger.info("Creating OperatorGroup for KMM operator")
        operatorgroup_data = {
            "apiVersion": "operators.coreos.com/v1",
            "kind": "OperatorGroup",
            "metadata": {
                "name": "kernel-module-management",
                "namespace": self.namespace,
            },
            "spec": {},
        }
        operatorgroup_yaml = OCS(**operatorgroup_data)
        try:
            operatorgroup_yaml.create()
            logger.info("KMM OperatorGroup created successfully (AllNamespaces mode)")
        except exceptions.CommandFailed as ef:
            if "kernel-module-management already exists" in str(ef):
                logger.info("kernel-module-management OperatorGroup already exists")
            else:
                raise ef

    def create_kmm_subscription(self):
        """
        Creates subscription for KMM operator

        """
        # Create an operator group for KMM
        logger.info("Creating OperatorGroup for KMM")
        self.create_kmm_operatorgroup()

        # Get the default channel for KMM operator
        kmm_channel = config.DEPLOYMENT.get("kmm_channel", "stable")

        catalog_source = constants.OPERATOR_CATALOG_SOURCE_NAME
        if config.DEPLOYMENT.get("disconnected"):
            catalog_source = PackageManifest(
                resource_name=self.operator_name,
            ).get()[
                "metadata"
            ]["labels"]["catalog"]

        subscription_data = {
            "apiVersion": "operators.coreos.com/v1alpha1",
            "kind": "Subscription",
            "metadata": {"name": self.subscription_name, "namespace": self.namespace},
            "spec": {
                "channel": kmm_channel,
                "installPlanApproval": "Automatic",
                "name": self.operator_name,
                "source": catalog_source,
                "sourceNamespace": constants.MARKETPLACE_NAMESPACE,
            },
        }

        subscription_manifest = tempfile.NamedTemporaryFile(
            mode="w+", prefix="kmm_subscription_manifest", delete=False
        )
        templating.dump_data_to_temp_yaml(subscription_data, subscription_manifest.name)
        logger.info("Creating subscription for KMM operator")
        retry(exceptions.CommandFailed, tries=25, delay=60, backoff=1)(run_cmd)(
            f"oc apply -f {subscription_manifest.name}"
        )

        # Wait for subscription to be created
        self.wait_for_the_resource_to_discover(
            kind=constants.SUBSCRIPTION_WITH_ACM,
            namespace=self.namespace,
            resource_name=self.subscription_name,
        )

        # Since installPlanApproval is set to Automatic, we don't need to wait for manual approval
        # Just wait for CSV to be ready
        logger.info(
            "Waiting for KMM operator CSV to be created and reach Succeeded phase"
        )
        csv = None
        for csv in TimeoutSampler(
            timeout=1200,
            sleep=15,
            func=get_csvs_start_with_prefix,
            csv_prefix=self.operator_name,
            namespace=self.namespace,
        ):
            if csv:
                break

        if not csv:
            raise exceptions.TimeoutExpiredError(
                f"Timeout waiting for CSV with prefix {self.operator_name}"
            )

        csv_name = csv[0]["metadata"]["name"]
        logger.info(f"Found KMM operator CSV: {csv_name}")
        csv_obj = CSV(resource_name=csv_name, namespace=self.namespace)
        csv_obj.wait_for_phase(phase="Succeeded", timeout=900)
        logger.info(f"KMM operator CSV {csv_name} is in Succeeded phase")

    def wait_for_the_resource_to_discover(self, kind, namespace, resource_name):
        """
        Waits for the specified resource to be discovered.

        Args:
            kind (str): The type of the resource to wait for.
            namespace (str): The namespace in which to wait for the resource.
            resource_name (str): The name of the resource to wait for.
        """
        logger.info(f"Waiting for resource {kind} to be discovered")
        for sample in TimeoutSampler(300, 10, OCP, kind=kind, namespace=namespace):
            resources = sample.get().get("items", [])
            for resource in resources:
                found_resource_name = resource.get("metadata", {}).get("name", "")
                if resource_name in found_resource_name:
                    logger.info(f"{kind} found: {found_resource_name}")
                    return
                logger.debug(f"Still waiting for the {kind}: {resource_name}")

    def kmm_operator_installed(self):
        """
        Check if KMM operator is already installed.

        Returns:
             bool: True if KMM operator is installed, False otherwise
        """
        ocp = OCP(kind=constants.SUBSCRIPTION_WITH_ACM, namespace=self.namespace)
        return ocp.check_resource_existence(
            timeout=12, should_exist=True, resource_name=self.subscription_name
        )

    def post_install_verification(self, raise_exception=False):
        """
        Performs KMM operator post-installation verification.

        Args:
            raise_exception: If True, allow function to fail the job and raise an exception.
                           If false, return False instead of raising an exception.

        Returns:
            bool: True if the verification conditions are met, False otherwise

        Raises:
            TimeoutExpiredError: If the verification conditions are not met within the timeout
                               and raise_exception is True.
            ResourceNotFoundError: If the namespace does not exist and raise_exception is True.
            ResourceWrongStatusException: If pods are not running and raise_exception is True.
        """
        logger.info("Performing KMM operator post-installation verification")

        try:
            OCP(kind="namespace").get(self.namespace)
        except exceptions.CommandFailed:
            if raise_exception:
                raise exceptions.ResourceNotFoundError(
                    f"Namespace {self.namespace} does not exist"
                )
            else:
                logger.warning(f"Namespace {self.namespace} does not exist")
                return False

        # Wait for KMM operator pods to be running
        if wait_for_pods_to_be_running(namespace=self.namespace, timeout=600):
            logger.info("All KMM operator pods are running")
        else:
            if raise_exception:
                raise exceptions.ResourceWrongStatusException(
                    "Not all KMM operator pods are running"
                )
            else:
                logger.warning("Not all KMM operator pods are running")
                return False

        # Verify that all the deployments in the KMM namespace are in 'Available' condition
        logger.info(f"Verify all the deployments status in {self.namespace}")
        ocp = OCP(kind="deployments", namespace=self.namespace)

        try:
            ocp.wait(condition="Available", timeout=600)
        except exceptions.TimeoutExpiredError:
            if raise_exception:
                raise exceptions.TimeoutExpiredError(
                    "Timeout occurred, one or more deployments did not meet condition: Available"
                )
            else:
                logger.warning(
                    "Timeout occurred, or one or more deployments did not meet condition: Available"
                )
                return False

        logger.info(
            f"All the deployments in the {self.namespace} namespace met condition: Available"
        )
        return True

    def deploy_kmm_operator(self, check_kmm_deployed=False, check_kmm_ready=False):
        """
        Installs KMM operator.

        Args:
            check_kmm_deployed (bool): If True, check if KMM is already deployed. If so, skip the deployment.
            check_kmm_ready (bool): If True, check if KMM is ready. If so, skip the deployment.
        """
        if check_kmm_deployed:
            if self.kmm_operator_installed():
                logger.info("KMM operator is already deployed, skipping the deployment")
                return

        if check_kmm_ready:
            if self.post_install_verification(raise_exception=False):
                logger.info("KMM operator ready, skipping the deployment")
                return

        logger.info("Installing KMM operator")
        # Create KMM namespace
        self.create_kmm_namespace()
        # Create KMM subscription
        self.create_kmm_subscription()
        # Post KMM installation checks
        self.post_install_verification(raise_exception=True)
        logger.info("KMM operator installation completed successfully")

    def uninstall_kmm_operator(self, check_kmm_installed=True):
        """
        Uninstall KMM operator deployment

        Args:
            check_kmm_installed (bool): True if want to check if KMM installed
        """
        if check_kmm_installed:
            if not self.kmm_operator_installed():
                logger.info("KMM operator is not installed, skipping the cleanup...")
                return

        logger.info("Removing the KMM operator subscription")
        try:
            kmm_sub = OCP(
                kind=constants.SUBSCRIPTION,
                resource_name=self.subscription_name,
                namespace=self.namespace,
            )
            kmm_sub.delete(resource_name=self.subscription_name)
            logger.info(f"Deleted subscription {self.subscription_name}")
        except exceptions.CommandFailed as e:
            logger.warning(f"Failed to delete subscription: {e}")

        logger.info("Removing the KMM operator CSV")
        try:
            kmm_csv = OCP(
                kind=constants.CLUSTER_SERVICE_VERSION,
                namespace=self.namespace,
            )
            csv_data = kmm_csv.get()
            if csv_data and isinstance(csv_data, dict):
                csvs = csv_data.get("items", [])
                for csv in csvs:
                    csv_name = csv.get("metadata", {}).get("name", "")
                    if self.operator_name in csv_name:
                        kmm_csv.delete(resource_name=csv_name)
                        logger.info(f"Deleted ClusterServiceVersion {csv_name}")
        except exceptions.CommandFailed as e:
            logger.warning(f"Failed to delete CSV: {e}")

        logger.info("Removing the namespace")
        try:
            kmm_namespace = OCP()
            kmm_namespace.delete_project(self.namespace)
            logger.info(f"Deleted the namespace {self.namespace}")
        except exceptions.CommandFailed as e:
            logger.warning(f"Failed to delete namespace: {e}")


def deploy_kmm_operator():
    """
    Deploy KMM operator based on configuration

    """
    if config.DEPLOYMENT.get("kmm_deployment"):
        logger.info("KMM deployment is enabled in configuration")
        kmm_installer = KMMInstaller()
        kmm_installer.deploy_kmm_operator(check_kmm_deployed=True, check_kmm_ready=True)
    else:
        logger.info("KMM deployment is not enabled in configuration")


# Made with Bob
