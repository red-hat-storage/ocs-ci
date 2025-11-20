import logging

from ocs_ci.ocs import constants
from ocs_ci.ocs import ocp
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
    """
    A class to manage KEDA installation and configuration.

    KEDA is an operator that scales workloads using metrics
    from external sources such as Prometheus,
    instead of relying only on CPU or memory.

    Prerequisites:
    - The Helm CLI must be available for installing/uninstalling KEDA

    """

    def __init__(
        self,
        workload_namespace,
        keda_namespace="keda",
    ):
        self.workload_namespace = workload_namespace
        self.keda_namespace = keda_namespace
        self.sa_name = None
        self.secret_name = None
        self.ta_name = None
        self.scaled_objects = []

    def install(self):
        """
        Install KEDA via the Helm CLI

        Raises:
            - FileNotFoundError: If Helm is not installed.
            - CommandFailed or AssertionError: If KEDA installation fails.
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

        Returns:
            bool: True if KEDA is installed, False otherwise.
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

        # Create a service account for KEDA and grant it read access
        self.sa_name = create_unique_resource_name("keda-prom", "serviceaccount")
        ocp_obj.exec_oc_cmd(f"create sa {self.sa_name}")
        ocp_obj.exec_oc_cmd(
            f"adm policy add-cluster-role-to-user {constants.CLUSTER_MONITORING_VIEW_ROLE} -z {self.sa_name}",
        )

        # Mint a token for the service account and store it in a secret
        self.secret_name = create_unique_resource_name("keda-prom-token", "secret")
        token = ocp_obj.exec_oc_cmd(f"create token {self.sa_name}")
        ocp_obj.exec_oc_cmd(
            f"create secret generic {self.secret_name} --from-literal=bearerToken={token}"
        )

        # Extract the cluster CA bundle so KEDA can verify Thanos TLS
        # TODO: This currently leaves a leftover we need to cleanup at teardown
        ca_data = OCP(namespace="openshift-monitoring").exec_oc_cmd(
            "get configmap serving-certs-ca-bundle -o jsonpath='{.data.service-ca\\.crt}'",
            out_yaml_format=False,
        )
        self.ca_secret_name = create_unique_resource_name("keda-prom-ca", "secret")
        ocp_obj.exec_oc_cmd(
            f"create secret generic {self.ca_secret_name} "
            f"--from-literal=ca.crt='{ca_data}'"
        )

        # Create a TriggerAuthentication that points KEDA to the token + CA bundle
        trigger_auth_data = templating.load_yaml(
            constants.KEDA_TRIGGER_AUTHENTICATION_YAML
        )
        self.ta_name = create_unique_resource_name(
            "keda-prom-auth", "triggerauthentication"
        )
        trigger_auth_data["metadata"]["name"] = self.ta_name
        trigger_auth_data["metadata"]["namespace"] = self.workload_namespace

        # Set bearer token reference
        trigger_auth_data["spec"]["secretTargetRef"][0]["name"] = self.secret_name

        # Add TLS CA reference
        trigger_auth_data["spec"]["secretTargetRef"][1]["name"] = self.ca_secret_name

        create_resource(**trigger_auth_data)

        logger.info("KEDA configured to read Thanos metrics")

    # TODO
    def check_keda_thanos_metrics_read(self):
        """
        Check if KEDA is configured to read Thanos metrics
        """
        logger.info("Checking if KEDA is configured to read Thanos metrics")
        return True

    def create_thanos_metric_scaled_object(self, deployment, query, threshold):
        """
        Create and register a KEDA ScaledObject driven by a Thanos metric.

        A ScaledObject defines how KEDA should scale a workload: which target
        to scale, what metric to watch, and the conditions that trigger scaling.

        Args:
            deployment (str): Deployment name to scale.
            namespace (str): Namespace of the deployment.
            query (str): Thanos query used as the scaling signal.
            threshold (str): Metric threshold that triggers scaling.

        Returns:
            ScaledObject: The configured ScaledObject instance.
        """
        # Common configuration to watch Thanos metrics
        scaled_obj = (
            ScaledObject()
            .set_trigger_address(constants.THANOS_QUERIER_ADDRESS)
            .set_trigger_authentication_ref(self.ta_name)
            .set_scaled_obj_namespace(self.workload_namespace)
        )

        # Specifics on what to scale and on what metric to scale
        scaled_obj = (
            scaled_obj.set_scale_target_ref(deployment)
            .set_trigger_query(query)
            .set_trigger_threshold(threshold)
        )
        self.scaled_objects.append(scaled_obj.create())
        return scaled_obj

    # TODO clean this mess
    def cleanup(self):
        """
        Cleanup KEDA
        """
        for resource_name, kind, namespace in [
            (self.ta_name, constants.TRIGGER_AUTHENTICATION, self.workload_namespace),
            (self.secret_name, constants.SECRET, self.workload_namespace),
            (self.sa_name, constants.SERVICE_ACCOUNT, self.workload_namespace),
        ]:
            try:
                ocp_obj = OCP(namespace=namespace, kind=kind)
                ocp_obj.delete(resource_name=resource_name, wait=True)
            except CommandFailed:
                logger.warning(f"Failed to delete {resource_name} of kind {kind}")

        try:
            for scaled_object in self.scaled_objects:
                scaled_object.delete()
        except CommandFailed as e:
            logger.warning(f"Failed to delete scaled objects: {e}")
        try:
            ocp.switch_to_default_rook_cluster_project()
            self.uninstall()

            OCP().exec_oc_cmd("delete apiservice v1beta1.external.metrics.k8s.io")
            OCP().exec_oc_cmd(f"delete namespace {self.keda_namespace}")

        except CommandFailed:
            logger.warning(f"Failed to delete project {self.keda_namespace}")

            try:
                # Sometimes this resources hangs the namespace deletion
                OCP().exec_oc_cmd("delete apiservice v1beta1.external.metrics.k8s.io")
            except CommandFailed:
                logger.warning(
                    "Failed to delete apiservice v1beta1.external.metrics.k8s.io"
                )

    def uninstall(self):
        """
        Cleanup KEDA via the Helm CLI
        """
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


class ScaledObject:
    """
    A class for managing scaled objects for KEDA.

    Uses the builder pattern to simplify its creation.
    """

    def __init__(self):
        self.scaled_object_name = create_unique_resource_name(
            "keda-scaled-object", "scaledobject"
        )
        self.data = templating.load_yaml(constants.KEDA_SCALED_OBJECT_YAML)
        self.data["metadata"]["name"] = self.scaled_object_name
        self.ocs_obj = None

    @property
    def is_created(self):
        return self.ocs_obj is not None

    def create(self):
        self.ocs_obj = create_resource(**self.data)
        return self.ocs_obj

    def _update(self, path, value):
        """
        Updates the data and applies it to the OCS object if it is already created

        Args:
            path (tuple): The path to the value to update
            value (any): The value to update the path to

        Returns:
            self: This allows for convenient chaining of methods
        """
        try:
            d = self.data
            for p in path[:-1]:
                d = d[p]
            d[path[-1]] = value
        except KeyError:
            raise KeyError(f"Path {path} not found in data")

        if self.is_created:
            self.ocs_obj.apply(**self.data)

        return self

    def set_scaled_obj_namespace(self, namespace):
        path = ("metadata", "namespace")
        return self._update(path, namespace)

    def set_scale_target_ref(self, ref):
        path = ("spec", "scaleTargetRef", "name")
        return self._update(path, ref)

    def set_min_replica_count(self, n):
        path = ("spec", "minReplicaCount")
        return self._update(path, n)

    def set_max_replica_count(self, n):
        path = ("spec", "maxReplicaCount")
        return self._update(path, n)

    def set_polling_interval(self, n):
        path = ("spec", "pollingInterval")
        return self._update(path, n)

    def set_cooldown_period(self, n):
        path = ("spec", "cooldownPeriod")
        return self._update(path, n)

    def set_trigger_address(self, addr):
        path = ("spec", "triggers", 0, "metadata", "serverAddress")
        return self._update(path, addr)

    def set_trigger_query(self, q):
        path = ("spec", "triggers", 0, "metadata", "query")
        return self._update(path, q)

    def set_trigger_threshold(self, t):
        path = ("spec", "triggers", 0, "metadata", "threshold")
        return self._update(path, t)

    def set_trigger_authentication_ref(self, ref):
        path = ("spec", "triggers", 0, "authenticationRef", "name")
        return self._update(path, ref)
