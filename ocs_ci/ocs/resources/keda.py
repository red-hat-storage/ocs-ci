import json
import logging
import shlex

from ocs_ci.ocs import constants
from ocs_ci.ocs import ocp
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.helpers.helpers import (
    create_unique_resource_name,
    create_resource,
)
from ocs_ci.ocs.resources.pod import Pod, get_pods_having_label
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
        self.token = None
        self.ca_data = None
        self.ca_secret_name = None
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

    def setup_access_to_thanos_metrics(self):
        """
        Setup access to Thanos metrics for KEDA

        This creates a service account, a secret with a token, and a secret with the CA bundle.
        It then creates a TriggerAuthentication that points KEDA to the token + CA bundle.
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
        self.token = ocp_obj.exec_oc_cmd(f"create token {self.sa_name}")
        ocp_obj.exec_oc_cmd(
            f"create secret generic {self.secret_name} --from-literal=bearerToken={self.token}"
        )

        # Extract the cluster CA bundle so KEDA can verify Thanos TLS
        # TODO: This currently leaves a leftover we need to cleanup at teardown
        self.ca_data = OCP(namespace="openshift-monitoring").exec_oc_cmd(
            "get configmap serving-certs-ca-bundle -o jsonpath='{.data.service-ca\\.crt}'",
            out_yaml_format=False,
        )
        self.ca_secret_name = create_unique_resource_name("keda-prom-ca", "secret")
        ocp_obj.exec_oc_cmd(
            f"create secret generic {self.ca_secret_name} "
            f"--from-literal=ca.crt='{self.ca_data}'"
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

    def can_read_thanos_metrics(self):
        """
        Check if the token and CA that KEDA is configured to use are valid
        by querying the Thanos Querier internal address from one of the Prometheus pods.

        Returns:
            bool: True if KEDA is configured to read Thanos metrics, False otherwise.
        """
        logger.info(
            "Checking if KEDA's token and CA can be used to read Thanos metrics"
        )

        if not self.token or not self.ca_secret_name:
            logger.warning("KEDA is not configured to read Thanos metrics")
            return False

        # Get one of the Prometheus pods
        prom_pod_obj = get_pods_having_label(
            constants.PROMETHEUS_POD_LABEL, constants.OPENSHIFT_MONITORING_NAMESPACE
        )[0]
        prom_pod = Pod(**prom_pod_obj)

        # Build the headers and URL for the query
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        header_args = " ".join(
            f"-H {shlex.quote(f'{k}: {v}')}" for k, v in headers.items()
        )
        url = f"{constants.THANOS_QUERIER_INTERNAL_ADDRESS}/api/v1/query?query=time()"

        # Feed the CA data to the curl command as a bash variable
        # shlex is needed to properly quote the CA data and the URL
        bash_script = (
            "CERT=$(cat << 'EOF'\n"
            f"{self.ca_data.strip()}\n"
            "EOF\n"
            ")\n"
            f"curl -s -k {header_args} --cacert <(printf '%s\\n' \"$CERT\") {shlex.quote(url)}\n"
        )
        # Wrap it for bash -lc (so <(...) works)
        cmd = f"bash -lc {shlex.quote(bash_script)}"

        raw_response = prom_pod.exec_cmd_on_pod(cmd, out_yaml_format=False)
        response = json.loads(raw_response)
        if response.get("status") == "success":
            logger.info("KEDA's token and CA can be used to read Thanos metrics")
            return True
        else:
            logger.warning(
                (
                    "KEDA's token and CA cannot be used to read Thanos metrics"
                    f"Got response: {response}"
                )
            )
            return False

    def create_thanos_metric_scaled_object(self, config_dict):
        """
        Create and register a KEDA ScaledObject driven by a Thanos metric.
        A ScaledObject defines how KEDA should scale a workload: which target
        to scale, what metric to watch, and the conditions that trigger scaling.

        Args:
            config_dict (dict): A dictionary containing the configuration for the ScaledObject.
            See ScaledObject.KEYS_TO_YAML_PATH for valid keys.
        Returns:
            ScaledObject: The configured ScaledObject instance.
        """
        config_dict.setdefault("namespace", self.workload_namespace)
        config_dict.setdefault(
            "serverAddress", constants.THANOS_QUERIER_INTERNAL_ADDRESS
        )
        config_dict.setdefault("authenticationRef", self.ta_name)

        scaled_obj = ScaledObject(config_dict)
        self.scaled_objects.append(scaled_obj.ocp_obj)
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
    """

    KEYS_TO_YAML_PATH = {
        "name": ("metadata", "name"),
        "namespace": ("metadata", "namespace"),
        "scaleTargetRef": ("spec", "scaleTargetRef"),
        "minReplicaCount": ("spec", "minReplicaCount"),
        "maxReplicaCount": ("spec", "maxReplicaCount"),
        "pollingInterval": ("spec", "pollingInterval"),
        "cooldownPeriod": ("spec", "cooldownPeriod"),
        "serverAddress": ("spec", "triggers", 0, "metadata", "serverAddress"),
        "query": ("spec", "triggers", 0, "metadata", "query"),
        "threshold": ("spec", "triggers", 0, "metadata", "threshold"),
        "authenticationRef": ("spec", "triggers", 0, "authenticationRef", "name"),
    }

    def __init__(self, config_dict):
        """
        Args:
            config_dict (dict): A dictionary containing the configuration for the ScaledObject.
        """
        self._validate_config_dict(config_dict)

        self.ocp_obj = None

        self.data = templating.load_yaml(constants.KEDA_SCALED_OBJECT_YAML)
        self.name = create_unique_resource_name("keda-scaled-object", "scaledobject")
        self.data["metadata"]["name"] = self.name

        for key, path in self.KEYS_TO_YAML_PATH.items():
            if key in config_dict:
                self._update_yaml_path(path, config_dict[key], apply=False)

        self.ocp_obj = create_resource(**self.data)

    @property
    def is_created(self):
        return self.ocp_obj is not None

    def _validate_config_dict(self, config_dict):
        """
        Validates the config_dict

        Args:
            config_dict (dict): A dictionary containing the configuration for the ScaledObject.
            See ScaledObject.KEYS_TO_YAML_PATH for valid keys.
        Raises:
            ValueError: If the config_dict is invalid.
        """
        for key in config_dict:
            if key not in self.KEYS_TO_YAML_PATH:
                raise ValueError(f"Invalid key: {key}")

    def _update_yaml_path(self, path, value, apply=True):
        """
        Updates the data and applies it to the OCS object if it is already created

        Args:
            path (tuple): The path to the value to update
            value (any): The value to update the path to
            apply (bool): Whether to apply the changes to the OCS object if it is already created

        Returns:
            self: This allows for convenient chaining of methods
        Raises:
            KeyError: If the path is not found in the data.
        """
        try:
            d = self.data
            for p in path[:-1]:
                d = d[p]
            d[path[-1]] = value
        except KeyError:
            raise KeyError(f"Path {path} not found in data")

        if self.is_created and apply:
            self.ocp_obj.apply(**self.data)

        return self

    def update_from_dict(self, config_dict):
        """
        Updates the ScaledObject from a dictionary

        Args:
            config_dict (dict): A dictionary containing the configuration for the ScaledObject.
            See ScaledObject.KEYS_TO_YAML_PATH for valid keys.
        Raises:
            ValueError: If the config_dict is invalid.
        """
        self._validate_config_dict(config_dict)
        for key, value in config_dict.items():
            self._update_yaml_path(self.KEYS_TO_YAML_PATH[key], value, apply=False)

        self.ocp_obj.apply(**self.data)
