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

    def create_scaled_object(self, config_dict):
        """
        Create and register a KEDA ScaledObject.

        Args:
            config_dict (dict): Configuration dictionary for the ScaledObject.
                See ScaledObject.__init__ for valid keys.

        Returns:
            ScaledObject: The created ScaledObject instance.
        """
        # Add defaults that are specific to this KEDA instance
        config_dict = config_dict.copy()
        config_dict.setdefault("namespace", self.workload_namespace)
        config_dict.setdefault(
            "trigger_address", constants.THANOS_QUERIER_INTERNAL_ADDRESS
        )
        config_dict.setdefault("authenticationRef", self.ta_name)

        scaled_obj = ScaledObject(config_dict)
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
    A class for managing KEDA ScaledObject custom resources.

    Accepts a flat dictionary of configurable values that map to the ScaledObject CR structure.
    """

    # Mapping from flat dict keys to nested paths in the YAML
    # Supports both camelCase and snake_case keys
    _CONFIG_MAPPING = {
        "namespace": ("metadata", "namespace"),
        "scaleTargetRef": ("spec", "scaleTargetRef"),
        "scale_target_ref": ("spec", "scaleTargetRef"),
        "minReplicaCount": ("spec", "minReplicaCount"),
        "min_replica_count": ("spec", "minReplicaCount"),
        "maxReplicaCount": ("spec", "maxReplicaCount"),
        "max_replica_count": ("spec", "maxReplicaCount"),
        "pollingInterval": ("spec", "pollingInterval"),
        "polling_interval": ("spec", "pollingInterval"),
        "cooldownPeriod": ("spec", "cooldownPeriod"),
        "cooldown_period": ("spec", "cooldownPeriod"),
        "trigger_address": ("spec", "triggers", 0, "metadata", "serverAddress"),
        "serverAddress": ("spec", "triggers", 0, "metadata", "serverAddress"),
        "query": ("spec", "triggers", 0, "metadata", "query"),
        "threshold": ("spec", "triggers", 0, "metadata", "threshold"),
        "authenticationRef": ("spec", "triggers", 0, "authenticationRef", "name"),
        "authentication_ref": ("spec", "triggers", 0, "authenticationRef", "name"),
        "trigger_authentication_ref": (
            "spec",
            "triggers",
            0,
            "authenticationRef",
            "name",
        ),
    }

    def __init__(self, config_dict=None):
        """
        Initialize a ScaledObject from a flat configuration dictionary.

        Args:
            config_dict (dict, optional): Flat dictionary with configurable values.
                Only specify the values you want to override from the template.
                Valid keys:
                - namespace: Namespace for the ScaledObject
                - scaleTargetRef: Target reference to scale (dict)
                - minReplicaCount: Minimum replica count (int)
                - maxReplicaCount: Maximum replica count (int)
                - pollingInterval: Polling interval in seconds (int)
                - cooldownPeriod: Cooldown period in seconds (int)
                - trigger_address or serverAddress: Thanos querier address (str)
                - query: Prometheus query string (str)
                - threshold: Metric threshold (str)
                - authenticationRef or trigger_authentication_ref: Auth ref name (str)

                Example:
                {
                    'namespace': 'openshift-storage',
                    'scaleTargetRef': {'name': 'my-deployment'},
                    'cooldownPeriod': 60,
                    'query': 'sum(rate(ceph_rgw_req[2m]))'
                }
        """
        config_dict = config_dict or {}

        # Load the template as a base
        self.data = templating.load_yaml(constants.KEDA_SCALED_OBJECT_YAML)

        # Generate a unique name if not provided
        self.scaled_object_name = create_unique_resource_name(
            "keda-scaled-object", "scaledobject"
        )
        self.data["metadata"]["name"] = self.scaled_object_name

        # Apply config dict values to the template
        self._apply_config(config_dict)

        self.ocs_obj = None

    def _apply_config(self, config_dict):
        """
        Apply flat config dict values to the nested YAML structure.

        Args:
            config_dict (dict): Flat dictionary of configurable values
        """
        for key, value in config_dict.items():
            if key not in self._CONFIG_MAPPING:
                raise ValueError(
                    f"Unknown config key: '{key}'. "
                    f"Valid keys: {list(self._CONFIG_MAPPING.keys())}"
                )

            path = self._CONFIG_MAPPING[key]
            self._set_nested_value(self.data, path, value)

    def _set_nested_value(self, data, path, value):
        """
        Set a nested value in the data structure.

        Args:
            data (dict): The data structure to update
            path (tuple): Tuple of keys representing the path
            value: The value to set
        """
        # Traverse to the parent container (all keys except the last)
        container = data
        for key in path[:-1]:
            container = self._ensure_and_get(container, key)

        # Ensure the final container is ready, then set the value
        final_key = path[-1]
        self._ensure_and_get(container, final_key)
        container[final_key] = value

    def _ensure_and_get(self, container, key):
        """
        Ensure the container has the key/index and return the nested value.

        Args:
            container: The container (dict or list) to access
            key: The key/index to access

        Returns:
            The nested container at the key
        """
        if isinstance(key, int):
            if not isinstance(container, list):
                raise ValueError(f"Expected list, got {type(container)}")
            # Extend list if needed
            while len(container) <= key:
                container.append({})
            return container[key]
        else:
            if not isinstance(container, dict):
                raise ValueError(f"Expected dict, got {type(container)}")
            # Create dict entry if missing
            if key not in container:
                container[key] = {}
            return container[key]

    @property
    def is_created(self):
        """Check if the ScaledObject has been created in the cluster."""
        return self.ocs_obj is not None

    def create(self):
        """
        Create the ScaledObject CR in the cluster.

        Returns:
            OCS: The created OCS object
        """
        self.ocs_obj = create_resource(**self.data)
        return self.ocs_obj

    def delete(self, wait=True):
        """
        Delete the ScaledObject CR from the cluster.

        Args:
            wait (bool): Whether to wait for deletion to complete
        """
        if not self.ocs_obj:
            raise ValueError("ScaledObject has not been created yet")
        self.ocs_obj.delete(wait=wait)
