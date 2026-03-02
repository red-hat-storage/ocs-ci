import logging
from time import sleep

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
from ocs_ci.ocs.resources.ocs import OCS

logger = logging.getLogger(__name__)


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
        self.cleanup_label = (
            f"{create_unique_resource_name('keda-cleanup', 'label')}=true"
        )
        self.workload_namespace = workload_namespace
        self.keda_namespace = keda_namespace
        self.token = None
        self.ca_data = None
        self.ca_secret_name = None
        self.ta_name = None

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
            # Add the KEDA repository to helm
            exec_cmd(f"helm repo add kedacore {constants.KEDACORE_REPO_URL}")
            exec_cmd("helm repo update")

        except CommandFailed as e:
            raise CommandFailed(f"Failed to add KEDA repository to Helm: {e}")

        # Install KEDA
        install_cmd = (
            f"helm install keda kedacore/keda --namespace {self.keda_namespace} --create-namespace"
            " --wait --timeout 5m"  # Waits for all of Keda's pods to be in running state
        )
        try:
            exec_cmd(install_cmd)
        except CommandFailed as e:
            e_msg = str(e).lower()
            if "customresourcedefinitions" in e_msg and "not found" in e_msg:
                logger.warning("Some KEDA CRDs are not yet established")

                # Targeted workaround for a rare race-condition error:
                # Sometimes KEDA's pods are ready but some of its CRDs (i.e. ScaledObject)
                # are still being processed. If installation still fails, there's a good
                # chance it's an environment blocker, so we don't keep retrying here.
                sleep(10)
                OCP().exec_oc_cmd(
                    (
                        "wait --for=condition=Established "
                        "crds -l app.kubernetes.io/part-of=keda-operator"
                    )
                )

                # Retry once more
                exec_cmd(install_cmd)

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
        It then creates a TriggerAuthentication that points KEDA to the token + CA bundle:

        1. Create a service account for KEDA and grant it read access
        2. Mint a token for the service account and store it in a secret
        3. Extract the cluster CA bundle so KEDA can verify Thanos TLS
        4. Create a secret with the CA bundle
        5. Create a TriggerAuthentication that points KEDA to the token + CA bundle
        """
        resources_to_cleanup = []
        logger.info("Configuring KEDA to read Thanos metrics")
        ocp_obj = OCP(namespace=self.workload_namespace)

        # Create a service account for KEDA and grant it read access
        self.sa_name = create_unique_resource_name("keda-prom", "serviceaccount")
        ocp_obj.exec_oc_cmd(f"create sa {self.sa_name}")
        ocp_obj.exec_oc_cmd(
            f"adm policy add-cluster-role-to-user {constants.CLUSTER_MONITORING_VIEW_ROLE} -z {self.sa_name}",
        )
        resources_to_cleanup.append(f"{constants.SERVICE_ACCOUNT}/{self.sa_name}")

        # Mint a token for the service account and store it in a secret
        self.token_secret_name = create_unique_resource_name(
            "keda-prom-token", "secret"
        )
        self.token = ocp_obj.exec_oc_cmd(f"create token {self.sa_name}")
        ocp_obj.exec_oc_cmd(
            f"create secret generic {self.token_secret_name} --from-literal=bearerToken={self.token}",
            silent=True,
        )
        logger.info(f"Created token secret {self.token_secret_name}")
        resources_to_cleanup.append(f"{constants.SECRET}/{self.token_secret_name}")

        # Extract the cluster CA bundle so KEDA can verify Thanos TLS
        self.ca_data = OCP(namespace="openshift-monitoring").exec_oc_cmd(
            "get configmap serving-certs-ca-bundle -o jsonpath='{.data.service-ca\\.crt}'",
            out_yaml_format=False,
        )
        self.ca_secret_name = create_unique_resource_name("keda-prom-ca", "secret")
        ocp_obj.exec_oc_cmd(
            f"create secret generic {self.ca_secret_name} "
            f"--from-literal=ca.crt='{self.ca_data}'",
            silent=True,
        )
        logger.info(f"Created CA secret {self.ca_secret_name}")
        resources_to_cleanup.append(f"{constants.SECRET}/{self.ca_secret_name}")

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
        trigger_auth_data["spec"]["secretTargetRef"][0]["name"] = self.token_secret_name

        # Add TLS CA reference
        trigger_auth_data["spec"]["secretTargetRef"][1]["name"] = self.ca_secret_name

        create_resource(**trigger_auth_data)
        resources_to_cleanup.append(
            f"{constants.TRIGGER_AUTHENTICATION}/{self.ta_name}"
        )

        # Label these resources for easy cleanup
        label_cmd = f"label {' '.join(resources_to_cleanup)} {self.cleanup_label}"
        ocp_obj.exec_oc_cmd(label_cmd)

        logger.info("KEDA configured to read Thanos metrics")

    def _find_existing_scaled_object(self, config_dict):
        """
        Find existing ScaledObject managing the same workload.

        Args:
            config_dict (dict): Configuration containing scaleTargetRef

        Returns:
            ScaledObject: Existing ScaledObject instance if found, None otherwise
        """

        ocp_obj = OCP(
            kind=constants.SCALED_OBJECT,
            namespace=config_dict.get("namespace", self.workload_namespace),
        )

        try:
            scaled_objects = ocp_obj.get()["items"]
            target_ref = config_dict.get("scaleTargetRef", {})

            for so in scaled_objects:
                so_target = so["spec"].get("scaleTargetRef", {})
                # Match by target name and kind
                if so_target.get("name") == target_ref.get("name") and so_target.get(
                    "kind"
                ) == target_ref.get("kind"):
                    # Reconstruct ScaledObject from existing data using Prototype pattern
                    existing_obj = ScaledObject.from_existing(so)
                    logger.info(f"Found existing ScaledObject: {existing_obj.name}")
                    return existing_obj
        except Exception as e:
            logger.debug(f"No existing ScaledObject found: {e}")

        return None

    def create_thanos_metric_scaled_object(self, config_dict):
        """
        Create or update a KEDA ScaledObject driven by a Thanos metric.
        A ScaledObject defines how KEDA should scale a workload: which target
        to scale, what metric to watch, and the conditions that trigger scaling.

        If a ScaledObject already exists for the same workload, it will be updated
        instead of creating a new one, making this method idempotent.

        Args:
            config_dict (dict): A dictionary containing the configuration for the ScaledObject.
            See ScaledObject.KEYS_TO_YAML_PATH for valid keys.
        Returns:
            ScaledObject: The configured ScaledObject instance.
        """
        logger.info(
            f"Creating/updating ScaledObject according to config: {config_dict}"
        )

        config_dict.setdefault("namespace", self.workload_namespace)
        config_dict.setdefault(
            "serverAddress", constants.THANOS_QUERIER_INTERNAL_ADDRESS
        )
        config_dict.setdefault("authenticationRef", self.ta_name)

        # Check if ScaledObject already exists for this workload
        existing_scaled_obj = self._find_existing_scaled_object(config_dict)

        if existing_scaled_obj:
            logger.info(
                f"ScaledObject {existing_scaled_obj.name} already exists, updating..."
            )
            existing_scaled_obj.update_from_dict(config_dict)
            scaled_obj = existing_scaled_obj
        else:
            logger.info("Creating new ScaledObject...")
            scaled_obj = ScaledObject(config_dict)

        # Ensure cleanup label is present (use --overwrite for existing objects)
        ocp_obj = OCP(namespace=self.workload_namespace)
        ocp_obj.exec_oc_cmd(
            f"label {constants.SCALED_OBJECT}/{scaled_obj.name} {self.cleanup_label} --overwrite"
        )

        logger.info(f"ScaledObject ready: {scaled_obj.name}")
        return scaled_obj

    def cleanup(self):
        """
        Cleanup KEDA
        """
        # Delete any resources that were created by this class
        ocp_obj = OCP(namespace=self.workload_namespace)
        resource_types_to_clean = [
            "all",
            constants.SERVICE_ACCOUNT,
            constants.SECRET,
            constants.TRIGGER_AUTHENTICATION,
            constants.SCALED_OBJECT,
        ]

        try:
            ocp_obj.exec_oc_cmd(
                f"delete {','.join(resource_types_to_clean)} -l {self.cleanup_label}"
            )
        except CommandFailed:
            logger.warning(
                f"Failed to delete some resources labeled with {self.cleanup_label}"
            )

        # Uninstall KEDA via Helm
        ocp.switch_to_default_rook_cluster_project()
        self.uninstall()

        # Delete the namespace
        try:
            OCP().exec_oc_cmd(f"delete namespace {self.keda_namespace}", timeout=120)

        except CommandFailed as e:
            logger.warning(f"Failed to delete project {self.keda_namespace}: {e}")
            try:
                # Sometimes this resources hangs the namespace deletion
                OCP().exec_oc_cmd(
                    "delete apiservice v1beta1.external.metrics.k8s.io", timeout=120
                )
            except CommandFailed:
                logger.warning(
                    "Failed to delete apiservice v1beta1.external.metrics.k8s.io"
                )
        OCP(kind=constants.NAMESPACE).wait_for_delete(self.keda_namespace, timeout=60)

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

        self.ocs_obj = None

        self.data = templating.load_yaml(constants.KEDA_SCALED_OBJECT_YAML)
        self.name = create_unique_resource_name("keda-scaled-object", "scaledobject")
        self.data["metadata"]["name"] = self.name

        for key, path in self.KEYS_TO_YAML_PATH.items():
            if key in config_dict:
                self._update_yaml_path(path, config_dict[key], apply=False)

        self.ocs_obj = create_resource(**self.data)
        self.namespace = self.ocs_obj.namespace

    @classmethod
    def from_existing(cls, resource_data):
        """
        Create a ScaledObject instance from existing resource data.

        Implements the Prototype design pattern: creates a new instance by
        cloning from existing Kubernetes resource data.

        Args:
            resource_data (dict): The existing ScaledObject resource data from Kubernetes

        Returns:
            ScaledObject: A new ScaledObject instance cloned from the prototype
        """
        instance = cls.__new__(cls)
        instance.name = resource_data["metadata"]["name"]
        instance.namespace = resource_data["metadata"]["namespace"]

        # Filter out the the rest of the metadata and the status fields
        # these can cause a Conflict error when applying the changes
        resource_data["metadata"] = {
            "name": resource_data["metadata"]["name"],
            "namespace": resource_data["metadata"]["namespace"],
        }
        resource_data["status"] = {}

        instance.data = resource_data
        instance.ocs_obj = OCS(**resource_data)
        return instance

    @property
    def is_created(self):
        return self.ocs_obj is not None

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
            self.ocs_obj.apply(**self.data)

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

        self.ocs_obj.apply(**self.data)
