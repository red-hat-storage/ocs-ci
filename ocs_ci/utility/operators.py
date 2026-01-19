"""
Operators utility functions
"""

# Properly order all imports

from ocs_ci.deployment.disconnected import prune_and_mirror_index_image
from ocs_ci.ocs import constants
from ocs_ci.utility import templating
from ocs_ci.framework import config
from ocs_ci.ocs.node import get_all_nodes
from ocs_ci.ocs.resources.csv import CSV, get_csvs_start_with_prefix
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.resources.packagemanifest import PackageManifest
from ocs_ci.ocs.exceptions import (
    ResourceNotFoundError,
    CommandFailed,
    TimeoutExpiredError,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility.utils import run_cmd, TimeoutSampler
from ocs_ci.utility.version import (
    get_semantic_ocp_version_from_config,
    get_ocp_ga_version,
    get_ocp_version,
    get_semantic_version,
)
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import wait_for_machineconfigpool_status

import logging
import os
import yaml
from distutils.version import LooseVersion
from tempfile import NamedTemporaryFile


logger = logging.getLogger(__name__)


# Operators related constants
UNRELEASED_OPERATORS_FBC_IMAGE = "quay.io/redhat-user-workloads/ocp-art-tenant/art-fbc"
OPERATORS_TEMPLATES_DIR = os.path.join(constants.TEMPLATE_DIR, "operators")
UNRELEASED_DEFAULT_MIRROR = (
    "quay.io/redhat-user-workloads/ocp-art-tenant/art-images-share"
)


class Operator:
    # expected to be overridden by child classes
    name: str = None
    catalog_name: str = constants.OPERATOR_CATALOG_SOURCE_NAME
    unreleased_catalog_image: str = UNRELEASED_OPERATORS_FBC_IMAGE
    unreleased_catalog_image_tag: str = None
    namespace: str = None
    disconnected_required_packages: list[str] = []
    """
    Get the list of related images from the unreleased catalog image
    Example:
    ```
    oras discover --format \
        json quay.io/redhat-user-workloads/ocp-art-tenant/art-fbc:ocp__4.21__kubernetes-nmstate-rhel9-operator \
        | jq -r '.referrers[] | select(.artifactType == "application/vnd.konflux-ci.attached-artifact") \
        | .digest' | xargs -I {} oras pull \
        quay.io/redhat-user-workloads/ocp-art-tenant/art-fbc@{} && cat related-images.json \
        | jq -r '.[]' | sed 's/@sha256:.*//'
    ```
    """
    unreleased_images: list[str] = []
    unreleased_mirror: str = UNRELEASED_DEFAULT_MIRROR

    def __init__(self, create_catalog: bool = False):
        """
        Initialize the operator

        Args:
            create_catalog (bool): whether to create the catalog if necessary for the operator
        """
        if not self.name:
            raise ValueError("Child class must define attribute `name`")
        if not self.catalog_name:
            raise ValueError("Child class must define attribute `catalog_name`")
        if not self.namespace:
            raise ValueError("Child class must define attribute `namespace`")
        ocp_version = get_semantic_ocp_version_from_config()
        ocp_ga_version = get_ocp_ga_version(ocp_version)
        available = self.is_available()
        if not ocp_ga_version or not available:
            self.set_unreleased_catalog()
            if create_catalog:
                self.create_catalog()

    @property
    def unreleased_catalog_full_image(self):
        if self.unreleased_catalog_image_tag and self.unreleased_catalog_image:
            return (
                f"{self.unreleased_catalog_image}:{self.unreleased_catalog_image_tag}"
            )
        raise ValueError(
            "Child class must define attribute `unreleased_catalog_image_tag` and `unreleased_catalog_image`"
        )

    @property
    def catalog_selector(self):
        return f"catalog={self.catalog_name}"

    @property
    def unreleased_idms_name(self):
        return f"{self.name}-unreleased-idms"

    @property
    def unreleased_catalog_name(self):
        return f"{self.name}-unreleased-catalog"

    def set_unreleased_catalog(self):
        self.catalog_name = self.unreleased_catalog_name

    def get_idms_data(self):
        if not self.unreleased_images:
            raise ValueError("Child class must define attribute `unreleased_images`")
        unreleased_catalog_idms_data = templating.load_yaml(
            os.path.join(
                OPERATORS_TEMPLATES_DIR, "unreleased-operator-catalog-idms.yaml"
            )
        )
        unreleased_catalog_idms_data["metadata"]["name"] = self.catalog_name
        unreleased_catalog_idms_data["spec"]["imageDigestMirrors"] = []
        for image in self.unreleased_images:
            unreleased_catalog_idms_data["spec"]["imageDigestMirrors"].append(
                {
                    "mirrors": [self.unreleased_mirror],
                    "source": image,
                }
            )
        return unreleased_catalog_idms_data

    def create_idms_for_unreleased_catalog(self):
        idms_data = self.get_idms_data()
        unreleased_catalog_idms_data_yaml = NamedTemporaryFile(
            mode="w+", prefix=f"{self.name}-unreleased-catalog", delete=False
        )
        with open(unreleased_catalog_idms_data_yaml.name, "w") as fd:
            fd.write(yaml.dump(idms_data))
        run_cmd(f"oc apply -f {unreleased_catalog_idms_data_yaml.name}")

    def create_catalog(self):
        """
        Create a catalog for the operator
        """
        if config.DEPLOYMENT.get("disconnected"):
            self.create_disconnected_catalog()
        else:
            self.create_unreleased_catalog()

    def _create_catalog(self, image: str, name: str):
        """
        Create a catalog for the operator

        Args:
            image (str): the image to use for the catalog
            name (str): the name of the catalog
        """
        catalog_data = templating.load_yaml(
            os.path.join(OPERATORS_TEMPLATES_DIR, "unreleased-operator-catalog.yaml")
        )
        catalog_data["spec"]["image"] = image
        catalog_data["metadata"]["name"] = name
        catalog_data_yaml = NamedTemporaryFile(
            mode="w+", prefix=f"{self.name}-catalog", delete=False
        )
        with open(catalog_data_yaml.name, "w") as fd:
            fd.write(yaml.dump(catalog_data))
        run_cmd(f"oc apply -f {catalog_data_yaml.name}")
        wait_for_machineconfigpool_status("all", force_delete_pods=False)

    def create_unreleased_catalog(self):
        """
        Create the unreleased catalog for the operator
        """
        if not self.unreleased_catalog_image:
            raise ValueError(
                "Child class must define attribute `unreleased_catalog_image`"
            )
        self.create_idms_for_unreleased_catalog()
        self._create_catalog(
            self.unreleased_catalog_full_image, self.unreleased_catalog_name
        )

    def create_disconnected_catalog(self):
        # in case of disconnected environment, we have to mirror all the
        # optional_operators images
        idms = self.get_idms_data()
        mirrored_index_image = (
            f"{config.DEPLOYMENT['mirror_registry']}/"
            f"{self.unreleased_catalog_full_image.split('/', 1)[-1]}"
        )
        prune_and_mirror_index_image(
            self.unreleased_catalog_full_image,
            mirrored_index_image,
            self.disconnected_required_packages,
            idms=idms,
        )
        self._create_catalog(mirrored_index_image, self.unreleased_catalog_name)

    def is_available(self):
        """
        Check if the operator is available

        Returns:
            bool: True if the operator is available, False otherwise

        """
        package_manifest = PackageManifest(
            resource_name=self.name,
            selector=f"catalog={self.catalog_name}",
        )
        try:
            package_manifest.get()
            return True
        except ResourceNotFoundError:
            return False

    @retry(CommandFailed, 5, 30, 1)
    def get_channel(self):
        """
        Get the channel to use for installing the operator

        Returns:
            str: operator channel

        """
        ocp_version = get_ocp_version()
        selector = self.catalog_selector
        # Retrieve available channels
        package_manifest = PackageManifest(resource_name=self.name, selector=selector)
        channels = package_manifest.get_channels()

        versions = []
        stable_channel_found = False
        for channel in channels:
            if ocp_version == channel["name"]:
                return ocp_version
            else:
                if channel["name"] != constants.STABLE:
                    versions.append(LooseVersion(channel["name"]))
                else:
                    logger.debug(f"channel with name {channel['name']} found")
                    stable_channel_found = True
                    stable_channel_full_version = channel["currentCSVDesc"]["version"]
                    stable_channel_version = get_semantic_version(
                        stable_channel_full_version, only_major_minor=True
                    )

        # Ensure versions are sorted
        versions.sort()
        sorted_versions = [v.vstring for v in versions]

        if len(sorted_versions) >= 1:
            # Use latest channel
            if stable_channel_found:
                if stable_channel_version > get_semantic_version(sorted_versions[-1]):
                    return constants.STABLE
                else:
                    return sorted_versions[-1]
        else:
            return channels[-1]["name"]

    def _customize_operatorgroup(self, operatorgroup_data: dict):
        """
        Hook for child classes to customize OperatorGroup YAML

        Args:
            operatorgroup_data (dict): the OperatorGroup YAML data
        """
        pass

    def create_operatorgroup(self):
        """
        Create an OperatorGroup for the operator
        """
        operatorgroup_data = templating.load_yaml(
            os.path.join(OPERATORS_TEMPLATES_DIR, "operatorgroup.yaml")
        )
        operatorgroup_data["metadata"]["name"] = self.name
        operatorgroup_data["metadata"]["namespace"] = self.namespace
        operatorgroup_data["spec"]["targetNamespaces"] = [self.namespace]
        self._customize_operatorgroup(operatorgroup_data)
        operatorgroup_data_yaml = NamedTemporaryFile(
            mode="w+", prefix=f"{self.name}-operatorgroup", delete=False
        )
        with open(operatorgroup_data_yaml.name, "w") as fd:
            fd.write(yaml.dump(operatorgroup_data))
        run_cmd(f"oc apply -f {operatorgroup_data_yaml.name}")

    def create_namespace(self):
        """
        Create a namespace for the operator
        """
        namespace_data = templating.load_yaml(
            os.path.join(OPERATORS_TEMPLATES_DIR, "namespace.yaml")
        )
        namespace_data["metadata"]["name"] = self.namespace
        self._customize_namespace(namespace_data)
        namespace_data_yaml = NamedTemporaryFile(
            mode="w+", prefix=f"{self.name}-namespace", delete=False
        )
        with open(namespace_data_yaml.name, "w") as fd:
            fd.write(yaml.dump(namespace_data))
        run_cmd(f"oc apply -f {namespace_data_yaml.name}")

    def _customize_namespace(self, namespace_data: dict):
        """
        Hook for child classes to customize Namespace YAML

        Args:
            namespace_data (dict): the Namespace YAML data
        """
        pass

    def create_subscription(self):
        """
        Create a subscription for the operator
        """
        subscription_data = templating.load_yaml(
            os.path.join(OPERATORS_TEMPLATES_DIR, "subscription.yaml")
        )
        subscription_data["metadata"]["name"] = self.name
        subscription_data["metadata"]["namespace"] = self.namespace
        subscription_data["spec"]["channel"] = self.get_channel()
        subscription_data["spec"]["name"] = self.name
        subscription_data["spec"]["source"] = self.catalog_name
        self._customize_subscription(subscription_data)
        subscription_data_yaml = NamedTemporaryFile(
            mode="w+", prefix=f"{self.name}-subscription", delete=False
        )
        with open(subscription_data_yaml.name, "w") as fd:
            fd.write(yaml.dump(subscription_data))
        run_cmd(f"oc apply -f {subscription_data_yaml.name}")

    def _customize_subscription(self, subscription_data: dict):
        """
        Hook for child classes to customize Subscription YAML

        Args:
            subscription_data (dict): the Subscription YAML data
        """
        pass

    def deploy(self):

        self.create_namespace()
        self.create_operatorgroup()
        self.create_subscription()
        self._customize_post_deployment_steps()
        self._deployment_verification()

    def _customize_post_deployment_steps(self):
        """
        Hook for child classes to customize post deployment steps
        """
        pass

    def _deployment_verification(self):
        """
        Hook for child classes to verify the deployment
        """
        pass


class NMStateOperator(Operator):
    def __init__(self, create_catalog: bool = False):
        self.name = constants.NMSTATE_OPERATOR
        ocp_version = get_semantic_ocp_version_from_config()
        self.unreleased_catalog_image_tag: str = (
            f"ocp__{ocp_version}__kubernetes-nmstate-rhel9-operator"
        )
        self.unreleased_images = [
            "registry.redhat.io/openshift4/kubernetes-nmstate-operator-bundle",
            "registry.redhat.io/openshift4/kubernetes-nmstate-rhel9-operator",
            "registry.redhat.io/openshift4/nmstate-console-plugin-rhel9",
            "registry.redhat.io/openshift4/ose-kube-rbac-proxy-rhel9",
            "registry.redhat.io/openshift4/ose-kubernetes-nmstate-handler-rhel9",
        ]
        self.namespace = constants.NMSTATE_NAMESPACE
        self.disconnected_required_packages = [
            "kubernetes-nmstate-operator",
        ]
        super().__init__(create_catalog)

    def _customize_operatorgroup(self, operatorgroup_data: dict):
        """
        Hook for NMStateOperator to customize OperatorGroup YAML

        Args:
            operatorgroup_data (dict): the OperatorGroup YAML data
        """
        operatorgroup_data["metadata"]["annotations"] = {
            "olm.providedAPIs": "NMState.v1.nmstate.io",
        }

    def _customize_namespace(self, namespace_data: dict):
        """
        Hook for NMStateOperator to customize Namespace YAML

        Args:
            namespace_data (dict): the Namespace YAML data
        """
        namespace_data["metadata"]["labels"] = {
            "kubernetes.io/metadata.name": self.namespace,
            "name": self.namespace,
        }
        namespace_data["spec"]["finalizers"] = [
            "kubernetes",
        ]

    def _customize_subscription(self, subscription_data: dict):
        """
        Hook for NMStateOperator to customize Subscription YAML

        Args:
            subscription_data (dict): the Subscription YAML data
        """
        subscription_data["metadata"]["labels"] = {
            "operators.coreos.com/kubernetes-nmstate-operator.openshift-nmstate": "",
        }

    def verify_nmstate_csv_status(self):
        """
        Verify the CSV status for the nmstate Operator deployment equals Succeeded

        """
        for csv in TimeoutSampler(
            timeout=900,
            sleep=15,
            func=get_csvs_start_with_prefix,
            csv_prefix=self.name,
            namespace=self.namespace,
        ):
            if csv:
                break
        csv_name = csv[0]["metadata"]["name"]
        csv_obj = CSV(resource_name=csv_name, namespace=self.namespace)
        csv_obj.wait_for_phase(phase="Succeeded", timeout=720)

    def create_nmstate_instance(self):
        """
        Create an instance of the nmstate Operator

        """
        logger.info("Creating NMState Instance")
        subscription_yaml_file = templating.load_yaml(constants.NMSTATE_INSTANCE_YAML)
        subscription_yaml = OCS(**subscription_yaml_file)
        subscription_yaml.create()
        logger.info("NMState Instance created successfully")

    def verify_nmstate_pods_running(self):
        """
        Verify the pods for NMState Operator are running

        """
        # the list of pods is this:
        # nmstate-console-plugin-*
        # nmstate-metrics-*
        # nmstate-operator-*
        # nmstate-webhook-*
        # nmstate-handler-* for each node
        number_of_expected_pods = 4 + len(get_all_nodes())
        sample = TimeoutSampler(
            timeout=300,
            sleep=10,
            func=self.count_nmstate_pods_running,
            count=number_of_expected_pods,
        )
        if not sample.wait_for_func_status(result=True):
            raise TimeoutExpiredError(
                "Not all nmstate pods in Running state after 300 seconds"
            )

    def count_nmstate_pods_running(self, count):
        """
        Count the pods for NMState Operator are running

        Returns:
            bool:

        """
        count_running_nmstate_pods = 0
        ocp_pod = OCP(kind=constants.POD, namespace=self.namespace)
        pod_items = ocp_pod.get().get("items")
        # Check if nmstate pods are in running state
        for nmstate_pod in pod_items:
            nmstate_pod_name = nmstate_pod.get("metadata").get("name")
            status = ocp_pod.get_resource_status(nmstate_pod_name)
            if status == constants.STATUS_RUNNING:
                logger.info(f"NMState pod {nmstate_pod_name} in running state")
                count_running_nmstate_pods += 1
        return count_running_nmstate_pods >= count

    def _customize_post_deployment_steps(self):
        """
        Customize post deployment steps for NMStateOperator
        """
        self.verify_nmstate_csv_status()
        self.create_nmstate_instance()

    def _deployment_verification(self):
        """
        Verify the deployment of the nmstate operator
        """
        self.verify_nmstate_pods_running()


class LocalStorageOperator(Operator):
    def __init__(self, create_catalog: bool = False):
        self.name = constants.LOCAL_STORAGE_OPERATOR_NAME
        ocp_version = get_semantic_ocp_version_from_config()
        self.unreleased_catalog_image_tag: str = (
            f"ocp__{ocp_version}__ose-local-storage-rhel9-operator"
        )
        self.unreleased_images = [
            "registry.redhat.io/openshift4/ose-kube-rbac-proxy-rhel9",
            "registry.redhat.io/openshift4/ose-local-storage-diskmaker-rhel9",
            "registry.redhat.io/openshift4/ose-local-storage-mustgather-rhel9",
            "registry.redhat.io/openshift4/ose-local-storage-operator-bundle",
            "registry.redhat.io/openshift4/ose-local-storage-rhel9-operator",
        ]
        self.disconnected_required_packages = [
            "local-storage-operator",
        ]
        self.namespace = constants.LOCAL_STORAGE_NAMESPACE
        super().__init__(create_catalog)

    def _customize_operatorgroup(self, operatorgroup_data: dict):
        """
        Hook for LSO to customize OperatorGroup YAML

        Args:
            operatorgroup_data (dict): the OperatorGroup YAML data
        """
        operatorgroup_data["metadata"]["annotations"] = {
            "olm.providedAPIs": "LocalVolume.v1.local.storage.openshift.io",
        }

    def _deployment_verification(self):
        """
        Verify the deployment of the local storage operator
        """
        local_storage_operator = OCP(kind=constants.POD, namespace=self.namespace)
        assert local_storage_operator.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=constants.LOCAL_STORAGE_OPERATOR_LABEL,
            timeout=600,
        ), "Local storage operator did not reach running phase"


class MetalLBOperator(Operator):
    def __init__(self, create_catalog: bool = False):
        self.name = constants.METALLB_OPERATOR_NAME
        ocp_version = get_semantic_ocp_version_from_config()
        self.unreleased_catalog_image_tag: str = (
            f"ocp__{ocp_version}__metallb-rhel9-operator"
        )
        self.unreleased_images = [
            "registry.redhat.io/openshift4/frr-rhel9",
            "registry.redhat.io/openshift4/metallb-rhel9-operator",
            "registry.redhat.io/openshift4/metallb-rhel9",
            "registry.redhat.io/openshift4/ose-kube-rbac-proxy-rhel9",
            "registry.redhat.io/openshift4/ose-metallb-operator-bundle",
        ]
        self.disconnected_required_packages = [
            "metallb-operator",
        ]
        self.namespace = constants.METALLB_DEFAULT_NAMESPACE
        super().__init__(create_catalog)

    def _customize_operatorgroup(self, operatorgroup_data: dict):
        """
        Hook for MetalLB to customize OperatorGroup YAML

        Args:
            operatorgroup_data (dict): the OperatorGroup YAML data
        """
        operatorgroup_data["metadata"]["annotations"] = {
            "olm.providedAPIs": "MetalLB.v1beta1.metallb.io",
        }
        # metallb does not support InstallMode OwnNamespace
        operatorgroup_data["spec"]["targetNamespaces"] = []

    def _deployment_verification(self):
        """
        Verify the deployment of the MetalLB operator
        """
        metallb_operator = OCP(kind=constants.POD, namespace=self.namespace)
        assert metallb_operator.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=constants.MANAGED_CONTROLLER_LABEL,
            timeout=600,
        ), "MetalLB operator did not reach running phase"
