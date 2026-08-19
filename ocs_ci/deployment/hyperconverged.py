import json
import logging
from functools import lru_cache

import semantic_version

from ocs_ci.framework import config
from ocs_ci.ocs.exceptions import HyperConvergedNotDeployedException, CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.deployment import Deployment
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.ocs.version import get_ocp_version
from ocs_ci.utility import templating
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import (
    get_url_content,
    wait_custom_resource_defenition_available,
)

logger = logging.getLogger(__name__)

# Fallback only if GitHub lookup fails during deployment.
HCO_STABLE_VERSION_FALLBACK = "1.18.1"
HCO_GITHUB_REPO = "kubevirt/hyperconverged-cluster-operator"
HCO_CATALOG_IMAGE = (
    "quay.io/kubevirt/hyperconverged-cluster-index:{hyperconverged_image_tag}"
)


class HyperConverged:
    """
    This class represent HyperConverged and contains all related methods we need to do with it.
    Hyperconverged Operator is used instead of unreleased CNV, to overcome catalogsource limitations on Client clusters
    """

    def __init__(self):
        self.namespace = constants.HYPERCONVERGED_NAMESPACE
        self.ns_obj = OCP(kind=constants.NAMESPACES)
        self.operator_group = OCP(
            kind=constants.OPERATOR_GROUP, namespace=self.namespace
        )
        self.catsrc = OCP(
            kind=constants.CATSRC, namespace=constants.MARKETPLACE_NAMESPACE
        )
        self.subs = OCP(kind=constants.PROVIDER_SUBSCRIPTION, namespace=self.namespace)
        # type of hyperconverged becomes available after the Hyperconverged operator is deployed
        self.hyperconverged = None
        self.ocp_version = get_ocp_version()

    def create_hyperconverged_namespace(self):
        """
        Creates the namespace for hyperconverged resources

        """
        if not self.ns_obj.is_exist(
            resource_name=self.namespace,
        ):
            logger.info(
                f"Creating namespace {self.namespace} for hyperconverged resources"
            )
            namespace_yaml_file = templating.load_yaml(
                constants.HYPERCONVERGED_NAMESPACE_YAML
            )
            namespace_yaml = OCS(**namespace_yaml_file)
            namespace_yaml.create()
        else:
            logger.info(f"{self.namespace} already exists")
        return self.ns_obj.check_resource_existence(
            should_exist=True, resource_name=self.namespace
        )

    def create_operator_group(self):
        """
        Creates operator group for hyperconverged resources

        """
        logger.info("Creating operator group for hyperconverged resources")

        if not self.operator_group.is_exist(
            resource_name=constants.HYPERCONVERGED_OPERATOR_GROUP_NAME
        ):
            operator_group_yaml_file = templating.load_yaml(
                constants.HYPERCONVERGED_OPERATOR_GROUP_YAML
            )
            operator_group_yaml = OCS(**operator_group_yaml_file)
            try:
                operator_group_yaml.create()
            except CommandFailed as e:
                if "AlreadyExists" in str(e):
                    logger.info("OperatorGroup already exists, continuing")
                else:
                    raise
        return self.operator_group.check_resource_existence(
            should_exist=True,
            resource_name=constants.HYPERCONVERGED_OPERATOR_GROUP_NAME,
        )

    def create_catalog_source(self):
        """
        Creates catalog source for hyperconverged resources
        ! No customization by purpose. Will always align with branch default image that is set in the default config.
        """
        logger.info("Check if catalog source already exist")
        if not self.catsrc.is_exist(
            resource_name=constants.HYPERCONVERGED_CATALOGSOURCE
        ):
            catalog_source_yaml_file = templating.load_yaml(
                constants.HYPERVERGED_CATALOGSOURCE_YAML
            )
            catalog_image = get_hyperconverged_catalog_image(self.ocp_version)
            logger.info("Using HyperConverged catalog image %s", catalog_image)
            catalog_source_yaml_file["spec"]["image"] = catalog_image
            catalog_source_yaml = OCS(**catalog_source_yaml_file)
            catalog_source_yaml.create()
        self.catsrc.check_resource_existence(
            should_exist=True, resource_name=constants.HYPERCONVERGED_CATALOGSOURCE
        )
        catalog_source_yaml = CatalogSource(
            constants.HYPERCONVERGED_CATALOGSOURCE, constants.MARKETPLACE_NAMESPACE
        )
        catalog_source_yaml.wait_for_state("READY")

    def create_subscription(self):
        """
        Creates subscription for hyperconverged operator

        """
        logger.info("Check if subscription already exist")
        if not self.subs.is_exist(resource_name=constants.HYPERCONVERGED_SUBSCRIPTION):
            subscription_yaml_data = templating.load_yaml(
                constants.HYPERCONVERGED_SUBSCRIPTION_YAML
            )
            channel = get_hyperconverged_subscription_channel(self.ocp_version)
            logger.info("Using HyperConverged subscription channel %s", channel)
            subscription_yaml_data["spec"]["channel"] = channel
            subscription_obj = OCS(**subscription_yaml_data)
            subscription_obj.create()
        self.subs.check_resource_existence(
            should_exist=True, resource_name=constants.HYPERCONVERGED_SUBSCRIPTION
        )

        pod_names = get_pod_name_by_pattern(
            "hco-operator", self.namespace
        ) + get_pod_name_by_pattern("virt-operator", self.namespace)
        wait_for_pods_to_be_running(namespace=self.namespace, pod_names=pod_names)

    def create_hyperconverged_instance(self):
        """
        Create Hyperconverged instance
        """
        self.hyperconverged = OCP(
            kind=constants.HYPERCONVERGED_KIND, namespace=self.namespace
        )
        if not self.hyperconverged.is_exist(
            resource_name=constants.HYPERCONVERGED_NAME
        ):
            hyperconverged_instance_yaml_file = templating.load_yaml(
                constants.HYPERCONVERGED_YAML
            )
            hyperconverged_instance_yaml = OCS(**hyperconverged_instance_yaml_file)
            retry(CommandFailed, tries=10, delay=60)(
                hyperconverged_instance_yaml.create
            )()

        self.hyperconverged.check_resource_existence(
            should_exist=True, resource_name=constants.HYPERCONVERGED_NAME
        )
        # wait for pods to be up and running
        deployments = ["virt-operator", "virt-api", "virt-controller"]

        for resource_name in deployments:
            depl_ocp_obj = OCP(
                kind=constants.DEPLOYMENT,
                namespace=self.namespace,
                resource_name=resource_name,
            )
            deployment_obj = Deployment(
                **depl_ocp_obj.get(retry=60, wait=10, dont_raise=True)
            )
            deployment_obj.wait_for_available_replicas(timeout=600)

    def deploy_hyperconverged(self):
        """
        Deploy Hyperconverged Operator and resources
        """
        # avoid mix in MRO calling explicitly the method of own class
        HyperConverged.create_hyperconverged_namespace(self)
        HyperConverged.create_operator_group(self)
        HyperConverged.create_catalog_source(self)
        HyperConverged.create_subscription(self)
        if not wait_custom_resource_defenition_available(constants.HYPERCONVERGED_CRD):
            raise HyperConvergedNotDeployedException(
                f"crd {constants.HYPERCONVERGED_CRD} is unavailable"
            )
        HyperConverged.create_hyperconverged_instance(self)


def _parse_ocp_version(ocp_version: str) -> semantic_version.Version:
    """
    Parse an OCP version string into a semantic_version.Version.

    Args:
        ocp_version (str): OCP version (e.g., "4.18", "4.18.3", "5.0")

    Returns:
        semantic_version.Version: Parsed OCP version

    """
    if not semantic_version.validate(ocp_version):
        ocp_version += ".0"
    return semantic_version.Version(ocp_version)


@lru_cache(maxsize=1)
def get_latest_stable_hyperconverged_version():
    """
    Resolve the latest stable community HCO release tag.

    Uses GitHub ``releases/latest`` for kubevirt/hyperconverged-cluster-operator
    (non-prerelease). Result is cached for the process lifetime via lru_cache.
    Call ``get_latest_stable_hyperconverged_version.cache_clear()`` to refresh.

    Returns:
        str: Stable HCO version without leading ``v`` (e.g. ``1.18.1``).

    """
    api_url = f"https://api.github.com/repos/{HCO_GITHUB_REPO}/releases/latest"
    try:
        release_data = json.loads(get_url_content(api_url, timeout=60))
        tag_name = release_data["tag_name"].lstrip("v")
        if not semantic_version.validate(tag_name):
            raise ValueError(f"Unexpected HCO release tag: {tag_name}")
        logger.info("Resolved latest stable HCO version: %s", tag_name)
        return tag_name
    except Exception as ex:
        logger.warning(
            "Failed to resolve latest stable HCO from GitHub (%s). "
            "Using fallback %s",
            ex,
            HCO_STABLE_VERSION_FALLBACK,
        )
        return HCO_STABLE_VERSION_FALLBACK


def use_stable_hyperconverged(ocp_version: str) -> bool:
    """
    Return True when deployment should use the latest stable HCO release.

    OCP 5.x cannot use the OCP 4.x version formula (produces invalid tags like
    2.-4). Config DEPLOYMENT.hyperconverged_use_stable can force stable on any
    OCP version.

    Args:
        ocp_version (str): OCP version string

    Returns:
        bool: Whether to use latest stable HCO

    """
    if config.DEPLOYMENT.get("hyperconverged_use_stable"):
        return True
    return _parse_ocp_version(ocp_version).major >= 5


def get_hyperconverged_corresponding_version(ocp_version: str) -> str:
    """
    Given an OCP version, return the corresponding Hyperconverged version.

    Rules:
    - Optional override: DEPLOYMENT.hyperconverged_version
    - OCP 5.x (or hyperconverged_use_stable): latest stable HCO
    - OCP 4.x: Hyperconverged Major = OCP Major - 3,
      Hyperconverged Minor = OCP Minor - 4 (e.g. 4.18 -> 1.14)

    Args:
        ocp_version: OCP version as a string (e.g., "4.18" or "4.18.3")
    Returns:
        Corresponding Hyperconverged version as a string (e.g., "1.14" or
        "1.18.1")
    """
    override = config.DEPLOYMENT.get("hyperconverged_version")
    if override:
        return str(override)

    if use_stable_hyperconverged(ocp_version):
        return get_latest_stable_hyperconverged_version()

    ocp_semver = _parse_ocp_version(ocp_version)
    hyperconverged_major = ocp_semver.major - 3
    hyperconverged_minor = ocp_semver.minor - 4

    return f"{hyperconverged_major}.{hyperconverged_minor}"


def get_hyperconverged_catalog_image(ocp_version: str) -> str:
    """
    Return the CatalogSource image for HyperConverged.

    Args:
        ocp_version (str): OCP version string

    Returns:
        str: Full catalog index image reference

    """
    override = config.DEPLOYMENT.get("hyperconverged_catalog_image")
    if override:
        return override

    version = get_hyperconverged_corresponding_version(ocp_version)
    if use_stable_hyperconverged(ocp_version):
        image_tag = version
    else:
        # OCP 4.x unreleased path uses X.Y.0-unstable index tags
        image_tag = f"{version}.0-unstable"
    return HCO_CATALOG_IMAGE.format(hyperconverged_image_tag=image_tag)


def get_hyperconverged_subscription_channel(ocp_version: str) -> str:
    """
    Return the Subscription channel for HyperConverged.

    Args:
        ocp_version (str): OCP version string

    Returns:
        str: OLM channel name (e.g. "stable-v1.18" or "candidate-v1.14")

    """
    override = config.DEPLOYMENT.get("hyperconverged_channel")
    if override:
        return override

    if use_stable_hyperconverged(ocp_version):
        version = get_hyperconverged_corresponding_version(ocp_version)
        # Quay stable indexes use stable-vX.Y (e.g. 1.18.1 -> stable-v1.18)
        if not semantic_version.validate(version):
            version = f"{version}.0"
        semver = semantic_version.Version(version)
        return f"stable-v{semver.major}.{semver.minor}"

    version = get_hyperconverged_corresponding_version(ocp_version)
    return f"candidate-v{version}"


def get_ocp_corresponding_version(hyperconverged_version: str) -> str:
    """
    Given a Hyperconverged version, return the corresponding OCP version.

    Rule:
    - OCP Major = Hyperconverged Major + 3
    - OCP Minor = Hyperconverged Minor + 4

    Args:
        hyperconverged_version: Hyperconverged version as a string (e.g., "1.14")
    Returns:
        Corresponding OCP version as a string (e.g., "4.18")
    """
    if not semantic_version.validate(hyperconverged_version):
        hyperconverged_version += (
            ".0"  # Ensure valid semantic versioning if patch is missing
        )

    hyperconverged_semver = semantic_version.Version(hyperconverged_version)
    ocp_major = hyperconverged_semver.major + 3
    ocp_minor = hyperconverged_semver.minor + 4

    return f"{ocp_major}.{ocp_minor}"
