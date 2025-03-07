import logging
import semantic_version

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
from ocs_ci.utility.utils import wait_custom_resource_defenition_available

logger = logging.getLogger(__name__)


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
            operator_group_yaml.create()
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
            hyperconverged_version = get_hyperconverged_corresponding_version(
                self.ocp_version
            )
            catalog_source_yaml_file["spec"]["image"] = catalog_source_yaml_file[
                "spec"
            ]["image"].format(hyperconverged_version=hyperconverged_version)
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
            hyperconverged_version = get_hyperconverged_corresponding_version(
                self.ocp_version
            )
            subscription_yaml_data["spec"]["channel"] = subscription_yaml_data["spec"][
                "channel"
            ].format(hyperconverged_version=hyperconverged_version)
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


def get_hyperconverged_corresponding_version(ocp_version: str) -> str:
    """
    Given an OCP version, return the corresponding Hyperconverged version.

    Rule:
    - Hyperconverged Major = OCP Major - 3
    - Hyperconverged Minor = OCP Minor - 4

    Args:
        ocp_version: OCP version as a string (e.g., "4.18" or "4.18.3")
    Returns:
        Corresponding Hyperconverged version as a string (e.g., "1.14")
    """
    if not semantic_version.validate(ocp_version):
        ocp_version += ".0"  # Ensure valid semantic versioning if patch is missing

    ocp_semver = semantic_version.Version(ocp_version)
    hyperconverged_major = ocp_semver.major - 3
    hyperconverged_minor = ocp_semver.minor - 4

    return f"{hyperconverged_major}.{hyperconverged_minor}"


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
