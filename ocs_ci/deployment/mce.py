"""
This module contains functionality required for mce installation.
"""

import logging
import tempfile

from ocs_ci.framework import config
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility import templating
from ocs_ci.ocs import constants
from ocs_ci.utility.utils import (
    run_cmd,
    exec_cmd,
)
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs import ocp
from ocs_ci.utility.utils import get_running_ocp_version
from ocs_ci.ocs.exceptions import CommandFailed, UnavailableResourceException

logger = logging.getLogger(__name__)


class MCEInstaller(object):
    """
    mce Installer class for mce deployment
    """

    def __init__(self):
        self.namespace = constants.MCE_NAMESPACE
        self.ns_obj = ocp.OCP(kind=constants.NAMESPACES)
        self.hypershift_override_image_cm = "hypershift-override-images-new"
        self.multicluster_engine = ocp.OCP(
            kind="MultiClusterEngine",
            resource_name=constants.MULTICLUSTER_ENGINE,
        )
        self.catsrc = ocp.OCP(
            kind=constants.CATSRC, namespace=constants.MARKETPLACE_NAMESPACE
        )
        self.subs = ocp.OCP(kind=constants.PROVIDER_SUBSCRIPTION)

    def create_mce_catalog_source(self):
        """
        Creates a catalogsource for mce operator.

        """
        if not self.catsrc.is_exist(
            resource_name=constants.MCE_CATSRC_NAME,
        ):
            logger.info("Adding CatalogSource for MCE")
            mce_catalog_source_data = templating.load_yaml(constants.MCE_CATSRC_YAML)
            if config.ENV_DATA.get("mce_image"):
                mce_image_tag = config.ENV_DATA.get("mce_image")
                mce_catalog_source_data["spec"]["image"] = (
                    "quay.io:443/acm-d/mce-custom-registry:" + mce_image_tag
                )
            mce_catalog_source_manifest = tempfile.NamedTemporaryFile(
                mode="w+", prefix="mce_catalog_source_manifest", delete=False
            )
            templating.dump_data_to_temp_yaml(
                mce_catalog_source_data, mce_catalog_source_manifest.name
            )
            run_cmd(f"oc apply -f {mce_catalog_source_manifest.name}", timeout=2400)
            mce_catalog_source = CatalogSource(
                resource_name=constants.MCE_CATSRC_NAME,
                namespace=constants.MARKETPLACE_NAMESPACE,
            )
            # Wait for catalog source is ready
            mce_catalog_source.wait_for_state("READY")
        else:
            logger.info("catalogsource exists")

    def create_mce_namespace(self):
        """
        Creates the namespace for mce resources

        Raises:
            CommandFailed: If the 'oc create' command fails.
        """
        if not self.ns_obj.is_exist(
            resource_name=self.namespace,
        ):
            logger.info(f"Creating namespace {self.namespace} for mce resources")
            namespace_yaml_file = templating.load_yaml(constants.MCE_NAMESPACE_YAML)
            namespace_yaml = OCS(**namespace_yaml_file)
            namespace_yaml.create()
            logger.info(f"MCE namespace {self.namespace} was created successfully")
        else:
            logger.info(f"{self.namespace} already exists")

    def create_multiclusterengine_operatorgroup(self):
        """
        Creates multiclusterengine operator group

        """
        logger.info("Check if mce operator already exist")
        if not self.multicluster_engine.is_exist(
            resource_name=constants.MULTICLUSTER_ENGINE
        ):

            operatorgroup_yaml_file = templating.load_yaml(
                constants.MCE_OPERATOR_GROUP_YAML
            )
            operatorgroup_yaml = OCS(**operatorgroup_yaml_file)
            operatorgroup_yaml.create()
            logger.info("mce OperatorGroup created successfully")
        self.multicluster_engine.wait_for_phase("Available")

    def create_multiclusterengine_resource(self):
        """
        Creates multiclusterengine resource

        """
        resource_yaml_file = templating.load_yaml(constants.MCE_RESOURCE_YAML)
        resource_yaml = OCS(**resource_yaml_file)
        resource_yaml.create()
        logger.info("mce resource created successfully")

    def create_mce_subscription(self):
        """
        Creates subscription for mce operator

        """
        logger.info("Check if mce subscription already exist")
        if not self.subs.is_exist(resource_name=constants.MCE_OPERATOR):
            mce_subscription_yaml_data = templating.load_yaml(
                constants.MCE_SUBSCRIPTION_YAML
            )

            if config.DEPLOYMENT.get("mce_latest_stable"):
                mce_subscription_yaml_data["spec"][
                    "source"
                ] = constants.OPERATOR_CATALOG_SOURCE_NAME
                mce_channel = "stable"
            else:
                mce_channel = config.DEPLOYMENT.get("mce_channel")

            mce_subscription_yaml_data["spec"]["channel"] = mce_channel
            mce_subscription_manifest = tempfile.NamedTemporaryFile(
                mode="w+", prefix="mce_subscription_manifest", delete=False
            )
            templating.dump_data_to_temp_yaml(
                mce_subscription_yaml_data, mce_subscription_manifest.name
            )
            logger.info("Creating subscription for mce operator")
            run_cmd(f"oc create -f {mce_subscription_manifest.name}")
            OCP(
                kind=constants.SUBSCRIPTION_COREOS,
                namespace=self.namespace,
                resource_name=constants.MCE_OPERATOR,
            ).check_resource_existence(
                should_exist=True, resource_name=constants.MCE_OPERATOR
            )
        else:
            logger.info("mce operator is already installed")

    def check_hypershift_namespace(self):
        """
        Check hypershift namespace created

        """
        logger.info(f"hypershift namespace {self.namespace} was created successfully")
        is_hypershift_ns_available = self.ns_obj.is_exist(
            resource_name=constants.HYPERSHIFT_NAMESPACE,
        )
        return is_hypershift_ns_available

    def check_supported_versions(self):
        """
        Check supported ocp versions for hcp cluster creation

        """
        configmaps_obj = OCP(
            kind=constants.CONFIGMAP,
            namespace=constants.HYPERSHIFT_NAMESPACE,
        )

        if not configmaps_obj.is_exist(
            resource_name=constants.SUPPORTED_VERSIONS_CONFIGMAP
        ):
            raise UnavailableResourceException(
                f"Configmap {constants.SUPPORTED_VERSIONS_CONFIGMAP} does not exist in hypershift namespace"
            )

        cmd = "oc get cm -n hypershift supported-versions -o jsonpath='{.data.supported-versions}'"
        cmd_res = exec_cmd(cmd, shell=True)
        if cmd_res.returncode == 0:
            supported_versions = cmd_res.stdout.decode("utf-8")
            logger.info(f"Supported versions: {supported_versions}")

        if not get_running_ocp_version() in supported_versions:
            self.create_image_override()

    def create_image_override(self):
        """
        Create hypershift image override cm
        """
        # Create image override configmap using the image override json
        cmd = (
            f"oc create cm {self.hypershift_override_image_cm} --from-file={constants.IMAGE_OVERRIDE_JSON}"
            "-n {self.namespace}"
        )
        cmd_res = exec_cmd(cmd, shell=True)
        if cmd_res.returncode:
            raise CommandFailed("override configmap not created successfully")

        # annotate multicluster engine operator with the override cm
        self.multicluster_engine.annotate(
            annotation=f"imageOverridesCM={self.hypershift_override_image_cm}"
        )
        self.multicluster_engine.wait_until_running()

    def deploy_mce(self, check_mce_deployed=False, check_mce_ready=False):
        """
        Installs mce enabling software emulation.

        Args:
            check_mce_deployed (bool): If True, check if mce is already deployed. If so, skip the deployment.
            check_mce_ready (bool): If True, check if mce is ready. If so, skip the deployment.
        """
        if check_mce_deployed:
            if self.mce_installed():
                logger.info("mce operator is already deployed, skipping the deployment")
                return

        if check_mce_ready:
            if self.post_install_verification(raise_exception=False):
                logger.info("mce operator ready, skipping the deployment")
                return

        logger.info("Installing mce")
        # we create catsrc with nightly builds only if config.DEPLOYMENT does not have mce_latest_stable
        if not config.DEPLOYMENT.get("mce_latest_stable"):
            # Create mce catalog source
            self.create_mce_catalog_source()
        # Create multicluster-engine namespace
        self.create_mce_namespace()
        # create mce subscription
        self.create_mce_subscription()
        # Deploy the multiclusterengine operatorgroup
        self.create_multiclusterengine_operatorgroup()
        # Create mce resource
        self.create_multiclusterengine_resource()
        # Check hypershift ns created
        if not self.check_hypershift_namespace():
            cmd = f"oc create namespace {constants.HYPERSHIFT_NAMESPACE}"
            cmd_res = exec_cmd(cmd, shell=True)
            if cmd_res.returncode:
                raise CommandFailed("Failed to create hypershift namespace")
        # Check supported versions in supported-versions configmap
        self.check_supported_versions()

    def mce_installed(self):
        """
        Check if MCE is already installed.

        Returns:
             bool: True if MCE is installed, False otherwise
        """
        ocp = OCP(kind=constants.ROOK_OPERATOR, namespace=self.namespace)
        return ocp.check_resource_existence(
            timeout=12, should_exist=True, resource_name=constants.MCE_OPERATOR
        )

    def post_install_verification(self, raise_exception=False):
        """
        Performs MCE post-installation verification, with raise_exception = False may be used safely to run on
        clusters with MCE installed or not installed.

        Args:
            raise_exception: If True, allow function to fail the job and raise an exception. If false, return False
        instead of raising an exception.

        Returns:
            bool: True if the verification conditions are met, False otherwise
        Raises:
            TimeoutExpiredError: If the verification conditions are not met within the timeout
            and raise_exception is True.
            ResourceNotFoundError if the namespace does not exist and raise_exception is True.
            ResourceWrongStatusException if the nodes are not ready, verification fails and raise_exception
            is True.
        """
        # TODO: implement
        pass

    def validate_mce_deployment(self):
        """
        Validate mce operator installation
        """
        if self.mce_hyperconverged_installed():
            logger.info("mce operator is already deployed")
            return
