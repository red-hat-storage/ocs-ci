"""
This module contains functionality required for mce installation.
"""

import logging
import tempfile
import json

from ocs_ci.deployment.qe_app_registry import QeAppRegistry
from ocs_ci.ocs import ocp
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.framework import config
from ocs_ci.utility import templating
from ocs_ci.ocs import constants
from ocs_ci.utility.utils import (
    run_cmd,
    exec_cmd,
    wait_custom_resource_defenition_available,
    TimeoutSampler,
)
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs.resources.deployment import Deployment
from ocs_ci.utility.utils import get_running_ocp_version
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    UnavailableResourceException,
    MultiClusterEngineNotDeployedException,
)

logger = logging.getLogger(__name__)


class MCEInstaller(object):
    """
    mce Installer class for mce deployment
    """

    def __init__(self):
        self.mce_namespace = constants.MCE_NAMESPACE
        self.ns_obj = ocp.OCP(kind=constants.NAMESPACES)
        self.hypershift_override_image_cm = "hypershift-override-images-new"
        self.multicluster_engine = ocp.OCP(
            kind="MultiClusterEngine",
            resource_name=constants.MULTICLUSTER_ENGINE,
            namespace=self.mce_namespace,
        )
        self.catsrc = ocp.OCP(
            kind=constants.CATSRC, namespace=constants.MARKETPLACE_NAMESPACE
        )
        self.subs = ocp.OCP(kind=constants.PROVIDER_SUBSCRIPTION)

    def _create_mce_catalog_source(self):
        """
        Creates a catalogsource for mce operator.

        We use qe-app-registry catalog source to install latest version
        In future if we want to install particular image we can use subscription.spec.startingCSV:<image> like
        quay.io:443/acm-d/mce-custom-registry:2.13.0-DOWNSTREAM-2025-03-02-02-49-35
        In this case we don't need another channel in subscription but may reuse stable-2.8 channel

        ! Important. This catalog source does not work without ICSP, this catsrc is not used in a moment.
        ! This method was left for reference only.
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
            resource_name=self.mce_namespace,
        ):
            logger.info(f"Creating namespace {self.mce_namespace} for mce resources")
            namespace_yaml_file = templating.load_yaml(constants.MCE_NAMESPACE_YAML)
            namespace_yaml = OCS(**namespace_yaml_file)
            namespace_yaml.create()
            logger.info(f"MCE namespace {self.mce_namespace} was created successfully")

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

            # ! Important, channel-2.8 becomes available after OCP 4.18 release
            if config.DEPLOYMENT.get("mce_channel"):
                mce_subscription_yaml_data["spec"]["channel"] = config.DEPLOYMENT.get(
                    "mce_channel"
                )

            mce_subscription_manifest = tempfile.NamedTemporaryFile(
                mode="w+", prefix="mce_subscription_manifest", delete=False
            )
            templating.dump_data_to_temp_yaml(
                mce_subscription_yaml_data, mce_subscription_manifest.name
            )
            logger.info("Creating subscription for the mce operator")
            exec_cmd(f"oc create -f {mce_subscription_manifest.name}")
            OCP(
                kind=constants.SUBSCRIPTION_COREOS,
                namespace=self.mce_namespace,
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
        logger.info(
            f"hypershift namespace {self.mce_namespace} was created successfully"
        )
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

        # configMap is created during hypershift installation in around 5 min.
        # Increasing this timeout to 10 min for safer deployment.
        if not configmaps_obj.check_resource_existence(
            should_exist=True,
            timeout=600,
            resource_name=constants.SUPPORTED_VERSIONS_CONFIGMAP,
        ):
            raise UnavailableResourceException(
                f"Configmap {constants.SUPPORTED_VERSIONS_CONFIGMAP} does not exist "
                f"in {constants.HYPERSHIFT_NAMESPACE} namespace"
            )

        ocp_version = get_running_ocp_version()
        supported_versions = self.get_supported_versions()

        if not get_running_ocp_version() in supported_versions:
            self.create_image_override()

        sampler = TimeoutSampler(
            timeout=600,
            sleep=10,
            func=lambda: ocp_version in self.get_supported_versions(),
        )
        if sampler.wait_for_func_value(True):
            logger.info(f"Version {ocp_version} found in supported-versions configmap")

    def get_supported_versions(self):
        """
        Get supported versions from the supported-versions configmap.

        Returns:
            str: Supported versions string or empty string if command fails.
        """
        cmd = f"oc get cm -n {constants.HYPERSHIFT_NAMESPACE} supported-versions "
        cmd += "-o jsonpath='{.data.supported-versions}'"
        cmd_res = exec_cmd(cmd, shell=True)
        if cmd_res.returncode == 0:
            versions_data = json.loads(cmd_res.stdout.decode("utf-8"))
            return versions_data.get("versions", [])
        return []

    def create_image_override(self):
        """
        Create hypershift image override cm
        """
        # Create image override configmap using the image override json
        cmd = (
            f"oc create cm {self.hypershift_override_image_cm} --from-file={constants.IMAGE_OVERRIDE_JSON} "
            f"-n {self.mce_namespace}"
        )
        cmd_res = exec_cmd(cmd, shell=True)
        if cmd_res.returncode:
            raise CommandFailed("override configmap not created successfully")

        # annotate multicluster engine operator with the override cm
        self.multicluster_engine.annotate(
            annotation=f"imageOverridesCM={self.hypershift_override_image_cm}"
        )

    def deploy_mce(self):
        """
        Installs mce enabling software emulation.

        """

        if not self.mce_installed():
            logger.info("Installing mce")
            # we create catsrc with nightly builds only if config.DEPLOYMENT does not have mce_latest_stable
            qe_app_registry = QeAppRegistry()
            qe_app_registry.icsp()
            qe_app_registry.catalog_source()
            self.create_mce_namespace()
            self.create_multiclusterengine_operatorgroup()
            self.create_mce_subscription()
            if not wait_custom_resource_defenition_available(
                constants.MULTICLUSTER_ENGINE_CRD
            ):
                raise MultiClusterEngineNotDeployedException(
                    f"crd {constants.MULTICLUSTER_ENGINE_CRD} is unavailable"
                )

        # check whether mce instance is created, if it is installed but mce don't pass validation we can not heal it in
        # script here, hence no sense for full validation of mce
        if not self.mce_exists():

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

        self.wait_mce_resources()

        # avoid circular dependency with hosted cluster
        from ocs_ci.deployment.hosted_cluster import (
            apply_hosted_cluster_mirrors_max_items_wa,
            apply_hosted_control_plane_mirrors_max_items_wa,
        )

        logger.info("Correct max items in hostedclsuters crd")
        apply_hosted_cluster_mirrors_max_items_wa()

        logger.info("Correct max items in hostedcontrolplane crd")
        apply_hosted_control_plane_mirrors_max_items_wa()

    def mce_installed(self):
        """
        Check if MCE is already installed.

        Returns:
             bool: True if MCE is installed, False otherwise
        """
        ocp_obj = OCP(kind=constants.ROOK_OPERATOR)
        # unlike other k8s resources, operators are OLM manager resources that identified by merged name.namespace
        return ocp_obj.check_resource_existence(
            timeout=12,
            should_exist=True,
            resource_name=constants.MCE_OPERATOR_OPERATOR_NAME_WITH_NS,
        )

    def mce_exists(self):
        """
        Check if MCE exists

        Returns:
            bool: True if MCE exists, False otherwise
        """
        return self.multicluster_engine.is_exist(
            resource_name=constants.MULTICLUSTER_ENGINE
        )

    def wait_mce_resources(self):
        """
        Wait for mce Available state and deployments Ready state

        Raises:
            TimeoutExpiredError: If the deployment is not in the 'Available' state within the timeout
        """
        if not self.mce_exists():
            raise UnavailableResourceException("MCE resource is not created")
        deployments = [
            "multicluster-engine-operator",
            "ocm-controller",
            "cluster-manager",
            "ocm-webhook",
        ]

        for resource_name in deployments:
            depl_ocp_obj = OCP(
                kind=constants.DEPLOYMENT,
                namespace=self.mce_namespace,
                resource_name=resource_name,
            )
            deployment_obj = Deployment(
                **depl_ocp_obj.get(retry=60, wait=10, dont_raise=True)
            )
            deployment_obj.wait_for_available_replicas(timeout=600)
