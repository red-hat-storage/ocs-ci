"""
This module contains functionality required for mce installation.
"""

import logging
import tempfile
import json
import re
from packaging.version import parse as parse_version

from ocs_ci.ocs import ocp
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.framework import config
from ocs_ci.utility import templating
from ocs_ci.ocs import constants
from ocs_ci.utility.multicluster import create_mce_catsrc
from ocs_ci.utility.utils import (
    run_cmd,
    exec_cmd,
    wait_custom_resource_defenition_available,
    TimeoutSampler,
    get_acm_mce_build_tag,
)
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs.resources.deployment import Deployment
from ocs_ci.utility.utils import get_running_ocp_version
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    UnavailableResourceException,
    MultiClusterEngineNotDeployedException,
    TimeoutExpiredError,
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
        self.version_before_upgrade = None
        # In case if we are using registry image
        self.version_change = None
        self.zstream_upgrade = None
        self.mce_registry_image = config.UPGRADE.get("upgrade_mce_registry_image", "")
        self.upgrade_version = config.UPGRADE.get("upgrade_mce_version", "")
        self.timeout_wait_csvs_minutes = 10

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
            else:
                mce_subscription_yaml_data["spec"][
                    "channel"
                ] = f"stable-{config.ENV_DATA.get('mce_version')}"
            mce_subscription_yaml_data["spec"][
                "source"
            ] = constants.MCE_DEV_CATALOG_SOURCE_NAME

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

        check_resources_timeout = 600
        if config.ENV_DATA["platform"] in constants.CLOUD_PLATFORMS:
            logger.info(
                "Cloud platform detected; wait for hypershift supported-versions configmap twice longer"
            )
            check_resources_timeout = check_resources_timeout * 2

        # configMap is created during hypershift installation in around 5 min.
        # Increasing this timeout to 10 min for safer deployment.
        if not configmaps_obj.check_resource_existence(
            should_exist=True,
            timeout=check_resources_timeout,
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
            create_mce_catsrc()
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
        from ocs_ci.deployment.hub_spoke import (
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

    def upgrade_mce(self):
        """
        Upgrade mce to the latest build of desired target version.
        Important ! Latest unreleased versions are not always available in the registry, and not always stable.
        Important ! scopeo cli tool must be installed and pull-secret must be in a location expected in a config
        Important ! MCE operator upgrade will be aborted if ACM operator is installed; use ACMUpgrade().run_upgrade()
        Important ! MCE operator upgrade will be aborted if MCE operator is not deployed
        New mce-dev-catalog catalogSource will be created and propagated, even if mce was initially installed with
        a different catalogSource


        Returns:
            str: upgrade pass type: "version change upgrade" or "z-stream upgrade" or "" (if no upgrade performed)

        Raises:
            MultiClusterEngineNotDeployedException: If MCE is not deployed

        """
        from ocs_ci.deployment.deployment import Deployment

        upgrade_pass = None

        if Deployment().acm_operator_installed():
            logger.warning(
                "ACM operator is installed, aborting MCE upgrade, use ACMUpgrade().run_upgrade()"
            )
            return upgrade_pass

        if not self.mce_installed():
            logger.warning("MCE operator is not deployed before upgrade, abort upgrade")
            return upgrade_pass

        # another upgrade logic automated in following block
        # When OCP upgrades, load_ocp_version_config_file func is called and mce version getting updated in env_data
        if not self.upgrade_version:
            self.upgrade_version = config.ENV_DATA["mce_version"]

        if parse_version(config.ENV_DATA["mce_version"]) <= parse_version(
            self.get_running_mce_version()
        ):
            logger.info(
                "MCE is already at the desired upgrade version or higher, no upgrade needed."
            )
            return upgrade_pass

        parsed_versions = self.get_parsed_versions()

        self.version_change = parsed_versions[1] > parsed_versions[0]
        if not self.version_change:
            self.zstream_upgrade = True
        # either this would be GA to Unreleased upgrade of same version OR
        # GA to unreleased upgrade to higher version
        if self.version_change:
            self.set_catalogsource_image()
            self.patch_channel()
            upgrade_pass = "version change upgrade"
        else:
            # Z stream upgrade
            self.set_catalogsource_image()
            upgrade_pass = "z-stream upgrade"

        # Post upgrade verification when engine is installed
        if self.mce_exists():
            self.wait_mce_resources()
            logger.info("mce engine is running after upgrade")
        else:
            logger.warning("mce engine does not exist after upgrade")

        logger.info(
            f"Upgrade passed: {upgrade_pass}. Waiting for csv to be Succeeded and mce version match"
        )
        return (
            self.wait_mce_csv_succeeded()
            and self.verify_mce_version_major_minor_matches()
        )

    def get_running_mce_version(self):
        """
        Get the currently running MCE version.

        Returns:
            str: The current MCE version or an empty string if not found.

        """
        if not self.mce_exists():
            logger.warning("MCE is not deployed, cannot get version")
            return ""
        cmd = (
            f"oc get mce -n {self.mce_namespace} {constants.MULTICLUSTER_ENGINE} "
            "-o jsonpath='{.status.currentVersion}'"
        )
        cmd_res = exec_cmd(cmd, shell=True)
        if cmd_res.returncode == 0:
            return cmd_res.stdout.decode("utf-8").strip()
        return ""

    def get_parsed_versions(self):
        """
        Get parsed versions for current running mce and upgrade target version.

        Returns:
            tuple: Parsed versions of current running MCE and upgrade target version.

        """
        self.version_before_upgrade = self.get_running_mce_version()
        parsed_version_before_upgrade = parse_version(self.version_before_upgrade)
        parsed_upgrade_version = parse_version(self.upgrade_version)

        return parsed_version_before_upgrade, parsed_upgrade_version

    def _get_desired_mce_catalog_image(self):
        """
        Compute the desired CatalogSource image for MCE, mirroring logic used in create_mce_catsrc().

        Returns:
            str: Full image reference for CatalogSource spec.image

        """
        if not config.ENV_DATA.get("mce_unreleased_image"):
            mce_image_tag = get_acm_mce_build_tag(
                constants.MCE_CATSRC_IMAGE, config.ENV_DATA.get("mce_version")
            )
        else:
            mce_image_tag = config.ENV_DATA.get("mce_unreleased_image")
        return f"{constants.MCE_CATSRC_IMAGE}:{mce_image_tag}"

    def set_catalogsource_image(self):
        """
        Set catalogsource image for mce upgrade. Works with catsrc already created or creates a new one.

        """
        logger.info("Upgrading mce using catalogsource image change")

        try:
            create_mce_catsrc()
        except CommandFailed as cf:
            logger.warning(f"Failed to create catalogsource: {cf}")
            if (
                "the object has been modified" in str(cf).lower()
                or "already exists" in str(cf).lower()
            ):
                logger.info(
                    "CatalogSource already exists or was modified, proceeding with patching"
                )
            else:
                raise
        self.patch_mce_catsrc_with_image_tag()

    def patch_mce_catsrc_with_image_tag(self):

        if not (desired_image := self.mce_registry_image):
            desired_image = self._get_desired_mce_catalog_image()
        logger.info(
            f"Patching CatalogSource '{constants.MCE_DEV_CATALOG_SOURCE_NAME}' with image: {desired_image}"
        )
        patch_json = json.dumps({"spec": {"image": desired_image}})
        patch_cmd = (
            f"oc -n {constants.MARKETPLACE_NAMESPACE} patch {constants.CATSRC} "
            f"{constants.MCE_DEV_CATALOG_SOURCE_NAME} --type=merge -p '{patch_json}'"
        )
        exec_cmd(patch_cmd)

        mce_operator_catsrc = CatalogSource(
            resource_name=constants.MCE_DEV_CATALOG_SOURCE_NAME,
            namespace=constants.MARKETPLACE_NAMESPACE,
        )
        mce_operator_catsrc.wait_for_state("READY")

    def patch_channel(self):
        """
        Method to patch mce subscription channel during upgrade where we do Y to Y upgrade

        """
        patch = f'\'{{"spec": {{"channel": "stable-{self.upgrade_version}"}}}}\''
        patch_cmd = (
            f"oc -n {self.mce_namespace} patch {constants.SUBSCRIPTION_WITH_ACM} "
            f"{constants.MCE_SUBSCRIPTION_NAME} -p {patch} --type merge"
        )
        exec_cmd(patch_cmd)

    def patch_subscription_with_mce_catsrc(self):
        """
        Patch subscription to use mce catalogsource

        """
        patch = (
            f'\'{{"spec":{{"source": "{constants.MCE_DEV_CATALOG_SOURCE_NAME}"}}}}\''
        )
        patch_cmd = (
            f"oc -n {self.mce_namespace} patch {constants.SUBSCRIPTION_WITH_ACM} "
            f"{constants.MCE_SUBSCRIPTION_NAME} -p {patch} --type merge"
        )
        exec_cmd(patch_cmd)

    def wait_csv_upgraded(self):
        """
        Wait for mce operator csv upgraded

        Raises:
            TimeoutExpiredError: If the CSV is not in the 'Succeeded' state within the timeout

        """
        csv_ocp_obj = OCP(
            kind=constants.CLUSTER_SERVICE_VERSION,
            namespace=self.mce_namespace,
        )
        csv_name = csv_ocp_obj.get(resource_name="")["items"][0]["metadata"]["name"]
        csv_obj = ocp.OCP(
            kind=constants.CLUSTER_SERVICE_VERSION,
            namespace=self.mce_namespace,
            resource_name=csv_name,
        )
        csv_obj.wait_for_phase("Succeeded", timeout=900)

    def get_mce_csv_name(self):
        """
        Get MCE CSV name

        Returns:
            str: MCE CSV name

        """
        csv_ocp_obj = OCP(
            kind=constants.CLUSTER_SERVICE_VERSION,
            namespace=self.mce_namespace,
        )
        csv_items = csv_ocp_obj.get(resource_name="").get("items", [])
        return next(
            (
                item.get("metadata", {}).get("name", "")
                for item in csv_items
                if item.get("metadata", {})
                .get("name", "")
                .startswith(constants.MCE_OPERATOR)
            ),
            "",
        )

    def csv_succeeded(self):
        """
        Check if MCE CSV is in succeeded phase

        Returns:
            bool: True if MCE CSV is in succeeded phase, False otherwise
        """
        csv_name = self.get_mce_csv_name()
        if not csv_name:
            logger.error("MCE CSV not found")
            return False
        csv_obj = ocp.OCP(
            kind=constants.CLUSTER_SERVICE_VERSION,
            namespace=self.mce_namespace,
            resource_name=csv_name,
        )
        csv_phase = (
            csv_obj.get(resource_name=csv_name).get("status", {}).get("phase", "")
        )
        return csv_phase == "Succeeded"

    def wait_mce_csv_succeeded(self, timeout=None, sleep=10):
        """
        Wait until the MCE CSV reaches the 'Succeeded' phase.

        Args:
            timeout (int): Timeout in seconds. If None, defaults to self.timeout_wait_csvs_minutes * 60.
            sleep (int): Sleep interval between checks in seconds.

        Returns:
            bool: True if CSV reached 'Succeeded' within timeout.

        Raises:
            TimeoutExpiredError: If the CSV does not reach 'Succeeded' within the timeout.

        """
        effective_timeout = timeout or (self.timeout_wait_csvs_minutes * 60)
        logger.info(
            f"Waiting up to {effective_timeout}s for MCE CSV to reach 'Succeeded' state"
        )
        sampler = TimeoutSampler(
            timeout=effective_timeout,
            sleep=sleep,
            func=self.csv_succeeded,
        )
        try:
            sampler.wait_for_func_value(True)
            logger.info("MCE CSV reached 'Succeeded' phase")
            return True
        except TimeoutExpiredError:
            logger.error(
                "Timeout expired waiting for MCE CSV to reach 'Succeeded' phase"
            )
            return False

    def verify_mce_version_major_minor_matches(self):
        """
        Verify that the major and minor version of MCE csv matches the desired upgrade version.

        Returns:
            bool: True if major and minor versions match, False otherwise.

        """
        # Determine desired version string
        desired_version_str = (
            self.upgrade_version or str(config.ENV_DATA.get("mce_version", "")).strip()
        )
        if not desired_version_str:
            logger.error(
                "Desired MCE upgrade version is not set (upgrade_version/ENV_DATA['mce_version'])."
            )
            return False

        def extract_major_minor(v: str):
            m = re.search(r"(\d+)\.(\d+)", str(v))
            if not m:
                return None, None
            return int(m.group(1)), int(m.group(2))

        desired_mm = extract_major_minor(desired_version_str)
        if None in desired_mm:
            logger.error(
                f"Unable to parse major.minor from desired version '{desired_version_str}'"
            )
            return False

        # Get current CSV and its version
        csv_name = self.get_mce_csv_name()
        if not csv_name:
            logger.error("MCE CSV not found")
            return False

        csv_api = ocp.OCP(
            kind=constants.CLUSTER_SERVICE_VERSION,
            namespace=self.mce_namespace,
            resource_name=csv_name,
        )
        csv_data = csv_api.get(resource_name=csv_name)
        csv_version_str = csv_data.get("spec", {}).get("version") or csv_data.get(
            "metadata", {}
        ).get("name", "")

        running_mm = extract_major_minor(csv_version_str)
        if None in running_mm:
            logger.error(
                f"Unable to parse major.minor from CSV version '{csv_version_str}'"
            )
            return False

        matches = running_mm == desired_mm
        logger.info(
            f"MCE CSV version major.minor is {running_mm[0]}.{running_mm[1]}, "
            f"desired is {desired_mm[0]}.{desired_mm[1]}: match={matches}"
        )
        return matches

    def set_mirror_registry_configmap(self):
        """
        Set mirror registry config cm for mce/hypershift

        Raises:
            CommandFailed: If the 'oc create' command fails.
        """
        logger.info("Setting mirror registry config cm for mce/hypershift")
        mirror_registry_cm_yaml = templating.load_yaml(
            constants.MIRROR_REGISTRY_CONFIG_CM_YAML
        )
        mirror_registry_cm_manifest = tempfile.NamedTemporaryFile(
            mode="w+", prefix="mirror_registry_cm_manifest", delete=False
        )
        templating.dump_data_to_temp_yaml(
            mirror_registry_cm_yaml, mirror_registry_cm_manifest.name
        )
        exec_cmd(
            f"oc apply -f {mirror_registry_cm_manifest.name} -n {self.mce_namespace}",
            timeout=2400,
        )
