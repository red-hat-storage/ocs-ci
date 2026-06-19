"""
This module contains functions needed to install IBM Fusion
"""

import logging
import tempfile
import time

import yaml

from ocs_ci.framework import config
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.constants import FUSION_SUBSCRIPTION_YAML, ISF_CATALOG_SOURCE_NAME
from ocs_ci.ocs.exceptions import ChannelNotFound, CommandFailed
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.csv import CSV
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.resources.packagemanifest import PackageManifest
from ocs_ci.utility import templating
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import TimeoutSampler, run_cmd
from ocs_ci.utility.version import VERSION_2_12, get_semantic_version


logger = logging.getLogger(__name__)


class FusionDeployment:
    def __init__(self):
        self.pre_release = config.DEPLOYMENT.get("fusion_pre_release", False)
        self.sds_version = config.DEPLOYMENT.get("fusion_pre_release_sds_version")
        self.image_tag = config.DEPLOYMENT.get("fusion_pre_release_image")
        self.operator_name = defaults.FUSION_OPERATOR_NAME
        self.namespace = defaults.FUSION_NAMESPACE
        self.kubeconfig = config.RUN["kubeconfig"]

    def deploy(self):
        """
        Install IBM Fusion Operator
        """
        logger.info("Installing IBM Fusion")
        self.create_catalog_source()
        self.create_namespace_and_operator_group()
        self.create_subscription()
        self.verify()
        self.create_spectrum_fusion_cr()

    def create_catalog_source(self):
        """
        Create Fusion CatalogSource
        """

        if self.pre_release:
            catalog_source_name = constants.ISF_CATALOG_SOURCE_NAME
        else:
            catalog_source_name = constants.IBM_OPERATOR_CATALOG_SOURCE_NAME

        ibm_catalog_source = CatalogSource(
            resource_name=catalog_source_name,
            namespace=constants.MARKETPLACE_NAMESPACE,
        )
        if ibm_catalog_source.check_state("READY"):
            logger.info(
                f"CatalogSource '{catalog_source_name}' already exists and is READY, skipping creation"
            )
            return

        if self.pre_release:
            if get_semantic_version(self.sds_version, True) >= VERSION_2_12:
                self.create_image_digest_mirror_set()
            logger.info("Adding pre-release CatalogSource")
            render_data = {
                "sds_version": self.sds_version,
                "image_tag": self.image_tag,
            }
            _templating = templating.Templating(
                base_path=constants.TEMPLATE_DEPLOYMENT_DIR_FUSION
            )
            template = _templating.render_template(
                constants.ISF_OPERATOR_SOFTWARE_CATALOG_SOURCE_YAML, render_data
            )
            fusion_catalog_source_data = yaml.load(template, Loader=yaml.Loader)
        else:
            logger.info("Adding GA CatalogSource")
            fusion_catalog_source_data = templating.load_yaml(
                constants.FUSION_CATALOG_SOURCE_YAML
            )
            if config.DEPLOYMENT.get("fusion_image"):
                fusion_catalog_source_data["spec"]["image"] = config.DEPLOYMENT[
                    "fusion_image"
                ]
        fusion_catalog_source_manifest = tempfile.NamedTemporaryFile(
            mode="w+", prefix="fusion_catalog_source_manifest", delete=False
        )
        templating.dump_data_to_temp_yaml(
            fusion_catalog_source_data, fusion_catalog_source_manifest.name
        )
        run_cmd(
            f"oc --kubeconfig {self.kubeconfig} apply -f {fusion_catalog_source_manifest.name}"
        )

        logger.info("Waiting for CatalogSource to be READY")
        ibm_catalog_source.wait_for_state("READY", timeout=960)

    def create_image_digest_mirror_set(self):
        """
        Create or update ImageDigestMirrorSet.
        """
        logger.info("Creating Fusion ImageDigestMirrorSet")
        render_data = {
            "sds_version": self.sds_version,
        }
        _templating = templating.Templating(
            base_path=constants.TEMPLATE_DEPLOYMENT_DIR_FUSION
        )
        template = _templating.render_template(
            constants.ISF_OPERATOR_IDMS_YAML, render_data
        )
        fusion_idms_data = yaml.load(template, Loader=yaml.Loader)

        fusion_idms = tempfile.NamedTemporaryFile(
            mode="w+", prefix="fusion_idms", delete=False
        )
        templating.dump_data_to_temp_yaml(fusion_idms_data, fusion_idms.name)
        run_cmd(
            f"oc --kubeconfig {self.kubeconfig} apply -f {fusion_idms.name}",
            silent=True,
        )

    def create_subscription(self):
        """
        Create Fusion Subscription
        """
        sub_ocp = OCP(kind=constants.SUBSCRIPTION_COREOS, namespace=self.namespace)
        if sub_ocp.is_exist(resource_name=self.operator_name):
            logger.info(
                f"Subscription '{self.operator_name}' already exists, skipping creation"
            )
            return
        logger.info("Creating Subscription")
        subscription_fusion_yaml_data = templating.load_yaml(FUSION_SUBSCRIPTION_YAML)
        subscription_fusion_yaml_data["spec"]["channel"] = config.DEPLOYMENT[
            "fusion_channel"
        ]
        if self.pre_release:
            subscription_fusion_yaml_data["spec"]["source"] = ISF_CATALOG_SOURCE_NAME
        subscription_fusion_manifest = tempfile.NamedTemporaryFile(
            mode="w+", prefix="subscription_fusion_manifest", delete=False
        )
        templating.dump_data_to_temp_yaml(
            subscription_fusion_yaml_data, subscription_fusion_manifest.name
        )
        run_cmd(
            f"oc --kubeconfig {self.kubeconfig} apply -f {subscription_fusion_manifest.name}"
        )

    def create_namespace_and_operator_group(self):
        """
        Create Fusion Namespace and OperatorGroup
        """
        ns_ocp = OCP(kind="Namespace")
        if ns_ocp.is_exist(resource_name=self.namespace):
            logger.info(
                f"Namespace '{self.namespace}' already exists, skipping creation"
            )
            return
        logger.info("Creating Namespace and OperatorGroup.")
        run_cmd(
            f"oc --kubeconfig {self.kubeconfig} create -f {constants.FUSION_NS_YAML}"
        )

    def verify(self, sleep=30):
        """
        Verify the Fusion deployment was successful.

        Args:
            sleep (int, optional): Seconds to wait before checking status. Defaults to 30.

        """
        package_manifest = PackageManifest(resource_name=self.operator_name)
        try:
            csv_name = package_manifest.get_current_csv()
            csv = CSV(resource_name=csv_name, namespace=self.namespace)
            if csv.check_phase("Succeeded"):
                logger.info(
                    f"Fusion already deployed (CSV '{csv_name}' is Succeeded), skipping verification"
                )
                self.get_installed_version()
                return
        except (CommandFailed, ChannelNotFound):
            pass

        logger.info("Verifying Fusion is deployed")
        logger.info("Waiting for Subscription and CSV to be found")
        wait_for_subscription(self.operator_name, self.namespace)
        wait_for_csv(self.operator_name, self.namespace)
        if sleep:
            logger.info(
                f"Sleeping for {sleep} seconds after {self.operator_name} created"
            )
            time.sleep(sleep)

        logger.info("Waiting for PackageManifest to be found and CSV Succeeded")
        package_manifest.wait_for_resource(timeout=120)
        csv_name = package_manifest.get_current_csv()
        csv = CSV(resource_name=csv_name, namespace=self.namespace)
        csv.wait_for_phase("Succeeded", timeout=300, sleep=10)
        self.get_installed_version()
        logger.info("Fusion deployed successfully")

    def get_installed_version(self):
        """
        Retrieve the installed Fusion version.

        Returns:
            str: Installed Fusion version.

        """
        logger.info("Retrieving installed Fusion version")
        results = run_cmd(
            f"oc get subscription {self.operator_name} -n {self.namespace} --kubeconfig {self.kubeconfig} -o yaml"
        )
        build = yaml.safe_load(results)["status"]["installedCSV"]
        version = ".".join(build.split(".")[1:])
        config.ENV_DATA["fusion_version"] = version
        logger.info(f"Installed Fusion version: {version}")
        return version

    def create_spectrum_fusion_cr(self):
        """
        Create SpectrumFusion CR.
        """
        # The isf-operator installs a validating webhook vspectrumfusion.kb.io
        # (with a random suffix) that rejects SpectrumFusion CR creation on
        # unsupported OCP versions. Delete it before applying the CR.
        webhooks = OCP(kind=constants.WEBHOOK)
        for item in webhooks.get().get("items", []):
            name = item["metadata"]["name"]
            if name.startswith("vspectrumfusion.kb.io"):
                logger.info(f"Deleting validating webhook '{name}'")
                try:
                    webhooks.delete(resource_name=name)
                except CommandFailed:
                    logger.warning(f"Webhook '{name}' already gone, skipping")

        sf_ocp = OCP(kind="SpectrumFusion", namespace=self.namespace)
        if sf_ocp.is_exist(resource_name="spectrumfusion"):
            logger.info("SpectrumFusion CR already exists, skipping creation")
            spectrum_fusion_status_check()
            return
        logger.info("Creating SpectrumFusion")
        run_cmd(
            f"oc --kubeconfig {self.kubeconfig} create -f {constants.SPECTRUM_FUSION_CR}"
        )
        spectrum_fusion_status_check()
        logger.info("SpectrumFusion created successfully")


def wait_for_subscription(subscription_name, namespace):
    """
    Wait for the subscription to appear.

    Args:
        subscription_name (str): Name of Subscription
        namespace (str): Namespace where Subscription exists

    """
    for sample in TimeoutSampler(
        300, 10, OCP, kind=constants.SUBSCRIPTION_COREOS, namespace=namespace
    ):
        subscriptions = sample.get().get("items", [])
        for subscription in subscriptions:
            found_subscription_name = subscription.get("metadata", {}).get("name", "")
            if subscription_name in found_subscription_name:
                logger.info(f"Subscription found: {found_subscription_name}")
                return
            logger.debug(f"Still waiting for the subscription: {subscription_name}")


def wait_for_csv(csv_name, namespace):
    """
    Wait for the CSV to appear.

    Args:
        csv_name (str): Name of CSV
        namespace (str): Namespace where CSV exists

    """
    for sample in TimeoutSampler(300, 10, OCP, kind="csv", namespace=namespace):
        csvs = sample.get().get("items", [])
        for csv in csvs:
            found_csv_name = csv.get("metadata", {}).get("name", "")
            if csv_name in found_csv_name:
                logger.info(f"CSV found: {found_csv_name}")
                return
            logger.debug(f"Still waiting for the CSV: {csv_name}")


@retry((AssertionError, KeyError), 10, 5)
def spectrum_fusion_status_check():
    """
    Ensure SpectrumFusion is in the Completed state.

    Raises:
        AssertionError: If SpectrumFusion is not in a completed state.
        KeyError: If the status isn't present in the SpectrumFusion data.

    """
    spectrumfusion = OCS(
        kind="SpectrumFusion",
        metadata={
            "namespace": defaults.FUSION_NAMESPACE,
            "name": "spectrumfusion",
        },
    )
    spectrumfusion.reload()
    spectrumfusion_status = spectrumfusion.data["status"]["status"]
    assert spectrumfusion_status == "Completed"
