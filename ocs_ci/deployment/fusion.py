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
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.csv import CSV
from ocs_ci.ocs.resources.packagemanifest import PackageManifest
from ocs_ci.utility import templating
from ocs_ci.utility.utils import TimeoutSampler, run_cmd


logger = logging.getLogger(__name__)


class FusionDeployment:
    def __init__(self):
        self.pre_release = config.DEPLOYMENT.get("fusion_pre_release", False)
        self.operator_name = defaults.FUSION_OPERATOR_NAME
        self.namespace = defaults.FUSION_NAMESPACE

    def deploy(self):
        """
        Install IBM Fusion Operator
        """
        logger.info("Installing IBM Fusion")
        self.create_catalog_source()
        self.create_namespace_and_operator_group()
        self.create_subscription()
        self.verify()

    def create_catalog_source(self):
        """
        Create Fusion CatalogSource
        """
        logger.info("Adding CatalogSource for IBM Fusion")

        if self.pre_release:
            render_data = {
                "sds_version": config.DEPLOYMENT.get("fusion_pre_release_sds_version"),
                "image_tag": config.DEPLOYMENT.get("fusion_pre_release_image"),
            }
            catalog_source_name = constants.ISF_CATALOG_SOURCE_NAME
            _templating = templating.Templating(base_path=constants.FDF_TEMPLATE_DIR)
            template = _templating.render_template(
                constants.ISF_OPERATOR_SOFTWARE_CATALOG_SOURCE_YAML, render_data
            )
            fusion_catalog_source_data = yaml.load(template, Loader=yaml.Loader)
        else:
            catalog_source_name = constants.IBM_OPERATOR_CATALOG_SOURCE_NAME
            fusion_catalog_source_data = templating.load_yaml(
                constants.FUSION_CATALOG_SOURCE_YAML
            )
        fusion_catalog_source_manifest = tempfile.NamedTemporaryFile(
            mode="w+", prefix="fusion_catalog_source_manifest", delete=False
        )
        templating.dump_data_to_temp_yaml(
            fusion_catalog_source_data, fusion_catalog_source_manifest.name
        )
        run_cmd(f"oc apply -f {fusion_catalog_source_manifest.name}")
        ibm_catalog_source = CatalogSource(
            resource_name=catalog_source_name,
            namespace=constants.MARKETPLACE_NAMESPACE,
        )

        logger.info("Waiting for CatalogSource to be READY")
        ibm_catalog_source.wait_for_state("READY")

    def create_subscription(self):
        """
        Create Fusion Subscription
        """
        logger.info("Installing IBM Fusion")
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
        run_cmd(f"oc create -f {subscription_fusion_manifest.name}")

    @staticmethod
    def create_namespace_and_operator_group():
        """
        Create Fusion Namespace and OperatorGroup
        """
        logger.info("Creating namespace and OperatorGroup.")
        run_cmd(f"oc create -f {constants.FUSION_NS_YAML}")

    def verify(self):
        """
        Verify the Fusion deployment was successful
        """
        logger.info("Verifying Fusion is deployed")
        logger.info("Waiting for Subscription and CSV to be found")
        wait_for_subscription(self.operator_name, self.namespace)
        wait_for_csv(self.operator_name, self.namespace)
        logger.info(f"Sleeping for 30 seconds after {self.operator_name} created")
        time.sleep(30)

        logger.info("Waiting for PackageManifest to be found and CSV Succeeded")
        package_manifest = PackageManifest(resource_name=self.operator_name)
        package_manifest.wait_for_resource(timeout=120)
        csv_name = package_manifest.get_current_csv()
        csv = CSV(resource_name=csv_name, namespace=self.namespace)
        csv.wait_for_phase("Succeeded", timeout=300, sleep=10)
        logger.info("Fusion deployed successfully")


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
