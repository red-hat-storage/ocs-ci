"""
All provider client operator upgrades implemented here

"""

import logging
from ocs_ci.framework import config
from ocs_ci.ocs.exceptions import TimeoutException
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import ocs_upgrade
from ocs_ci.ocs.ocs_upgrade import OCSUpgrade
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.csv import CSV, check_all_csvs_are_succeeded
from ocs_ci.ocs.resources.install_plan import wait_for_install_plan_and_approve
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.resources.storage_client import StorageClient
from ocs_ci.deployment.metallb import MetalLBInstaller
from ocs_ci.deployment.cnv import CNVInstaller
from ocs_ci.ocs.acm_upgrade import ACMUpgrade
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    skipif_ocp_version,
    skipif_managed_service,
    runs_on_provider,
    skipif_external_mode,
)


log = logging.getLogger(__name__)


@skipif_ocs_version("<4.15")
@skipif_ocp_version("<4.15")
@skipif_external_mode
@skipif_managed_service
@runs_on_provider
class ProviderUpgrade(OCSUpgrade):
    """
    Base class for all provider operator upgrades

    """

    def __init__(
        self,
        namespace=constants.OPENSHIFT_OPERATORS,
        version_before_upgrade=None,
        ocs_registry_image=None,
        upgrade_in_current_source=config.UPGRADE.get(
            "upgrade_in_current_source", False
        ),
        resource_name=None,
    ):
        if not version_before_upgrade:
            if config.PREUPGRADE_CONFIG.get("ENV_DATA").get("ocs_version", ""):
                version_before_upgrade = config.PREUPGRADE_CONFIG["ENV_DATA"].get(
                    "ocs_version"
                )
            else:
                version_before_upgrade = config.ENV_DATA.get("ocs_version")
        if not ocs_registry_image:
            ocs_registry_image = config.UPGRADE.get("upgrade_ocs_registry_image")
        self.external_cluster = None
        self.operator_name = None
        self.subscription_name = None
        self.pre_upgrade_data = dict()
        self.post_upgrade_data = dict()
        self.namespace = namespace
        # Upgraded phases [pre_upgrade, post_upgrade]
        self.upgrade_phase = "pre_upgrade"
        if resource_name:
            self.resource_name = resource_name

        super().__init__(
            namespace,
            version_before_upgrade,
            ocs_registry_image,
            upgrade_in_current_source,
        )
        self.upgrade_version = self.get_upgrade_version()

    def run_upgrade(self):
        assert self.get_parsed_versions()[1] >= self.get_parsed_versions()[0], (
            f"Version you would like to upgrade to: {self.upgrade_version} "
            f"is not higher or equal to the version you currently running: "
            f"{self.version_before_upgrade}"
        )

        self.csv_name_pre_upgrade = self.get_csv_name_pre_upgrade(
            resource_name=self.resource_name
        )
        self.pre_upgrade_images = self.get_pre_upgrade_image(self.csv_name_pre_upgrade)
        self.load_version_config_file(self.upgrade_version)

        self.channel = self.set_upgrade_channel(resource_name=self.operator_name)
        self.set_upgrade_images()
        self.update_subscription(self.channel, self.subscription_name)
        subscription_plan_approval = config.DEPLOYMENT.get("subscription_plan_approval")
        if subscription_plan_approval == "Manual":
            wait_for_install_plan_and_approve(config.ENV_DATA["cluster_namespace"])

        for sample in TimeoutSampler(
            timeout=725,
            sleep=5,
            func=self.check_if_upgrade_completed,
            channel=self.channel,
            csv_name_pre_upgrade=self.csv_name_pre_upgrade,
        ):
            try:
                if sample:
                    log.info("Upgrade success!")
                    break
            except TimeoutException:
                raise TimeoutException("No new CSV found after upgrade!")

    def collect_data(self, pod_name_pattern):
        """
        Collect operators installed on provider related pods and csv data
        """
        pod_data = pod.get_all_pods(namespace=self.namespace)
        self.pod_name_pattern = pod_name_pattern
        for p in pod_data:
            if self.pod_name_pattern in p.get()["metadata"]["name"]:
                pod_obj = OCP(
                    namespace=self.namespace,
                    resource_name=p.get()["metadata"]["name"],
                    kind="Pod",
                )
                if self.upgrade_phase == "pre_upgrade":
                    self.pre_upgrade_data["age"] = pod_obj.get_resource(
                        resource_name=p.get()["metadata"]["name"], column="AGE"
                    )
                    self.pre_upgrade_data["pod_status"] = pod_obj.get_resource_status(
                        resource_name=p.get()["metadata"]["name"]
                    )
                if self.upgrade_phase == "post_upgrade":
                    self.post_upgrade_data["age"] = pod_obj.get_resource(
                        resource_name=p.get()["metadata"]["name"], column="AGE"
                    )
                    self.post_upgrade_data["pod_status"] = pod_obj.get_resource_status(
                        resource_name=p.get()["metadata"]["name"]
                    )

        # get pre-upgrade csv
        csv_objs = CSV(namespace=self.namespace)
        for csv in csv_objs.get()["items"]:
            if self.operator_name in csv["metadata"]["name"]:
                csv_obj = CSV(
                    namespace=self.namespace, resource_name=csv["metadata"]["name"]
                )
                if self.upgrade_phase == "pre_upgrade":
                    self.pre_upgrade_data["version"] = csv_obj.get_resource(
                        resource_name=csv_obj.resource_name, column="VERSION"
                    )
                    try:
                        self.pre_upgrade_data["version"]
                    except KeyError:
                        log.error(
                            f"Couldn't capture Pre-upgrade CSV version for {self.operator_name}"
                        )
                if self.upgrade_phase == "post_upgrade":
                    if self.upgrade_version in csv["metadata"]["name"]:
                        self.post_upgrade_data["version"] = csv_obj.get_resource(
                            resource_name=csv_obj.resource_name, column="VERSION"
                        )
                    try:
                        self.post_upgrade_data["version"]
                    except KeyError:
                        log.error(
                            f"Couldn't capture Post upgrade CSV version for {self.operator_name}"
                        )
        # Make sure all csvs are in succeeded state
        check_all_csvs_are_succeeded(namespace=self.namespace)


class OperatorUpgrade(ProviderUpgrade):
    """
    A class to handle installed operators on provider upgrades

    """

    def __init__(self):
        self.storage_clients = StorageClient()
        self.metallb_installer_obj = MetalLBInstaller()
        self.cnv_installer_obj = CNVInstaller()
        self.acm_hub_upgrade_obj = ACMUpgrade()

    def collect_pre_upgrade_data(self):
        # Collect some pre-upgrade data for comparision after the upgrade
        self.metallb_pod_name_pattern = "metallb-operator"
        self.collect_data(self.metallb_pod_name_pattern)
        assert (
            self.pre_upgrade_data.get("pod_status", "") == "Running"
        ), "metallb operator pod is not in Running status"
        # Collect some pre-upgrade data for comparision after the upgrade
        self.cnv_pod_name_pattern = "klusterlet"
        self.collect_data(self.cnv_pod_name_pattern)
        assert (
            self.pre_upgrade_data.get("pod_status", "") == "Running"
        ), "klusterlet pods are not in Running status"
        self.run_upgrade()
        ocs_upgrade.run_ocs_upgrade()
        self.upgrade_phase = "post_upgrade"
        self.provider_pod_name_pattern = "ocs-provider-server"
        self.collect_data(self.provider_pod_name_pattern)

    def run_upgrade(self):
        self.acm_hub_upgrade_obj.run_upgrade()
        self.cnv_installer_obj.upgrade_cnv()
        self.metallb_installer_obj.upgrade_metallb()


class ProviderClusterOperatorUpgrade(ProviderUpgrade):
    """
    A class to handle Provider Cluster operator upgrades

    """

    def run_provider_upgrade(self):
        try:
            log.info("Starting the operator upgrade process...")
            operator_upgrade = OperatorUpgrade()
            operator_upgrade.collect_pre_upgrade_data()
            log.info("Operator upgrade completed successfully.")
        except Exception as e:
            log.error(f"Operator upgrade failed: {e}")
            raise
