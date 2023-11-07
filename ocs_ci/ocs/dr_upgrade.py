"""
All DR operators upgrades implemented here ex: MulticlusterOrchestrator, Openshift DR operator

"""

import logging

from ocs_ci.framework import config
from ocs_ci.ocs.cluster import CephCluster, CephClusterExternal, CephHealthMonitor
from ocs_ci.ocs.exceptions import TimeoutException
from ocs_ci.ocs.ocs_upgrade import OCSUpgrade, verify_image_versions
from ocs_ci.ocs import constants
from ocs_ci.ocs import defaults
from ocs_ci.deployment.helpers.external_cluster_helpers import (
    ExternalCluster,
    get_external_cluster_client,
)
from ocs_ci.ocs.resources.install_plan import wait_for_install_plan_and_approve
from ocs_ci.utility.utils import TimeoutSampler


log = logging.getLogger(__name__)

DR_TO_CEPH_CLUSTER_MAP = {"regional-dr": CephCluster, "metro-dr": CephClusterExternal}


class DRUpgrade(OCSUpgrade):
    """
    Base class for all DR operator upgrades

    """

    def __init__(
        self,
        namespace=constants.OPENSHIFT_OPERATORS,
        version_before_upgrade=config.ENV_DATA.get("ocs_version"),
        ocs_registry_image=config.UPGRADE.get("upgrade_ocs_registry_image"),
        upgrade_in_current_source=config.UPGRADE.get(
            "upgrade_in_current_source", False
        ),
    ):
        self.namespace = (
            namespace if namespace else config.ENV_DATA["cluster_namespace"]
        )
        self.version_before_upgrade = version_before_upgrade
        self.ocs_registry_image = ocs_registry_image
        self.upgrade_in_current_source = upgrade_in_current_source
        self.ceph_cluster = DR_TO_CEPH_CLUSTER_MAP[
            config.MULTICLUSTER["multicluster_mode"]
        ]
        self.external_cluster = None
        self.operator_name = None

        super.__init__(
            self.namespace,
            self.version_before_upgrade,
            self.ocs_registry_image,
            self.upgrade_in_current_source,
        )

    def run_upgrade(self):
        self.upgrade_version = self.get_upgrade_version()
        assert self.get_parsed_versions()[1] >= self.get_parsed_versions()[0], (
            f"Version you would like to upgrade to: {self.upgrade_version} "
            f"is not higher or equal to the version you currently running: "
            f"{self.version_before_upgrade}"
        )

        # create external cluster object
        if config.DEPLOYMENT["external_mode"]:
            host, user, password, ssh_key = get_external_cluster_client()
            self.external_cluster = ExternalCluster(host, user, password, ssh_key)
        self.csv_name_pre_upgrade = self.get_csv_name_pre_upgrade()
        self.pre_upgrade_images = self.get_pre_upgrade_image(self.csv_name_pre_upgrade)
        self.load_version_config_file(self.upgrade_version)

        with CephHealthMonitor(self.ceph_cluster):
            self.channel = self.set_upgrade_channel(resource_name=self.operator_name)
            self.set_upgrade_images()
            # TODO: Overload this function
            self.update_subscription(self.channel)
            # In the case upgrade is not from 4.8 to 4.9 and we have manual approval strategy
            # we need to wait and approve install plan, otherwise it's approved in the
            # subscribe_ocs method.
            subscription_plan_approval = config.DEPLOYMENT.get(
                "subscription_plan_approval"
            )
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
            old_image = self.get_images_post_upgrade(
                self.channel, self.pre_upgrade_images, self.upgrade_version
            )

        verify_image_versions(
            old_image,
            self.get_parsed_versions()[1],
            self.version_before_upgrade,
        )


class MultiClusterOrchestratorUpgrade(DRUpgrade):
    """
    A class to handle ODF MCO operator upgrades

    """

    def __init__(self):
        super.__init__()
        self.operator_name = defaults.MCO_OPERATOR_NAME


class DRHubUpgrade(DRUpgrade):
    """
    A class to handle DR Hub operator upgrades

    """

    def __init__(self):
        super.__init__()
        self.operator_name = defaults.DR_HUB_OPERATOR_NAME
