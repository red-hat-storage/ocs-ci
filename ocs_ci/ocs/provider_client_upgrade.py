"""
All provider client operator upgrades implemented here

"""

import logging

from ocs_ci.deployment.hosted_cluster import HostedODF
from ocs_ci.ocs.dr_upgrade import DRUpgrade
from ocs_ci.framework import config
from ocs_ci.ocs import ocs_upgrade
from ocs_ci.ocs.ocs_upgrade import OCSUpgrade
from ocs_ci.ocs import constants
from ocs_ci.deployment.metallb import MetalLBInstaller
from ocs_ci.deployment.cnv import CNVInstaller
from ocs_ci.deployment.deployment import Deployment
from ocs_ci.ocs.acm_upgrade import ACMUpgrade
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    skipif_ocp_version,
    skipif_managed_service,
    runs_on_provider,
    skipif_external_mode,
)
from ocs_ci.deployment.helpers.lso_helpers import lso_upgrade

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


class OperatorUpgrade(ProviderUpgrade):
    """
    A class to handle installed operators on provider upgrades

    """

    def __init__(self):
        super().__init__()
        self.drupgrade_obj = DRUpgrade()
        self.metallb_installer_obj = MetalLBInstaller()
        self.cnv_installer_obj = CNVInstaller()
        self.acm_hub_upgrade_obj = ACMUpgrade()

    def run_acm_operator_upgrade(self):
        """
        This method is for acm operator upgrade
        """
        if not Deployment().acm_operator_installed():
            log.info("ACM operator is unavailable")
            log.info("Upgrade mce operator")
        try:
            self.acm_hub_upgrade_obj.run_upgrade()
        except Exception as e:
            log.error(f"ACM Operator upgrade failed: {e}")

    def run_operators_upgrade(self):
        """
        This method is for upgrade of all operators required for provider clusters,
        ACM, Metallb, Cnv, lso

        To do: MCE

        """
        try:
            if not self.metallb_installer_obj.upgrade_metallb():
                log.error("Failed to upgrade Metallb operator")
            else:
                log.info("Upgrade successful")
        except Exception as e:
            log.error(f"Failed to upgrade Metallb operator: {e}")

        try:
            if not self.cnv_installer_obj.upgrade_cnv():
                raise Exception("CNV Operator upgrade failed")
        except Exception as e:
            log.error(f"Failed to upgrade CNV operator: {e}")

        try:
            self.run_acm_operator_upgrade()
        except Exception as e:
            log.error(f"Failed to upgrade ACM operator: {e}")

        try:
            if not lso_upgrade():
                log.error("Failed to upgrade lso operator")
            else:
                log.info("Upgrade successful")
        except Exception as e:
            log.error(f"Failed to upgrade lso operator: {e}")

    def bump_ocs_version_on_clients(self, cluster_names=None):
        """
        Bump the OCS version to the latest version available in the registry

        Args:
            cluster_names (list): List of cluster names to upgrade. If None, all clusters will be upgraded.

        """
        log.info(
            "Bumping OCS version to the latest available in the registry of client clusters"
        )

        if not cluster_names:
            cluster_names = list(config.ENV_DATA.get("clusters").keys())

        for cluster_name in cluster_names:
            log.info(
                f"Validate ODF client operator installed on hosted OCP cluster '{cluster_name}'"
            )
            try:
                hosted_odf = HostedODF(cluster_name)
                if not hosted_odf.odf_client_installed():
                    log.info(
                        f"ODF client operator not installed on HCP cluster '{cluster_name}', skipping this client"
                    )
                    continue
                hosted_odf.create_catalog_source(
                    reapply=True,
                    odf_version_tag=f"latest-stable-{self.upgrade_version}",
                )
            except Exception as e:
                # we don't want to abort the upgrade process if one client upgrade fails because:
                # It will be easier to address issue manually with one client
                # It allows to run negative tests, when one client fails to upgrade
                log.error(
                    f"Failed to bump ODF client operator version on hosted OCP cluster '{cluster_name}': {e}"
                )


class ProviderClusterOperatorUpgrade(ProviderUpgrade):
    """
    A class to handle Provider Cluster operator upgrades

    """

    def run_provider_upgrade(self):
        """
        This method is for running the upgrade of ocs, metallb, acm and cnv opertaors
        """
        try:
            log.info("Starting the operator upgrade process...")
            operator_upgrade = OperatorUpgrade()
            # Bump OCS version on clients. This function will not fail upgrade if bump of any client fails
            operator_upgrade.bump_ocs_version_on_clients()
            ocs_upgrade.run_ocs_upgrade()
            operator_upgrade.run_operators_upgrade()
            log.info("Operator upgrade completed successfully.")
        except Exception as e:
            log.error(f"Operator upgrade failed: {e}")
            raise
