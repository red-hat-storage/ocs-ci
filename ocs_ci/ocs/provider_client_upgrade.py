"""
All provider client operator upgrades implemented here

"""

import logging

from ocs_ci.deployment.hub_spoke import HostedODF, HostedClients
from ocs_ci.ocs.dr_upgrade import DRUpgrade
from ocs_ci.framework import config
from ocs_ci.ocs import ocs_upgrade
from ocs_ci.ocs.ocs_upgrade import OCSUpgrade, prune_old_df_repo_idms
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
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility.retry import retry

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
        # clients whose CatalogSource could not be bumped, their Subscription
        # channel must not be advanced to a version the catalog cannot serve
        self.clients_with_failed_catalog_bump = set()

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
        Bump the CatalogSource of the client clusters to the new ODF version.

        This only makes the new content available to OLM. The Subscription
        channel is moved separately, by bump_channel_on_clients(), once the
        provider itself is upgraded - see that method for the reason.

        Args:
            cluster_names (list): List of cluster names to upgrade. If None, all clusters will be upgraded.

        """
        log.info(
            "Bumping OCS version to the latest available in the registry of client clusters"
        )

        if not cluster_names:
            cluster_names = list(config.ENV_DATA.get("clusters").keys())

        odf_version_tag = f"latest-stable-{self.upgrade_version}"

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
                if not hosted_odf.create_catalog_source(
                    reapply=True,
                    odf_version_tag=odf_version_tag,
                ):
                    self.clients_with_failed_catalog_bump.add(cluster_name)
                    log.error(
                        f"Failed to bump CatalogSource on hosted OCP cluster '{cluster_name}'"
                    )
            except Exception as e:
                self.clients_with_failed_catalog_bump.add(cluster_name)
                # we don't want to abort the upgrade process if one client upgrade fails because:
                # It will be easier to address issue manually with one client
                # It allows to run negative tests, when one client fails to upgrade
                log.error(
                    f"Failed to bump ODF client operator version on hosted OCP cluster '{cluster_name}': {e}"
                )

    def bump_channel_on_clients(self, cluster_names=None):
        """
        Move the ODF client Subscription channel of the client clusters to the
        new ODF version.

        Bumping only the CatalogSource leaves the Subscription pinned to the
        channel it was created with, so OLM keeps resolving the previous ODF
        stream while the mirrors on the cluster already point at the new one,
        which leaves the client with unpullable images.

        This must run only after the provider has been upgraded. The
        'subscription.ocs.openshift.io' validating webhook of the ocs-client
        operator rejects any channel that does not satisfy the requirements of
        the StorageClient, so a channel ahead of the provider is denied with
        'subscription channel <channel> not allowed as it'll violate
        storageclient ... requirements'. The provider also needs a moment to
        advertise its new version to the clients, hence the retry.

        Args:
            cluster_names (list): List of cluster names to upgrade. If None, all clusters will be upgraded.

        """
        log.info("Bumping ODF client subscription channel of client clusters")

        if not cluster_names:
            cluster_names = list(config.ENV_DATA.get("clusters").keys())

        odf_version_tag = f"latest-stable-{self.upgrade_version}"

        for cluster_name in cluster_names:
            if cluster_name in self.clients_with_failed_catalog_bump:
                log.error(
                    f"CatalogSource bump failed earlier on hosted OCP cluster '{cluster_name}', "
                    "not advancing its subscription channel to a version the catalog cannot serve"
                )
                continue
            try:
                hosted_odf = HostedODF(cluster_name)
                if not hosted_odf.odf_client_installed():
                    log.info(
                        f"ODF client operator not installed on HCP cluster '{cluster_name}', skipping this client"
                    )
                    continue
                retry(CommandFailed, tries=10, delay=30, backoff=1)(
                    hosted_odf.update_subscription_channel
                )(odf_version_tag=odf_version_tag)
            except Exception as e:
                # consistent with bump_ocs_version_on_clients: a single failing
                # client should not abort the upgrade of the remaining ones
                log.error(
                    f"Failed to bump ODF client subscription channel on hosted OCP cluster "
                    f"'{cluster_name}': {e}"
                )


class KubevirtClusterUpgrade(ProviderUpgrade):
    """
    A class to handle Kubevirt Cluster(s) upgrade

    """

    def run_upgrade_ocp_on_kubevirt_clusters(self):
        hosted_clients = HostedClients()
        hosted_clients.upgrade_ocp_on_kubevirt_clusters()


class ProviderClusterOperatorUpgrade(ProviderUpgrade):
    """
    A class to handle Provider Cluster operator upgrades

    """

    def __init__(self):
        super().__init__(namespace=config.ENV_DATA["cluster_namespace"])

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

            hosted_clients = HostedClients()
            hosted_clients.apply_idms_to_hosted_clusters()

            # Only now that the provider is upgraded and the mirrors of the new
            # ODF version are applied to the hosted clusters, the clients can
            # follow. Moving the channel earlier is rejected by the ocs-client
            # operator webhook and would pull images that are not yet mirrored.
            operator_upgrade.bump_channel_on_clients()

            operator_upgrade.run_operators_upgrade()
            # Delete old idms left over from previous upgrades. This has to run
            # only once the provider and all clients are on the new ODF version:
            # the older df-repo IDMS still carries the mirrors for the version
            # that is being replaced, and on a Hypershift provider the surviving
            # IDMS is propagated down to every hosted cluster. Pruning too early
            # removes mirrors that the running workloads still resolve against.
            prune_old_df_repo_idms(force_delete_pods=True)
            log.info("Operator upgrade completed successfully.")
        except Exception as e:
            log.error(f"Operator upgrade failed: {e}")
            raise
