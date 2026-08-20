"""
All provider client operator upgrades implemented here

"""

import logging

from ocs_ci.deployment.hub_spoke import (
    HostedODF,
    HostedFDF,
    HostedClients,
    clear_fdf_catalog_image_cache,
    is_fdf_on_provider,
)
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

log = logging.getLogger(__name__)


def _is_fdf_upgrade():
    """
    Return True when this upgrade run targets FDF rather than ODF.

    Checks the explicit ``product_type`` config key first (set via
    ``--product-type fdf``). Falls back to a live cluster probe so that
    ad-hoc runs without the CLI flag still route correctly.
    """
    if config.ENV_DATA.get("product_type") == "fdf":
        return True
    return is_fdf_on_provider()


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
        Bump the ODF/FDF catalog on all HCP client clusters.

        For ODF: updates the ocs-catalogsource image tag to the target version.
        For FDF: patches the isf-data-foundation-catalog with the post-upgrade
        image resolved from the provider (mirror-resolved for offline racks).

        Args:
            cluster_names (list): Cluster names to update. Defaults to all
                hci_client clusters from config.

        """
        log.info("Bumping OCS/FDF catalog version on client clusters")

        if not cluster_names:
            cluster_names = list((config.ENV_DATA.get("clusters") or {}).keys())
        if not cluster_names:
            from ocs_ci.deployment.helpers.hypershift_base import (
                get_hosted_cluster_names,
            )

            cluster_names = get_hosted_cluster_names()

        is_fdf = _is_fdf_upgrade()

        for cluster_name in cluster_names:
            log.info(f"Bumping catalog on hosted OCP cluster '{cluster_name}'")
            try:
                if is_fdf:
                    hosted_odf = HostedFDF(cluster_name)
                    if not hosted_odf.odf_client_installed():
                        log.info(
                            f"FDF client operator not installed on HCP cluster "
                            f"'{cluster_name}', skipping this client"
                        )
                        continue
                    hosted_odf.create_catalog_source(reapply=True)
                else:
                    hosted_odf = HostedODF(cluster_name)
                    if not hosted_odf.odf_client_installed():
                        log.info(
                            f"ODF client operator not installed on HCP cluster "
                            f"'{cluster_name}', skipping this client"
                        )
                        continue
                    hosted_odf.create_catalog_source(
                        reapply=True,
                        odf_version_tag=f"latest-stable-{self.upgrade_version}",
                    )
            except Exception as e:
                # Non-fatal: easier to fix one client manually; also enables
                # negative tests where a single client is expected to fail.
                log.error(
                    f"Failed to bump catalog on hosted OCP cluster '{cluster_name}': {e}"
                )

    def verify_fdf_clients_upgraded(self, cluster_names=None):
        """
        Verify that FDF client operator CSVs are Succeeded on all HCP clusters.

        Called after the provider FDF upgrade and catalog push so we confirm
        that the auto-upgrade propagated to every hosted client cluster.

        Args:
            cluster_names (list): Cluster names to check. Defaults to all
                hci_client clusters from config.

        Raises:
            AssertionError: If one or more client clusters did not upgrade.
        """
        if not cluster_names:
            cluster_names = list((config.ENV_DATA.get("clusters") or {}).keys())

        results = []
        for cluster_name in cluster_names:
            log.info(f"Verifying FDF client upgrade on HCP cluster '{cluster_name}'")
            try:
                hosted_fdf = HostedFDF(cluster_name)
                client_upgraded = hosted_fdf.odf_client_installed()
                results.append(client_upgraded)
                if client_upgraded:
                    log.info(f"FDF client CSVs Succeeded on '{cluster_name}'")
                else:
                    log.error(f"FDF client CSVs not all Succeeded on '{cluster_name}'")
            except Exception as e:
                log.error(
                    f"Failed to verify FDF client upgrade on '{cluster_name}': {e}"
                )
                results.append(False)

        assert all(
            results
        ), "FDF client upgrade verification failed on one or more HCP clusters"


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
        Upgrade all operators on the provider cluster, routing between ODF and
        FDF paths based on product_type config or runtime detection.

        ODF path (existing behaviour, unchanged):
            1. Prune old IDMS
            2. Bump OCS catalog on clients
            3. OCS upgrade on provider
            4. Propagate IDMS to hosted clusters
            5. Upgrade supporting operators (MetalLB, CNV, ACM, LSO)

        FDF path:
            1. Upgrade Fusion + FDF operator on provider (ITMS/IDMS handled
               inside FDFUpgrade.run_upgrade() for offline racks)
            2. Invalidate the cached FDF catalog image
            3. Push updated FDF catalog to client clusters (mirror-resolved
               image for offline racks)
            4. Propagate IDMS mirror config to hosted clusters
            5. Verify FDF auto-upgraded on all client clusters
            6. Upgrade supporting operators (MetalLB, CNV, ACM, LSO)
        """
        try:
            log.info("Starting the operator upgrade process...")
            operator_upgrade = OperatorUpgrade()
            is_fdf = _is_fdf_upgrade()

            if is_fdf:
                log.info("FDF detected -- running FDF provider upgrade path")

                from ocs_ci.deployment.fusion_data_foundation import (
                    FusionDataFoundationDeployment,
                )
                from ocs_ci.ocs.fdf_upgrade import FDFUpgrade

                # Step 1: Upgrade Fusion + FDF operator on the provider.
                # For offline racks this also creates ITMS/IDMS, waits for
                # MCP, and patches FusionServiceDefinition.
                namespace = config.ENV_DATA["cluster_namespace"]
                fdf_deployment = FusionDataFoundationDeployment()
                fdf_version = fdf_deployment.get_installed_version()
                if fdf_version.startswith("v"):
                    fdf_version = fdf_version[1:]
                FDFUpgrade(
                    namespace=namespace,
                    version_before_upgrade=fdf_version,
                ).run_upgrade()

                # Step 2: Invalidate cached catalog image so clients get the
                # post-upgrade image from the provider on the next fetch.
                clear_fdf_catalog_image_cache()

                # Step 3: Push updated FDF catalog to client clusters.
                # For offline racks the image is resolved through the
                # provider's ITMS before being sent to clients.
                operator_upgrade.bump_ocs_version_on_clients()

                # Step 4: Propagate IDMS mirror config to hosted clusters so
                # client nodes can pull FDF operator images from local mirrors.
                hosted_clients = HostedClients()
                hosted_clients.apply_idms_to_hosted_clusters()

                # Step 5: Verify FDF auto-upgraded on all client clusters.
                operator_upgrade.verify_fdf_clients_upgraded()
            else:
                # ODF path (existing logic, unchanged)
                prune_old_df_repo_idms(force_delete_pods=True)
                operator_upgrade.bump_ocs_version_on_clients()
                ocs_upgrade.run_ocs_upgrade()

                hosted_clients = HostedClients()
                hosted_clients.apply_idms_to_hosted_clusters()

            # Step 6: Upgrade supporting operators (common to both paths).
            operator_upgrade.run_operators_upgrade()
            log.info("Operator upgrade completed successfully.")
        except Exception as e:
            log.error(f"Operator upgrade failed: {e}")
            raise
