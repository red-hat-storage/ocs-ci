import os

import logging

from ocs_ci.deployment.deployment import Deployment
from ocs_ci.framework import config
from ocs_ci.ocs.exceptions import ACMClusterDeployException, ACMClusterDestroyException
from ocs_ci.ocs.ui import acm_ui
from ocs_ci.ocs.utils import get_non_acm_cluster_config
from ocs_ci.ocs.acm import acm
from ocs_ci.ocs import constants, defaults


logger = logging.getLogger(__name__)


class OCPDeployWithACM(Deployment):
    """
    When we instantiate this class, the assumption is we already have
    an OCP cluster with ACM installed and current context is ACM

    """

    def __init__(self):
        """
        When we init Deployment class it will have all the
        ACM cluster's context
        """
        super().__init__()
        self.multicluster_mode = config.MULTICLUSTER.get("multicluster_mode", None)
        # Used for housekeeping during multiple OCP cluster deployments
        self.deployment_cluster_list = list()
        # Whether to start deployment in asynchronous mode or synchronous mode
        # In async deploy mode, we will have a single wait method waiting for
        # all the cluster deployments to finish
        self.deploy_sync_mode = config.MULTICLUSTER.get("deploy_sync_mode", "async")
        self.ui_driver = None
        self.factory = acm_ui.ACMOCPDeploymentFactory()

    def do_deploy_ocp(self, log_cli_level="INFO"):
        """
        This function overrides the parent's function in order accomodate
        ACM based OCP cluster deployments

        """
        if config.ENV_DATA["skip_ocp_deployment"]:
            logger.warning(
                "Skipping OCP deployment through ACM because skip_ocp_deployment "
                "has been specified"
            )
            return
        if self.multicluster_mode == constants.RDR_MODE:
            self.do_rdr_acm_ocp_deploy()
            self.post_deploy_ops()

    def post_deploy_ops(self):
        """
        1. Install ingress certificates on OCP clusters deployed through ACM
        2. Run post_ocp_deploy on OCP clusters

        """
        prev = config.cur_index
        for cluster in get_non_acm_cluster_config():
            config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
            ssl_key = config.DEPLOYMENT.get("ingress_ssl_key", defaults.INGRESS_SSL_KEY)
            ssl_cert = config.DEPLOYMENT.get(
                "ingress_ssl_cert", defaults.INGRESS_SSL_CERT
            )
            for key in [ssl_key, ssl_cert]:
                if os.path.exists(key):
                    os.unlink(key)
            logger.info("Running post ocp deploy ops")
            self.post_ocp_deploy()
        config.switch_ctx(prev)

    def do_rdr_acm_ocp_deploy(self):
        """
        Specific to regional DR OCP cluster deployments

        """
        self.ui_driver = acm.login_to_acm()

        if self.deploy_sync_mode == "async":
            rdr_clusters = get_non_acm_cluster_config()
            for c in rdr_clusters:
                logger.info(f"{c.ENV_DATA['cluster_name']}")
                logger.info(f"{c.ENV_DATA['platform']}")
                logger.info(f"{c.ENV_DATA['deployment_type']}")
            for cluster_conf in rdr_clusters:
                deployer = self.factory.get_platform_instance(
                    self.ui_driver, cluster_conf
                )
                deployer.create_cluster_prereq()
                deployer.create_cluster()
                self.deployment_cluster_list.append(deployer)
            # At this point deployment of all non-acm ocp clusters have been
            # triggered, we need to wait for all of them to succeed
            self.wait_for_all_clusters_async()
            # Download kubeconfig to the respective directories
            for cluster in self.deployment_cluster_list:
                cluster.download_cluster_conf_files()

    def wait_for_all_clusters_async(self):
        # We will say done only when none of the clusters are in
        # 'Creating' state. Either they have to be in 'Ready' state
        # OR 'Failed' state
        done_waiting = False
        while not done_waiting:
            done_waiting = True
            for cluster in self.deployment_cluster_list:
                if cluster.deployment_status not in ["ready", "failed"]:
                    cluster.get_deployment_status()
                    done_waiting = False
        # We will fail even if one of the clusters failed to deploy
        failed_list = list()
        success_list = list()
        for cluster in self.deployment_cluster_list:
            if cluster.deployment_status == "failed":
                failed_list.append(cluster)
            else:
                success_list.append(cluster)

        if success_list:
            logger.info("Deployment for following clusters Passed")
            logger.info(f"{[c.cluster_name for c in success_list]}")
        if failed_list:
            logger.error("Deployment failed for following clusters")
            logger.error(f"{[c.cluster_name for c in failed_list]}")
            raise ACMClusterDeployException("one or more Cluster Deployment failed ")

    def deploy_cluster(self, log_cli_level="INFO"):
        """
        We deploy new OCP clusters using ACM
        Note: Importing cluster through ACM has been implemented as part
        of Jenkins pipeline

        """

        super().deploy_cluster(log_cli_level=log_cli_level)

    def destroy_cluster(self, log_cli_level=None):
        """
        Teardown OCP clusters deployed through ACM

        """
        self.ui_driver = acm.login_to_acm()
        cluster_list = list()

        rdr_clusters = get_non_acm_cluster_config()
        logger.info("Following ACM deployed OCP clusters will be destroyed")
        for cluster in rdr_clusters:
            logger.info(
                f"[{cluster.ENV_DATA['cluster_name']}"
                f"{cluster.ENV_DATA['platform']}_"
                f"{cluster.ENV_DATA['deployment_type']}]"
            )
        for cluster_conf in rdr_clusters:
            destroyer = self.factory.get_platform_instance(self.ui_driver, cluster_conf)
            destroyer.destroy_cluster()
            cluster_list.append(destroyer)

        self.wait_for_all_cluster_async_destroy(cluster_list)
        self.post_destroy_ops(cluster_list)

    def wait_for_all_cluster_async_destroy(self, destroy_cluster_list):
        # Wait until all the clusters are in 'Done' or 'Failed state
        destroyed_clusters = list()
        failed_clusters = list()
        done_waiting = False
        while not done_waiting:
            done_waiting = True
            for cluster in destroy_cluster_list:
                if cluster.destroy_status not in ["Done", "Failed"]:
                    cluster.get_destroy_status()
                    done_waiting = False
        for cluster in destroy_cluster_list:
            if cluster.destroy_status == "Done":
                destroyed_clusters.append(cluster)
            else:
                failed_clusters.append(cluster)

        if destroyed_clusters:
            logger.info(
                f"Destroyed clusters: {[c.cluster_name for c in destroyed_clusters]}"
            )
        if failed_clusters:
            raise ACMClusterDestroyException(
                f"Failed to destroy clusters:{[c.cluster_name for c in failed_clusters]} "
            )

    def post_destroy_ops(self, cluster_list):
        """
        Post destroy ops mainly includes ip clean up and dns cleanup

        Args:
            cluster_list (list[ACMOCPClusterDeploy]): list of platform specific instances

        """
        for cluster in cluster_list:
            cluster.post_destroy_ops()
