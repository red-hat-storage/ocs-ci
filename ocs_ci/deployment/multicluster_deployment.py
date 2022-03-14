import logging

from ocs_ci.deployment.deployment import Deployment
from ocs_ci.framework import config
from ocs_ci.ocs.exceptions import ACMClusterDeployException
from ocs_ci.ocs.ui import acm_ui
from ocs_ci.ocs.utils import get_non_acm_cluster_config


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
        self.multicluster_mode = config.MULTCLUSTER.get("multicluster_mode", None)
        # Used for housekeeping during multiple OCP cluster deployments
        self.deployment_cluster_list = list()
        # Whether to start deployment in asynchronous mode or synchronous mode
        # In async deploy mode, we will have a single wait method waiting for
        # all the cluster deployments to finish
        self.deploy_sync_mode = config.MULTICLUSTER.get("deploy_sync_mode", "async")

    def do_deploy_ocp(self):
        """
        This function overrides the parent's function in order accomodate
        ACM based OCP cluster deployments

        """
        if self.multicluster_mode == "regional_dr":
            self.do_rdr_acm_ocp_deploy()

    def do_rdr_acm_ocp_deploy(self):
        """
        Specific to regional DR OCP cluster deployments

        """
        factory = acm_ui.ACMOCPDeploymentFactory()

        if self.deploy_sync_mode == "async":
            rdr_clusters = get_non_acm_cluster_config()
            for cluster_conf in rdr_clusters:
                deployer = factory.get_platform_instance(cluster_conf)
                deployer.create_cluster_prereq()
                deployer.create_cluster()
                self.deployment_status_list.append(deployer)
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

    def deploy_cluster(self):
        """
        We deploy new OCP clusters using ACM
        Note: Importing cluster through ACM has been implemented as part
        of Jenkins pipeline

        """

        super().deploy_cluster()
