"""
All ACM related deployment classes and functions should go here.

"""
import os
import logging
import tempfile
import shutil
import requests

from ocs_ci.deployment.deployment import Deployment
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import (
    ACMClusterDeployException,
    CommandFailed,
    DRPrimaryNotFoundException,
)
from ocs_ci.ocs.utils import get_non_acm_cluster_config
from ocs_ci.utility.utils import run_cmd, run_cmd_interactive
from ocs_ci.ocs.node import get_typed_worker_nodes, label_nodes
from ocs_ci.ocs.ui import acm_ui


logger = logging.getLogger(__name__)


def run_subctl_cmd(cmd=None):
    """
    Run subctl command

    Args:
        cmd: subctl command to be executed

    """
    cmd = " ".join(["subctl", cmd])
    run_cmd(cmd)


def run_subctl_cmd_interactive(cmd, prompt, answer):
    """
    Handle interactive prompts with answers during subctl command

    Args:
        cmd (str): Command to be executed
        prompt (str): Expected question during command run which needs to be provided
        answer (str): Answer for the prompt

    Raises:
        InteractivePromptException: in case something goes wrong

    """
    cmd = " ".join(["subctl", cmd])
    run_cmd_interactive(
        cmd, {prompt: answer}, timeout=config.ENV_DATA["submariner_prompt_timeout"]
    )


class Submariner(object):
    """
    Submariner configuaration and deployment
    """

    def __init__(self):
        # whether upstream OR downstream
        self.source = config.ENV_DATA["submariner_source"]
        # Designated broker cluster index where broker will be deployed
        self.designated_broker_cluster_index = self.get_primary_cluster_index()
        # sequence number for the clusters from submariner perspective
        # Used mainly to run submariner commands, for each cluster(except ACM hub) we will
        # assign a seq number with 1 as primary and continue with subsequent numbers
        self.cluster_seq = 1
        # List of index to all the clusters which are participating in DR (except ACM)
        # i.e index in the config.clusters list
        self.dr_only_list = []

    def deploy(self):
        if self.source == "upstream":
            self.deploy_upstream()
        else:
            self.deploy_downstream()

    def deploy_upstream(self):
        self.download_binary()
        self.submariner_configure_upstream()

    def deploy_downstream(self):
        raise NotImplementedError("Deploy downstream functionality not implemented")

    def download_binary(self):
        if self.source == "upstream":
            # This script puts the platform specific binary in ~/.local/bin
            # we need to move the subctl binary to ocs-ci/bin dir
            try:
                resp = requests.get(constants.SUBMARINER_DOWNLOAD_URL)
            except requests.ConnectionError:
                logger.exception(
                    "Failed to download the downloader script from submariner site"
                )
                raise
            tempf = tempfile.NamedTemporaryFile(
                dir=".", mode="wb", prefix="submariner_downloader_", delete=False
            )
            tempf.write(resp.content)

            # Actual submariner binary download
            cmd = f"bash {tempf.name}"
            try:
                run_cmd(cmd)
            except CommandFailed:
                logger.exception("Failed to download submariner binary")
                raise

            # Copy submariner from ~/.local/bin to ocs-ci/bin
            # ~/.local/bin is the default path selected by submariner script
            shutil.copyfile(
                os.path.expanduser("~/.local/bin/subctl"),
                os.path.join(config.RUN["bin_dir"], "subctl"),
            )

    def submariner_configure_upstream(self):
        """
        Deploy and Configure upstream submariner

        Raises:
            DRPrimaryNotFoundException: If there is no designated primary cluster found

        """
        if self.designated_broker_cluster_index < 0:
            raise DRPrimaryNotFoundException("Designated primary cluster not found")

        # Deploy broker on designated cluster
        # follow this config switch statement carefully to be mindful
        # about the context with which we are performing the operations
        config.switch_ctx(self.designated_broker_cluster_index)
        logger.info(f"Switched context: {config.cluster_ctx.ENV_DATA['cluster_name']}")

        deploy_broker_cmd = "deploy-broker"
        try:
            run_subctl_cmd(deploy_broker_cmd)
        except CommandFailed:
            logger.exception("Failed to deploy submariner broker")
            raise

        # Label the gateway nodes on all non acm cluster
        restore_index = config.cur_index
        for cluster in get_non_acm_cluster_config():
            config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
            gateway_node = self.get_default_gateway_node()
            label_nodes([gateway_node], constants.SUBMARINER_GATEWAY_NODE_LABEL)
        config.switch_ctx(restore_index)

        # Join all the clusters (except ACM cluster in case of hub deployment)
        for cluster in config.clusters:
            print(len(config.clusters))
            cluster_index = cluster.MULTICLUSTER["multicluster_index"]
            if cluster_index != config.get_acm_index():
                join_cmd = (
                    f"join --kubeconfig {cluster.RUN['kubeconfig']} "
                    f"{config.ENV_DATA['submariner_info_file']} "
                    f"--clusterid c{self.cluster_seq} --natt=false"
                )
                try:
                    run_subctl_cmd(
                        join_cmd,
                    )
                    logger.info(
                        f"Subctl join succeded for {cluster.ENV_DATA['cluster_name']}"
                    )
                except CommandFailed:
                    logger.exception("Cluster failed to join")
                    raise

                self.cluster_seq = self.cluster_seq + 1
                self.dr_only_list.append(cluster_index)
        # Verify submariner connectivity between clusters(excluding ACM)
        kubeconf_list = []
        for i in self.dr_only_list:
            kubeconf_list.append(config.clusters[i].RUN["kubeconfig"])
        connct_check = f"verify {' '.join(kubeconf_list)} --only connectivity"
        run_subctl_cmd(connct_check)

    def get_primary_cluster_index(self):
        """
        Return list index (in the config list) of the primary cluster
        A cluster is primary from DR perspective

        Returns:
            int: Index of the cluster designated as primary

        """
        for i in range(len(config.clusters)):
            if config.clusters[i].MULTICLUSTER.get("primary_cluster"):
                return i
        return -1

    def get_default_gateway_node(self):
        """
        Return the default node to be used as submariner gateway

        Returns:
            str: Name of the gateway node

        """
        # Always return the first worker node
        return get_typed_worker_nodes()[0]


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
