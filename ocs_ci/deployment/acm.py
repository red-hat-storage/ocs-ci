"""
All ACM related deployment classes and functions should go here.

"""
import logging
import subprocess
import shlex
import pexpect

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.constants import SUBMARINER_GATEWAY_PROMPT
from ocs_ci.ocs.exceptions import (
    CommandFailed, 
    DRPrimaryNotFoundException,
    InteractivePromptException,
)
from ocs_ci.utility.utils import run_cmd


logger = logging.getLogger(__name__)


def run_subctl_cmd(cmd=None):
    """
    Run subctl command

    Args:
        cmd: subctl command to be executed

    Returns:

    """
    cmd = " ".join("subctl", cmd) 
    out = run_cmd(cmd)
    logger.info(out)

def run_subctl_cmd_interactive(cmd, prompt, answer):
    """
    Handle interactive prompts with answers during subctl command

    Args:
        cmd(str): Command to be executed
        prompt(str): Expected question during command run which needs to be provided
        answer(str): Answer for the prompt

    Raises:
        InteractivePromptException: in case something goes wrong

    """
    cmd = " ".join("subctl", cmd)

    child = pexpect.spawn(cmd)
    if child.expect(
        prompt,
        timeout=config.ENV_DATA["submariner_prompt_timeout"]
    ):
        raise InteractivePromptException("Unexpected Prompt")

    if not child.sendline("".join(answer, constants.ENTER_KEY)):
        raise InteractivePromptException("Failed to provide answer to the prompt")


class Submariner(object):
    """
    Submariner configuaration and deployment
    """
    def __init__(self, index):
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
        pass

    def download_binary(self):
        if self.source == "upstream":
            # This script puts the platform specific binary in ~/.local/bin
            # we need to move the subctl binary to ocs-ci/bin dir
            download_cmd = "curl -Ls https://get.submariner.io | bash"
            try:
                run_cmd(download_cmd)
            except CommandFailed:
                logger.exception("Failed to download submariner binary")
                raise

            # Copy submariner from ~/.local/bin to ocs-ci/bin
            # ~/.local/bin is the default path selected by submariner script
            # TODO: create symlink
            cp_cmd = f"cp ~/.local/bin/subctl {config.RUN['bin_dir']}"
            ret = subprocess.run(shlex.split(cp_cmd))
            try:
                ret.check_returncode()
            except subprocess.CalledProcessError:
                logger.exception("Couldn't find subctl binary")
                raise

    def submariner_configure_upstream(self):
        """
        Deploy and Configure upstream submariner

        Raises:
            DRPrimaryNotFoundException: If there is no designated primary cluster found

        """
        gateway_node = self.get_default_gateway_node()
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

        # Join all the clusters (except ACM cluster in case of hub deployment)
        for cluster in config.clusters:
            cluster_index = config.clusters.index(cluster)
            if not cluster_index == config.acm_index:
                join_cmd = (
                    f"join --kubeconfig {cluster.RUN['kubeconfig']} "
                    f"{config.ENV_DATA['submariner_info_file']} "
                    f"--clusterid c{self.cluster_seq} --natt=false"
                )
                try:
                    run_subctl_cmd_interactive(
                        join_cmd, 
                        SUBMARINER_GATEWAY_PROMPT,
                        gateway_node, 
                    )
                except InteractivePromptException:
                    logger.exception(f"Cluster failed to join")
                    raise

                self.cluster_seq = self.cluster_seq + 1
                self.dr_only_list.append(cluster_index)
        # Verify submariner connectivity between clusters(excluding ACM)
        kubeconf_list = [] 
        for i in self.dr_only_list:
            kubeconf_list.append(config.clusters[i].RUN["kubeconfig"])    
        connct_check = "verify " + " ".join(kubeconf_list) + "--only connectivity"
        run_subctl_cmd(connct_check)

    def get_primary_cluster_index(self):
        """
        Return list index (in the config list) of the primary cluster 
        A cluster is primary from DR perspective

        Returns:
            int: Index of the cluster designated as primary

        """
        for i in range(config.clusters):
            if config.clusters[i].get("ENV_DATA").get("designated_primary_cluster"):
                return i
        return -1


    def get_default_gateway_node(self):
        """
        Return the default node to be used as submariner gateway

        Returns:
            str: Name of the gateway node

        """
        # TODO: For now we are just returning compute-1, later (when we move to AWS) we need to find a way
        # to get actual nodes of the cluster
        return config.ENV_DATA["submariner_default_gateway_node"]



