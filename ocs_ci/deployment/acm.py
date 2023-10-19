"""
All ACM related deployment classes and functions should go here.

"""
import os
import logging
import tempfile
import shutil
import requests

import semantic_version

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    DRPrimaryNotFoundException,
)
from ocs_ci.utility import templating
from ocs_ci.ocs.utils import get_non_acm_cluster_config
from ocs_ci.utility.utils import run_cmd, run_cmd_interactive
from ocs_ci.ocs.node import get_typed_worker_nodes, label_nodes

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
        # released/unreleased
        self.submariner_release_type = config.ENV_DATA.get("submariner_release_type")
        # Deployment type:
        self.deployment_type = config.ENV_DATA.get("submariner_deployment")
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
        elif self.source == "downstream":
            self.deploy_downstream()
        else:
            raise Exception(f"The Submariner source: {self.source} is not recognized")

    def deploy_upstream(self):
        self.download_binary()
        self.submariner_configure_upstream()

    def deploy_downstream(self):
        config.switch_acm_ctx()
        # Get the Selenium driver obj after logging in to ACM
        # Using import here, to avoid partly circular import
        from ocs_ci.ocs.acm.acm import AcmAddClusters, login_to_acm

        login_to_acm()
        acm_obj = AcmAddClusters()
        if self.submariner_release_type == "unreleased":
            old_ctx = config.cur_index
            for cluster in get_non_acm_cluster_config():
                config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
                self.create_acm_brew_icsp()
            config.switch_ctx(old_ctx)
        acm_obj.install_submariner_ui()
        acm_obj.submariner_validation_ui()

    def create_acm_brew_icsp(self):
        """
        This is a prereq for downstream unreleased submariner

        """
        icsp_data = templating.load_yaml(constants.ACM_DOWNSTREAM_BREW_ICSP)
        icsp_data_yaml = tempfile.NamedTemporaryFile(
            mode="w+", prefix="acm_icsp", delete=False
        )
        templating.dump_data_to_temp_yaml(icsp_data, icsp_data_yaml.name)
        run_cmd(f"oc create -f {icsp_data_yaml.name}", timeout=300)

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
            if config.ENV_DATA.get("submariner_upstream_version_tag"):
                os.environ["VERSION"] = config.ENV_DATA.get(
                    "submariner_upstream_version_tag"
                )
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
            if cluster_index != config.get_active_acm_index():
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

        connct_check = None
        if config.ENV_DATA.get("submariner_upstream_version_tag") != "devel":
            subctl_vers = self.get_subctl_version()
            if subctl_vers.minor <= 15:
                connct_check = f"verify {' '.join(kubeconf_list)} --only connectivity"
        if not connct_check:
            # New cmd format
            connct_check = f"verify --kubeconfig {kubeconf_list[0]} --toconfig {kubeconf_list[1]} --only connectivity"

        # Workaround for now, ignoring verify faliures
        # need to be fixed once pod security issue is fixed
        try:
            run_subctl_cmd(connct_check)
        except Exception:
            if not config.ENV_DATA["submariner_ignore_connectivity_test"]:
                logger.error("Submariner verification has issues")
                raise
            else:
                logger.warning("Submariner verification has issues but ignored for now")

    def get_subctl_version(self):
        """
        Run 'subctl version ' command and return a Version object

        Returns:
            vers (Version): semanctic version object

        """
        out = run_cmd("subctl version")
        vstr = out.split(":")[1].rstrip().lstrip()[1:]
        vers = semantic_version.Version(vstr)
        return vers

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
