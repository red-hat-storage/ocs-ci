"""
All ACM related deployment classes and functions should go here.

"""

import os
import logging
import tempfile
import shutil
import requests
import subprocess
import time
import glob

import semantic_version
import platform
import tarfile

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    DRPrimaryNotFoundException,
    SubctlDownloadFailed,
    UnsupportedPlatformError,
)
from ocs_ci.utility import templating
from ocs_ci.ocs.utils import get_non_acm_cluster_config, get_primary_cluster_config
from ocs_ci.utility.ibmcloud import (
    set_region,
    login,
    assign_floating_ips_to_workers,
    set_resource_group_name,
    is_ibm_platform,
)
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import (
    run_cmd,
    run_cmd_interactive,
    wait_for_machineconfigpool_status,
)
from ocs_ci.ocs.node import get_typed_worker_nodes, label_nodes, get_worker_nodes

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
        # Download subctl binary in any case.
        self.download_binary()
        if self.source == "upstream":
            self.deploy_upstream()
        elif self.source == "downstream":
            self.deploy_downstream()
        else:
            raise Exception(f"The Submariner source: {self.source} is not recognized")

    def deploy_upstream(self):
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
                self.create_acm_brew_idms()
            config.switch_ctx(old_ctx)

        global_net = get_primary_cluster_config().ENV_DATA.get("enable_globalnet", True)
        if (
            is_ibm_platform()
            and get_primary_cluster_config().ENV_DATA.get("deployment_type")
            == constants.IPI_DEPL_TYPE
        ):
            logger.info("Logging into IBMCLOUD CLI")
            login()

            for cluster in get_non_acm_cluster_config():
                config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])

                set_region()
                set_resource_group_name()
                floating_ips_dict = assign_floating_ips_to_workers()
                for node in get_worker_nodes():
                    cmd = (
                        f"oc annotate node {node} "
                        f"gateway.submariner.io/public-ip=ipv4:{floating_ips_dict.get(node)} --overwrite"
                    )
                    run_cmd(cmd=cmd, secrets=[floating_ips_dict.get(node)])

            acm_obj.install_submariner_cli(globalnet=global_net)
        else:
            acm_obj.install_submariner_ui(globalnet=global_net)

        acm_obj.submariner_validation_ui()

    def create_acm_brew_idms(self):
        """
        This is a prereq for downstream unreleased submariner

        """
        idms_data = templating.load_yaml(constants.SUBMARINER_DOWNSTREAM_BREW_IDMS)
        idms_data_yaml = tempfile.NamedTemporaryFile(
            mode="w+", prefix="acm_idms", delete=False
        )
        templating.dump_data_to_temp_yaml(idms_data, idms_data_yaml.name)
        run_cmd(f"oc apply -f {idms_data_yaml.name}", timeout=300)
        wait_for_machineconfigpool_status(node_type="all")

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
        elif self.source == "downstream":
            self.download_downstream_binary()

    @retry((tarfile.TarError, EOFError, FileNotFoundError), tries=8, delay=5)
    def wait_for_tar_file(self, subctl_download_tar_file):
        """
        1. First check if file exists
        2. Check if file is accessible
        3. check if tarfile is intact
        """
        if not os.path.exists(subctl_download_tar_file):
            raise FileNotFoundError(f"Tar file not found {subctl_download_tar_file}")
        else:
            logger.info(f"Found the tar file {subctl_download_tar_file}")

        if os.path.isfile(f"{subctl_download_tar_file}") and os.access(
            f"{subctl_download_tar_file}", os.R_OK
        ):
            logger.info(f"File {subctl_download_tar_file} successfully downloaded")
            try:
                # Check if tar is readable without any errors
                with tarfile.open(subctl_download_tar_file) as tar:
                    tar.getmembers()
                logger.info("the tar.xz is intact and readable")
            except (tarfile.TarError, EOFError) as tar_error:
                logger.error(f"The tar.xz is corrupted or not readable {tar_error}")
                raise
        else:
            logger.warning(
                f"File {subctl_download_tar_file} is not accessible or corrupted,"
                f"Retrying reading the tar file again"
            )
            raise tarfile.TarError()

    @retry((SubctlDownloadFailed, CommandFailed))
    def download_downstream_binary(self):
        """
        Download downstream subctl binary

        Raises:
            UnsupportedPlatformError : If current platform has no supported subctl binary
        """

        subctl_ver = config.ENV_DATA["subctl_version"]
        version_str = subctl_ver.split(":")[1]
        pull_secret_path = os.path.join(constants.DATA_DIR, "pull-secret")
        processor = platform.processor()
        arch = platform.machine()
        if arch == "x86_64" and processor == "x86_64":
            binary_pltfrm = "amd64"
        elif arch == "arm64" and processor == "arm":
            binary_pltfrm = "arm64"
        else:
            raise UnsupportedPlatformError(
                "Not a supported architecture for subctl binary"
            )
        cmd = (
            f"oc image extract --filter-by-os linux/{binary_pltfrm} --registry-config "
            f"{pull_secret_path} {constants.SUBCTL_DOWNSTREAM_URL}{subctl_ver} "
            f'--path="/dist/subctl-{version_str}*-linux-{binary_pltfrm}.tar.xz":/tmp --confirm'
        )
        run_cmd(cmd)
        # After oc image extract wait for some time
        # so that tar won't fail
        time.sleep(10)
        # Check if file exists before calling tar
        subctl_download_tar_file = glob.glob(
            f"/tmp/subctl-{version_str}*-linux-{binary_pltfrm}.tar.xz"
        )[0]
        try:
            self.wait_for_tar_file(subctl_download_tar_file)
        except Exception as e:
            logger.error(
                f"Unexpected error during subctl tar file download/extract: {e}"
            )
            raise SubctlDownloadFailed("Failed to download subctl tar file")

        decompress = f"tar -C /tmp/ -xf {subctl_download_tar_file}"
        p = subprocess.run(
            decompress,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            text=True,
        )
        if p.returncode:
            logger.error("Failed to untar subctl")
            if p.stderr:
                logger.error(f"{p.stderr}")
            raise CommandFailed
        else:
            logger.info(f"Tar decompressed successfully {p.stdout}")
        target_dir = config.RUN["bin_dir"]
        install_cmd = (
            f"install -m744 /tmp/subctl-{version_str}*/subctl-{version_str}*-linux-{binary_pltfrm} "
            f"{target_dir} "
        )
        run_cmd(install_cmd, shell=True)
        subctl_bin_file = glob.glob(
            f"{target_dir}/subctl-{version_str}*-linux-{binary_pltfrm}"
        )[0]
        run_cmd(f"mv {subctl_bin_file} {target_dir}/subctl", shell=True)
        os.environ["PATH"] = os.environ["PATH"] + ":" + os.path.abspath(f"{target_dir}")

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
