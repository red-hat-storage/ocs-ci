"""
All ACM related deployment classes and functions should go here.

"""

import os
import logging
import tempfile
import shutil
import requests

import semantic_version
import platform

from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    DRPrimaryNotFoundException,
    SubctlDownloadFailed,
    UnexpectedDeploymentConfiguration,
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

    def is_submariner_installed(self, target_clusters):
        """
        Check via CLI whether submariner is already installed on the target managed clusters.

        Rules:
        1. Both managed clusters have submariner installed -> return True (skip installation).
        2. Exactly one managed cluster has submariner installed -> raise UnexpectedDeploymentConfiguration.
        3. Neither managed cluster has submariner installed -> return False (proceed with installation).
        4. If both have submariner installed, verify they belong to the same ManagedClusterSet.
           If their ManagedClusterSets differ or are not configured, raise UnexpectedDeploymentConfiguration.

        Args:
            target_clusters (list): List of cluster config objects for the target managed clusters.

        Returns:
            bool: True if submariner is installed and valid on all target clusters, False if installed on none.

        Raises:
            UnexpectedDeploymentConfiguration: If submariner state is mismatched across clusters or
                if ManagedClusterSets do not match.
        """
        logger.info(
            f"Checking via CLI if Submariner is already installed on target clusters: "
            f"{[c.ENV_DATA['cluster_name'] for c in target_clusters]}"
        )
        installed_clusters = []
        missing_clusters = []
        cluster_set_map = {}

        for cluster in target_clusters:
            cluster_name = cluster.ENV_DATA["cluster_name"]
            mc_obj = ocp.OCP(
                kind=constants.ACM_MANAGEDCLUSTER,
                resource_name=cluster_name,
            )
            mc_data = mc_obj.get(dont_raise=True)
            if not mc_data:
                logger.warning(
                    f"ManagedCluster resource '{cluster_name}' not found on ACM hub"
                )
                missing_clusters.append(cluster_name)
                continue

            labels = mc_data.get("metadata", {}).get("labels", {})
            cluster_set = labels.get(constants.ACM_CLUSTERSET_LABEL)
            cluster_set_map[cluster_name] = cluster_set

            # Check for submariner feature label on ManagedCluster
            submariner_feature = labels.get(
                "feature.open-cluster-management.io/addon-submariner"
            )

            # Check for ManagedClusterAddOn 'submariner' in the managed cluster namespace on ACM hub
            addon_obj = ocp.OCP(
                kind=constants.ACM_MANAGEDCLUSTER_ADDONS,
                namespace=cluster_name,
                resource_name="submariner",
            )
            addon_data = addon_obj.get(dont_raise=True)

            if submariner_feature == "available" or addon_data:
                logger.info(
                    f"Submariner addon detected on ManagedCluster '{cluster_name}' "
                    f"(feature label: {submariner_feature}, clusterset: {cluster_set})"
                )
                installed_clusters.append(cluster_name)
            else:
                logger.info(
                    f"Submariner addon not detected on ManagedCluster '{cluster_name}'"
                )
                missing_clusters.append(cluster_name)

        if installed_clusters and missing_clusters:
            raise UnexpectedDeploymentConfiguration(
                f"Submariner is only installed on {installed_clusters} but missing on {missing_clusters}. "
                f"Submariner must be installed on all target managed clusters or none."
            )

        if len(installed_clusters) == len(target_clusters):
            # Both (all) target managed clusters have submariner installed.
            # Verify they are bound to the same ManagedClusterSet.
            cluster_sets = set(cluster_set_map.values())
            if None in cluster_sets or not cluster_sets:
                raise UnexpectedDeploymentConfiguration(
                    f"Target clusters have Submariner installed but missing "
                    f"'{constants.ACM_CLUSTERSET_LABEL}' label: {cluster_set_map}"
                )
            if len(cluster_sets) > 1:
                raise UnexpectedDeploymentConfiguration(
                    f"Target clusters are not in the same ManagedClusterSet: {cluster_set_map}. "
                    f"Both managed clusters must belong to the same ManagedClusterSet."
                )
            logger.info(
                f"Submariner is already installed on all target clusters {installed_clusters} "
                f"under ManagedClusterSet '{cluster_sets.pop()}'. Skipping deployment."
            )
            return True

        logger.info(
            "Submariner is not installed on any target clusters. Proceeding with deployment."
        )
        return False

    def deploy_downstream(self):
        config.switch_acm_ctx()

        dr_cluster_relations = config.MULTICLUSTER.get("dr_cluster_relations", [])
        if dr_cluster_relations:
            dr_cluster_names = dr_cluster_relations[0]
            cluster_configs = [
                cluster
                for cluster in config.clusters
                if cluster.ENV_DATA["cluster_name"] in dr_cluster_names
            ]
        else:
            cluster_configs = get_non_acm_cluster_config()

        if self.is_submariner_installed(cluster_configs):
            logger.info(
                "Submariner is already installed and verified. Skipping downstream deployment."
            )
            return

        # Get the Selenium driver obj after logging in to ACM
        # Using import here, to avoid partly circular import
        from ocs_ci.ocs.acm.acm import AcmAddClusters, login_to_acm

        login_to_acm()
        acm_obj = AcmAddClusters()
        if self.submariner_release_type == "unreleased":
            old_ctx = config.cur_index
            for cluster in cluster_configs:
                # TODO: Skip if hosted cluster only
                if cluster.ENV_DATA.get("cluster_type").lower() == constants.HCI_CLIENT:
                    continue
                config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
                self.create_acm_brew_idms()
            config.switch_ctx(old_ctx)

        global_net = get_primary_cluster_config().ENV_DATA.get("enable_globalnet", True)
        # W/A for ROKS deployment
        roks_deployment = (
            config.ENV_DATA["platform"] == constants.IBMCLOUD_PLATFORM
            and config.ENV_DATA["deployment_type"] == "managed"
        )
        if roks_deployment:
            # get all cluster configs except acm
            non_acm_clusters = [
                cluster
                for cluster in config.clusters
                if not cluster.MULTICLUSTER.get("acm_cluster")
            ]
            for cluster in non_acm_clusters:
                with config.RunWithConfigContext(
                    cluster.MULTICLUSTER["multicluster_index"]
                ):
                    run_cmd(
                        f'oc patch --kubeconfig {cluster.RUN["kubeconfig"]} '
                        '--type=json Installation default -p \'[{"op": "replace", "path":'
                        ' "/spec/calicoNetwork/ipPools/0/encapsulation", "value": "IPIP"}]\''
                    )
        if (
            is_ibm_platform()
            and get_primary_cluster_config().ENV_DATA.get("deployment_type")
            == constants.IPI_DEPL_TYPE
        ) or config.DEPLOYMENT.get("submariner_cli_deployment"):

            for cluster in get_non_acm_cluster_config():
                config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
                if (
                    config.ENV_DATA.get("platform") == constants.IBMCLOUD_PLATFORM
                    and config.ENV_DATA.get("deployment_type")
                    == constants.IPI_DEPL_TYPE
                ):
                    logger.info("Logging into IBMCLOUD CLI")
                    login()
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
                resp = requests.get(constants.SUBMARINER_DOWNLOAD_URL, timeout=120)
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

    @retry((SubctlDownloadFailed, CommandFailed))
    def download_downstream_binary(self, download_url=constants.SUBCTL_DOWNSTREAM_URL):
        """
        Download downstream subctl binary from container image.
        Extracts the binary directly from /usr/local/bin/subctl in the image.

        Raises:
            UnsupportedPlatformError : If current platform has no supported subctl binary
            SubctlDownloadFailed : If the binary extraction fails
        """

        subctl_ver = config.ENV_DATA["subctl_version"]
        pull_secret_path = os.path.join(constants.DATA_DIR, "pull-secret")
        arch = platform.machine()
        if arch == "x86_64":
            binary_pltfrm = "amd64"
        elif arch == "arm64":
            binary_pltfrm = "arm64"
        else:
            raise UnsupportedPlatformError(
                f"Not a supported architecture for subctl binary: {arch}"
            )
        target_dir = config.RUN["bin_dir"]
        os.makedirs(target_dir, exist_ok=True)
        cmd = (
            f"oc image extract --filter-by-os linux/{binary_pltfrm} --registry-config "
            f"{pull_secret_path} {download_url}{subctl_ver} "
            f"--path=/usr/local/bin/subctl:{target_dir} --confirm"
        )
        try:
            run_cmd(cmd)
        except CommandFailed as e:
            logger.error(f"Failed to extract subctl binary: {e}")
            raise SubctlDownloadFailed(
                f"Failed to extract subctl from {download_url}{subctl_ver}"
            )
        subctl_path = os.path.join(target_dir, "subctl")
        if not os.path.isfile(subctl_path):
            raise SubctlDownloadFailed(
                f"subctl binary not found at {subctl_path} after extraction"
            )
        os.chmod(subctl_path, 0o744)
        os.environ["PATH"] = os.environ["PATH"] + ":" + os.path.abspath(target_dir)
        logger.info(f"subctl binary downloaded to {subctl_path}")

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
