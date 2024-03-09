import logging
import os
import tempfile

from ocs_ci.deployment.cnv import CNVInstaller
from ocs_ci.deployment.helpers.hypershift_base import (
    HyperShiftBase,
    get_hosted_cluster_names,
)
from ocs_ci.deployment.metallb import MetalLBInstaller
from ocs_ci.framework import config
from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.constants import HCI_PROVIDER_CLIENT_PLATFORMS
from ocs_ci.ocs.exceptions import ProviderModeNotFoundException
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility import templating
from ocs_ci.utility.utils import exec_cmd

logger = logging.getLogger(__name__)


class DeployClients:
    def __init__(self):
        pass

    def do_deploy(self):
        hypershiftHostedOCP = HypershiftHostedOCP()
        hypershiftHostedOCP.deploy_multiple_ocp_clusters()

        logger.info(
            "Ensure clusters were deployed successfully, wait for them to be ready"
        )
        hypershiftHostedOCP.verify_multiple_hosted_ocp_cluster_from_provider()

        logger.info("Download kubeconfig for all clusters")
        hypershiftHostedOCP.download_hosted_cluster_kubeconfig_multiple()


class HypershiftHostedOCP(HyperShiftBase, MetalLBInstaller, CNVInstaller):
    def __init__(self):
        HyperShiftBase.__init__(self)
        MetalLBInstaller.__init__(self)
        CNVInstaller.__init__(self)

    def deploy_ocp(
        self,
        deploy_cnv=True,
        deploy_acm_hub=True,
        deploy_metallb=True,
        download_hcp_binary=True,
    ):
        """
        Deploy hosted OCP cluster on provisioned Provider platform
        :param deploy_cnv: (bool) Deploy CNV
        :param deploy_acm_hub: (bool) Deploy ACM Hub
        :param deploy_metallb: (bool) Deploy MetalLB
        :param download_hcp_binary: (bool) Download HCP binary

        :returns:
            str: Name of the hosted cluster
        """
        if (
            not config.default_cluster_ctx.ENV_DATA["platform"].lower()
            in HCI_PROVIDER_CLIENT_PLATFORMS
        ):
            raise ProviderModeNotFoundException()

        initial_default_sc = helpers.get_default_storage_class()
        logger.info(f"Initial default StorageClass: {initial_default_sc}")

        if not initial_default_sc == constants.CEPHBLOCKPOOL_SC:
            logger.info(
                f"Changing the default StorageClass to {constants.CEPHBLOCKPOOL_SC}"
            )
            helpers.change_default_storageclass(scname=constants.CEPHBLOCKPOOL_SC)

        if deploy_cnv:
            self.deploy_cnv(check_cnv_ready=True)
        if deploy_acm_hub:
            self.deploy_acm_hub()
        if deploy_metallb:
            self.deploy_lb()
        if download_hcp_binary:
            self.download_hcp_binary()

        cluster_names_desired = []
        if "cluster_paths" in config.default_cluster_ctx.ENV_DATA:
            cluster_paths = config.default_cluster_ctx.ENV_DATA["cluster_paths"]
            for path in cluster_paths:
                if path.find("clusters/") == -1:
                    raise ValueError(
                        "cluster_paths must contain a path with 'clusters/', "
                        "similar to '~/clusters/hcp-739881/openshift-cluster-dir'"
                    )

                start_index = path.find("clusters/") + len("clusters/")

                end_index = path.find("/", start_index)
                cluster_name = path[start_index:end_index]
                cluster_names_desired.append(cluster_name)

            cluster_names_existing = get_hosted_cluster_names()
            cluster_names_desired_left = [
                cluster_name
                for cluster_name in cluster_names_desired
                if cluster_name not in cluster_names_existing
            ]
            if cluster_names_desired_left:
                logger.info(
                    f"Creating hosted OCP cluster: {cluster_names_desired_left[-1]}"
                )
                return self.create_kubevirt_OCP_cluster(
                    name=cluster_names_desired_left[-1]
                )
            else:
                logger.info("All desired hosted OCP clusters already exist")
                return None

        logger.info(
            "\n--- No cluster_paths set to ENV_DATA. Creating hosted OCP cluster with random name ---\n"
        )
        return self.create_kubevirt_OCP_cluster()

    def deploy_multiple_ocp_clusters(
        self,
    ):
        """
        Deploy multiple hosted OCP clusters on provisioned Provider platform
        """
        # we need to ensure that all dependencies are installed so for the first cluster we will install all operators
        # and finish the rest preparation steps. For the rest of the clusters we will only deploy OCP with hcp.

        if "cluster_names" in config.default_cluster_ctx.ENV_DATA:
            number_of_clusters_to_deploy = len(
                config.default_cluster_ctx.ENV_DATA["cluster_names"]
            )
        else:
            cluster_paths = config.default_cluster_ctx.ENV_DATA["cluster_paths"]
            number_of_clusters_to_deploy = len(cluster_paths)

        logger.info(f"Deploying {number_of_clusters_to_deploy} clusters")

        deployment_states = []
        for _ in range(number_of_clusters_to_deploy):

            deployment_states.append(
                self.deploy_ocp(
                    deploy_cnv=False,
                    deploy_acm_hub=False,
                    deploy_metallb=False,
                    download_hcp_binary=False,
                )
            )

        logger.info(f"All deployment jobs have finished: {deployment_states}")


class HostedODF:
    def __init__(self, name: str):
        self.namespace_client = constants.OPENSHIFT_STORAGE_CLIENT_NAMESPACE
        self.timeout_check_resources_existence = 6
        self.name = name
        self.cluster_kubeconfig = os.path.expanduser(
            f"{constants.auth_path_pattern.format(name)}/kubeconfig"
        )

    def exec_oc_cmd(self, cmd, timeout=300, ignore_error=False, **kwargs):
        """
        Execute command on the system
        Args:
            cmd (str): Command to execute
            timeout (int): Timeout for the command
            ignore_error (bool): True for ignoring error
            **kwargs: Additional arguments for exec_cmd

        Returns:
            tuple: (retcode, stdout, stderr)

        """
        cmd = "oc --kubeconfig {} {}".format(self.cluster_kubeconfig, cmd)
        return helpers.exec_cmd(
            cmd=cmd, timeout=timeout, ignore_error=ignore_error, **kwargs
        )

    def create_ns(self):
        logger.info(f"Creating namespace {self.namespace_client} for storage client")

        ocp = OCP(
            kind="namespace",
            resource_name=self.namespace_client,
            cluster_kubeconfig=self.cluster_kubeconfig,
        )

        if ocp.check_resource_existence(
            timeout=self.timeout_check_resources_existence,
            resource_name=self.namespace_client,
            should_exist=True,
        ):
            logger.info(f"Namespace {self.namespace_client} already exists")
            return

        self.exec_oc_cmd(f"create namespace {self.namespace_client}")

    def apply_network_policy(self):
        """
        Apply network policy to the client namespace. Network policy is created always on Provider side.

        Returns:
            bool: True if network policy is created, False otherwise
        """
        namespace = f"clusters-{self.name}"
        logger.info(f"Applying network policy to the namespace {namespace}")

        network_policy_data = templating.load_yaml(
            constants.NETWORK_POLICY_PROVIDER_TO_CLIENT_TEMPLATE
        )
        network_policy_data["metadata"]["namespace"] = f"clusters-{self.name}"

        if self.network_policy_created(namespace=namespace):
            logger.info(f"Network policy {namespace} already exists")
            return

        network_policy_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="network_policy", delete=False
        )
        templating.dump_data_to_temp_yaml(network_policy_data, network_policy_file.name)

        exec_cmd(f"oc apply -f {network_policy_file.name}", timeout=120)

        return self.network_policy_created(namespace=namespace)

    def do_deploy(self):

        logger.info(f"Deploying ODF client on hosted OCP cluster '{self.name}'")
        self.apply_network_policy()

        self.create_ns()

    def network_policy_created(self, namespace):
        """
        Check if the network policy is created
        :return:
        """
        ocp = OCP(kind=constants.NETWORK_POLICY, namespace=namespace)
        if ocp.check_resource_existence(
            timeout=self.timeout_check_resources_existence,
            resource_name="openshift-storage-egress",
            should_exist=True,
        ):
            return
