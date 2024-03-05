import logging
import os
import tempfile
import time
from datetime import datetime

from ocs_ci.deployment.deployment import Deployment
from ocs_ci.deployment.helpers.icsp_parser import parse_ICSP_json_to_mirrors_file
from ocs_ci.framework import config
from ocs_ci.helpers.helpers import download_pull_secret
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_in_statuses_concurrently
from ocs_ci.ocs.version import get_ocp_version
from ocs_ci.utility import templating
from ocs_ci.utility.utils import exec_cmd, TimeoutSampler

"""
This module contains the base class for HyperShift hosted cluster management.
Main tasks include:
- Downloading hcp binary
- Creating HyperShift hosted cluster
- Destroying HyperShift hosted cluster

"""


logger = logging.getLogger(__name__)


class HyperShiftBase(Deployment):
    """
    Class to handle HyperShift hosted cluster management
    """

    def __init__(self):
        super().__init__()
        bin_dir_rel_path = os.path.expanduser(config.RUN["bin_dir"])
        self.bin_dir = os.path.abspath(bin_dir_rel_path)
        self.hcp_binary_path = os.path.join(self.bin_dir, "hcp")
        # ocp instance for running oc commands
        self.ocp = OCP()
        self.icsp_mirrors_path = tempfile.NamedTemporaryFile(
            mode="w+", prefix="icsp_mirrors-", delete=False
        ).name

    def download_hcp_binary(self):
        """
        Download hcp binary to bin_dir

        """
        if os.path.isfile(self.hcp_binary_path):
            logger.info(
                f"hcp binary already exists {self.hcp_binary_path}, skipping download."
            )
            return

        hcp_version = config.ENV_DATA["hcp_version"]

        logger.info(
            f"Downloading hcp archive file from quay.io, version: {hcp_version}"
        )

        exec_cmd(
            f"podman create --authfile {os.path.join(constants.DATA_DIR, 'pull-secret')} --name hcp "
            f"quay.io/hypershift/hypershift-operator:{hcp_version}",
        )
        logger.info("wait for 20 seconds to download the hcp binary file")
        # I was unable to wait until the file is downloaded in subprocess and decided not to invest in
        # finding the solution and adjust exec_cmd. This 20 sec is a workaround.
        time.sleep(20)
        exec_cmd(f"podman cp hcp:/bin/hcp {self.bin_dir}")
        # check hcp binary is downloaded
        if os.path.isfile(self.hcp_binary_path):
            logger.info(
                f"hcp binary downloaded successfully to path:{self.hcp_binary_path}"
            )
            os.chmod(self.hcp_binary_path, 0o755)
        else:
            raise CommandFailed(
                f"hcp binary download failed to path:{self.hcp_binary_path}"
            )

    def update_hcp_binary(self):
        """
        Update hcp binary
        """
        if os.path.isfile(self.hcp_binary_path):
            logger.info(f"Updating hcp binary {self.hcp_binary_path}")
            exec_cmd(f"rm -f {self.hcp_binary_path}")
        self.download_hcp_binary()

    def create_kubevirt_OCP_cluster(
        self,
        name: str = None,
        nodepool_replicas: int = 2,
        memory: str = "8Gi",
        cpu_cores: int = 4,
        root_volume_size: str = 40,
        ocp_version=None,
    ):
        """
        Create HyperShift hosted cluster. Default parameters have minimal requirements for the cluster.

        Args:
            name (str): Name of the cluster
            nodepool_replicas (int): Number of nodes in the cluster
            memory (str): Memory size of the cluster, minimum 12Gi
            cpu_cores (str): CPU cores of the cluster, minimum 6
            ocp_version (str): OCP version of the cluster, if not specified, will use the version from Hosting Platform
            root_volume_size (str): Root volume size of the cluster, default 40 (Gi is not required)

        Returns:
            str: Name of the hosted cluster
            bool: True if hosted cluster is verified, False otherwise
        """
        logger.debug("create_kubevirt_OCP_cluster method is called")

        self.get_ICSP_list()
        pull_secret_path = download_pull_secret()

        # If ocp_version is not provided, get the version from Hosting Platform
        if not ocp_version:
            index_image = f"{constants.QUAY_REGISTRY_SVC}:{get_ocp_version()}-x86_64"
        else:
            index_image = f"{constants.QUAY_REGISTRY_SVC}:{ocp_version}-x86_64"

        if not name:
            name = "hcp-" + datetime.utcnow().strftime("%f")

        logger.info(
            f"Creating HyperShift hosted cluster with specs: name:{name}, "
            f"nodepool_replicas:{nodepool_replicas}, memory_size:{memory}, cpu_cores:{cpu_cores}, "
            f"ocp image:'{index_image}', root_volume_size:{root_volume_size}"
        )

        create_hcp_cluster_cmd = (
            f"{self.hcp_binary_path} create cluster kubevirt "
            f"--name {name} "
            f"--release-image {index_image} "
            f"--node-pool-replicas {nodepool_replicas} "
            f"--memory {memory} "
            f"--cores {cpu_cores} "
            f"--root-volume-size {root_volume_size} "
            f"--pull-secret {pull_secret_path} "
            f"--image-content-sources {self.icsp_mirrors_path}"
        )

        logger.info("Creating HyperShift hosted cluster")
        exec_cmd(create_hcp_cluster_cmd)

        passed = self.verify_hosted_ocp_cluster_from_provider(name)

        logger.info(f"HyperShift hosted cluster {name} passed validation: {passed}")
        return name, passed

    def verify_hosted_ocp_cluster_from_provider(self, name):
        """
        Verify HyperShift hosted cluster from provider
        :param name: hosted OCP cluster name
        :return: True if hosted OCP cluster is verified, False otherwise
        """

        timeout_pods_wait_min = 20
        timeout_hosted_cluster_completed_min = 30
        timeout_worker_nodes_ready_min = 40

        namespace = f"clusters-{name}"
        logger.info(
            f"Waiting for HyperShift hosted cluster pods to be ready in the namespace: {namespace}"
        )

        app_selectors_to_resource_count_list = [
            {"app=capi-provider-controller-manager": 1},
            {"app=catalog-operator": 1},
            {"app=certified-operators-catalog": 1},
            {"app=cluster-api": 1},
            {"app=redhat-operators-catalog": 1},
        ]

        validation_passed = True

        if not wait_for_pods_to_be_in_statuses_concurrently(
            app_selectors_to_resource_count_list,
            namespace,
            timeout_pods_wait_min * 60,
        ):
            logger.error(f"HyperShift hosted cluster '{name}' pods are not running")
            validation_passed = False
        else:
            logger.info("HyperShift hosted cluster pods are running")

        if not self.wait_hosted_cluster_completed(
            name, timeout=timeout_hosted_cluster_completed_min * 60
        ):
            logger.error(
                f"HyperShift hosted cluster '{name}' creation is not Completed"
            )
            validation_passed = False
        else:
            logger.info("HyperShift hosted cluster create is OK")

        if not self.wait_for_worker_nodes_to_be_ready(
            name, timeout=timeout_worker_nodes_ready_min * 60
        ):
            logger.error(
                f"HyperShift hosted cluster '{name}' worker nodes are not ready"
            )
            validation_passed = False
        else:
            logger.info("HyperShift hosted cluster worker nodes are ready")
        return validation_passed

    def get_current_nodepool_size(self, name):
        """
        Get existing nodepool of HyperShift hosted cluster
        :param name: name of the cluster
        :return: int number of nodes in the nodepool
        """

        logger.info(f"Getting existing nodepool of HyperShift hosted cluster {name}")
        cmd = (
            f"oc get --namespace clusters nodepools | awk '$1==\"{name}\" {{print $4}}'"
        )
        return exec_cmd(cmd, shell=True).stdout.decode("utf-8").strip()

    def worker_nodes_deployed(self, name):
        """
        Check if worker nodes are deployed for HyperShift hosted cluster
        :param name: name of the cluster
        :return: True if worker nodes are deployed, False otherwise
        """
        logger.info(f"Checking if worker nodes are deployed for cluster {name}")
        return self.get_current_nodepool_size(name) == self.get_desired_nodepool_size(
            name
        )

    def get_desired_nodepool_size(self, name):
        """
        Get desired nodepool of HyperShift hosted cluster
        :param name: name of the cluster
        :return: int number of nodes in the nodepool
        """

        logger.info(f"Getting desired nodepool of HyperShift hosted cluster {name}")
        cmd = (
            f"oc get --namespace clusters nodepools | awk '$1==\"{name}\" {{print $3}}'"
        )
        return exec_cmd(cmd, shell=True).stdout.decode("utf-8").strip()

    def wait_hosted_cluster_completed(self, name, timeout=3600):
        """
        Wait for HyperShift hosted cluster creation to complete
        :param name: name of the cluster
        :param timeout: timeout in seconds
        :return: True if cluster creation completed, False otherwise
        """
        logger.info(f"Verifying HyperShift hosted cluster {name} creation is Completed")
        for sample in TimeoutSampler(
            timeout=timeout,
            sleep=60,
            func=self.get_hosted_cluster_progress,
            name=name,
        ):
            if sample == "Completed":
                return True

    def wait_for_worker_nodes_to_be_ready(self, name, timeout=2400):
        """
        Wait for worker nodes to be ready for HyperShift hosted cluster
        :param name: name of the cluster
        :param timeout: timeout in seconds
        :return: True if worker nodes are ready, False otherwise
        """

        logger.info(
            f"Verifying worker nodes to be ready for HyperShift hosted cluster {name}. "
            f"Max wait time: {timeout} sec "
        )
        for sample in TimeoutSampler(
            timeout=timeout,
            sleep=60,
            func=self.worker_nodes_deployed,
            name=name,
        ):
            if sample:
                return True

    def get_hosted_cluster_kubeconfig_name(self, name):
        """
        Get HyperShift hosted cluster kubeconfig, for example 'hcp414-bm2-a-admin-kubeconfig'
        :param name: name of the cluster
        :return: hosted cluster kubeconfig name
        """
        logger.info(f"Getting kubeconfig for HyperShift hosted cluster {name}")
        cmd = f"oc get --namespace clusters hostedclusters | awk '$1==\"{name}\" {{print $3}}'"
        return exec_cmd(cmd, shell=True).stdout.decode("utf-8").strip()

    def get_hosted_cluster_names(self):
        """
        Get HyperShift hosted cluster names
        :return: list of hosted cluster names
        """
        logger.info("Getting HyperShift hosted cluster names")
        cmd = "oc get --namespace clusters hostedclusters -o custom-columns=NAME:.metadata.name --no-headers"
        return exec_cmd(cmd, shell=True).stdout.decode("utf-8").strip().split()

    def download_hosted_cluster_kubeconfig(
        self, name: str, kubeconfig_path: str = None
    ):
        """
        Download HyperShift hosted cluster kubeconfig
        :param name: name of the cluster
        :param kubeconfig_path: path to download kubeconfig
        :return: True if kubeconfig downloaded successfully, False otherwise
        """
        if not kubeconfig_path:
            kubeconfig_path = f"~/clusters/{name}/openshift-cluster-dir/auth"

        kubeadmin_path_abs = os.path.expanduser(kubeconfig_path)
        os.makedirs(os.path.dirname(kubeadmin_path_abs), exist_ok=True)

        if os.path.isfile(f"{kubeadmin_path_abs}/kubeconfig"):
            logger.info(
                f"Kubeconfig for HyperShift hosted cluster {name} already exists at {kubeadmin_path_abs}, removing it"
            )
            exec_cmd(f"rm -f {kubeadmin_path_abs}/kubeconfig")

        logger.info(
            f"Downloading kubeconfig for HyperShift hosted cluster {name} to {kubeadmin_path_abs}"
        )
        resp = exec_cmd(
            f"{self.hcp_binary_path} create kubeconfig --name {name} > {kubeadmin_path_abs}/kubeconfig",
            shell=True,
        )
        if resp.returncode != 0:
            logger.error(
                f"Failed to download kubeconfig for HyperShift hosted cluster {name}\n{resp.stderr.decode('utf-8')}"
            )
            return False
        if (
            os.path.isfile(f"{kubeadmin_path_abs}/kubeconfig")
            and os.stat(f"{kubeadmin_path_abs}/kubeconfig").st_size > 0
        ):
            return True

    def get_hosted_cluster_progress(self, name):
        """
        Get HyperShift hosted cluster creation progress
        :param name: name of the cluster
        :return: progress status; 'Completed' is expected in most cases
        """
        cmd = f"oc get --namespace clusters hostedclusters | awk '$1==\"{name}\" {{print $4}}'"
        return exec_cmd(cmd, shell=True).stdout.decode("utf-8").strip()

    def get_ICSP_list(self):
        """
        Get list of ICSP clusters

        """
        if (
            not os.path.getsize(self.icsp_mirrors_path)
            and not os.path.getsize(self.icsp_mirrors_path) == 0
        ):
            logger.info(
                f"ICSP mirrors list already exists at '{self.icsp_mirrors_path}'"
            )
            return
        logger.info(f"Saving ICSP mirrors list to '{self.icsp_mirrors_path}'")

        icsp_json = self.ocp.exec_oc_cmd("get imagecontentsourcepolicy -o json")
        parse_ICSP_json_to_mirrors_file(icsp_json, self.icsp_mirrors_path)

    def update_ICSP_list(self):
        """
        Update ICSP list
        """
        if os.path.isfile(self.icsp_mirrors_path):
            logger.info(f"Updating ICSP list from '{self.icsp_mirrors_path}'")
            exec_cmd(f"rm -f {self.icsp_mirrors_path}")
        else:
            self.icsp_mirrors_path = tempfile.NamedTemporaryFile(
                mode="w+", prefix="icsp_mirrors-", delete=False
            ).name
        self.get_ICSP_list()

    def destroy_kubevirt_cluster(self, name):
        """
        Destroy HyperShift hosted cluster

        Args:
            name (str): Name of the cluster
        """
        destroy_timeout_min = 10
        logger.info(
            f"Destroying HyperShift hosted cluster {name}. Timeout: {destroy_timeout_min} min"
        )
        exec_cmd(f"{self.hcp_binary_path} destroy cluster --name {name}")

        logger.info("Waiting for HyperShift hosted cluster to be deleted...")
        for sample in TimeoutSampler(
            timeout=destroy_timeout_min * 60,
            sleep=60,
            func=self.get_hosted_cluster_progress,
            name=name,
        ):
            if sample == "":
                return True

    def apply_network_policy(self, namespace):
        """
        Apply network policy to the namespace. Should be done before every hosted cluster creation
        :param namespace: namespace to apply network policy
        :return:
        """

        logger.info(f"Applying network policy to namespace {namespace}")

        network_policy_data = templating.load_yaml(
            constants.NETWORK_POLICY_PROVIDER_TO_CLIENT_TEMPLATE
        )
        network_policy_data["metadata"]["namespace"] = namespace
        network_policy_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="network_policy_provider_mode", delete=False
        )
        templating.dump_data_to_temp_yaml(network_policy_data, network_policy_file.name)
        exec_cmd(f"oc create -f {network_policy_file.name}")

        ocp = OCP(kind=constants.NETWORK_POLICY, namespace=namespace)
        network_policy_created = ocp.check_resource_existence(
            resource_name=f"network-policy-{namespace}", should_exist=True, timeout=60
        )
        logger.info(
            f"Network policy in namespace {namespace} created: {network_policy_created}"
        )
        return network_policy_created
