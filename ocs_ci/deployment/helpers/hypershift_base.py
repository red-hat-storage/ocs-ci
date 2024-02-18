import logging
import os
import tempfile
from datetime import datetime

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.version import get_ocp_version
from ocs_ci.utility.utils import exec_cmd, TimeoutSampler

logger = logging.getLogger(__name__)


class HyperShiftBase:
    """
    Class to handle HyperShift hosted cluster management
    """

    def __init__(self):
        self.hcp_binary_path = None
        # ocp instance for running oc commands
        self.ocp = OCP()

    def download_hcp_binary(self):
        """
        Download hcp binary to bin_dir

        """
        # Prepare bin directory for hcp
        bin_dir_rel_path = os.path.expanduser(config.RUN["bin_dir"])
        bin_dir = os.path.abspath(bin_dir_rel_path)
        self.hcp_binary_path = os.path.join(bin_dir, "hcp")
        if os.path.isfile(self.hcp_binary_path):
            logger.info(
                f"hcp binary already exists {self.hcp_binary_path}, skipping download."
            )
        else:
            endpoint_url = "quay.io"
            exec_cmd(
                f"podman login {endpoint_url} -u {constants.QUAY_SUPERUSER} -p {constants.QUAY_PW} --tls-verify=false"
            )
            hcp_version = config.ENV_DATA["hcp_version"]

            logger.info(
                f"Downloading hcp archive file from quay.io, version: {hcp_version}"
            )
            bin_dir_rel_path = os.path.expanduser(bin_dir or config.RUN["bin_dir"])
            bin_dir = os.path.abspath(bin_dir_rel_path)
            exec_cmd(
                f"podman create --name hcp quay.io/hypershift/hypershift-operator:{hcp_version} "
                f"&& podman cp hcp:/bin/hcp {bin_dir}"
            )
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

    def create_kubevirt_OCP_cluster(
        self,
        name: str = None,
        nodepool_replicas: int = 2,
        memory: str = "12Gi",
        cpu_cores: int = 6,
        root_volume_size: str = "12Gi",
        ocp_version=None,
    ):
        """
        Create HyperShift hosted cluster

        Args:
            name (str): Name of the cluster
            nodepool_replicas (int): Number of nodes in the cluster
            memory (str): Memory size of the cluster, minimum 12Gi
            cpu_cores (str): CPU cores of the cluster, minimum 6
            ocp_version (str): OCP version of the cluster, if not specified, will use the version from Hosting Platform
            root_volume_size (str): Root volume size of the cluster, default 40 Gi (Gi is not required)
        """
        logger.debug("create_kubevirt_OCP_cluster method is called")

        pull_secret_path = os.path.join(constants.DATA_DIR, "pull-secret")
        icsp_file_path = self.get_ICSP_list()
        logger.debug(f"ICSP file path: {icsp_file_path}")

        # If ocp_version is not provided, get the version from Hosting Platform
        if not ocp_version:
            index_image = f"{constants.REGISTRY_SVC}:{get_ocp_version()}"
        else:
            index_image = f"{constants.REGISTRY_SVC}:{ocp_version}"

        if not name:
            name = "hcp-".join(datetime.utcnow().strftime("%Y%m%d%H%M%S"))

        logger.info(
            f"Creating HyperShift hosted cluster with specs: name:{name}, "
            f"nodepool_replicas:{nodepool_replicas}, memory_size:{memory}, cpu_cores:{cpu_cores}, "
            f"ocp image:'{index_image}', root_volume_size:{root_volume_size}"
        )

        create_hcp_cluster_cmd = (
            f"{self.hcp_binary_path} create cluster kubevirt "
            f"--name {name} "
            f"--release-image {index_image} "
            f"--nodepool-replicas {nodepool_replicas} "
            f"--memory {memory} "
            f"--cores {cpu_cores} "
            f"--root-volume-size {root_volume_size} "
            f"--pull-secret {pull_secret_path} "
            f"--image-content-sources {icsp_file_path}"
        )

        logger.info(
            f"Creating HyperShift hosted cluster with command: {create_hcp_cluster_cmd}"
        )
        exec_cmd(create_hcp_cluster_cmd)

        namespace = f"clusters-{name}"

        logger.info("Waiting for HyperShift hosted cluster pods to be ready...")
        app_selectors_to_resource_count = [
            {"app=capi-provider-controller-manager": 1},
            {"app=catalog-operator": 2},
            {"app=certified-operators-catalog": 1},
            {"app=cluster-api": 1},
            {"app=redhat-operators-catalog": 1},
        ]

        pod = OCP(kind=constants.POD, namespace=namespace)

        for app_selector, resource_count in app_selectors_to_resource_count:
            assert pod.wait_for_resource(
                condition=constants.STATUS_RUNNING,
                selector=app_selector,
                resource_count=resource_count,
                timeout=300,
            ), f"pods with label {app_selector} are not in running state"

        self.wait_hosted_cluster_completed(name)

        logger.info(
            "HyperShift hosted cluster create request completed, progressing with node-pool creation"
        )

        self.wait_for_worker_nodes_to_be_ready(name)

        logger.info("HyperShift hosted cluster node-pool creation completed")

    def get_current_nodepool_size(self, name):
        """
        Get existing nodepool of HyperShift hosted cluster
        :param name: name of the cluster
        :return: int number of nodes in the nodepool
        """

        logger.info(f"Getting existing nodepool of HyperShift hosted cluster {name}")
        return self.ocp.exec_oc_cmd(
            f"get --namespace clusters nodepools | awk '$1==\"{name}\" {{print $4}}'"
        )

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
        return self.ocp.exec_oc_cmd(
            f"get --namespace clusters nodepools | awk '$1==\"{name}\" {{print $3}}'"
        )

    def wait_hosted_cluster_completed(self, name):
        """
        Wait for HyperShift hosted cluster creation to complete
        :param name:
        :return: True if cluster creation completed, False otherwise
        """
        logger.info(
            f"Waiting for HyperShift hosted cluster {name} creation to complete"
        )
        for sample in TimeoutSampler(
            timeout=3600,
            sleep=60,
            func=self.get_hosted_cluster_progress,
            name=name,
        ):
            if sample == "Completed":
                return True

    def wait_for_worker_nodes_to_be_ready(self, name):
        """
        Wait for worker nodes to be ready for HyperShift hosted cluster
        :param name: name of the cluster
        :return: True if worker nodes are ready, False otherwise
        """

        wait_timeout_min = 40
        logger.info(
            f"Waiting for worker nodes to be ready for HyperShift hosted cluster {name}. "
            f"Max wait time: {wait_timeout_min} min "
        )
        for sample in TimeoutSampler(
            timeout=wait_timeout_min * 60,
            sleep=60,
            func=self.worker_nodes_deployed,
            name=name,
        ):
            if sample:
                return True

    def get_hosted_cluster_kubeconfig_name(self, name):
        """
        Get HyperShift hosted cluster kubeconfig
        :param name: name of the cluster
        :return: hosted cluster kubeconfig name
        """
        logger.info(f"Getting kubeconfig for HyperShift hosted cluster {name}")
        return self.ocp.exec_oc_cmd(
            f"get --namespace clusters hostedclusters | awk '$1==\"{name}\" {{print $3}}'"
        )

    def download_hosted_cluster_kubeconfig(self, name, kubeconfig_path):
        """
        Download HyperShift hosted cluster kubeconfig
        :param name: name of the cluster
        :param kubeconfig_path: path to download kubeconfig
        :return: True if kubeconfig downloaded successfully, False otherwise
        """
        logger.info(
            f"Downloading kubeconfig for HyperShift hosted cluster {name} to {kubeconfig_path}"
        )
        exec_cmd(
            f"{self.hcp_binary_path} create kubeconfig --name {name} > {kubeconfig_path}"
        )
        if os.path.isfile(kubeconfig_path) and os.stat(kubeconfig_path).st_size > 0:
            return True

    def get_hosted_cluster_progress(self, name):
        """
        Get HyperShift hosted cluster creation progress
        :param name: name of the cluster
        :return: progress status; 'Completed' is expected in most cases
        """
        return self.ocp.exec_oc_cmd(
            f"get --namespace clusters hostedclusters | awk '$1==\"{name}\" {{print $4}}'"
        )

    def get_ICSP_list(self, output_file: str = None):
        """
        Get list of ICSP clusters

        Args:
            output_file (str): full Path to the file where the list will be saved, if not will be saved in tmp dir

        Returns:
            str: Path to the file where the list is saved
        """
        logger.info("Getting list of ICSP clusters")

        if not os.path.isfile(output_file):
            output_file = tempfile.NamedTemporaryFile(
                mode="w+", prefix="icsp_mirrors", delete=False
            ).name

        self.ocp.exec_oc_cmd(
            "get imagecontentsourcepolicy -o json | jq -r '.items[].spec.repositoryDigestMirrors[] | "
            f"- mirrors:\n  - \\(.mirrors[0])\n  source: \\(.source)'> {output_file}"
        )
        return output_file

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
