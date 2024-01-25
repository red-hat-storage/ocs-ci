import logging
import os
import tempfile

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.version import get_ocp_version
from ocs_ci.utility.utils import exec_cmd
from ocs_ci.deployment.ocp import OCPDeployment as BaseOCPDeployment

logger = logging.getLogger(__name__)


class HyperShift:
    """
    Class to handle HyperShift hosted cluster management
    """

    def __init__(self):
        self.hcp_binary_path = None
        self.base_deployment = BaseOCPDeployment()

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
            else:
                raise CommandFailed(
                    f"hcp binary download failed to path:{self.hcp_binary_path}"
                )

    def create_kubevirt_cluster(
        self,
        name,
        nodepool_replicas,
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
        logger.info(
            f"Creating HyperShift hosted cluster with specs: name:{name}, "
            f"nodepool_replicas:{nodepool_replicas}, memory_size:{memory}, cpu_cores:{cpu_cores}, "
            f"ocp_version:{ocp_version}, root_volume_size:{root_volume_size}"
        )
        pull_secret_path = os.path.join(constants.DATA_DIR, "pull-secret")
        icsp_file_path = self.get_ICSP_list()
        index_image = f"{constants.REGISTRY_SVC}:{get_ocp_version()}"

        exec_cmd(
            f"{self.hcp_binary_path} create cluster kubevirt "
            f"--release-image {index_image} "
            f"--name {name} "
            f"--nodepool-replicas {nodepool_replicas} "
            f"--memory {memory} "
            f"--cores {cpu_cores} "
            f"--root-volume-size {root_volume_size} "
            f"--pull-secret {pull_secret_path}"
            f"--image-content-sources {icsp_file_path}"
        )
        logger.info("HyperShift hosted cluster created successfully")

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

        ocp = OCP()
        ocp.exec_oc_cmd(
            "get imagecontentsourcepolicy -o json | jq -r '.items[].spec.repositoryDigestMirrors[] | "
            f"- mirrors:\n  - \\(.mirrors[0])\n  source: \\(.source)'> {output_file}"
        )
        return output_file
