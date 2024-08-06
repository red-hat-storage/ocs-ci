import logging
import os
import shutil
import tempfile
import time
from datetime import datetime

from ocs_ci.deployment.helpers.icsp_parser import parse_ICSP_json_to_mirrors_file
from ocs_ci.deployment.ocp import download_pull_secret
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_in_statuses_concurrently
from ocs_ci.ocs.version import get_ocp_version
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import exec_cmd, TimeoutSampler

"""
This module contains the base class for HyperShift hosted cluster management.
Main tasks include:
- Downloading hcp binary
- Creating HyperShift hosted cluster
- Destroying HyperShift hosted cluster

"""


logger = logging.getLogger(__name__)


@retry((CommandFailed, TimeoutError), tries=3, delay=5, backoff=1)
def get_hosted_cluster_names():
    """
    Get HyperShift hosted cluster names
    Returns:
        list: the list of hosted cluster names
    """
    logger.info("Getting HyperShift hosted cluster names")
    cmd = "oc get --namespace clusters hostedclusters -o custom-columns=NAME:.metadata.name --no-headers"
    return exec_cmd(cmd, shell=True).stdout.decode("utf-8").strip().split()


def kubeconfig_exists_decorator(func):
    """
    Decorator to check if the kubeconfig exists before executing the decorated method
    :param func: func to decorate; should be used only for methods of class having 'cluster_kubeconfig' attribute !
    :return: wrapper
    """

    def wrapper(self, *args, **kwargs):
        if not os.path.exists(self.cluster_kubeconfig):
            error_out = f"no kubeconfig found for cluster {self.name}."
            if hasattr(self, "cluster_kubeconfig"):
                error_out += f" Searched location is {str(self.cluster_kubeconfig)}."
            else:
                error_out += f" 'cluster_kubeconfig' instance attribute is not set within class {type(self)}."
            logger.error(error_out)
            return  # Skip executing the decorated method
        return func(self, *args, **kwargs)

    return wrapper


class HyperShiftBase:
    """
    Class to handle HyperShift hosted cluster management
    """

    def __init__(self):
        super().__init__()
        bin_dir_rel_path = os.path.expanduser(config.RUN["bin_dir"])
        self.bin_dir = os.path.abspath(bin_dir_rel_path)
        self.hcp_binary_path = os.path.join(self.bin_dir, "hcp")
        self.hypershift_binary_path = os.path.join(self.bin_dir, "hypershift")
        # ocp instance for running oc commands
        self.ocp = OCP()
        self.icsp_mirrors_path = tempfile.NamedTemporaryFile(
            mode="w+", prefix="icsp_mirrors-", delete=False
        ).name

    def hcp_binary_exists(self):
        """
        Check if hcp binary exists
        Returns:
            bool: True if hcp binary exists, False otherwise
        """
        return os.path.isfile(self.hcp_binary_path)

    def hypershift_binary_exists(self):
        """
        Check if hypershift binary exists
        Returns:
            bool: True if hypershift binary exists, False otherwise
        """
        return os.path.isfile(self.hypershift_binary_path)

    def install_hcp_and_hypershift_from_git(self):
        """
        Install hcp binary from git
        """
        if self.hcp_binary_exists() and self.hypershift_binary_exists():
            logger.info(
                f"hcp and hypershift binary exist {self.hcp_binary_path}, skipping download."
            )
            return

        hcp_version = config.ENV_DATA["hcp_version"]

        logger.info("Downloading hcp binary from git")

        temp_dir = tempfile.mkdtemp()

        exec_cmd(
            f"git clone --single-branch --branch release-{hcp_version} "
            f"--depth 1 {constants.HCP_REPOSITORY} {temp_dir}"
        )

        exec_cmd(f"cd {temp_dir} && make hypershift product-cli", shell=True)

        shutil.move(
            os.path.join(temp_dir, "bin", "hypershift"), self.hypershift_binary_path
        )
        shutil.move(os.path.join(temp_dir, "bin", "hcp"), self.hcp_binary_path)

        if not (self.hcp_binary_exists() and self.hypershift_binary_exists()):
            raise Exception("Failed to download hcp binary from git")

        hcp_version = exec_cmd("hcp --version").stdout.decode("utf-8").strip()
        logger.info(f"hcp binary version: {hcp_version}")

        shutil.rmtree(temp_dir)

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
        if os.path.isfile(self.hypershift_binary_path):
            logger.info(
                f"hypershift binary downloaded successfully to path:{self.hypershift_binary_path}"
            )
            os.chmod(self.hypershift_binary_path, 0o755)
        else:
            raise CommandFailed(
                f"hypershift binary download failed to path:{self.hypershift_binary_path}"
            )

    def download_hcp_binary_with_podman(self):
        """
        Download hcp binary to bin_dir
        """
        if self.hcp_binary_exists():
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
            f"{constants.HCP_REGISTRY}:{hcp_version}",
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
        if not config.ENV_DATA.get("hcp_version"):
            logger.error("hcp_version is not set in config.ENV_DATA")
            return

        self.delete_hcp_and_hypershift()
        self.install_hcp_and_hypershift_from_git()

    def delete_hcp_and_hypershift(self):
        """
        Delete hcp binary
        """
        logger.info(f"deleting hcp binary {self.hcp_binary_path}")

        try:
            os.remove(self.hcp_binary_path)
        except FileNotFoundError:
            logger.warning(f"The file {self.hcp_binary_path} does not exist.")

        logger.info(f"deleting hypershift binary {self.hypershift_binary_path}")

        try:
            os.remove(self.hypershift_binary_path)
        except FileNotFoundError:
            logger.warning(f"The file {self.hypershift_binary_path} does not exist.")

    def delete_hcp_podman_container(self):
        """
        Delete hcp podman container.
        This method will not fail if the container does not exist.
        """
        cmd = "podman ps -a --format '{{.ID}} {{.Names}}' | awk '$2 == \"hcp\" {print $1}'"
        container_id = exec_cmd(cmd, shell=True).stdout.decode("utf-8").strip()
        if container_id:
            exec_cmd(f"podman rm {container_id}")
            exec_cmd(
                f"podman rmi {constants.HCP_REGISTRY}:{config.ENV_DATA['hcp_version']}"
            )

    def create_kubevirt_ocp_cluster(
        self,
        name: str = None,
        nodepool_replicas: int = 2,
        memory: str = "12Gi",
        cpu_cores: int = 6,
        root_volume_size: str = 40,
        ocp_version=None,
        cp_availability_policy=None,
        disable_default_sources=None,
    ):
        """
        Create HyperShift hosted cluster. Default parameters have minimal requirements for the cluster.

        Args:
            name (str): Name of the cluster
            nodepool_replicas (int): Number of nodes in the cluster
            memory (str): Memory size of the cluster, minimum 12Gi
            cpu_cores (str): CPU cores of the cluster, minimum 6
            ocp_version (str): OCP version of the cluster
            root_volume_size (str): Root volume size of the cluster, default 40 (Gi is not required)
            cp_availability_policy (str): Control plane availability policy, default HighlyAvailable, if no value
            provided and argument is not used in the command the single replica mode cluster will be created
            disable_default_sources (bool): Disable default sources on hosted cluster, such as 'redhat-operators'
        Returns:
            str: Name of the hosted cluster
        """
        logger.debug("create_kubevirt_OCP_cluster method is called")

        if name in get_hosted_cluster_names():
            logger.info(f"HyperShift hosted cluster {name} already exists")
            return name

        self.save_mirrors_list_to_file()
        pull_secret_path = download_pull_secret()

        # If ocp_version is not provided, get the version from Hosting Platform
        if not ocp_version:
            provider_version = get_ocp_version()
            if "nightly" in provider_version:
                index_image = f"{constants.REGISTRY_SVC}:{provider_version}"
            else:
                index_image = f"{constants.QUAY_REGISTRY_SVC}:{provider_version}-x86_64"
        else:
            index_image = f"{constants.QUAY_REGISTRY_SVC}:{ocp_version}-x86_64"

        if not name:
            name = "hcp-" + datetime.utcnow().strftime("%f")

        logger.info(
            f"Creating HyperShift hosted cluster with specs: name:{name}, "
            f"nodepool_replicas:{nodepool_replicas}, memory_size:{memory}, cpu_cores:{cpu_cores}, "
            f"ocp image:'{index_image}', root_volume_size:{root_volume_size}, release_image:{index_image}"
        )

        create_hcp_cluster_cmd = (
            f"{self.hypershift_binary_path} create cluster kubevirt "
            f"--name {name} "
            f"--release-image {index_image} "
            f"--node-pool-replicas {nodepool_replicas} "
            f"--memory {memory} "
            f"--cores {cpu_cores} "
            f"--root-volume-size {root_volume_size} "
            f"--pull-secret {pull_secret_path} "
            f"--image-content-sources {self.icsp_mirrors_path} "
            "--annotations 'hypershift.openshift.io/skip-release-image-validation=true' "
            "--olm-catalog-placement Guest"
        )

        if (
            cp_availability_policy
            and cp_availability_policy in constants.CONTROL_PLANE_AVAILABILITY_POLICIES
        ):
            logger.error(
                f"Control plane availability policy {cp_availability_policy} is not valid. "
                f"Valid values are: {constants.CONTROL_PLANE_AVAILABILITY_POLICIES}"
            )
            create_hcp_cluster_cmd += (
                f" --control-plane-availability-policy {cp_availability_policy} "
            )

        if disable_default_sources:
            create_hcp_cluster_cmd += " --olm-disable-default-sources"

        logger.info("Creating HyperShift hosted cluster")
        exec_cmd(create_hcp_cluster_cmd)

        return name

    def verify_hosted_ocp_cluster_from_provider(self, name):
        """
        Verify HyperShift hosted cluster from provider
        Args:
            name (str): hosted OCP cluster name
        Returns:
            bool: True if hosted OCP cluster is verified, False otherwise
        """
        timeout_pods_wait_min = 40
        timeout_hosted_cluster_completed_min = 40
        timeout_worker_nodes_ready_min = 60

        namespace = f"clusters-{name}"
        logger.info(
            f"Waiting for HyperShift hosted cluster pods to be ready in the namespace: {namespace}"
        )

        app_selectors_to_resource_count_list = [
            {"app=capi-provider-controller-manager": 1},
            {"app=catalog-operator": 1},
            {"app=cluster-api": 1},
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

        logger.info(
            f"HyperShift hosted cluster {name} passed validation: {validation_passed}"
        )
        return validation_passed

    def get_current_nodepool_size(self, name):
        """
        Get existing nodepool of HyperShift hosted cluster
        Args:
            name (str): name of the cluster
        Returns:
             int: number of nodes in the nodepool
        """
        logger.info(f"Getting existing nodepool of HyperShift hosted cluster {name}")
        cmd = (
            f"oc get --namespace clusters nodepools | awk '$1==\"{name}\" {{print $4}}'"
        )
        return exec_cmd(cmd, shell=True).stdout.decode("utf-8").strip()

    def worker_nodes_deployed(self, name: str):
        """
        Check if worker nodes are deployed for HyperShift hosted cluster
        Args:
            name (str): name of the cluster
        Returns:
             bool: True if worker nodes are deployed, False otherwise
        """
        logger.info(f"Checking if worker nodes are deployed for cluster {name}")
        return self.get_current_nodepool_size(name) == self.get_desired_nodepool_size(
            name
        )

    def get_desired_nodepool_size(self, name: str):
        """
        Get desired nodepool of HyperShift hosted cluster
        Args:
            name (str): of the cluster
        Returns:
            int: number of nodes in the nodepool
        """
        logger.info(f"Getting desired nodepool of HyperShift hosted cluster {name}")
        cmd = (
            f"oc get --namespace clusters nodepools | awk '$1==\"{name}\" {{print $3}}'"
        )
        return exec_cmd(cmd, shell=True).stdout.decode("utf-8").strip()

    def wait_hosted_cluster_completed(self, name: str, timeout=3600):
        """
        Wait for HyperShift hosted cluster creation to complete
        Args:
            name: name of the cluster
            timeout: timeout in seconds
        Returns:
             bool: True if cluster creation completed, False otherwise
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

    def wait_for_worker_nodes_to_be_ready(self, name: str, timeout=2400):
        """
        Wait for worker nodes to be ready for HyperShift hosted cluster
        Args:
            name (str): name of the cluster
            timeout: timeout in seconds
        Returns:
            bool: True if worker nodes are ready, False otherwise
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

    def get_hosted_cluster_kubeconfig_name(self, name: str):
        """
        Get HyperShift hosted cluster kubeconfig, for example 'hcp414-bm2-a-admin-kubeconfig'
        Args:
            name: name of the cluster
        Returns:
            str: hosted cluster kubeconfig name
        """
        logger.info(f"Getting kubeconfig for HyperShift hosted cluster {name}")
        cmd = f"oc get --namespace clusters hostedclusters | awk '$1==\"{name}\" {{print $3}}'"
        return exec_cmd(cmd, shell=True).stdout.decode("utf-8").strip()

    def download_hosted_cluster_kubeconfig(self, name: str, hosted_cluster_path: str):
        """
        Download HyperShift hosted cluster kubeconfig
        Args:
            name (str): name of the cluster
            hosted_cluster_path (str): path to create auth_path folder and download kubeconfig there
        Returns:
            str: path to the downloaded kubeconfig, None if failed
        """

        path_abs = os.path.expanduser(hosted_cluster_path)
        auth_path = os.path.join(path_abs, "auth_path")
        os.makedirs(auth_path, exist_ok=True)
        kubeconfig_path = os.path.join(auth_path, "kubeconfig")

        if os.path.isfile(kubeconfig_path):
            logger.info(
                f"Kubeconfig file for HyperShift hosted cluster {name} already exists at {path_abs}, removing it"
            )
            os.remove(kubeconfig_path)

        # touch the file
        time.sleep(0.5)
        open(kubeconfig_path, "a").close()

        logger.info(
            f"Downloading kubeconfig for HyperShift hosted cluster {name} to {kubeconfig_path}"
        )

        try:
            resp = exec_cmd(
                f"{self.hcp_binary_path} create kubeconfig --name {name} > {kubeconfig_path}",
                shell=True,
            )
            if resp.returncode != 0:
                logger.error(
                    f"Failed to download kubeconfig for HyperShift hosted cluster {name}\n{resp.stderr.decode('utf-8')}"
                )
                return
        except Exception as e:
            logger.error(
                f"Failed to download kubeconfig for HyperShift hosted cluster {name}\n{e}"
            )
            return

        if not os.stat(kubeconfig_path).st_size > 0:
            logger.error(
                f"Failed to download kubeconfig for HyperShift hosted cluster {name}"
            )
            return
        return kubeconfig_path

    def get_hosted_cluster_progress(self, name: str):
        """
        Get HyperShift hosted cluster creation progress
        Args:
            name (str): name of the cluster
        Returns:
            str: progress status; 'Completed' is expected in most cases
        """
        cmd = f"oc get --namespace clusters hostedclusters | awk '$1==\"{name}\" {{print $4}}'"
        return exec_cmd(cmd, shell=True).stdout.decode("utf-8").strip()

    def save_mirrors_list_to_file(self):
        """
        Save ICSP mirrors list to a file

        """
        if os.path.getsize(self.icsp_mirrors_path) > 0:
            logger.info(
                f"ICSP mirrors list already exists at '{self.icsp_mirrors_path}'"
            )
            return
        logger.info(f"Saving ICSP mirrors list to '{self.icsp_mirrors_path}'")

        icsp_json = self.ocp.exec_oc_cmd("get imagecontentsourcepolicy -o json")
        parse_ICSP_json_to_mirrors_file(icsp_json, self.icsp_mirrors_path)

    def update_mirrors_list_to_file(self):
        """
        Update ICSP list to a file
        """
        if os.path.isfile(self.icsp_mirrors_path):
            logger.info(f"Updating ICSP list from '{self.icsp_mirrors_path}'")
            exec_cmd(f"rm -f {self.icsp_mirrors_path}")
        else:
            self.icsp_mirrors_path = tempfile.NamedTemporaryFile(
                mode="w+", prefix="icsp_mirrors-", delete=False
            ).name
        self.save_mirrors_list_to_file()

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

    def get_hypershift_csv_version(self):
        """
        Get hypershift operator version
        Returns:
            str: hypershift operator version
        """
        cmd = "oc get csv -n openshift-cnv -o jsonpath='{.items[0].spec.version}'"
        cmd_res = exec_cmd(cmd, shell=True)
        if cmd_res.returncode != 0:
            logger.error(f"Failed to get hypershift operator version\n{cmd_res.stderr}")
            return

        logger.info(f"Hypershift operator version: {cmd_res.stdout.decode('utf-8')}")
        return cmd_res.stdout.decode("utf-8")

    def get_mce_version(self):
        """
        Get multicluster engine version
        Returns:
            str: multicluster engine version
        """
        cmd = "oc get mce multiclusterengine -o jsonpath='{.status.currentVersion}'"
        cmd_res = exec_cmd(cmd, shell=True)
        if cmd_res.returncode != 0:
            logger.error(f"Failed to get multicluster engine version\n{cmd_res.stderr}")
            return

        logger.info(f"Multicluster engine version: {cmd_res.stdout.decode('utf-8')}")
        return cmd_res.stdout.decode("utf-8")

    def hypershift_clusters_exist(self):
        """
        Check if hypershift is installed on the cluster
        Returns:
            bool: True if hypershift is installed, False otherwise
        """
        cmd = "oc get deployments -n hypershift | awk 'NR>1 {print \"true\"; exit}' "
        cmd_res = exec_cmd(cmd, shell=True)
        if cmd_res.returncode != 0:
            logger.error(f"Failed to get hypershift operator version\n{cmd_res.stderr}")
            return False

        return cmd_res.stdout.decode("utf-8").strip() == "true"

    def install_hypershift_upstream_on_cluster(self):
        """
        Install hypershift on the cluster
        Returns:
            bool: True if hypershift is installed, False otherwise
        """
        logger.info("Installing hypershift upstream on the cluster")
        hypershift_image = f"{constants.HCP_REGISTRY}:{config.ENV_DATA['hcp_version']}"
        cmd_res = exec_cmd(
            f"{self.hypershift_binary_path} install "
            f"--hypershift-image {hypershift_image} "
            "--platform-monitoring=All "
            "--enable-ci-debug-output "
            "--wait-until-available"
        )
        if cmd_res.returncode != 0:
            logger.error(
                f"Failed to install hypershift on the cluster\n{cmd_res.stderr}"
            )
            return False
        logger.info(cmd_res.stdout.decode("utf-8").splitlines())
        return True
