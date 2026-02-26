import logging
import os
import re
import shutil
import tempfile
import time
from datetime import datetime
import json

from ocs_ci.deployment.helpers.idms_parser import (
    parse_IDMS_json_to_mirrors_file,
    extract_image_content_sources,
)
from ocs_ci.deployment.ocp import download_pull_secret
from ocs_ci.framework import config, Config
from ocs_ci.ocs import constants
from ocs_ci.ocs import defaults
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    TimeoutExpiredError,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_in_statuses_concurrently
from ocs_ci.ocs.version import get_ocp_version
from ocs_ci.utility.retry import retry, catch_exceptions
from ocs_ci.utility.utils import (
    exec_cmd,
    TimeoutSampler,
    get_latest_release_version,
    get_random_letters,
)
from ocs_ci.utility.decorators import switch_to_orig_index_at_last
from ocs_ci.ocs.utils import get_namespce_name_by_pattern
from ocs_ci.deployment.ocp import OCPDeployment as BaseOCPDeployment
from ocs_ci.deployment.mce import MCEInstaller
from packaging.version import parse as parse_version

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
    hosted_clusters_obj = OCP(
        kind=constants.HOSTED_CLUSTERS, namespace=constants.CLUSTERS_NAMESPACE
    ).get()
    return [
        cluster.get("metadata").get("name") for cluster in hosted_clusters_obj["items"]
    ]


@catch_exceptions((CommandFailed, TimeoutExpiredError))
def get_available_hosted_clusters_to_ocp_ver_dict():
    """
    Get available HyperShift hosted clusters with their versions

    Returns:
        dict: hosted clusters available with their versions. Example: {'cl-418-x': '4.18.7', 'cl-418-c': '4.19.0'}

    """

    logger.info("Getting HyperShift hosted clusters available")
    cmd = (
        "get hostedclusters -n clusters -o json | "
        "jq -r '.items[] | "
        'select(.metadata.annotations["hypershift.openshift.io/HasBeenAvailable"] == "true") | '
        '"\\(.metadata.name)|\\(.status.version.history[0].version)"\''
    )
    with config.RunWithProviderConfigContextIfAvailable():

        out = OCP().exec_oc_cmd(
            command=cmd,
            cluster_config=config,
            shell=True,
            out_yaml_format=False,
            silent=True,
        )
        if not out:
            return {}
        return {
            line.split("|")[0]: line.split("|")[1]
            for line in out.splitlines()
            if "|" in line
        }


def kubeconfig_exists_decorator(func):
    """
    Decorator to check if the kubeconfig exists before executing the decorated method

    Args:
        func: Function to decorate. Should be used only for methods of a class having a 'cluster_kubeconfig' attribute.

    Returns:
        wrapper: The decorated function.
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


def get_random_hosted_cluster_name():
    """
    Get a random cluster name

    Returns:
        str: random cluster name

    """

    # getting the cluster name from the env data, for instance "ibm_cloud_baremetal3; mandatory conf field"
    bm_name = config.ENV_DATA.get("baremetal", {}).get("env_name")
    ocp_version = get_latest_release_version()
    hcp_version = "".join([c for c in ocp_version if c.isdigit()][:3])
    match = re.search(r"\d+$", bm_name)
    if match:
        random_letters = get_random_letters(3)
        cluster_name = (
            "hcp"
            + hcp_version
            + "-bm"
            + bm_name[match.start() :]
            + "-"
            + random_letters
        )
    else:
        raise ValueError("Cluster name not found in the env data")
    return cluster_name


def get_binary_hcp_version():
    """
    Get hcp version output. Handles hcp 4.16 and 4.17 cmd differences

    Returns:
        str: hcp version output

    """

    try:
        return exec_cmd("hcp version").stdout.decode("utf-8").strip()
    except CommandFailed:
        return exec_cmd("hcp --version").stdout.decode("utf-8").strip()


def get_binary_hcp_ocp_version():
    """
    Extract the OCP version from hypershift version output.

    Example output:
        Client Version: openshift/hypershift: 4be85900d761c04ab69fdf79408ced718cef5628. Latest supported OCP: 4.21.0
        Server Version: c20bbad4d528bfb51687f02684ef5eb79669b850
        Server Supports OCP Versions: 4.21, 4.20, 4.19, 4.18, 4.17, 4.16, 4.15, 4.14

    Returns:
        str: OCP version in 'major.minor' format (e.g., '4.21') or None if not found

    """
    try:
        version_output = exec_cmd("hypershift version").stdout.decode("utf-8").strip()
        # Parse "Latest supported OCP: 4.21.0" from the output
        for line in version_output.split("\n"):
            if "Latest supported OCP:" in line:
                # Extract version like "4.21.0"
                version_str = line.split("Latest supported OCP:")[1].strip()
                # Parse to get major.minor (e.g., "4.21")
                v = parse_version(version_str)
                return f"{v.major}.{v.minor}"
        logger.warning(
            "Could not find 'Latest supported OCP' in hypershift version output"
        )
        return None
    except CommandFailed as e:
        logger.warning(f"Failed to get hypershift version: {e}")
        return None


@switch_to_orig_index_at_last
def get_cluster_vm_namespace(cluster_name=None):
    """
    Get the cluster virtual machines namespace by the cluster name

    Args:
        cluster_name (str): The cluster name.

    Returns:
        str: The cluster virtual machines namespace

    """

    cluster_name = cluster_name or config.ENV_DATA["cluster_name"]
    pattern = f"clusters-{cluster_name}"
    config.switch_to_provider()
    cluster_vm_namespaces = get_namespce_name_by_pattern(pattern=pattern)
    assert (
        cluster_vm_namespaces
    ), f"Didn't find the cluster namespace for the cluster {cluster_name}"

    return cluster_vm_namespaces[0]


@switch_to_orig_index_at_last
def is_hosted_cluster(cluster_name=None):
    """
    Check if the cluster is a hosted cluster

    Args:
        cluster_name (str): The cluster name

    Returns:
        bool: True, if the cluster is a hosted cluster. False, otherwise.

    """

    cluster_name = cluster_name or config.ENV_DATA["cluster_name"]
    config.switch_ctx(config.get_cluster_index_by_name(cluster_name))
    config.switch_to_provider()
    ocp_obj = OCP(
        kind=constants.HOSTED_CLUSTERS, namespace=constants.CLUSTERS_NAMESPACE
    )
    return ocp_obj.is_exist(resource_name=cluster_name)


@switch_to_orig_index_at_last
def get_hosted_cluster_type(cluster_name=None):
    """
    Get the hosted cluster type

    Args:
        cluster_name (str): The cluster name

    Returns:
        str: The hosted cluster type in lowercase


    """
    cluster_name = cluster_name or config.ENV_DATA["cluster_name"]
    config.switch_to_provider()
    ocp_hosted_cluster_obj = OCP(
        kind=constants.HOSTED_CLUSTERS,
        namespace=constants.CLUSTERS_NAMESPACE,
        resource_name=cluster_name,
    )
    return ocp_hosted_cluster_obj.get()["spec"]["platform"]["type"].lower()


@catch_exceptions((CommandFailed, TimeoutExpiredError))
def get_current_nodepool_size(name):
    """
    Get existing nodepool of HyperShift hosted cluster

    Args:
        name (str): name of the cluster

    Returns:
         str: number of nodes in the nodepool

    """

    logger.info(f"Getting existing nodepool of HyperShift hosted cluster {name}")
    cmd = f"get --namespace {constants.CLUSTERS_NAMESPACE} nodepools | awk '$1==\"{name}\" {{print $4}}'"
    with config.RunWithProviderConfigContextIfAvailable():
        out = OCP().exec_oc_cmd(
            command=cmd,
            cluster_config=config,
            shell=True,
            out_yaml_format=False,
            silent=True,
        )

    return out.strip() if out else ""


def get_desired_nodepool_size(name: str):
    """
    Get desired nodepool of HyperShift hosted cluster

    Args:
        name (str): of the cluster

    Returns:
        int: number of nodes in the nodepool

    """

    logger.info(f"Getting desired nodepool of HyperShift hosted cluster {name}")
    with config.RunWithProviderConfigContextIfAvailable():
        out = OCP().exec_oc_cmd(
            command=f"get --namespace {constants.CLUSTERS_NAMESPACE} nodepools | awk '$1==\"{name}\" {{print $3}}'",
            cluster_config=config,
            shell=True,
            out_yaml_format=False,
            silent=True,
        )
    if not out:
        return ""

    return out.strip() if out else ""


def worker_nodes_deployed(name: str):
    """
    Check if worker nodes are deployed for HyperShift hosted cluster

    Args:
        name (str): name of the cluster

    Returns:
         bool: True if worker nodes are deployed, False otherwise

    """

    logger.info(f"Checking if worker nodes are deployed for cluster {name}")
    return get_current_nodepool_size(name) == get_desired_nodepool_size(name)


def wait_for_worker_nodes_to_be_ready(name: str, timeout: int = 2400):
    """
    Wait for worker nodes to be ready for HyperShift hosted cluster

    Args:
        name (str): name of the cluster
        timeout (int): timeout in seconds

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
        func=worker_nodes_deployed,
        name=name,
    ):
        if sample:
            return True


def get_hosted_cluster_kubeconfig_name(name: str):
    """
    Get HyperShift hosted cluster kubeconfig, for example 'hcp414-bm2-a-admin-kubeconfig'

    Args:
        name: name of the cluster

    Returns:
        str: hosted cluster kubeconfig name

    """

    logger.info(f"Getting kubeconfig for HyperShift hosted cluster {name}")
    cmd = f"get --namespace {constants.CLUSTERS_NAMESPACE} hostedclusters | awk '$1==\"{name}\" {{print $3}}'"

    with config.RunWithProviderConfigContextIfAvailable():
        out = OCP().exec_oc_cmd(
            command=cmd,
            cluster_config=config,
            shell=True,
            out_yaml_format=False,
            silent=True,
        )
    return out.strip() if out else ""


def delete_hcp_podman_container():
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


def get_latest_supported_hypershift_version() -> str | None:
    """
    Get the latest supported Hypershift version from the hub cluster

    Returns:
        str: latest supported Hypershift version in 'major.minor' format, e.g. '4.18' or None in case of failure

    """

    hcp_version = None
    try:
        mce_installer = MCEInstaller()
        if mce_installer.check_hypershift_namespace():
            versions = mce_installer.get_supported_versions() or []
            if versions:
                latest = sorted(versions, key=parse_version)[-1]
                v = parse_version(latest)
                hcp_version = f"{v.major}.{v.minor}"
                logger.info(
                    f"Hypershift detected on hub. Using latest supported version {latest} "
                    f"(branch release-{hcp_version})."
                )
            else:
                logger.warning(
                    "No supported versions returned from hub cluster. Falling back to config hcp_version."
                )
    except Exception as e:
        logger.debug(
            f"Could not determine Hypershift installation or supported versions due to: {e}"
        )
    return hcp_version


def resolve_ocp_image(ocp_version: str) -> str:
    """
    Resolve OCP image based on the provided ocp_version.
    If ocp_version is not provided, it will use the version from the hosting platform.

    Args:
        ocp_version (str): OCP version of the cluster

    Returns:
        str: OCP index image (registry:tag)

    """
    if not ocp_version:
        with config.RunWithProviderConfigContextIfAvailable():
            provider_version = get_ocp_version()
        if "nightly" in provider_version:
            index_image = f"{constants.REGISTRY_SVC}:{provider_version}"
        else:
            index_image = f"{constants.QUAY_REGISTRY_SVC}:{provider_version}-x86_64"
    else:
        if "nightly" in ocp_version:
            index_image = f"{constants.REGISTRY_SVC}:{ocp_version}"
        else:
            index_image = f"{constants.QUAY_REGISTRY_SVC}:{ocp_version}-x86_64"
    return index_image


class HyperShiftBase:
    """
    Class to handle HyperShift hosted cluster management
    """

    def __init__(self):
        super().__init__()
        BaseOCPDeployment(skip_download_installer=True).test_cluster()

        bin_dir_rel_path = os.path.expanduser(config.RUN["bin_dir"])
        self.bin_dir = os.path.abspath(bin_dir_rel_path)
        self.hcp_binary_path = os.path.join(self.bin_dir, "hcp")
        self.hypershift_binary_path = os.path.join(self.bin_dir, "hypershift")
        # ocp instance for running oc commands
        self.ocp = OCP()
        self.idms_mirrors_path = tempfile.NamedTemporaryFile(
            mode="w+", prefix="idms_mirrors-", delete=False
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

    def install_hcp_and_hypershift_from_git(self, install_latest=False):
        """
        Install hcp binary from git

        Args:
            install_latest (bool): If True, install the latest Hypershift version from git.
            If False, use the configured hcp_version or latest supported version from hub.

        """

        if self.hcp_binary_exists() and self.hypershift_binary_exists():
            logger.info(
                f"hcp and hypershift binary exist {self.hcp_binary_path}, skipping download."
            )
            return

        if install_latest:
            # Decide which version to use for cloning Hypershift
            # If Hypershift is already installed on the hub (namespace exists), pick the latest supported version
            # from the supported-versions configmap. Otherwise, use the configured hcp_version.
            hcp_version = get_latest_supported_hypershift_version()
            if not hcp_version:
                logger.error("Falling back to configured hcp_version.")
                return self.install_hcp_and_hypershift_from_git(install_latest=False)
        else:
            hcp_version = config.ENV_DATA.get("hcp_version")
            logger.info(
                f"Using configured hcp_version: {hcp_version} (branch release-{hcp_version})."
            )

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
        hcp_version = get_binary_hcp_version()
        logger.info(f"hcp binary version output: '{hcp_version}'")

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

    def _download_hcp_binary_with_podman(self):
        """
        Download hcp binary to bin_dir

        !!! This method is not used in the code, but it is kept for reference !!!
        Use install_hcp_and_hypershift_from_git instead
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

    def update_hcp_binary(self, install_latest=False):
        """
        Update hcp binary only if a newer version is available.

        Compares the current installed hypershift binary version with the latest
        supported version from the hub cluster. Only performs update if the hub
        version is higher than the current binary version.

        Args:
            install_latest (bool): If True, install the latest Hypershift version from git.
            If False, use the configured hcp_version.
        """

        if not config.ENV_DATA.get("hcp_version"):
            logger.error("hcp_version is not set in config.ENV_DATA")
            install_latest = True

        # Get current binary version
        current_version = None
        if self.hcp_binary_exists() and self.hypershift_binary_exists():
            current_version = get_binary_hcp_ocp_version()
            if current_version:
                logger.info(f"Current hypershift binary version: {current_version}")

        # Get latest supported version from hub
        latest_version = get_latest_supported_hypershift_version()

        if current_version and latest_version:
            # Compare versions
            current_parsed = parse_version(current_version)
            latest_parsed = parse_version(latest_version)

            if current_parsed >= latest_parsed:
                logger.info(
                    f"Current hypershift binary version {current_version} is already "
                    f"up to date (>= {latest_version}). Skipping update."
                )
                return
            else:
                logger.info(
                    f"Newer hypershift version available: {latest_version} "
                    f"(current: {current_version}). Updating binary..."
                )
        elif current_version:
            logger.info(
                f"Could not determine latest version from hub. "
                f"Current version: {current_version}. Proceeding with update..."
            )
        else:
            logger.info(
                "Hypershift binary not found or version could not be determined. Installing..."
            )

        self.delete_hcp_and_hypershift_bin()
        self.install_hcp_and_hypershift_from_git(install_latest)

    def delete_hcp_and_hypershift_bin(self):
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

    def create_kubevirt_ocp_cluster(
        self,
        name: str = None,
        nodepool_replicas: int = defaults.HYPERSHIFT_NODEPOOL_REPLICAS_DEFAULT,
        memory: str = defaults.HYPERSHIFT_MEMORY_DEFAULT,
        cpu_cores: int = defaults.HYPERSHIFT_CPU_CORES_DEFAULT,
        root_volume_size: str = 40,
        ocp_version=None,
        cp_availability_policy=None,
        infra_availability_policy=None,
        disable_default_sources=None,
        data_replication_separation=False,
        auto_repair=True,
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
            cp_availability_policy (str): Control plane availability policy, default HighlyAvailable; if SingleReplica
                selected, cluster will be created with etcd kube-apiserver, kube-controller-manager,
                openshift-oauth-apiserver, openshift-controller-manager, kube-scheduler with min available
                quorum 1 in pdb.
            infra_availability_policy (str): Infra availability policy, default HighlyAvailable, if SingleReplica
                selected, cluster will be created with etcd ingress controller, monitoring, cloud controller with min
                available quorum 1 in pdb.
            disable_default_sources (bool): Disable default sources on hosted cluster, such as 'redhat-operators'
            data_replication_separation (bool): If the deployment uses data replication separation
                then add additional network
            auto_repair (bool): Enables machine autorepair with machine health checks, default True

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
            f"--image-content-sources {self.idms_mirrors_path} "
            "--annotations 'hypershift.openshift.io/skip-release-image-validation=true' "
            "--olm-catalog-placement Guest "
        )

        if auto_repair:
            create_hcp_cluster_cmd += " --auto-repair"

        if (
            cp_availability_policy
            and cp_availability_policy in constants.AVAILABILITY_POLICIES
        ):
            create_hcp_cluster_cmd += (
                f" --control-plane-availability-policy {cp_availability_policy} "
            )
        else:
            logger.error(
                f"Control plane availability policy {cp_availability_policy} is not valid. "
                f"Valid values are: {constants.AVAILABILITY_POLICIES}"
            )

        if (
            infra_availability_policy
            and infra_availability_policy in constants.AVAILABILITY_POLICIES
        ):
            create_hcp_cluster_cmd += (
                f" --infra-availability-policy {infra_availability_policy} "
            )
        else:
            logger.error(
                f"Infrastructure availability policy {infra_availability_policy} is not valid. "
                f"Valid values are: {constants.AVAILABILITY_POLICIES}"
            )

        if data_replication_separation:
            create_hcp_cluster_cmd += (
                f" --additional-network name:clusters-{name}/storage"
            )

        if disable_default_sources:
            create_hcp_cluster_cmd += " --olm-disable-default-sources"

        logger.info("Creating HyperShift hosted cluster")
        exec_cmd(create_hcp_cluster_cmd)

        return name

    @config.run_with_provider_context_if_available
    def create_agent_ocp_cluster(
        self,
        name: str = None,
        nodepool_replicas: int = defaults.HYPERSHIFT_NODEPOOL_REPLICAS_DEFAULT,
        ocp_version=None,
        cp_availability_policy=None,
        infra_availability_policy=None,
        disable_default_sources=None,
        auto_repair=True,
    ):
        """
        Create agent hosted cluster. Default parameters have minimal requirements for the cluster.

        Args:
            name (str): Name of the cluster
            nodepool_replicas (int): Number of nodes in the cluster
            ocp_version (str): OCP version of the cluster
            cp_availability_policy (str): Control plane availability policy, default HighlyAvailable; if SingleReplica
                selected, cluster will be created with etcd kube-apiserver, kube-controller-manager,
                openshift-oauth-apiserver, openshift-controller-manager, kube-scheduler with min available
                quorum 1 in pdb.
            infra_availability_policy (str): Infra availability policy, default HighlyAvailable, if SingleReplica
                selected, cluster will be created with etcd ingress controller, monitoring, cloud controller with min
                available quorum 1 in pdb.
            disable_default_sources (bool): Disable default sources on hosted cluster, such as 'redhat-operators'
            auto_repair (bool): Enables machine autorepair with machine health checks, default True

        Returns:
            str: Name of the hosted cluster

        """

        self.save_mirrors_list_to_file()
        pull_secret_path = download_pull_secret()

        # If ocp_version is not provided, get the version from Hosting Platform
        index_image = resolve_ocp_image(ocp_version)

        if not name:
            name = "hcp-" + datetime.now().strftime("%f")

        logger.info("Creating agent hosted cluster")

        create_hcp_cluster_cmd = (
            f"{self.hypershift_binary_path} create cluster agent "
            f"--name {name} "
            f"--agent-namespace {name} "
            f"--base-domain {config.ENV_DATA['base_domain']} "
            f"--api-server-address api.{name}.{config.ENV_DATA['base_domain']} "
            f"--release-image {index_image} "
            f"--node-pool-replicas {nodepool_replicas} "
            f"--pull-secret {pull_secret_path} "
            f"--ssh-key {os.path.expanduser(config.DEPLOYMENT.get('ssh_key'))} "
            f"--image-content-sources {self.idms_mirrors_path} "
            "--annotations 'hypershift.openshift.io/skip-release-image-validation=true' "
            "--olm-catalog-placement Guest "
            f"--etcd-storage-class {constants.DEFAULT_STORAGECLASS_RBD} "
        )

        if auto_repair:
            create_hcp_cluster_cmd += " --auto-repair"

        if (
            cp_availability_policy
            and cp_availability_policy in constants.AVAILABILITY_POLICIES
        ):
            create_hcp_cluster_cmd += (
                f" --control-plane-availability-policy {cp_availability_policy} "
            )
        else:
            logger.error(
                f"Control plane availability policy {cp_availability_policy} is not valid. "
                f"Valid values are: {constants.AVAILABILITY_POLICIES}"
            )

        if (
            infra_availability_policy
            and infra_availability_policy in constants.AVAILABILITY_POLICIES
        ):
            create_hcp_cluster_cmd += (
                f" --infra-availability-policy {infra_availability_policy} "
            )
        else:
            logger.error(
                f"Infrastructure availability policy {infra_availability_policy} is not valid. "
                f"Valid values are: {constants.AVAILABILITY_POLICIES}"
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

        if not wait_for_worker_nodes_to_be_ready(
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

    def download_hosted_cluster_kubeconfig(
        self, name: str, cluster_path: str, from_hcp: bool = True
    ):
        """
        Download HyperShift hosted cluster kubeconfig

        Args:
            name (str): name of the cluster
            cluster_path (str): path to create auth_path folder and download kubeconfig there
            from_hcp (bool): if True, use hcp binary to download kubeconfig, otherwise use ocp secret

        Returns:
            str: path to the downloaded kubeconfig, None if failed

        """
        path_abs = os.path.expanduser(cluster_path)
        auth_path = os.path.join(path_abs, "auth")
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
            with config.RunWithProviderConfigContextIfAvailable():
                if from_hcp:
                    exec_cmd(
                        f"{self.hcp_binary_path} create kubeconfig --name {name} > {kubeconfig_path}",
                        shell=True,
                    )
                else:
                    # kubeconfig will be stored with name 'kubeconfig'
                    OCP().exec_oc_cmd(
                        f"extract secret/admin-kubeconfig -n clusters-{name} "
                        f"--to {os.path.dirname(kubeconfig_path)} --confirm"
                    )
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

        cmd = f"oc get --namespace {constants.CLUSTERS_NAMESPACE} hostedclusters | awk '$1==\"{name}\" {{print $4}}'"
        return exec_cmd(cmd, shell=True).stdout.decode("utf-8").strip()

    def save_mirrors_list_to_file(self):
        """
        Save IDMS mirrors list to a file
        Note: here this function has been converted from icsp to idms
        """
        if os.path.getsize(self.idms_mirrors_path) > 0:
            logger.info(
                f"IDMS mirrors list already exists at '{self.idms_mirrors_path}'"
            )
            return
        logger.info(f"Saving IDMS mirrors list to '{self.idms_mirrors_path}'")

        idms_json = self.ocp.exec_oc_cmd("get imagedigestmirrorsets -o json")
        parse_IDMS_json_to_mirrors_file(idms_json, self.idms_mirrors_path)

    def update_mirrors_list_to_file(self):
        """
        Update IDMS list to a file
        Note: here this function has been converted from icsp to idms
        """
        if os.path.isfile(self.idms_mirrors_path):
            logger.info(f"Updating IDMS list from '{self.idms_mirrors_path}'")
            exec_cmd(f"rm -f {self.idms_mirrors_path}")
        else:
            self.idms_mirrors_path = tempfile.NamedTemporaryFile(
                mode="w+", prefix="idms_mirrors-", delete=False
            ).name
        self.save_mirrors_list_to_file()

    def apply_idms_to_hosted_cluster(self, name, idms_json_dict=None, replace=False):
        """
        Apply ImageDigestMirrorSet data to an existing HostedCluster as imageContentSources.
        This patches spec.imageContentSources of the HostedCluster resource in the management (hub) cluster.

        Args:
            name (str): HostedCluster name (namespace is clusters-<name> but resource lives in clusters namespace)
            idms_json_dict (dict|None): If provided, use this pre-fetched dict
                (output of 'oc get imagedigestmirrorsets -o json').
                If None, it will be fetched automatically.
            replace (bool): If True, replace any existing spec.imageContentSources with the new list.
                            If False, merge (append new unique entries after existing ones).

        Returns:
            bool: True if patch applied (or nothing to do), False on failure.

        Notes:
            - Empty or missing IDMS items will result in a no-op (returns True).
            - Deduplication retains first occurrence (existing entries preserved if replace=False).
        """
        try:
            with config.RunWithProviderConfigContextIfAvailable():
                if idms_json_dict is None:
                    logger.info(
                        "Fetching ImageDigestMirrorSets JSON for HostedCluster patch"
                    )
                    idms_json_dict = self.ocp.exec_oc_cmd(
                        "get imagedigestmirrorsets -o json"
                    )
                ics_new = extract_image_content_sources(idms_json_dict)
                if not ics_new:
                    logger.info(
                        "No imageContentSources extracted from IDMS; skipping patch"
                    )
                    return True
                ocp_hc = OCP(
                    kind=constants.HOSTED_CLUSTERS,
                    namespace=constants.CLUSTERS_NAMESPACE,
                )
                hosted = ocp_hc.get(name)
                existing = hosted.get("spec", {}).get("imageContentSources") or []
                if replace:
                    combined = ics_new
                else:
                    seen = set(
                        (
                            e.get("source"),
                            tuple(e.get("mirrors", [])),
                        )
                        for e in existing
                    )
                    combined = list(existing)
                    for entry in ics_new:
                        key = (entry.get("source"), tuple(entry.get("mirrors", [])))
                        if key not in seen:
                            seen.add(key)
                            combined.append(entry)
                if combined == existing:
                    logger.info(
                        "HostedCluster already has desired imageContentSources; no patch needed"
                    )
                    return True
                patch_body = json.dumps({"spec": {"imageContentSources": combined}})
                logger.info(
                    f"Patching HostedCluster '{name}' with {len(combined)} imageContentSources "
                    f"entries (replace={replace})"
                )
                ocp_hc.exec_oc_cmd(
                    f"patch hostedclusters {name} --type=merge -p '{patch_body}'"
                )
                return True
        except Exception as e:
            # this is non-critical operation, it should not fail deployment or upgrade on multiple clusters,
            # thus exception is broad
            logger.error(f"Failed to apply IDMS mirrors to HostedCluster '{name}': {e}")
            return False

    def destroy_kubevirt_cluster(self, name):
        """
        Destroy HyperShift hosted cluster

        Args:
            name (str): Name of the cluster
        """

        destroy_timeout_min = 15
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


def create_cluster_dir(cluster_name):
    """
    Create the kubeconfig directory for the cluster

    Args:
        cluster_name (str): Name of the cluster

    Returns:
        str: Path to the kubeconfig directory

    """

    path = os.path.join(
        config.ENV_DATA["cluster_path"],
        constants.CLUSTERS_NAMESPACE,
        cluster_name,
        "openshift-cluster-dir",
    )
    os.makedirs(path, exist_ok=True)
    return path


def prepare_vsphere_agent_host_cluster_config():
    """
    Prepares config object for the HUB Cluster to be used as host cluster
    for vSphere Agent Assisted Installer deployment

    Returns:
        Config: Config object for the HUB Cluster

    """
    cluster_config = Config()
    cluster_path = os.path.expanduser(config.DEPLOYMENT.get("hub_cluster_path"))
    def_client_config_dict = {
        "DEPLOYMENT": {},
        "ENV_DATA": {
            "cluster_name": config.DEPLOYMENT.get("hub_cluster_name"),
            "cluster_path": cluster_path,
            "platform": "vsphere",
            "deployment_type": "ai",
            "cluster_type": "provider",
        },
        "RUN": {
            "kubeconfig": os.path.join(
                cluster_path, cluster_config.RUN["kubeconfig_location"]
            )
        },
    }
    keys = [
        "run_id",
        "log_dir",
        "bin_dir",
        "jenkins_build_url",
        "logs_url",
    ]
    for key in keys:
        def_client_config_dict["RUN"][key] = config.RUN.get(key, "")
    cluster_config.update(def_client_config_dict)
    return cluster_config


@retry(CommandFailed, tries=5, delay=30, backoff=1)
def create_kubeconfig_file_hosted_cluster():
    """
    Export kubeconfig to auth directory in cluster path.

    This function is wrapped with retry decorator to handle CommandFailed errors.
    It will retry up to 5 times with 30 sec  delay between attempts.
    """
    cluster_path = config.ENV_DATA["cluster_path"]
    cluster_name = config.ENV_DATA["cluster_name"]

    with config.RunWithProviderConfigContextIfAvailable():
        hypershift = HyperShiftBase()
        kubeconfig_path = hypershift.download_hosted_cluster_kubeconfig(
            cluster_name, cluster_path=cluster_path
        )

    config.RUN["kubeconfig"] = kubeconfig_path
    logger.info("Created kubeconfig file")


def wait_for_worker_nodes_ready(timeout=600, sleep=10, expected_nodes=None):
    """
    Wait for worker nodes to appear in the agent cluster inventory and become ready.
    This function operates on the default context (agent cluster).

    Args:
        timeout (int): Maximum time in seconds to wait for nodes to be ready (default: 600)
        sleep (int): Time in seconds to sleep between checks (default: 10)
        expected_nodes (int): Expected number of worker nodes. If None, waits for at least one node.

    Raises:
        TimeoutExpiredError: If worker nodes don't appear or become ready within the timeout

    """
    from ocs_ci.ocs.node import get_nodes, wait_for_nodes_status

    logger.info("Waiting for worker nodes to appear in agent cluster inventory")

    try:
        worker_nodes = []
        for sample in TimeoutSampler(
            timeout=timeout,
            sleep=sleep,
            func=get_nodes,
            node_type=constants.WORKER_MACHINE,
        ):
            if sample:
                worker_nodes = sample
                if expected_nodes is None and len(worker_nodes) > 0:
                    logger.info(
                        f"Found {len(worker_nodes)} worker node(s): "
                        f"{[node.name for node in worker_nodes]}"
                    )
                    break
                elif expected_nodes and len(worker_nodes) >= expected_nodes:
                    logger.info(
                        f"Found {len(worker_nodes)} worker node(s) (expected: {expected_nodes}): "
                        f"{[node.name for node in worker_nodes]}"
                    )
                    break
                else:
                    current_count = len(worker_nodes) if worker_nodes else 0
                    expected_count = expected_nodes if expected_nodes else "at least 1"
                    logger.info(
                        f"Waiting for worker nodes to appear. "
                        f"Current: {current_count}, Expected: {expected_count}"
                    )
    except TimeoutExpiredError:
        logger.error(
            f"Worker nodes did not appear in agent cluster inventory within {timeout} seconds"
        )
        raise

    # Wait for worker nodes to become Ready
    logger.info("Waiting for worker nodes to become Ready")
    node_names = [node.name for node in worker_nodes]
    wait_for_nodes_status(
        node_names=node_names,
        status=constants.NODE_READY,
        timeout=timeout,
        sleep=sleep,
    )
    logger.info("All worker nodes are ready")


@config.run_with_provider_context_if_available
def get_hosted_cluster_condition_status(cluster_name, condition_type="Available"):
    """
    Get hosted cluster condition status

    Equivalent to: oc get hostedcluster <cluster_name> -n clusters
                   -o jsonpath='{.status.conditions[?(@.type=="<condition_type>")].status}'

    Args:
        cluster_name (str): Name of the hosted cluster
        condition_type (str): Type of condition to check (default: "Available")
                             Other common types: "Ready", "Progressing", "Degraded"

    Returns:
        str: Condition status (typically "True", "False", or "Unknown")
        None: If condition not found or command fails

    """

    logger.info(
        f"Getting hosted cluster '{cluster_name}' condition status for type '{condition_type}'"
    )

    try:
        ocp_hc = OCP(
            kind=constants.HOSTED_CLUSTERS, namespace=constants.CLUSTERS_NAMESPACE
        )
        jsonpath = f'{{.status.conditions[?(@.type=="{condition_type}")].status}}'

        status = ocp_hc.exec_oc_cmd(
            f"get hostedclusters {cluster_name} -o jsonpath='{jsonpath}'",
            out_yaml_format=False,
        )

        # status is a string when out_yaml_format=False
        status_str = str(status) if status else ""

        logger.info(
            f"Hosted cluster '{cluster_name}' condition '{condition_type}' status: {status_str}"
        )
        return status_str.strip() if status_str else None

    except CommandFailed as e:
        logger.warning(
            f"Failed to get hosted cluster '{cluster_name}' condition status: {e}"
        )
        return None


def wait_for_hosted_cluster_available(cluster_name, timeout=600, sleep=30):
    """
    Wait for hosted cluster to become available using TimeoutSampler

    ! Executed always from Provider context !

    Args:
        cluster_name (str): Name of the hosted cluster
        timeout (int): Timeout in seconds (default: 600 = 10 minutes)
        sleep (int): Sleep interval between checks in seconds (default: 30)

    Returns:
        bool: True if cluster becomes available, False if timeout

    Raises:
        TimeoutExpiredError: If cluster doesn't become available within timeout

    """
    logger.info(f"Waiting for hosted cluster '{cluster_name}' to become available")

    for sample in TimeoutSampler(
        timeout,
        sleep,
        get_hosted_cluster_condition_status,
        cluster_name,
        constants.STATUS_AVAILABLE,
    ):
        if sample == "True":
            logger.info(f"Hosted cluster '{cluster_name}' is available")
            return True
        else:
            logger.info(
                f"Hosted cluster '{cluster_name}' availability status: {sample}. Waiting..."
            )

    return False
