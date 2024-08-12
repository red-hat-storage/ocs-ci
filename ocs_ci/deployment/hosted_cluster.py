import base64
import logging
import os
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from ocs_ci.deployment.cnv import CNVInstaller
from ocs_ci.deployment.deployment import Deployment
from ocs_ci.deployment.helpers.hypershift_base import (
    HyperShiftBase,
    get_hosted_cluster_names,
    kubeconfig_exists_decorator,
)
from ocs_ci.deployment.metallb import MetalLBInstaller
from ocs_ci.framework import config
from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.constants import HCI_PROVIDER_CLIENT_PLATFORMS
from ocs_ci.ocs.exceptions import (
    ProviderModeNotFoundException,
    CommandFailed,
    TimeoutExpiredError,
    ResourceWrongStatusException,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs.resources.csv import check_all_csvs_are_succeeded
from ocs_ci.ocs.resources.pod import (
    wait_for_pods_to_be_in_statuses_concurrently,
)
from ocs_ci.utility import templating, version
from ocs_ci.utility.managedservice import generate_onboarding_token
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import (
    exec_cmd,
    TimeoutSampler,
    get_ocp_version,
    get_latest_release_version,
)
from ocs_ci.utility.version import get_semantic_version

logger = logging.getLogger(__name__)


class HostedClients(HyperShiftBase):
    """
    The class is intended to deploy multiple hosted OCP clusters on Provider platform and setup ODF client on them.
    All functions are for multiple clusters deployment or the helper functions.
    All functions related to OCP deployment or ODF client setup are in the respective classes.
    """

    def __init__(self):
        HyperShiftBase.__init__(self)
        self.kubeconfig_paths = []

    def do_deploy(self, cluster_names=None):
        """
        Deploy multiple hosted OCP clusters on Provider platform and setup ODF client on them
        Perform the 7 stages of deployment:
        1. Deploy multiple hosted OCP clusters
        2. Verify OCP clusters are ready
        3. Download kubeconfig files
        4. Deploy ODF on all hosted clusters if version set in ENV_DATA
        5. Verify ODF client is installed on all hosted clusters if deployed
        6. Setup storage client on all hosted clusters if ENV_DATA.clusters.<cluster_name> has setup_storage_client:true
        7. Verify all hosted clusters are ready and print kubeconfig paths to the console

        If the CNV, OCP versions are unreleased we can not use that with released upstream MCE which is
        a component of Openshift Virtualization operator, MCE will be always behind failing the cluster creation.
        solution: disable MCE and install upstream Hypershift on the cluster

        ! Important !
        due to n-1 logic we are assuming that desired CNV version <= OCP version of managing/Provider cluster

        Args:
            cluster_names (list): cluster names to deploy, if None, all clusters from ENV_DATA will be deployed

        Returns:
            list: the list of HostedODF objects for all hosted OCP clusters deployed by the method successfully
        """

        # stage 1 deploy multiple hosted OCP clusters
        # If all desired clusters were already deployed and self.deploy_hosted_ocp_clusters() returns None instead of
        # the list, in this case we assume the stage of Hosted OCP clusters creation is done, and we
        # proceed to ODF installation and storage client setup.
        # If specific cluster names were provided, we will deploy only those.
        if not cluster_names:
            cluster_names = self.deploy_hosted_ocp_clusters() or list(
                config.ENV_DATA.get("clusters").keys()
            )
        if cluster_names:
            cluster_names = self.deploy_hosted_ocp_clusters(cluster_names)

        # stage 2 verify OCP clusters are ready
        logger.info(
            "Ensure clusters were deployed successfully, wait for them to be ready"
        )
        verification_passed = self.verify_hosted_ocp_clusters_from_provider()
        if not verification_passed:
            logger.error("\n\n*** Some of the clusters are not ready ***\n")

        # stage 3 download all available kubeconfig files
        logger.info("Download kubeconfig for all clusters")
        kubeconfig_paths = self.download_hosted_clusters_kubeconfig_files()

        # stage 4 deploy ODF on all hosted clusters if not already deployed
        for cluster_name in cluster_names:

            if not self.config_has_hosted_odf_image(cluster_name):
                logger.info(
                    f"Hosted ODF image not set for cluster '{cluster_name}', skipping ODF deployment"
                )
                continue

            logger.info(f"Setup ODF client on hosted OCP cluster '{cluster_name}'")
            hosted_odf = HostedODF(cluster_name)
            hosted_odf.do_deploy()

        # stage 5 verify ODF client is installed on all hosted clusters
        odf_installed = []
        for cluster_name in cluster_names:
            if self.config_has_hosted_odf_image(cluster_name):
                logger.info(
                    f"Validate ODF client operator installed on hosted OCP cluster '{cluster_name}'"
                )
                hosted_odf = HostedODF(cluster_name)
                if not hosted_odf.odf_client_installed():
                    hosted_odf.exec_oc_cmd(
                        "delete catalogsource --all -n openshift-marketplace"
                    )
                    logger.info("wait 30 sec and create catalogsource again")
                    time.sleep(30)
                    hosted_odf.create_catalog_source()
                odf_installed.append(hosted_odf.odf_client_installed())

        # stage 6 setup storage client on all hosted clusters
        client_setup_res = []
        hosted_odf_clusters_installed = []
        for cluster_name in cluster_names:
            if self.storage_installation_requested(cluster_name):
                logger.info(
                    f"Setting up Storage client on hosted OCP cluster '{cluster_name}'"
                )
                hosted_odf = HostedODF(cluster_name)
                client_installed = hosted_odf.setup_storage_client()
                client_setup_res.append(client_installed)
                if client_installed:
                    hosted_odf_clusters_installed.append(hosted_odf)
            else:
                logger.info(
                    f"Storage client installation not requested for cluster '{cluster_name}', "
                    "skipping storage client setup"
                )
        # stage 7 verify all hosted clusters are ready and print kubeconfig paths on Agent
        logger.info(
            "kubeconfig files for all hosted OCP clusters:\n"
            + "\n".join(
                [
                    f"kubeconfig path: {kubeconfig_path}"
                    for kubeconfig_path in kubeconfig_paths
                ]
            )
        )

        assert verification_passed, "Some of the hosted OCP clusters are not ready"
        assert all(
            odf_installed
        ), "ODF client was not deployed on all hosted OCP clusters"
        assert all(
            client_setup_res
        ), "Storage client was not set up on all hosted ODF clusters"

        return hosted_odf_clusters_installed

    def config_has_hosted_odf_image(self, cluster_name):
        """
        Check if the config has hosted ODF image set for the cluster

        Args:
            cluster_name:

        Returns:
            bool: True if the config has hosted ODF image, False otherwise

        """
        regestry_exists = (
            config.ENV_DATA.get("clusters")
            .get(cluster_name)
            .get("hosted_odf_registry", False)
        )
        version_exists = (
            config.ENV_DATA.get("clusters")
            .get(cluster_name)
            .get("hosted_odf_version", False)
        )

        return regestry_exists and version_exists

    def storage_installation_requested(self, cluster_name):
        """
        Check if the storage client installation was requested in the config

        Args:
            cluster_name (str): Name of the cluster

        Returns:
            bool: True if the storage client installation was requested, False otherwise
        """
        return (
            config.ENV_DATA.get("clusters", {})
            .get(cluster_name, {})
            .get("setup_storage_client", False)
        )

    def deploy_hosted_ocp_clusters(self, cluster_names_list=None):
        """
        Deploy multiple hosted OCP clusters on Provider platform

        Args:
            cluster_names_list (list): List of cluster names to deploy. If not provided, all clusters
                                                 in config.ENV_DATA["clusters"] will be deployed (optional argument)

        Returns:
            list: The list of cluster names for all hosted OCP clusters deployed by the func successfully
        """

        # Get the list of cluster names to deploy
        if cluster_names_list:
            cluster_names_desired = [
                name
                for name in cluster_names_list
                if name in config.ENV_DATA["clusters"].keys()
            ]
        else:
            cluster_names_desired = list(config.ENV_DATA["clusters"].keys())
        number_of_clusters_to_deploy = len(cluster_names_desired)
        deployment_mode = (
            "only specified clusters"
            if cluster_names_list
            else "clusters from deployment configuration"
        )
        logger.info(
            f"Deploying '{number_of_clusters_to_deploy}' number of {deployment_mode}"
        )

        cluster_names = []

        for index, cluster_name in enumerate(cluster_names_desired):
            logger.info(f"Creating hosted OCP cluster: {cluster_name}")
            hosted_ocp_cluster = HypershiftHostedOCP(cluster_name)
            # we need to ensure that all dependencies are installed so for the first cluster we will install all,
            # operators and finish the rest preparation steps. For the rest of the clusters we will only deploy OCP
            # with hcp command.
            first_ocp_deployment = index == 0
            cluster_name = hosted_ocp_cluster.deploy_ocp(
                deploy_cnv=first_ocp_deployment,
                deploy_acm_hub=first_ocp_deployment,
                deploy_metallb=first_ocp_deployment,
                download_hcp_binary=first_ocp_deployment,
            )
            if cluster_name:
                cluster_names.append(cluster_name)

        cluster_names_existing = get_hosted_cluster_names()
        cluster_names_desired_left = [
            cluster_name
            for cluster_name in cluster_names_desired
            if cluster_name not in cluster_names_existing
        ]
        if cluster_names_desired_left:
            logger.error("Some of the desired hosted OCP clusters were not created")
        else:
            logger.info("All desired hosted OCP clusters exist")

        return cluster_names

    def verify_hosted_ocp_clusters_from_provider(self):
        """
        Verify multiple HyperShift hosted clusters from provider. If cluster_names is not provided at ENV_DATA,
        it will get the list of hosted clusters from the provider to verify them all

        Returns:
            bool: True if all hosted clusters passed verification, False otherwise
        """
        cluster_names = list(config.ENV_DATA.get("clusters").keys())
        if not cluster_names:
            cluster_names = get_hosted_cluster_names()
        futures = []
        try:
            with ThreadPoolExecutor(len(cluster_names)) as executor:
                for name in cluster_names:
                    futures.append(
                        executor.submit(
                            self.verify_hosted_ocp_cluster_from_provider,
                            name,
                        )
                    )
            return all(future.result() for future in futures)
        except Exception as e:
            logger.error(
                f"Failed to verify HyperShift hosted clusters from provider: {e}"
            )
            return False

    def download_hosted_clusters_kubeconfig_files(self):
        """
        Get HyperShift hosted cluster kubeconfig for multiple clusters
        Returns:
            list: the list of hosted cluster kubeconfig paths
        """

        if not (self.hcp_binary_exists() and self.hypershift_binary_exists()):
            self.download_hcp_binary_with_podman()

        for name in config.ENV_DATA.get("clusters").keys():
            path = config.ENV_DATA.get("clusters").get(name).get("hosted_cluster_path")
            self.kubeconfig_paths.append(
                self.download_hosted_cluster_kubeconfig(name, path)
            )

        return self.kubeconfig_paths

    def get_kubeconfig_path(self, cluster_name):
        """
        Get the kubeconfig path for the cluster

        Args:
            cluster_name (str): Name of the cluster
        Returns:
            str: Path to the kubeconfig file
        """
        if not self.kubeconfig_paths:
            self.download_hosted_clusters_kubeconfig_files()
        for kubeconfig_path in self.kubeconfig_paths:
            if cluster_name in kubeconfig_path:
                return kubeconfig_path
        return

    def deploy_multiple_odf_clients(self):
        """
        Deploy multiple ODF clients on hosted OCP clusters. Method tries to deploy ODF client on all hosted OCP clusters
        If ODF was already deployed on some of the clusters, it will be skipped for those clusters.

        """
        self.update_hcp_binary()

        hosted_cluster_names = get_hosted_cluster_names()

        for cluster_name in hosted_cluster_names:
            logger.info(f"Deploying ODF client on hosted OCP cluster '{cluster_name}'")
            hosted_odf = HostedODF(cluster_name)
            hosted_odf.do_deploy()


class HypershiftHostedOCP(HyperShiftBase, MetalLBInstaller, CNVInstaller, Deployment):
    def __init__(self, name):
        Deployment.__init__(self)
        HyperShiftBase.__init__(self)
        MetalLBInstaller.__init__(self)
        CNVInstaller.__init__(self)
        self.name = name
        if config.ENV_DATA.get("clusters", {}).get(self.name):
            cluster_path = (
                config.ENV_DATA["clusters"].get(self.name).get("hosted_cluster_path")
            )
            self.cluster_kubeconfig = os.path.expanduser(
                os.path.join(cluster_path, "auth_path", "kubeconfig")
            )
        else:
            # avoid throwing an exception if the cluster path is not found for some reason
            # this way we can continue with the next cluster
            logger.error(
                f"Cluster path for desired cluster with name '{self.name}' was not found in ENV_DATA.clusters"
            )

    def deploy_ocp(
        self,
        deploy_cnv=True,
        deploy_acm_hub=True,
        deploy_metallb=True,
        download_hcp_binary=True,
    ):
        """
        Deploy hosted OCP cluster on provisioned Provider platform

        Args:
            deploy_cnv: (bool) Deploy CNV
            deploy_acm_hub: (bool) Deploy ACM Hub
            deploy_metallb: (bool) Deploy MetalLB
            download_hcp_binary: (bool) Download HCP binary

        Returns:
            str: Name of the hosted cluster
        """
        if not config.ENV_DATA["platform"].lower() in HCI_PROVIDER_CLIENT_PLATFORMS:
            raise ProviderModeNotFoundException()

        self.deploy_dependencies(
            deploy_acm_hub, deploy_cnv, deploy_metallb, download_hcp_binary
        )

        ocp_version = config.ENV_DATA["clusters"].get(self.name).get("ocp_version")
        cpu_cores_per_hosted_cluster = (
            config.ENV_DATA["clusters"]
            .get(self.name)
            .get("cpu_cores_per_hosted_cluster")
        )
        memory_per_hosted_cluster = (
            config.ENV_DATA["clusters"].get(self.name).get("memory_per_hosted_cluster")
        )
        nodepool_replicas = (
            config.ENV_DATA["clusters"].get(self.name).get("nodepool_replicas")
        )
        cp_availability_policy = (
            config.ENV_DATA["clusters"].get(self.name).get("cp_availability_policy")
        )
        disable_default_sources = (
            config.ENV_DATA["clusters"]
            .get(self.name)
            .get("disable_default_sources", False)
        )
        return self.create_kubevirt_ocp_cluster(
            name=self.name,
            nodepool_replicas=nodepool_replicas,
            cpu_cores=cpu_cores_per_hosted_cluster,
            memory=memory_per_hosted_cluster,
            ocp_version=ocp_version,
            cp_availability_policy=cp_availability_policy,
            disable_default_sources=disable_default_sources,
        )

    def deploy_dependencies(
        self, deploy_acm_hub, deploy_cnv, deploy_metallb, download_hcp_binary
    ):
        """
        Deploy dependencies for hosted OCP cluster
        Args:
            deploy_acm_hub: bool Deploy ACM Hub
            deploy_cnv: bool Deploy CNV
            deploy_metallb: bool Deploy MetalLB
            download_hcp_binary: bool Download HCP binary

        """
        initial_default_sc = helpers.get_default_storage_class()
        logger.info(f"Initial default StorageClass: {initial_default_sc}")
        if not initial_default_sc == constants.CEPHBLOCKPOOL_SC:
            logger.info(
                f"Changing the default StorageClass to {constants.CEPHBLOCKPOOL_SC}"
            )
            try:
                helpers.change_default_storageclass(scname=constants.CEPHBLOCKPOOL_SC)
            except CommandFailed as e:
                logger.error(f"Failed to change default StorageClass: {e}")

        if deploy_cnv:
            self.deploy_cnv(check_cnv_ready=True)
        if deploy_acm_hub:
            self.deploy_acm_hub()
        if deploy_metallb:
            self.deploy_lb()
        if download_hcp_binary:
            self.update_hcp_binary()

        provider_ocp_version = str(
            get_semantic_version(get_ocp_version(), only_major_minor=True)
        )
        latest_released_ocp_version = str(
            get_semantic_version(get_latest_release_version(), only_major_minor=True)
        )

        if provider_ocp_version > latest_released_ocp_version:
            logger.info("running on unreleased OCP version")
            if config.ENV_DATA.get("install_hypershift_upstream"):
                try:
                    self.disable_multicluster_engine()
                    # avoid timelapse error "
                    # Error: [serviceaccounts "operator" is forbidden: unable to create new content"
                    logger.info(
                        "Sleeping for 5 minutes after disable_multicluster_engine()"
                    )
                    time.sleep(5 * 60)
                    self.install_hypershift_upstream_on_cluster()
                except CommandFailed as e:
                    raise AssertionError(
                        f"Failed to install Hypershift on the cluster: {e}"
                    )

        # Enable central infrastructure management service for agent
        if config.DEPLOYMENT.get("hosted_cluster_platform") == "agent":
            provisioning_obj = OCP(**OCP(kind=constants.PROVISIONING).get()[0])
            if not provisioning_obj.data["spec"].get("watchAllNamespaces") == "true":
                provisioning_obj.patch(
                    resource_name=provisioning_obj.resource_name,
                    params='{"spec":{"watchAllNamespaces": true }}',
                    format_type="merge",
                )
                assert (
                    provisioning_obj.get()["spec"].get("watchAllNamespaces") == "true"
                ), "Cannot proceed with hosted cluster creation using agent."

            if not OCP(kind=constants.AGENT_SERVICE_CONFIG).get(dont_raise=True):
                create_agent_service_config()
            if not OCP(kind=constants.INFRA_ENV).get(dont_raise=True):
                create_host_inventory()


def create_agent_service_config():
    """
    Create AgentServiceConfig resource

    """
    template_yaml = os.path.join(
        constants.TEMPLATE_DIR, "hosted-cluster", "agent_service_config.yaml"
    )
    agent_service_config_data = templating.load_yaml(template_yaml)
    # TODO: Add custom OS image details
    helpers.create_resource(**agent_service_config_data)

    # Verify new pods that should be created
    wait_for_pods_to_be_in_statuses_concurrently(
        app_selectors_to_resource_count_list=[
            "app=assisted-service",
            "app=assisted-image-service",
        ],
        namespace="multicluster-engine",
        timeout=600,
        status=constants.STATUS_RUNNING,
    )


def create_host_inventory():
    """
    Create InfraEnv resource for host inventory

    """
    # Create new project
    project_name = helpers.create_project(project_name="bm-agents").resource_name

    # Create pull secret for InfraEnv
    secret_obj = OCP(
        kind=constants.POD,
        resource_name="pull-secret",
        namespace=constants.OPENSHIFT_CONFIG_NAMESPACE,
    )
    secret_data = secret_obj.get()
    # This is the name of pull secret and namespace used in InfraEnv template
    secret_data["metadata"]["name"] = "pull-secret-agents"
    secret_data["metadata"]["namespace"] = project_name
    helpers.create_resource(**secret_data)

    # Create InfraEnv
    template_yaml = os.path.join(
        constants.TEMPLATE_DIR, "hosted-cluster", "infra-env.yaml"
    )
    infra_env_data = templating.load_yaml(template_yaml)
    ssh_pub_file_path = config.DEPLOYMENT["ssh_key"]
    with open(ssh_pub_file_path, "r") as ssh_key:
        ssh_pub_key = ssh_key.read().strip()
    infra_env_data["spec"]["sshAuthorizedKey"] = ssh_pub_key
    # TODO: Add custom OS image details. Reference https://access.redhat.com/documentation/en-us/red_hat_advanced_
    #  cluster_management_for_kubernetes/2.10/html-single/clusters/index#create-host-inventory-cli-steps
    helpers.create_resource(**infra_env_data)


class HostedODF(HypershiftHostedOCP):
    def __init__(self, name: str):
        HyperShiftBase.__init__(self)
        HypershiftHostedOCP.__init__(self, name)
        self.namespace_client = config.ENV_DATA.get(
            "client_namespace", "openshift-storage-client"
        )
        self.timeout_check_resources_exist_sec = 6
        self.timeout_wait_csvs_minutes = 20
        self.timeout_wait_pod_minutes = 30

        # default cluster name picked from the storage client yaml
        storage_client_data = templating.load_yaml(
            constants.PROVIDER_MODE_STORAGE_CLIENT
        )
        self.storage_client_name = storage_client_data["metadata"]["name"]

    @kubeconfig_exists_decorator
    def exec_oc_cmd(self, cmd, timeout=300, ignore_error=False, **kwargs):
        """
        Execute command on the system
        Args:
          cmd (str): Command to execute
          timeout (int): Timeout for the command
          ignore_error (bool): True for ignoring error
          **kwargs: Additional arguments for exec_cmd

        Raises:
          CommandFailed: In case the command execution fails

        Returns:
          (CompletedProcess) A CompletedProcess object of the command that was executed
          CompletedProcess attributes:
          args: The list or str args passed to run().
          returncode (str): The exit code of the process, negative for signals.
          stdout     (str): The standard output (None if not captured).
          stderr     (str): The standard error (None if not captured).

        """
        cmd = "oc --kubeconfig {} {}".format(self.cluster_kubeconfig, cmd)
        return helpers.exec_cmd(
            cmd=cmd, timeout=timeout, ignore_error=ignore_error, **kwargs
        )

    @kubeconfig_exists_decorator
    def create_ns(self):
        """
        Create namespace for ODF client

        Returns:
            bool: True if namespace is created, False if command execution fails
        """
        ocp = OCP(
            kind="namespace",
            resource_name=self.namespace_client,
            cluster_kubeconfig=self.cluster_kubeconfig,
        )

        if ocp.check_resource_existence(
            timeout=self.timeout_check_resources_exist_sec,
            resource_name=self.namespace_client,
            should_exist=True,
        ):
            logger.info(f"Namespace {self.namespace_client} already exists")
            return True

        try:
            self.exec_oc_cmd(f"create namespace {self.namespace_client}")
        except CommandFailed as e:
            logger.error(f"Error during namespace creation: {e}")
            return False

        return ocp.check_resource_existence(
            timeout=self.timeout_check_resources_exist_sec,
            resource_name=self.namespace_client,
            should_exist=True,
        )

    def apply_network_policy(self):
        """
        Apply network policy to the client namespace. Network policy is created always on Provider side.

        Returns:
            bool: True if network policy is created or existed before, False otherwise
        """
        namespace = f"clusters-{self.name}"

        network_policy_data = templating.load_yaml(
            constants.NETWORK_POLICY_PROVIDER_TO_CLIENT_TEMPLATE
        )
        network_policy_data["metadata"]["namespace"] = f"clusters-{self.name}"

        if self.network_policy_exists(namespace=namespace):
            logger.info(f"Network policy {namespace} already exists")
            return True

        network_policy_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="network_policy", delete=False
        )
        templating.dump_data_to_temp_yaml(network_policy_data, network_policy_file.name)

        try:
            exec_cmd(f"oc apply -f {network_policy_file.name}", timeout=120)
        except CommandFailed as e:
            logger.error(f"Error during network policy creation: {e}")
            return False

        return self.network_policy_exists(namespace=namespace)

    @kubeconfig_exists_decorator
    def do_deploy(self):
        """
        Deploy ODF client on hosted OCP cluster
        """
        logger.info(
            f"Deploying ODF client on hosted OCP cluster '{self.name}'. Creating ODF client namespace"
        )
        self.create_ns()

        if self.odf_csv_installed():
            logger.info(
                "ODF CSV exists at namespace, assuming ODF client is already installed, skipping further steps"
            )
            return

        logger.info("Creating ODF client operator group")
        self.create_operator_group()

        logger.info("Creating ODF client catalog source")
        self.create_catalog_source()

        logger.info("Creating ODF client subscription")
        self.create_subscription()

    @kubeconfig_exists_decorator
    def setup_storage_client(self):
        """
        Setup storage client

        Returns:
            bool: True if storage client is setup, False otherwise
        """
        logger.info("Creating storage client")

        try:
            storage_client_created = self.create_storage_client()
        except TimeoutExpiredError as e:
            logger.error(f"Error during storage client creation: {e}")
            storage_client_created = False

        # if storage client is not created, there is no point in continuing
        if not storage_client_created:
            logger.error("storage client is not ready; abort further steps")
            return False

        # starting from ODF 4.16 on StorageClient creation Storage Claims created automatically

        logger.info("Verify Storage Class cephfs exists")
        if not self.wait_storage_claim_cephfs():
            logger.error("Storage class claim cephfs does not exist")
            return False

        logger.info("Verify Storage Class rbd exists")
        if not self.wait_storage_claim_rbd():
            logger.error("Storage class claim rbd does not exist")
            return False

        cephfs_storage_class_name = f"{self.storage_client_name}-cephfs"
        if not self.storage_class_exists(cephfs_storage_class_name):
            logger.error(f"cephfs storage class does not exist on cluster {self.name}")
            return False

        rbd_storage_class_name = f"{self.storage_client_name}-ceph-rbd"
        if not self.storage_class_exists(rbd_storage_class_name):
            logger.error(f"rbd storage class does not exist on cluster {self.name}")
            return False

        return True

    @kubeconfig_exists_decorator
    def odf_client_installed(self):
        """
        Check if ODF client is installed

        Returns:
            bool: True if ODF client is installed, False otherwise
        """
        logger.info("Waiting for ODF client CSV's to be installed")

        try:
            sample = TimeoutSampler(
                timeout=self.timeout_wait_csvs_minutes * 60,
                sleep=15,
                func=check_all_csvs_are_succeeded,
                namespace=self.namespace_client,
                cluster_kubeconfig=self.cluster_kubeconfig,
            )
            sample.wait_for_func_value(value=True)

            app_selectors_to_resource_count_list = [
                {"app.kubernetes.io/name=ocs-client-operator-console": 1},
                {"control-plane=controller-manager": 1},
            ]
        except Exception as e:
            logger.error(f"Error during ODF client CSV's installation: {e}")
            return False

        try:
            pods_are_running = wait_for_pods_to_be_in_statuses_concurrently(
                app_selectors_to_resource_count_list=app_selectors_to_resource_count_list,
                namespace=self.namespace_client,
                timeout=self.timeout_wait_pod_minutes * 60,
                cluster_kubeconfig=self.cluster_kubeconfig,
            )
        except Exception as e:
            logger.error(f"Error during ODF client pods status check: {e}")
            pods_are_running = False

        if not pods_are_running:
            logger.error(
                f"ODF client pods with labels {app_selectors_to_resource_count_list} are not running"
            )
            return False
        else:
            logger.info("ODF client pods are running, CSV's are installed")
            return True

    @kubeconfig_exists_decorator
    def storage_client_exists(self):
        """
        Check if the storage client exists

        Returns:
            bool: True if storage client exists, False otherwise
        """
        ocp = OCP(
            kind=constants.STORAGECLIENTS,
            namespace=self.namespace_client,
            cluster_kubeconfig=self.cluster_kubeconfig,
        )
        return ocp.check_resource_existence(
            timeout=self.timeout_check_resources_exist_sec,
            resource_name=config.ENV_DATA.get(
                "storage_client_name", constants.STORAGE_CLIENT_NAME
            ),
            should_exist=True,
        )

    @kubeconfig_exists_decorator
    def create_storage_client(self):
        """
        Create storage client

        Returns:
            bool: True if storage client is created, False otherwise
        """

        storage_client_connected_timeout_min = 5

        if self.storage_client_exists():
            logger.info("Storage client already exists")
            return True

        @retry((CommandFailed, TimeoutError), tries=3, delay=30, backoff=1)
        def _apply_storage_client_cr():
            """
            Internal function to apply storage client CR
            Returns:
                bool: True if storage client is created, False otherwise
            """
            storage_client_data = templating.load_yaml(
                constants.PROVIDER_MODE_STORAGE_CLIENT
            )
            storage_client_data["spec"][
                "storageProviderEndpoint"
            ] = self.get_provider_address()

            onboarding_key = self.get_onboarding_key()

            if not len(onboarding_key):
                return False

            storage_client_data["spec"]["onboardingTicket"] = onboarding_key

            self.storage_client_name = storage_client_data["metadata"]["name"]

            storage_client_file = tempfile.NamedTemporaryFile(
                mode="w+", prefix="storage_client", delete=False
            )
            templating.dump_data_to_temp_yaml(
                storage_client_data, storage_client_file.name
            )

            self.exec_oc_cmd(f"apply -f {storage_client_file.name}", timeout=120)

        _apply_storage_client_cr()

        if not self.storage_client_exists():
            logger.info("Storage client create Failed")
            return False

        # wait for storage client to Connected
        for sample in TimeoutSampler(
            timeout=storage_client_connected_timeout_min * 60,
            sleep=15,
            func=self.get_storage_client_status,
        ):
            if "Connected" in sample:
                break
            logger.info(f"Storage client status: {sample}")
        else:
            logger.error("Storage client did not reach Connected status in given time")
            return False

        return True

    @kubeconfig_exists_decorator
    def get_storage_client_status(self):
        """
        Check the status of the storage client

        Returns:
            str: status of the storage client
        """
        cmd = (
            f"get {constants.STORAGECLIENTS} storage-client -n {self.namespace_client} | "
            "awk '/storage-client/{{print $2}}'"
        )
        return self.exec_oc_cmd(cmd, shell=True).stdout.decode("utf-8").strip()

    def get_onboarding_key(self):
        """
        Get onboarding key using the private key from the secret

        Returns:
             str: onboarding token key
        """
        secret_ocp_obj = ocp.OCP(
            kind=constants.SECRET, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )

        key = (
            secret_ocp_obj.get(
                resource_name=constants.ONBOARDING_PRIVATE_KEY, out_yaml_format=True
            )
            .get("data")
            .get("key")
        )
        decoded_key = base64.b64decode(key).decode("utf-8").strip()

        if not decoded_key or "BEGIN PRIVATE KEY" not in decoded_key:
            logger.error(
                "Onboarding token could not be generated, secret key is missing or invalid"
            )

        config.AUTH.setdefault("managed_service", {}).setdefault(
            "private_key", decoded_key
        )

        try:
            token = generate_onboarding_token(private_key=decoded_key)
        except Exception as e:
            logger.error(f"Error during onboarding token generation: {e}")
            token = ""

        if len(token) == 0:
            logger.error("ticketgen.sh failed to generate Onboarding token")
        return token

    def get_onboarding_key_ui(self):
        """
        Get onboarding key from UI

        Returns:
            str: onboarding key from Provider UI
        """
        from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator

        storage_clients = PageNavigator().nav_to_storageclients_page()
        onboarding_key = storage_clients.generate_client_onboarding_ticket()

        return onboarding_key

    @kubeconfig_exists_decorator
    def operator_group_exists(self):
        """
        Check if the operator group exists
        Returns:
            bool: True if the operator group exists, False otherwise
        """
        ocp = OCP(
            kind=constants.OPERATOR_GROUP,
            namespace=self.namespace_client,
            cluster_kubeconfig=self.cluster_kubeconfig,
        )
        return ocp.check_resource_existence(
            timeout=self.timeout_check_resources_exist_sec,
            resource_name="openshift-storage-client-operator-group",
            should_exist=True,
        )

    @kubeconfig_exists_decorator
    def create_operator_group(self):
        """
        Create operator group for ODF

        Returns:
            bool: True if the operator group is created, False otherwise
        """
        if self.operator_group_exists():
            logger.info("OperatorGroup already exists")
            return True

        operator_group_data = templating.load_yaml(
            constants.PROVIDER_MODE_OPERATORGROUP
        )

        operator_group_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="operator_group", delete=False
        )
        templating.dump_data_to_temp_yaml(operator_group_data, operator_group_file.name)

        try:
            self.exec_oc_cmd(f"apply -f {operator_group_file.name}", timeout=120)
        except CommandFailed as e:
            logger.error(f"Error during OperatorGroup creation: {e}")
            return False
        return self.operator_group_exists()

    @kubeconfig_exists_decorator
    def catalog_source_exists(self):
        """
        Check if the catalog source exists

        Returns:
            bool: True if the catalog source exists, False otherwise
        """
        ocp = OCP(
            kind=constants.CATSRC,
            namespace=constants.MARKETPLACE_NAMESPACE,
            cluster_kubeconfig=self.cluster_kubeconfig,
        )
        return ocp.check_resource_existence(
            timeout=self.timeout_check_resources_exist_sec,
            resource_name="ocs-catalogsource",
            should_exist=True,
        )

    @kubeconfig_exists_decorator
    def create_catalog_source(self):
        """
        Create catalog source for ODF

        Returns:
            bool: True if the catalog source is created, False otherwise
        """
        if self.catalog_source_exists():
            logger.info("CatalogSource already exists")
            return True

        catalog_source_data = templating.load_yaml(
            constants.PROVIDER_MODE_CATALOGSOURCE
        )

        if not config.ENV_DATA.get("clusters").get(self.name).get("hosted_odf_version"):
            raise ValueError(
                "OCS version is not set in the config file, should be set in format similar to '4.14.5-8'"
                "in the 'hosted_odf_version' key in the 'ENV_DATA.clusters.<name>' section of the config file. "
            )
        if (
            not config.ENV_DATA.get("clusters")
            .get(self.name)
            .get("hosted_odf_registry")
        ):
            raise ValueError(
                "OCS registry is not set in the config file, should be set in format similar to "
                "'quay.io/rhceph-dev/ocs-registry' in the 'hosted_odf_registry' key in the 'ENV_DATA.clusters.<name>' "
                "section of the config file. "
            )

        odf_version = (
            config.ENV_DATA.get("clusters").get(self.name).get("hosted_odf_version")
        )
        odf_registry = (
            config.ENV_DATA.get("clusters").get(self.name).get("hosted_odf_registry")
        )

        logger.info(
            f"ODF version: {odf_version} will be installed on client. Setting up CatalogSource"
        )

        catalog_source_data["spec"]["image"] = f"{odf_registry}:{odf_version}"

        catalog_source_name = catalog_source_data["metadata"]["name"]

        catalog_source_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="catalog_source", delete=False
        )
        templating.dump_data_to_temp_yaml(catalog_source_data, catalog_source_file.name)

        try:
            self.exec_oc_cmd(f"apply -f {catalog_source_file.name}", timeout=120)
        except CommandFailed as e:
            logger.error(f"Error during CatalogSource creation: {e}")
            return False

        ocs_client_catsrc = CatalogSource(
            resource_name=catalog_source_name,
            namespace=constants.MARKETPLACE_NAMESPACE,
            cluster_kubeconfig=self.cluster_kubeconfig,
        )

        try:
            ocs_client_catsrc.wait_for_state("READY")
        except (TimeoutExpiredError, ResourceWrongStatusException) as e:
            logger.error(f"Error during CatalogSource creation: {e}")
            return False

        return self.catalog_source_exists()

    def network_policy_exists(self, namespace):
        """
        Check if the network policy is created

        Returns:
            bool: True if the network policy exists, False otherwise
        """
        ocp = OCP(kind=constants.NETWORK_POLICY, namespace=namespace)
        return ocp.check_resource_existence(
            timeout=self.timeout_check_resources_exist_sec,
            resource_name="openshift-storage-egress",
            should_exist=True,
        )

    @kubeconfig_exists_decorator
    def subscription_exists(self):
        """
        Check if the subscription exists

        Returns:
            bool: True if the subscription exists, False otherwise
        """
        ocp = OCP(
            kind=constants.SUBSCRIPTION_COREOS,
            namespace=self.namespace_client,
            cluster_kubeconfig=self.cluster_kubeconfig,
        )
        return ocp.check_resource_existence(
            timeout=self.timeout_check_resources_exist_sec,
            resource_name="ocs-client-operator",
            should_exist=True,
        )

    @kubeconfig_exists_decorator
    def create_subscription(self):
        """
        Create subscription for ODF

        Returns:
            bool: True if the subscription is created, False otherwise
        """
        if self.subscription_exists():
            logger.info("Subscription already exists")
            return

        subscription_data = templating.load_yaml(constants.PROVIDER_MODE_SUBSCRIPTION)

        # since we are allowed to install N+1 on hosted clusters we can not rely on PackageManifest default channel
        hosted_odf_version = (
            config.ENV_DATA.get("clusters").get(self.name).get("hosted_odf_version")
        )
        if "latest" in hosted_odf_version:
            hosted_odf_version = hosted_odf_version.split("-")[-1]

        subscription_data["spec"]["channel"] = f"stable-{str(hosted_odf_version)}"

        subscription_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="subscription", delete=False
        )
        templating.dump_data_to_temp_yaml(subscription_data, subscription_file.name)

        self.exec_oc_cmd(f"apply -f {subscription_file.name}", timeout=120)

        return self.subscription_exists()

    def get_provider_address(self):
        """
        Get the provider address
        """
        ocp = OCP(namespace=constants.OPENSHIFT_STORAGE_NAMESPACE)
        storage_provider_endpoint = ocp.exec_oc_cmd(
            (
                "get storageclusters.ocs.openshift.io -o jsonpath={'.items[*].status.storageProviderEndpoint'}"
            ),
            out_yaml_format=False,
        )
        logger.info(f"Provider address: {storage_provider_endpoint}")
        return storage_provider_endpoint

    def wait_storage_claim_cephfs(self):
        """
        Wait for storage class claim for CephFS to be created

        Returns:
            bool: True if storage class claim for CephFS is created, False otherwise
        """
        for sample in TimeoutSampler(
            timeout=300,
            sleep=60,
            func=self.storage_claim_exists_cephfs,
        ):
            if sample:
                return True
        return False

    @kubeconfig_exists_decorator
    def storage_claim_exists_cephfs(self):
        """
        Check if storage class claim for CephFS exists

        Returns:
            bool: True if storage class claim for CephFS exists, False otherwise
        """

        hosted_odf_version = (
            config.ENV_DATA.get("clusters").get(self.name).get("hosted_odf_version")
        )
        if "latest" in hosted_odf_version:
            hosted_odf_version = hosted_odf_version.split("-")[-1]

        if get_semantic_version(hosted_odf_version, True) < version.VERSION_4_16:
            ocp = OCP(
                kind=constants.STORAGECLASSCLAIM,
                namespace=self.namespace_client,
                cluster_kubeconfig=self.cluster_kubeconfig,
            )
        else:
            ocp = OCP(
                kind=constants.STORAGECLAIM,
                namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
                cluster_kubeconfig=self.cluster_kubeconfig,
            )

        if hasattr(self, "storage_client_name"):
            storage_claim_name = self.storage_client_name + "-cephfs"
        else:
            storage_claim_name = "storage-client-cephfs"

        return ocp.check_resource_existence(
            timeout=self.timeout_check_resources_exist_sec,
            resource_name=storage_claim_name,
            should_exist=True,
        )

    @kubeconfig_exists_decorator
    def create_storage_claim_cephfs(self):
        """
        Create storage class claim for CephFS

        Returns:
            bool: True if storage class claim for CephFS is created, False otherwise
        """

        if self.storage_claim_exists_cephfs():
            logger.info("Storage class claim for CephFS already exists")
            return True

        storage_class_claim_data = templating.load_yaml(
            constants.PROVIDER_MODE_STORAGE_CLASS_CLAIM_CEPHFS
        )

        storage_class_claim_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="storage_class_claim_cephfs", delete=False
        )
        templating.dump_data_to_temp_yaml(
            storage_class_claim_data, storage_class_claim_file.name
        )

        try:
            self.exec_oc_cmd(f"apply -f {storage_class_claim_file.name}", timeout=120)
        except CommandFailed as e:
            logger.error(f"Error during storage class claim creation: {e}")
            return False

        return self.storage_claim_exists_cephfs()

    def wait_storage_claim_rbd(self):
        """
        Wait for storage class claim for RBD to be created

        Returns:
            bool: True if storage class claim for RBD is created, False otherwise
        """
        for sample in TimeoutSampler(
            timeout=300,
            sleep=60,
            func=self.storage_claim_exists_rbd,
        ):
            if sample:
                return True
        return False

    @kubeconfig_exists_decorator
    def storage_claim_exists_rbd(self):
        """
        Check if storage class claim for RBD exists

        Returns:
             bool: True if storage class claim for RBD exists, False otherwise
        """

        hosted_odf_version = (
            config.ENV_DATA.get("clusters").get(self.name).get("hosted_odf_version")
        )
        if "latest" in hosted_odf_version:
            hosted_odf_version = hosted_odf_version.split("-")[-1]

        if get_semantic_version(hosted_odf_version, True) < version.VERSION_4_16:
            ocp = OCP(
                kind=constants.STORAGECLASSCLAIM,
                namespace=self.namespace_client,
                cluster_kubeconfig=self.cluster_kubeconfig,
            )
        else:
            ocp = OCP(
                kind=constants.STORAGECLAIM,
                namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
                cluster_kubeconfig=self.cluster_kubeconfig,
            )

        if hasattr(self, "storage_client_name"):
            storage_claim_name = self.storage_client_name + "-ceph-rbd"
        else:
            storage_claim_name = "storage-client-ceph-rbd"

        return ocp.check_resource_existence(
            timeout=self.timeout_check_resources_exist_sec,
            resource_name=storage_claim_name,
            should_exist=True,
        )

    @kubeconfig_exists_decorator
    def create_storage_claim_rbd(self):
        """
        Create storage class claim for RBD

        Returns:
            bool: True if storage class claim for RBD is created, False otherwise
        """

        if self.storage_claim_exists_rbd():
            logger.info("Storage class claim for RBD already exists")
            return True

        storage_class_claim_data = templating.load_yaml(
            constants.PROVIDER_MODE_STORAGE_CLASS_CLAIM_RBD
        )

        storage_class_claim_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="storage_class_claim_rbd", delete=False
        )
        templating.dump_data_to_temp_yaml(
            storage_class_claim_data, storage_class_claim_file.name
        )

        try:
            self.exec_oc_cmd(f"apply -f {storage_class_claim_file.name}", timeout=120)
        except CommandFailed as e:
            logger.error(f"Error during storage class claim creation: {e}")
            return False

        return self.storage_claim_exists_rbd()

    @kubeconfig_exists_decorator
    def storage_class_exists(self, sc_name):
        """
        Check if storage class is ready

        Args:
            sc_name: Name of the storage class

        Returns:
            bool: True if storage class is ready, False otherwise
        """
        timeout_min = 5

        ocp = OCP(
            kind=constants.STORAGECLASS,
            namespace=self.namespace_client,
            cluster_kubeconfig=self.cluster_kubeconfig,
        )
        return ocp.check_resource_existence(
            timeout=timeout_min * 60,
            resource_name=sc_name,
            should_exist=True,
        )

    def csi_pods_exist(self):
        """
        Check if the CSI pods exist

        Returns:
            bool: True if the CSI pods exist, False otherwise
        """
        ocp = OCP(
            kind=constants.POD,
            namespace=self.namespace_client,
            cluster_kubeconfig=self.cluster_kubeconfig,
        )
        return ocp.check_resource_existence(
            timeout=self.timeout_check_resources_exist_sec,
            selector="app=csi-cephfsplugin",
            should_exist=True,
        )

    def odf_csv_installed(self):
        """
        Check if ODF CSV is installed at client's namespace

        Returns:
            bool: True if ODF CSV is installed, False otherwise
        """
        sample = TimeoutSampler(
            timeout=self.timeout_wait_csvs_minutes * 60,
            sleep=15,
            func=check_all_csvs_are_succeeded,
            namespace=self.namespace_client,
            cluster_kubeconfig=self.cluster_kubeconfig,
        )
        return sample.wait_for_func_value(value=True)
