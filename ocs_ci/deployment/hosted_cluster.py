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
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.constants import HCI_PROVIDER_CLIENT_PLATFORMS
from ocs_ci.ocs.exceptions import ProviderModeNotFoundException
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.csv import check_all_csvs_are_succeeded
from ocs_ci.ocs.resources.packagemanifest import (
    PackageManifest,
    get_selector_for_ocs_operator,
)
from ocs_ci.ocs.resources.pod import (
    wait_for_pods_to_be_in_statuses_concurrently,
)
from ocs_ci.utility import templating
from ocs_ci.utility.managedservice import generate_onboarding_token
from ocs_ci.utility.utils import exec_cmd, TimeoutSampler

logger = logging.getLogger(__name__)


class DeployClients:
    def __init__(self):
        pass

    def do_deploy(self):
        hypershiftHostedOCP = HypershiftHostedOCP()

        # stage 1 deploy multiple hosted OCP clusters
        cluster_names = hypershiftHostedOCP.deploy_hosted_ocp_clusters()

        # stage 2 verify OCP clusters are ready
        logger.info(
            "Ensure clusters were deployed successfully, wait for them to be ready"
        )
        verification_passed = (
            hypershiftHostedOCP.verify_hosted_ocp_clusters_from_provider()
        )
        if not verification_passed:
            logger.error("\nSome of the clusters are not ready\n")

        # stage 3 download all available kubeconfig files
        logger.info("Download kubeconfig for all clusters")
        kubeconfig_paths = (
            hypershiftHostedOCP.download_hosted_clusters_kubeconfig_files()
        )

        # stage 4 deploy ODF on all hosted clusters if not already deployed
        for cluster_name in cluster_names:
            logger.info(f"Setup ODF client on hosted OCP cluster '{cluster_name}'")
            hosted_odf = HostedODF(cluster_name)
            hosted_odf.do_deploy()

        # stage 5 verify ODF client is installed on all hosted clusters
        odf_installed = []
        for cluster_name in cluster_names:
            logger.info(f"Deploying ODF client on hosted OCP cluster '{cluster_name}'")
            hosted_odf = HostedODF(cluster_name)
            odf_installed.append(hosted_odf.verify_odf_installed())

        # stage 6 setup storage client on all hosted clusters
        client_setup = []
        for cluster_name in cluster_names:
            logger.info(
                f"Setting up Storage client on hosted OCP cluster '{cluster_name}'"
            )
            hosted_odf = HostedODF(cluster_name)
            client_setup.append(hosted_odf.setup_storage_client())

        # stage 7 verify all hosted clusters are ready and print kubeconfig paths
        logger.info("kubeconfig files for all hosted OCP clusters:\n")
        for kubeconfig_path in kubeconfig_paths:
            logger.info(f"kubeconfig path: {kubeconfig_path}\n")

        assert verification_passed, "Some of the hosted OCP clusters are not ready"
        assert all(
            odf_installed
        ), "ODF client was not deployed on all hosted OCP clusters"
        assert all(
            client_setup
        ), "Storage client was not setup on all hosted ODF clusters"

    def deploy_multiple_odf_clients(self):
        """
        Deploy multiple ODF clients on hosted OCP clusters. Method tries to deploy ODF client on all hosted OCP clusters
        If ODF was already deployed on some of the clusters, it will be skipped for those clusters.

        :returns: list of kubeconfig paths for all hosted OCP clusters
        """
        kubeconfig_paths = HyperShiftBase().download_hosted_clusters_kubeconfig_files()

        hosted_cluster_names = get_hosted_cluster_names()

        for cluster_name in hosted_cluster_names:
            logger.info(f"Deploying ODF client on hosted OCP cluster '{cluster_name}'")
            hosted_odf = HostedODF(cluster_name)
            hosted_odf.do_deploy()

        return kubeconfig_paths


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

        if "cluster_names" in config.default_cluster_ctx.ENV_DATA:
            cluster_names_desired = config.default_cluster_ctx.ENV_DATA["cluster_names"]

        if cluster_names_desired:
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
                self.deploy_dependencies(
                    deploy_acm_hub, deploy_cnv, deploy_metallb, download_hcp_binary
                )

                return self.create_kubevirt_OCP_cluster(
                    name=cluster_names_desired_left[-1]
                )
            else:
                logger.info("All desired hosted OCP clusters already exist")
                return None
        else:
            logger.info(
                "\n--- No cluster_paths or cluster_names set to ENV_DATA. "
                "Creating hosted OCP cluster with random name ---\n"
            )
            self.deploy_dependencies(
                deploy_acm_hub, deploy_cnv, deploy_metallb, download_hcp_binary
            )
            return self.create_kubevirt_OCP_cluster()

    def deploy_dependencies(
        self, deploy_acm_hub, deploy_cnv, deploy_metallb, download_hcp_binary
    ):
        """
        Deploy dependencies for hosted OCP cluster
        :param deploy_acm_hub: bool Deploy ACM Hub
        :param deploy_cnv: bool Deploy CNV
        :param deploy_metallb: bool Deploy MetalLB
        :param download_hcp_binary: bool Download HCP binary

        """
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

    def deploy_hosted_ocp_clusters(
        self,
    ):
        """
        Deploy multiple hosted OCP clusters on Provider platform
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

        cluster_names = []
        for i in range(number_of_clusters_to_deploy):

            if i == 0:
                cluster_names.append(
                    self.deploy_ocp(
                        deploy_cnv=True,
                        deploy_acm_hub=True,
                        deploy_metallb=True,
                        download_hcp_binary=True,
                    )
                )
            else:
                cluster_names.append(
                    self.deploy_ocp(
                        deploy_cnv=False,
                        deploy_acm_hub=False,
                        deploy_metallb=False,
                        download_hcp_binary=False,
                    )
                )

        logger.info(f"All deployment jobs have finished: {cluster_names}")
        return cluster_names


class HostedODF:
    def __init__(self, name: str):
        self.namespace_client = constants.OPENSHIFT_STORAGE_CLIENT_NAMESPACE
        self.timeout_check_resources_existence = 6
        self.name = name
        self.cluster_kubeconfig = os.path.expanduser(
            f"{constants.AUTH_PATH_PATTERN.format(name)}/kubeconfig"
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

        return ocp.check_resource_existence(
            timeout=self.timeout_check_resources_existence,
            resource_name=self.namespace_client,
            should_exist=True,
        )

    def apply_network_policy(self):
        """
        Apply network policy to the client namespace. Network policy is created always on Provider side.

        Returns:
            bool: True if network policy is created, False otherwise
        """
        namespace = f"clusters-{self.name}"

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
        """
        Deploy ODF client on hosted OCP cluster
        """
        logger.info(f"Deploying ODF client on hosted OCP cluster '{self.name}'")

        logger.info("Applying network policy")
        self.apply_network_policy()

        logger.info("Creating ODF client namespace")
        self.create_ns()

        logger.info("Creating ODF client operator group")
        self.create_operator_group()

        logger.info("Creating ODF client catalog source")
        self.create_catalog_source()

        logger.info("Creating ODF client subscription")
        self.create_subscription()

    def verify_odf_installed(self):
        """
        Verify ODF client is installed
        :return: bool True if ODF client is installed, False otherwise
        """
        logger.info("Waiting for ODF client to be installed")
        odf_client_installed = self.odf_client_installed()
        if not odf_client_installed:
            logger.error("ODF client not installed or not all CSVs/Pods are ready")
            return False
        else:
            logger.info("ODF client installed successfully")
            return True

    def setup_storage_client(self):
        """
        Setup storage client
        :return: bool True if storage client is setup, False otherwise
        """
        logger.info("Creating storage client")
        self.create_storage_client()
        logger.info("Creating storage class claim cephfs")
        self.create_storage_class_claim_cephfs()
        logger.info("Creating storage class claim rbd")
        self.create_storage_class_claim_rbd()
        logger.info("Verify Storage Class cephfs exists")
        if not self.storage_class_exists(constants.CEPHFILESYSTEM_SC):
            logger.error("Storage Class cephfs does not exist")
            return False
        logger.info("Verify Storage Class rbd exists")
        if not self.storage_class_exists(constants.CEPHBLOCKPOOL_SC):
            logger.error("Storage Class rbd does not exist")
            return False
        return True

    def odf_client_installed(self):
        """
        Check if ODF client is installed

        :returns: True if ODF client is installed, False otherwise
        """
        logger.info("Waiting for ODF client CSV's to be installed")
        timeout_wait_csvs = 10
        timeout_wait_pod = 5

        sample = TimeoutSampler(
            timeout=timeout_wait_csvs * 60,
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

        if not wait_for_pods_to_be_in_statuses_concurrently(
            app_selectors_to_resource_count_list,
            self.namespace_client,
            timeout_wait_pod * 60,
        ):
            logger.error(
                f"ODF client pods with labels {app_selectors_to_resource_count_list} are not running"
            )
            return False
        else:
            logger.info("ODF client pods are running")
            return True

    def storage_client_exists(self):
        """
        Check if the storage client exists
        :return:
        """
        ocp = OCP(
            kind=constants.STORAGECLIENTS,
            namespace=self.namespace_client,
            cluster_kubeconfig=self.cluster_kubeconfig,
        )
        return ocp.check_resource_existence(
            timeout=self.timeout_check_resources_existence,
            resource_name="openshift-storage-client",
            should_exist=True,
        )

    def create_storage_client(self):
        """
        Create storage client
        """

        if self.storage_client_exists():
            logger.info("Storage client already exists")
            return

        storage_client_data = templating.load_yaml(
            constants.PROVIDER_MODE_STORAGE_CLIENT
        )
        storage_client_data["spec"][
            "storageProviderEndpoint"
        ] = self.get_provider_address()

        # onboarding_key = self.get_onboarding_key_ui()
        onboarding_key = self.get_onboarding_key()

        storage_client_data["spec"]["onboardingKey"] = onboarding_key

        storage_client_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="storage_client", delete=False
        )
        templating.dump_data_to_temp_yaml(storage_client_data, storage_client_file.name)

        self.exec_oc_cmd(f"apply -f {storage_client_file.name}", timeout=120)

        return self.storage_client_exists()

    def get_onboarding_key(self):
        """
        Get onboarding key using the private key from the secret
        :return: onboarding token key
        """
        secret_ocp_obj = ocp.OCP(
            kind=constants.SECRET, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        secret_ocp_obj.get(
            resource_name=constants.ONBOARDING_PRIVATE_KEY, out_yaml_format=False
        )
        key = secret_ocp_obj.get().get("data").get("key")

        config.AUTH.setdefault("managed_service", {}).setdefault("private_key", key)
        """
        credentials_dict["AWS_ACCESS_KEY_ID"] = base64.b64decode(
            creds_secret_obj.get("data").get("AWS_ACCESS_KEY_ID")
        ).decode("utf-8")
        """
        token = generate_onboarding_token()
        logger.info(f"Generated onboarding token: {token}")

        return token

    def get_onboarding_key_ui(self):
        """
        Get onboarding key from UI
        :return: str Onboarding key from Provider UI
        """
        from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator

        storage_clients = PageNavigator().nav_to_storageclients_page()
        onboarding_key = storage_clients.generate_client_onboarding_ticket()

        return onboarding_key

    def operator_group_exists(self):
        """
        Check if the operator group exists
        :return:
        """
        ocp = OCP(
            kind=constants.OPERATOR_GROUP,
            namespace=self.namespace_client,
            cluster_kubeconfig=self.cluster_kubeconfig,
        )
        return ocp.check_resource_existence(
            timeout=self.timeout_check_resources_existence,
            resource_name="openshift-storage-client-operator-group",
            should_exist=True,
        )

    def create_operator_group(self):
        """
        Create operator group for ODF
        """
        if self.operator_group_exists():
            logger.info("OperatorGroup already exists")
            return

        operator_group_data = templating.load_yaml(
            constants.PROVIDER_MODE_OPERATORGROUP
        )

        operator_group_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="operator_group", delete=False
        )
        templating.dump_data_to_temp_yaml(operator_group_data, operator_group_file.name)

        self.exec_oc_cmd(f"apply -f {operator_group_file.name}", timeout=120)

        return self.operator_group_exists()

    def catalog_source_exists(self):
        """
        Check if the catalog source exists
        :return:
        """
        ocp = OCP(
            kind=constants.CATSRC,
            namespace=constants.MARKETPLACE_NAMESPACE,
            cluster_kubeconfig=self.cluster_kubeconfig,
        )
        return ocp.check_resource_existence(
            timeout=self.timeout_check_resources_existence,
            resource_name="ocs-catalogsource",
            should_exist=True,
        )

    def create_catalog_source(self):
        """
        Create catalog source for ODF
        """
        if self.catalog_source_exists():
            logger.info("CatalogSource already exists")
            return

        catalog_source_data = templating.load_yaml(
            constants.PROVIDER_MODE_CATALOGSOURCE
        )
        image_placeholder = catalog_source_data["spec"]["image"]

        if not config.ENV_DATA.get("hosted_odf_version"):
            raise ValueError(
                "OCS version is not set in the config file, should be set in format similar to '4.14.5-8'"
                "in the 'hosted_odf_version' key in the 'ENV_DATA' section of the config file. "
                "image will be pulled from the 'quay.io/rhceph-dev/ocs-registry'"
            )

        provider_odf_version = config.ENV_DATA.get("hosted_odf_version")

        logger.info(
            f"ODF version: {provider_odf_version} will be installed on client. Setting up CatalogSource"
        )

        catalog_source_data["spec"]["image"] = image_placeholder.format(
            provider_odf_version
        )

        catalog_source_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="catalog_source", delete=False
        )
        templating.dump_data_to_temp_yaml(catalog_source_data, catalog_source_file.name)

        self.exec_oc_cmd(f"apply -f {catalog_source_file.name}", timeout=120)

        return self.catalog_source_exists()

    def network_policy_created(self, namespace):
        """
        Check if the network policy is created
        :return:
        """
        ocp = OCP(kind=constants.NETWORK_POLICY, namespace=namespace)
        return ocp.check_resource_existence(
            timeout=self.timeout_check_resources_existence,
            resource_name="openshift-storage-egress",
            should_exist=True,
        )

    def subscription_exists(self):
        """
        Check if the subscription exists
        :return:
        """
        ocp = OCP(
            kind=constants.SUBSCRIPTION_COREOS,
            namespace=self.namespace_client,
            cluster_kubeconfig=self.cluster_kubeconfig,
        )
        return ocp.check_resource_existence(
            timeout=self.timeout_check_resources_existence,
            resource_name="ocs-client-operator",
            should_exist=True,
        )

    def create_subscription(self):
        """
        Create subscription for ODF
        """
        if self.subscription_exists():
            logger.info("Subscription already exists")
            return

        subscription_data = templating.load_yaml(constants.PROVIDER_MODE_SUBSCRIPTION)

        default_channel = PackageManifest(
            resource_name=constants.OCS_CLIENT_OPERATOR,
            selector=get_selector_for_ocs_operator(),
        ).get_default_channel()

        subscription_data["spec"]["channel"] = default_channel

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

    def storage_class_claim_exists_cephfs(self):
        """
        Check if storage class claim for CephFS exists
        :return: True if storage class claim for CephFS exists, False otherwise
        """
        ocp = OCP(
            kind=constants.STORAGECLASSCLAIM,
            namespace=self.namespace_client,
            cluster_kubeconfig=self.cluster_kubeconfig,
        )
        return ocp.check_resource_existence(
            timeout=self.timeout_check_resources_existence,
            resource_name="ocs-storagecluster-cephfs",
            should_exist=True,
        )

    def create_storage_class_claim_cephfs(self):
        """
        Create storage class claim for CephFS
        """

        if self.storage_class_claim_exists_cephfs():
            logger.info("Storage class claim for CephFS already exists")
            return

        storage_class_claim_data = templating.load_yaml(
            constants.PROVIDER_MODE_STORAGE_CLASS_CLAIM_CEPHFS
        )

        storage_class_claim_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="storage_class_claim_cephfs", delete=False
        )
        templating.dump_data_to_temp_yaml(
            storage_class_claim_data, storage_class_claim_file.name
        )

        self.exec_oc_cmd(f"apply -f {storage_class_claim_file.name}", timeout=120)

        return self.storage_class_claim_exists_cephfs()

    def storage_class_claim_exists_rbd(self):
        """
        Check if storage class claim for RBD exists
        :return: True if storage class claim for RBD exists, False otherwise
        """
        ocp = OCP(
            kind=constants.STORAGECLASSCLAIM,
            namespace=self.namespace_client,
            cluster_kubeconfig=self.cluster_kubeconfig,
        )
        return ocp.check_resource_existence(
            timeout=self.timeout_check_resources_existence,
            resource_name="ocs-storagecluster-ceph-rbd",
            should_exist=True,
        )

    def create_storage_class_claim_rbd(self):
        """
        Create storage class claim for RBD
        """

        if self.storage_class_claim_exists_rbd():
            logger.info("Storage class claim for RBD already exists")
            return

        storage_class_claim_data = templating.load_yaml(
            constants.PROVIDER_MODE_STORAGE_CLASS_CLAIM_RBD
        )

        storage_class_claim_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="storage_class_claim_rbd", delete=False
        )
        templating.dump_data_to_temp_yaml(
            storage_class_claim_data, storage_class_claim_file.name
        )

        self.exec_oc_cmd(f"apply -f {storage_class_claim_file.name}", timeout=120)

        return self.storage_class_claim_exists_rbd()

    def storage_class_exists(self, sc_name):
        """
        Check if storage class is ready
        :param sc_name: Name of the storage class
        :return: True if storage class is ready, False otherwise
        """
        ocp = OCP(
            kind=constants.STORAGECLASS,
            namespace=self.namespace_client,
            cluster_kubeconfig=self.cluster_kubeconfig,
        )
        return ocp.check_resource_existence(
            timeout=self.timeout_check_resources_existence,
            resource_name=sc_name,
            should_exist=True,
        )
