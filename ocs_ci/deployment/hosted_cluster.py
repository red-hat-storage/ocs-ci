import base64
import json
import logging
import os
import tempfile
import time
import yaml
import copy
from concurrent.futures import ThreadPoolExecutor

from ocs_ci.deployment.cnv import CNVInstaller
from ocs_ci.deployment.hyperconverged import HyperConverged
from ocs_ci.deployment.mce import MCEInstaller
from ocs_ci.deployment.deployment import Deployment
from ocs_ci.deployment.helpers.hypershift_base import (
    HyperShiftBase,
    get_hosted_cluster_names,
    kubeconfig_exists_decorator,
    get_current_nodepool_size,
    get_available_hosted_clusters_to_ocp_ver_dict,
    create_cluster_dir,
)
from ocs_ci.deployment.metallb import MetalLBInstaller
from ocs_ci.framework.logger_helper import log_step
from ocs_ci.framework import config as ocsci_config, Config, config
from ocs_ci.helpers import helpers
from ocs_ci.helpers.helpers import get_cephfs_subvolumegroup_names
from ocs_ci.ocs import constants, defaults, ocp
from ocs_ci.ocs.constants import HCI_PROVIDER_CLIENT_PLATFORMS, FUSION_CONF_DIR
from ocs_ci.ocs.exceptions import (
    ProviderModeNotFoundException,
    CommandFailed,
    TimeoutExpiredError,
    ResourceWrongStatusException,
    UnexpectedDeploymentConfiguration,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.rados_utils import (
    fetch_pool_names,
    fetch_rados_namespaces,
    fetch_filesystem_names,
)
from ocs_ci.ocs.resources import storage_cluster
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs.resources.csv import check_all_csvs_are_succeeded
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.resources.pod import (
    wait_for_pods_to_be_in_statuses_concurrently,
    wait_for_pods_to_be_running,
    get_pod_logs,
)
from ocs_ci.ocs.resources.storageconsumer import (
    create_storage_consumer_on_default_cluster,
    check_consumers_rns,
    check_consumers_svg,
    check_consumer_rns,
    get_ready_consumers_names,
    check_consumer_svg,
    verify_storage_consumer_resources,
)
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.ocs.version import if_version
from ocs_ci.utility import templating, version
from ocs_ci.utility.deployment import get_ocp_ga_version
from ocs_ci.utility.json import SetToListJSONEncoder
from ocs_ci.utility.managedservice import generate_onboarding_token
from ocs_ci.utility.retry import retry, catch_exceptions
from ocs_ci.utility.utils import (
    exec_cmd,
    TimeoutSampler,
)
from ocs_ci.ocs.resources.storage_client import StorageClient
from ocs_ci.utility.version import get_running_odf_version
from ocs_ci.utility.ssl_certs import (
    create_ocs_ca_bundle,
    get_root_ca_cert,
)

logger = logging.getLogger(__name__)


@if_version(">4.17")
@catch_exceptions((CommandFailed, TimeoutExpiredError))
def apply_hosted_cluster_mirrors_max_items_wa():
    """
    Apply workaround for MCE mirrors max items issue.
    This workaround is needed to avoid the error:
    "The number of items in the mirrors list exceeds the maximum allowed limit of 25"
    """
    logger.warning(
        "!!! Workaround for OCPBUGS-57957: apply MCE mirrors max items workaround !!!"
    )
    logger.warning("!!! Remove when resolved !!!")
    ocp_obj = OCP(kind=constants.CRD_KIND)
    params = (
        '[{"op": "replace", '
        '"path": "/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/imageContentSources/items'
        '/properties/mirrors/maxItems",'
        '"value": 255}]'
    )
    ocp_obj.patch(
        resource_name=constants.HOSTED_CLUSTERS_CRD_NAME,
        params=params,
        format_type="json",
    )


@if_version(">4.17")
@catch_exceptions((CommandFailed, TimeoutExpiredError))
def apply_hosted_control_plane_mirrors_max_items_wa():
    """
    Apply workaround for Hosted Control Plane mirrors max items issue.
    This workaround is needed to avoid the error:
    "The number of items in the mirrors list exceeds the maximum allowed limit of 25"
    """
    logger.warning(
        "!!! Workaround for OCPBUGS-56015: apply Hosted Control Plane mirrors max items workaround !!!"
    )
    logger.warning("!!! Remove when resolved !!!")
    ocp_obj = OCP(kind=constants.CRD_KIND)
    patch_paths = [
        "/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/imageContentSources/items/properties"
        "/mirrors/maxItems",
        "/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/imageContentSources/maxItems",
    ]
    for path in patch_paths:
        params = f'[{{"op": "replace", "path": "{path}", "value": 255}}]'
        ocp_obj.patch(
            resource_name=constants.HOSTED_CONTROL_PLANE_CRD_NAME,
            params=params,
            format_type="json",
        )


def apply_cluster_roles_wa(cluster_names):
    logger.warning(
        "!!! Workaround for OCPBUGS-56015: apply cluster roles to all hosted clusters !!!"
    )
    logger.warning("!!! Remove when resolved !!!")
    rbac_wa_file = os.path.join(
        constants.TEMPLATE_DIR, "hosted-cluster", "rbac-wa.yaml"
    )
    rbac_wa_data = templating.load_yaml(rbac_wa_file, multi_document=True)
    for cluster_name in cluster_names:
        # Deep copy the original data to avoid modifying it for subsequent clusters
        cluster_rbac_data = [copy.deepcopy(doc) for doc in rbac_wa_data]

        # Modify each document as needed
        for doc in cluster_rbac_data:
            if "namespace" in doc.get("metadata", {}):
                doc["metadata"]["namespace"] = doc["metadata"]["namespace"].format(
                    cluster_name
                )

            if doc.get("kind") == "RoleBinding":
                for subject in doc.get("subjects", []):
                    if "namespace" in subject:
                        subject["namespace"] = subject["namespace"].format(cluster_name)

        # Create a temporary file for the modified YAML
        rbac_wa_file_modified_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="rbac_wa_modified", delete=False
        )
        templating.dump_data_to_temp_yaml(
            cluster_rbac_data, rbac_wa_file_modified_file.name
        )
        try:
            exec_cmd(
                f"oc create -f {format(rbac_wa_file_modified_file.name)}",
                shell=True,
                silent=True,
            )
        except CommandFailed:
            logger.warning("rbac w/a already exist")


@if_version(">4.18")
def verify_backing_ceph_storage_for_clients():
    """
    Verify that backing Ceph storage classes exist on the Provider cluster

    Returns:
        bool: True if all checks passed, False otherwise
    """

    all_checks = [check_consumers_svg(), check_consumers_rns()]
    return all(all_checks)


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
        2. Download kubeconfig files
        3. Verify OCP clusters are ready
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

        # stage 2 download all available kubeconfig files
        log_step("Download kubeconfig for all clusters")
        kubeconfig_paths = self.download_hosted_clusters_kubeconfig_files()

        # stage 3 verify OCP clusters are ready
        log_step(
            "Ensure clusters were deployed successfully, wait for them to be ready"
        )
        hosted_ocp_verification_passed = self.verify_hosted_ocp_clusters_from_provider()
        if not hosted_ocp_verification_passed:
            logger.error("\n\n*** Some of the clusters are not ready ***\n")

            apply_cluster_roles_wa(cluster_names)

            logger.warning("Going through the verification process again")
            hosted_ocp_verification_passed = (
                self.verify_hosted_ocp_clusters_from_provider()
            )

        # configure proxy object with trusted ca bundle for custom ingress ssl certificate
        if config.DEPLOYMENT.get("use_custom_ingress_ssl_cert"):
            ssl_ca_cert = get_root_ca_cert()
            ocs_ca_bundle_name = "ocs-ca-bundle"
            create_ocs_ca_bundle(ssl_ca_cert, ocs_ca_bundle_name, namespace="clusters")
            patch = f'{{"spec":{{"configuration":{{"proxy":{{"trustedCA":{{"name":"{ocs_ca_bundle_name}"}}}}}}}}}}'
            if ssl_ca_cert:
                for cluster_name in cluster_names:
                    cmd = (
                        f"oc patch -n clusters {constants.HOSTED_CLUSTERS}/{cluster_name} --type=merge "
                        f"--patch='{patch}'"
                    )
                    exec_cmd(cmd)

        # Need to create networkpolicy as mentioned in bug 2281536,
        # https://bugzilla.redhat.com/show_bug.cgi?id=2281536#c21

        # Create Network Policy
        storage_client = StorageClient()
        for cluster_name in cluster_names:
            storage_client.create_network_policy(
                namespace_to_create_storage_client=f"clusters-{cluster_name}"
            )

        self.check_odf_prerequisites()

        # stage 4 deploy ODF on all hosted clusters if not already deployed
        log_step("Deploy ODF client on hosted OCP clusters")
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
        log_step("Verify ODF client is installed on all hosted OCP clusters")
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

        # stage 6 setup storage client on all requested hosted clusters
        log_step("Setup storage client on hosted OCP clusters")
        client_setup_res = []
        hosted_odf_clusters_installed = []
        for cluster_name in cluster_names:
            if self.storage_installation_requested(cluster_name):
                logger.info(
                    f"Setting up Storage client on hosted OCP cluster '{cluster_name}'"
                )
                hosted_odf = HostedODF(cluster_name)
                if (
                    version.get_semantic_ocs_version_from_config()
                    < version.VERSION_4_19
                ):
                    client_installed = hosted_odf.setup_storage_client()
                else:
                    start_time = time.time()
                    client_installed = hosted_odf.setup_storage_client_converged(
                        storage_consumer_name=f"{constants.STORAGECONSUMER_NAME_PREFIX}{cluster_name}"
                    )
                    time_taken = time.time() - start_time
                    time_sec = int(time_taken % 60) + 1
                    provider_server_pod = get_pod_name_by_pattern(
                        "ocs-provider-server"
                    )[0]
                    logs = get_pod_logs(
                        pod_name=provider_server_pod, since=f"{time_sec}s"
                    )
                    logger.info(
                        f"Logs from provider-server pod:\n******************\n{logs}\n******************\n"
                    )
                client_setup_res.append(client_installed)
                if client_installed:
                    hosted_odf_clusters_installed.append(hosted_odf)
                    logger.info("enable client console plugin")
                    if not hosted_odf.enable_client_console_plugin():
                        # we may want to skip UI tests for this client in the future, setting config value to skip UI
                        logger.error("Client console plugin enable failed")
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

        log_step("Verify storage is available on all hosted ODF clusters")
        hosted_odf_storage_verified = []
        hosted_odf_storage_verified.extend(
            self.verify_client_cluster_storage(name)
            for name in cluster_names
            if self.storage_installation_requested(name)
        )

        log_step("storage consumers and configmaps for newly deployed clients")
        storage_consumers_verified = []
        for cluster_name in hosted_odf_clusters_installed:
            try:
                verify_storage_consumer_resources(
                    f"{constants.STORAGECONSUMER_NAME_PREFIX}{cluster_name}"
                )
                storage_consumers_verified.append(True)
            except Exception as e:
                logger.error(
                    f"Storage consumer resources verification failed for cluster {cluster_name}: {e}"
                )
                storage_consumers_verified.append(False)

        log_step("verify backing Ceph storage for newly deployed clients")

        consumer_names = get_ready_consumers_names()
        # we want to validate only consumers that are in ready status, that are newly deployed
        # and storage installation for them was requested from ENV_DATA.clusters.<cluster_name>.setup_storage_client
        consumers_to_validate = [
            consumer_name
            for consumer_name in consumer_names
            if any(
                [
                    cluster_name
                    for cluster_name in cluster_names
                    if (
                        (cluster_name in consumer_name)
                        and self.storage_installation_requested(cluster_name)
                    )
                ]
            )
        ]

        logger.info(
            f"Consumers to validate: {consumers_to_validate} "
            f"from all consumers: {consumer_names}"
        )
        pool_names = fetch_pool_names()
        rados_namespaces = fetch_rados_namespaces()
        svg_names = get_cephfs_subvolumegroup_names()
        filesystems = fetch_filesystem_names()
        rns_for_consumer_verified = []
        svg_for_consumer_verified = []
        for consumer in consumers_to_validate:
            consumer_rns_verified = check_consumer_rns(
                consumer, pool_names, rados_namespaces
            )
            consumer_svg_verified = check_consumer_svg(consumer, filesystems, svg_names)
            rns_for_consumer_verified.append(consumer_rns_verified)
            svg_for_consumer_verified.append(consumer_svg_verified)

        assert (
            hosted_ocp_verification_passed
        ), "Some of the hosted OCP clusters are not ready"
        assert all(
            odf_installed
        ), "ODF client was not deployed on all hosted OCP clusters"
        assert all(
            client_setup_res
        ), "Storage client was not set up on all hosted ODF clusters"
        assert all(
            hosted_odf_storage_verified
        ), "Storage is not available on all hosted ODF clusters"
        assert all(
            rns_for_consumer_verified
        ), "RNS for consumers of deployed clusters failed verification"
        assert all(
            svg_for_consumer_verified
        ), "SVG for consumers of deployed clusters failed verification"
        assert all(
            storage_consumers_verified
        ), "Storage consumer resources verification failed for some of the clusters"

        return hosted_odf_clusters_installed

    def verify_client_cluster_storage(self, cluster_name):
        """
        Verify storage connectivity for a single cluster by checking storage class existence

        Args:
            cluster_name (str): Name of the cluster to verify

        Returns:
            bool: True if storage classes exist and are properly configured, False otherwise
        """
        hosted_odf = HostedODF(cluster_name)
        # starting from ODF 4.16 on StorageClient creation Storage Claims created automatically
        # StorageClassClaims are deprecated from ODF 4.16 in favor of StorageClaims
        # StorageClaims are deprecated from ODF 4.19 and data from CR available in StorageClient
        if version.get_semantic_ocs_version_from_config() < version.VERSION_4_19:
            has_cephfs_claim = hosted_odf.wait_storage_claim_cephfs()
            has_rbd_claim = hosted_odf.wait_storage_claim_rbd()
            if not has_cephfs_claim:
                logger.error(
                    f"Storage class claim cephfs does not exist for cluster {cluster_name}"
                )
                return False
            if not has_rbd_claim:
                logger.error(
                    f"Storage class claim rbd does not exist for cluster {cluster_name}"
                )
                return False

        logger.info(f"Verify Storage Classes exist for cluster {cluster_name}")
        cephfs_storage_class_name = f"{hosted_odf.storage_client_name}-cephfs"
        rbd_storage_class_name = f"{hosted_odf.storage_client_name}-ceph-rbd"

        if not hosted_odf.storage_class_exists(cephfs_storage_class_name):
            logger.error(
                f"CephFS storage class does not exist for cluster {cluster_name}"
            )
            return False

        if not hosted_odf.storage_class_exists(rbd_storage_class_name):
            logger.error(f"RBD storage class does not exist for cluster {cluster_name}")
            return False

        return True

    def check_odf_prerequisites(self):
        """
        Check prerequisites for ODF installation and Client cluster connection
        """
        # Storage Cluster resource of hub cluster should have hostNetwork set to true
        # If hostNetwork is true, then providerAPIServerServiceType is set to NodePort automatically

        sc = storage_cluster.get_storage_cluster()
        sc_spec = sc.get()["items"][0]["spec"]
        if sc_spec.get("hostNetwork"):
            logger.info(
                "Storage Cluster resource of hub cluster has hostNetwork set to true"
            )
            if sc_spec.get("providerAPIServerServiceType") == "NodePort":
                logger.info(
                    "Storage Cluster resource of hub cluster has providerAPIServerServiceType set to NodePort"
                )
                return
            else:
                raise AssertionError(
                    "Storage Cluster resource of hub cluster has providerAPIServerServiceType not set to NodePort"
                )
        else:
            raise AssertionError(
                "Storage Cluster resource of hub cluster has hostNetwork not set to true"
            )

    def config_has_hosted_odf_image(self, cluster_name):
        """
        Check if the config has hosted ODF image set for the cluster

        Args:
            cluster_name:

        Returns:
            bool: True if the config has hosted ODF image, False otherwise

        """
        version_exists = (
            config.ENV_DATA.get("clusters")
            .get(cluster_name)
            .get("hosted_odf_version", False)
        )

        return version_exists

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

            # Put logic on checking and deploying dependencies here
            if first_ocp_deployment:
                # ACM installation is a default for Provider/Converged deployments
                deploy_acm_hub = config.ENV_DATA.get("deploy_acm_hub_cluster", False)
                # CNV installation is a default for Provider/Converged deployments
                deploy_cnv = config.DEPLOYMENT.get("cnv_deployment", False)
                deploy_mce = config.DEPLOYMENT.get("deploy_mce", False)
                deploy_hyperconverged = config.ENV_DATA.get(
                    "deploy_hyperconverged", False
                )

                # Validate conflicting deployments
                if deploy_acm_hub and deploy_mce:
                    raise UnexpectedDeploymentConfiguration(
                        "Conflict: Both 'deploy_acm_hub_cluster' and 'deploy_mce' are enabled. Choose one."
                    )
                if deploy_cnv and deploy_hyperconverged:
                    raise UnexpectedDeploymentConfiguration(
                        "Conflict: Both 'cnv_deployment' and 'deploy_hyperconverged' are enabled. Choose one."
                    )

            else:
                deploy_acm_hub = False
                deploy_cnv = False
                deploy_hyperconverged = False
                deploy_mce = False

            if not config.ENV_DATA["platform"].lower() in HCI_PROVIDER_CLIENT_PLATFORMS:
                raise ProviderModeNotFoundException()

            hosted_ocp_cluster.deploy_dependencies(
                deploy_acm_hub=deploy_acm_hub,
                deploy_cnv=deploy_cnv,
                deploy_metallb=first_ocp_deployment,
                download_hcp_binary=first_ocp_deployment,
                deploy_hyperconverged=deploy_hyperconverged,
                deploy_mce=deploy_mce,
            )

            cluster_name = hosted_ocp_cluster.deploy_ocp()
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

    def download_hosted_clusters_kubeconfig_files(self, cluster_names_paths_dict=None):
        """
        Get HyperShift hosted cluster kubeconfig for multiple clusters.
        Provided cluster_names_paths_dict will always be a default source of cluster names and paths

        Args:
            cluster_names_paths_dict (dict): Optional argument. The function will download all kubeconfigs
            to the folders specified in the configuration, or download a specific cluster's kubeconfig
            to the folder provided as an argument.

        Returns:
            list: the list of hosted cluster kubeconfig paths
        """

        if cluster_names_paths_dict is None:
            cluster_names_paths_dict = dict()
        if not (self.hcp_binary_exists() and self.hypershift_binary_exists()):
            self.update_hcp_binary()

        cluster_names = (
            list(cluster_names_paths_dict.keys())
            if cluster_names_paths_dict
            else list(config.ENV_DATA.get("clusters", {}).keys())
        )

        for name in cluster_names:
            path = cluster_names_paths_dict.get(name) or config.ENV_DATA.setdefault(
                "clusters", {}
            ).setdefault(name, {}).get("hosted_cluster_path")

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


class HypershiftHostedOCP(
    HyperShiftBase,
    MetalLBInstaller,
    CNVInstaller,
    Deployment,
    MCEInstaller,
    HyperConverged,
):
    def __init__(self, name):
        Deployment.__init__(self)
        HyperShiftBase.__init__(self)
        MetalLBInstaller.__init__(self)
        CNVInstaller.__init__(self)
        MCEInstaller.__init__(self)
        HyperConverged.__init__(self)
        self.name = name
        if config.ENV_DATA.get("clusters", {}).get(self.name):
            cluster_path = (
                config.ENV_DATA["clusters"].get(self.name).get("hosted_cluster_path")
            )
            self.cluster_kubeconfig = os.path.expanduser(
                os.path.join(cluster_path, "auth", "kubeconfig")
            )
        else:
            # avoid throwing an exception if the cluster path is not found for some reason
            # this way we can continue with the next cluster
            logger.error(
                f"Cluster path for desired cluster with name '{self.name}' was not found in ENV_DATA.clusters"
            )

    def deploy_ocp(self, **kwargs) -> str:
        """
        Deploy hosted OCP cluster on provisioned Provider platform

        Args:
            **kwargs: Additional arguments for create_kubevirt_ocp_cluster (currently not in use)

        Returns:
            str: Name of the hosted cluster
        """
        ocp_version = str(config.ENV_DATA["clusters"][self.name].get("ocp_version"))
        if ocp_version and len(ocp_version.split(".")) == 2:
            # if ocp_version is provided in form x.y, we need to get the full form x.y.z
            ocp_version = get_ocp_ga_version(ocp_version)
        # use default value 6 for cpu_cores_per_hosted_cluster as used in create_kubevirt_ocp_cluster()
        cpu_cores_per_hosted_cluster = (
            config.ENV_DATA["clusters"]
            .get(self.name)
            .get("cpu_cores_per_hosted_cluster", defaults.HYPERSHIFT_CPU_CORES_DEFAULT)
        )
        # use default value 12Gi for memory_per_hosted_cluster as used in create_kubevirt_ocp_cluster()
        memory_per_hosted_cluster = (
            config.ENV_DATA["clusters"]
            .get(self.name)
            .get("memory_per_hosted_cluster", defaults.HYPERSHIFT_MEMORY_DEFAULT)
        )
        # use default value 2 for nodepool_replicas as used in create_kubevirt_ocp_cluster()
        nodepool_replicas = (
            config.ENV_DATA["clusters"]
            .get(self.name)
            .get("nodepool_replicas", defaults.HYPERSHIFT_NODEPOOL_REPLICAS_DEFAULT)
        )
        cp_availability_policy = (
            config.ENV_DATA["clusters"]
            .get(self.name)
            .get("cp_availability_policy", constants.AVAILABILITY_POLICY_HA)
        )
        infra_availability_policy = (
            config.ENV_DATA["clusters"]
            .get(self.name)
            .get("infra_availability_policy", constants.AVAILABILITY_POLICY_HA)
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
            infra_availability_policy=infra_availability_policy,
            disable_default_sources=disable_default_sources,
        )

    def deploy_dependencies(
        self,
        deploy_acm_hub,
        deploy_cnv,
        deploy_metallb,
        download_hcp_binary,
        deploy_mce,
        deploy_hyperconverged,
    ):
        """
        Deploy dependencies for hosted OCP cluster
        Args:
            deploy_acm_hub: bool Deploy ACM Hub
            deploy_cnv: bool Deploy CNV
            deploy_metallb: bool Deploy MetalLB
            download_hcp_binary: bool Download HCP binary
            deploy_mce: bool Deploy mce
            deploy_hyperconverged: bool Deploy Hyperconverged

        """

        # log out all args in one log.info
        logger.info(
            f"Deploying dependencies for hosted OCP cluster '{self.name}': "
            f"deploy_acm_hub={deploy_acm_hub}, deploy_cnv={deploy_cnv}, "
            f"deploy_metallb={deploy_metallb}, download_hcp_binary={download_hcp_binary}, "
            f"deploy_mce={deploy_mce}, deploy_hyperconverged={deploy_hyperconverged}"
        )
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

        if deploy_cnv and not deploy_hyperconverged:
            self.deploy_cnv(check_cnv_ready=True)
        elif deploy_hyperconverged and not deploy_cnv:
            self.deploy_hyperconverged()
        elif deploy_cnv and deploy_hyperconverged:
            raise UnexpectedDeploymentConfiguration(
                "Both deploy_cnv and deploy_hyperconverged are set to True. "
                "Please choose only one of them."
            )

        if deploy_acm_hub and not deploy_mce:
            self.deploy_acm_hub()
        elif deploy_mce and not deploy_acm_hub:
            self.deploy_mce()
        elif deploy_acm_hub and deploy_mce:
            raise UnexpectedDeploymentConfiguration(
                "Both deploy_acm_hub and deploy_mce are set to True. "
                "Please choose only one of them."
            )

        logger.info("Correct max items in hostedclsuters crd")
        apply_hosted_cluster_mirrors_max_items_wa()

        logger.info("Correct max items in hostedcontrolplane crd")
        apply_hosted_control_plane_mirrors_max_items_wa()

        if deploy_metallb:
            self.deploy_lb()
        if download_hcp_binary:
            self.update_hcp_binary()

        # Enable central infrastructure management service for agent
        if config.DEPLOYMENT.get("hosted_cluster_platform") == "agent":
            provisioning_obj = OCS(
                **OCP(kind=constants.PROVISIONING).get().get("items")[0]
            )
            if not provisioning_obj.data["spec"].get("watchAllNamespaces"):
                provisioning_obj.ocp.patch(
                    resource_name=provisioning_obj.name,
                    params='{"spec":{"watchAllNamespaces": true }}',
                    format_type="merge",
                )
                assert provisioning_obj.get()["spec"].get(
                    "watchAllNamespaces"
                ), "Cannot proceed with hosted cluster creation using agent."

            if not len(
                OCP(kind=constants.AGENT_SERVICE_CONFIG).get(dont_raise=True)["items"]
            ):
                create_agent_service_config()
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
            {"app=assisted-service": 1},
            {"app=assisted-image-service": 1},
        ],
        namespace="multicluster-engine",
        timeout=600,
        status=constants.STATUS_RUNNING,
    )
    logger.info("Created AgentServiceConfig.")


def create_host_inventory():
    """
    Create InfraEnv resource for host inventory

    """
    # Create InfraEnv
    template_yaml = os.path.join(
        constants.TEMPLATE_DIR, "hosted-cluster", "infra-env.yaml"
    )
    infra_env_data = templating.load_yaml(file=template_yaml, multi_document=True)
    ssh_pub_file_path = os.path.expanduser(config.DEPLOYMENT["ssh_key"])
    with open(ssh_pub_file_path, "r") as ssh_key:
        ssh_pub_key = ssh_key.read().strip()
    # TODO: Add custom OS image details. Reference https://access.redhat.com/documentation/en-us/red_hat_advanced_
    #  cluster_management_for_kubernetes/2.10/html-single/clusters/index#create-host-inventory-cli-steps
    for data in infra_env_data:
        if data["kind"] == constants.INFRA_ENV:
            data["spec"]["sshAuthorizedKey"] = ssh_pub_key
            infra_env_namespace = data["metadata"]["namespace"]
            # Create project
            helpers.create_project(project_name=infra_env_namespace)
            # Create new secret in the namespace using the existing secret
            secret_obj = OCP(
                kind=constants.SECRET,
                resource_name="pull-secret",
                namespace=constants.OPENSHIFT_CONFIG_NAMESPACE,
            )
            secret_info = secret_obj.get()
            secret_data = templating.load_yaml(constants.OCS_SECRET_YAML)
            secret_data["data"][".dockerconfigjson"] = secret_info["data"][
                ".dockerconfigjson"
            ]
            secret_data["metadata"]["namespace"] = infra_env_namespace
            secret_data["metadata"]["name"] = "pull-secret"
            secret_manifest = tempfile.NamedTemporaryFile(
                mode="w+", prefix="pull_secret", delete=False
            )
            templating.dump_data_to_temp_yaml(secret_data, secret_manifest.name)
            exec_cmd(cmd=f"oc create -f {secret_manifest.name}")
        helpers.create_resource(**data)
    logger.info("Created InfraEnv.")


def get_onboarding_token_from_secret(secret_name):
    """
    Get onboarding token from the secret

    Args:
        secret_name (str): Name of the secret

    Returns:
        str: Onboarding token
    """
    ocp_obj = OCP(
        kind="secret",
        namespace=config.ENV_DATA["cluster_namespace"],
        resource_name=secret_name,
    )
    secret_obj = ocp_obj.get(retry=6, wait=10, silent=True)
    return secret_obj.get("data", {}).get("onboarding-token")


def get_autodistributed_storage_classes():
    """
    Get the list of StorageClasses that were provisioned by ODF and should be auto-distributed

    Returns:
        list: List of StorageClass names that were provisioned by ODF

    """

    storage_class = OCP(
        kind=constants.STORAGECLASS, namespace=config.ENV_DATA["cluster_namespace"]
    )
    storage_classes = storage_class.get()
    # filter only those that were provisioned by ODF
    storage_classes["items"] = [
        item
        for item in storage_classes["items"]
        if item["provisioner"]
        in [
            constants.RBD_PROVISIONER,
            constants.CEPHFS_PROVISIONER,
        ]
    ]
    # filter out virtualization storage class. We supposed to have it on vSphere and BM, where CRD created
    storage_classes["items"] = [
        item
        for item in storage_classes["items"]
        if item["metadata"]["name"] != constants.DEFAULT_STORAGECLASS_VIRTUALIZATION
    ]
    storage_class_names = [
        item["metadata"]["name"] for item in storage_classes["items"]
    ]
    return storage_class_names


def get_autodistributed_volume_snapshot_classes():
    """
    Get the list of VolumeSnapshotClasses that were provisioned by ODF and should be auto-distributed
    upon client connection

    Returns:
        list: List of VolumeSnapshotClass names that were provisioned by ODF

    """
    snapshot_class = OCP(
        kind=constants.VOLUMESNAPSHOTCLASS,
        namespace=config.ENV_DATA["cluster_namespace"],
    )
    snapshot_classes = snapshot_class.get()
    # filter only those that were provisioned by ODF
    snapshot_classes["items"] = [
        item
        for item in snapshot_classes["items"]
        if item["driver"]
        in [
            constants.RBD_PROVISIONER,
            constants.CEPHFS_PROVISIONER,
        ]
    ]
    snapshot_class_names = [
        item["metadata"]["name"] for item in snapshot_classes["items"]
    ]
    return snapshot_class_names


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
        self.storage_quota = (
            config.ENV_DATA.get("clusters", {})
            .get(self.name, {})
            .get("storage_quota", None)
        )

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
            # create_storage_client func will generate onboarding token for versions bellow 4.19
            storage_client_created = self.create_storage_client()
        except TimeoutExpiredError as e:
            logger.error(f"Error during storage client creation: {e}")
            storage_client_created = False

        # if storage client is not created, there is no point in continuing
        if not storage_client_created:
            logger.error("storage client is not ready; abort further steps")
            return False

        return True

    @if_version(">4.18")
    def setup_storage_client_converged(self, storage_consumer_name):
        """
        Setup storage client for converged cluster

        Returns:
            bool: True if storage client is setup, False otherwise
        """

        log_step("Creating storage consumer")

        storage_class_names = get_autodistributed_storage_classes()
        volumesnapshot_class_names = get_autodistributed_volume_snapshot_classes()

        storage_consumer_obj = create_storage_consumer_on_default_cluster(
            storage_consumer_name,
            storage_classes=storage_class_names,
            volume_snapshot_classes=volumesnapshot_class_names,
        )
        secret_name = storage_consumer_obj.get_onboarding_ticket_secret()

        log_step("Getting onboarding key from secret")
        onboarding_key = get_onboarding_token_from_secret(secret_name)
        if not onboarding_key:
            logger.error(f"Onboarding key not found in secret {secret_name}")
            return False

        onboarding_key_decrypted = base64.b64decode(onboarding_key).decode("utf-8")

        log_step("Creating storage client")
        try:
            storage_client_created = self.create_storage_client(
                onboarding_key_decrypted
            )
        except TimeoutExpiredError as e:
            logger.error(f"Error during storage client creation: {e}")
            storage_client_created = False
        return storage_client_created

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
    def create_storage_client(self, onboarding_key_decrypted=None):
        """
        Create storage client

        Args:
            onboarding_key_decrypted (str): Onboarding key for the storage client.
            After version 4.18 onboarding key is generated and stored in secret.
            Get secret name from configmap created with storageconsumer

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

            nonlocal onboarding_key_decrypted

            if not onboarding_key_decrypted:
                onboarding_key_decrypted = self.get_onboarding_key()

            if not len(onboarding_key_decrypted):
                return False

            storage_client_data["spec"]["onboardingTicket"] = onboarding_key_decrypted

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
            f"get {constants.STORAGECLIENTS} {constants.DEFAULT_CLUSTERNAME} -n {self.namespace_client} | "
            f"awk '/{constants.STORAGE_CLIENT_NAME}/{{print $2}}'"
        )
        return self.exec_oc_cmd(cmd, shell=True).stdout.decode("utf-8").strip()

    @if_version("<4.19")
    def get_onboarding_key(self):
        """
        Get onboarding key using the private key from the secret

        Returns:
             str: onboarding token key
        """
        secret_ocp_obj = ocp.OCP(
            kind=constants.SECRET, namespace=config.ENV_DATA["cluster_namespace"]
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
            token = generate_onboarding_token(
                private_key=decoded_key,
                use_ticketgen_with_quota=True,
                storage_quota=self.storage_quota,
            )
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
    def create_catalog_source(self, reapply=False, odf_version_tag=None):
        """
        Create catalog source for ODF

        Args:
            reapply (bool): If True, will reapply the catalog source even if it exists
            odf_version_tag (str): Optional ODF version tag to use for the catalog source image.

        Returns:
            bool: True if the catalog source is created, False otherwise

        """
        if self.catalog_source_exists():
            logger.info("CatalogSource already exists")
            if not reapply:
                return True

        catalog_source_data = templating.load_yaml(
            constants.PROVIDER_MODE_CATALOGSOURCE
        )

        if not config.ENV_DATA.get("clusters").get(self.name).get("hosted_odf_version"):
            if not reapply:
                raise ValueError(
                    "OCS version is not set in the config file, should be set in format similar to '4.14.5-8'"
                    "in the 'hosted_odf_version' key in the 'ENV_DATA.clusters.<name>' section of the config file. "
                )

        if odf_version_tag:
            # If odf_version_tag is provided, use it instead of the one from config
            odf_version = odf_version_tag
        else:
            odf_version = (
                config.ENV_DATA.get("clusters").get(self.name).get("hosted_odf_version")
            )
        odf_registry = (
            config.ENV_DATA.get("clusters")
            .get(self.name)
            .get("hosted_odf_registry", defaults.HOSTED_ODF_REGISTRY_DEFAULT)
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
        if any(tag in hosted_odf_version for tag in ["latest", "stable"]):
            hosted_odf_version = hosted_odf_version.split("-")[-1]

        version_semantic = version.get_semantic_version(hosted_odf_version)

        hosted_odf_version = f"{version_semantic.major}.{version_semantic.minor}"
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
        ocp = OCP(namespace=config.ENV_DATA["cluster_namespace"])
        storage_provider_endpoint = ocp.exec_oc_cmd(
            (
                "get storageclusters.ocs.openshift.io -o jsonpath={'.items[*].status.storageProviderEndpoint'}"
            ),
            out_yaml_format=False,
        )
        logger.info(f"Provider address: {storage_provider_endpoint}")
        return storage_provider_endpoint

    @if_version("<4.19")
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

    @if_version("<4.19")
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

            ocp = OCP(
                kind=constants.STORAGECLAIM,
                namespace=config.ENV_DATA["cluster_namespace"],
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

    @if_version("<4.19")
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

    @if_version("<4.19")
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

    @if_version("<4.19")
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

        ocp_obj = OCP(
            kind=constants.STORAGECLAIM,
            namespace=config.ENV_DATA["cluster_namespace"],
            cluster_kubeconfig=self.cluster_kubeconfig,
        )

        if hasattr(self, "storage_client_name"):
            storage_claim_name = self.storage_client_name + "-ceph-rbd"
        else:
            storage_claim_name = "storage-client-ceph-rbd"

        return ocp_obj.check_resource_existence(
            timeout=self.timeout_check_resources_exist_sec,
            resource_name=storage_claim_name,
            should_exist=True,
        )

    @if_version("<4.19")
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
            selector=helpers.get_node_plugin_label(constants.CEPHFILESYSTEM),
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

    @kubeconfig_exists_decorator
    def enable_client_console_plugin(self):
        """
        Enable the ODF client console plugin by patching the console operator

        Returns:
            bool: True if the patch is applied successfully, False otherwise
        """
        try:
            self.exec_oc_cmd(
                "patch console.operator cluster --type json "
                '-p \'[{"op": "add", "path": "/spec/plugins", "value": ["odf-client-console"]}]\'',
                timeout=30,
            )
            # console pod exist from the start, but we want ensure no crash happened
            self.wait_console_plugin_pod_running()

            return True
        except CommandFailed as e:
            logger.error(f"Failed to enable client console plugin: {e}")
            return False

    @kubeconfig_exists_decorator
    def wait_console_plugin_pod_running(self):
        """
        Check if the ODF client console plugin pod is running

        Returns:
            bool: True if the console plugin pod is running, False otherwise
        """
        for sample in TimeoutSampler(
            timeout=300,
            sleep=10,
            func=get_pod_name_by_pattern,
            pattern="ocs-client-operator-console",
            namespace=self.namespace_client,
            cluster_kubeconfig=self.cluster_kubeconfig,
        ):
            if sample:
                return wait_for_pods_to_be_running(
                    pod_names=sample,
                    timeout=300,
                    sleep=10,
                    cluster_kubeconfig=self.cluster_kubeconfig,
                )
        return False


def hypershift_cluster_factory(
    cluster_names=None,
    ocp_version=None,
    odf_version=None,
    setup_storage_client=None,
    nodepool_replicas=None,
    duty="",
):
    """
    Factory function to create or use existing HyperShift clusters.

    Args:
        cluster_names (list): List of cluster names. Only for duty=="create_hosted_cluster_push_config"
        ocp_version (str): OCP version. Only for duty=="create_hosted_cluster_push_config"
        odf_version (str): ODF version. Only for duty=="create_hosted_cluster_push_config"
        setup_storage_client (bool): Optional. Setup storage client. Only for duty=="create_hosted_cluster_push_config"
        nodepool_replicas (int): Nodepool replicas; supported values are 2,3.
        Only for duty=="create_hosted_cluster_push_config"
        duty (str): Duty to perform; "create_hosted_cluster_push_config" (for creation of hypershift cluster) or
                    "use_existing_hosted_clusters_force_push_configs" (for pushing config even if config exists) or
                    "use_existing_hosted_clusters_push_missing_configs" (for adding only missing configs)
    """

    hosted_clients_obj = HostedClients()
    logger.info(f"Factory duty is '{duty}'")

    if duty == "create_hosted_cluster_push_config":
        hosted_cluster_conf_on_provider = {"ENV_DATA": {"clusters": {}}}
        for cluster_name in cluster_names:
            # this configuration is necessary to deploy hosted cluster, but not for running tests with multicluster job
            cluster_path = create_cluster_dir(cluster_name)
            hosted_cluster_conf_on_provider["ENV_DATA"]["clusters"][cluster_name] = {
                "hosted_cluster_path": cluster_path,
                "ocp_version": ocp_version,
                "cpu_cores_per_hosted_cluster": 8,
                "memory_per_hosted_cluster": "12Gi",
                "hosted_odf_registry": "quay.io/rhceph-dev/ocs-registry",
                "hosted_odf_version": odf_version,
                "setup_storage_client": setup_storage_client,
                "nodepool_replicas": nodepool_replicas,
            }

        logger.info(
            "Creating a hosted clusters with following deployment config: \n%s",
            json.dumps(
                hosted_cluster_conf_on_provider, indent=4, cls=SetToListJSONEncoder
            ),
        )
        ocsci_config.update(hosted_cluster_conf_on_provider)

        deployed_hosted_cluster_objects = hosted_clients_obj.do_deploy(cluster_names)
        deployed_clusters = [obj.name for obj in deployed_hosted_cluster_objects]

    elif duty in [
        "use_existing_hosted_clusters_force_push_configs",
        "use_existing_hosted_clusters_push_missing_configs",
    ]:
        cl_name_ver_dict = get_available_hosted_clusters_to_ocp_ver_dict()
        deployed_clusters = list(cl_name_ver_dict.keys())

        if "use_existing_hosted_clusters_force_push_configs" in duty:
            existing_clusters = {
                conf.ENV_DATA.get("cluster_name") for conf in config.clusters
            }
            clusters_to_remove = existing_clusters.intersection(deployed_clusters)
            if clusters_to_remove:
                for cluster_name in clusters_to_remove:
                    logger.info(
                        f"Removing cluster config {cluster_name} from config file, as it is already deployed"
                    )
                    config.remove_cluster_by_name(cluster_name)

        if duty == "use_existing_hosted_clusters_push_missing_configs":
            clusters_in_config = {
                conf.ENV_DATA.get("cluster_name") for conf in config.clusters
            }
            deployed_clusters = [
                c for c in deployed_clusters if c not in clusters_in_config
            ]

    else:
        logger.warning("Factory function was called without deployment duty")
        deployed_clusters = []

    for cluster_name in deployed_clusters:

        if not nodepool_replicas:
            nodepool_replicas = get_current_nodepool_size(cluster_name)

        try:
            nodepool_size = int(nodepool_replicas)
            if nodepool_size not in [2, 3]:
                raise ValueError
        except (TypeError, ValueError):
            logger.error(
                "Invalid nodepool size %s for cluster %s",
                nodepool_replicas,
                cluster_name,
            )
            continue

        # creating this configuration is necessary to run multicluster job. It will have actual specs of cluster.
        client_conf_default_dir = os.path.join(
            FUSION_CONF_DIR, f"hypershift_client_bm_{nodepool_replicas}w.yaml"
        )
        if not os.path.exists(client_conf_default_dir):
            raise FileNotFoundError(f"File {client_conf_default_dir} not found")
        with open(client_conf_default_dir) as file_stream:
            def_client_config_dict = {
                k: (v if v is not None else {})
                for (k, v) in yaml.safe_load(file_stream).items()
            }
            def_client_config_dict.get("ENV_DATA").update(
                {"cluster_name": cluster_name}
            )
            running_odf_version = get_running_odf_version()
            if running_odf_version:
                env_data = def_client_config_dict.setdefault("ENV_DATA", {})
                env_data["ocs_version"] = running_odf_version

            # upd cl_name_ver_dict for both deployment and using existing clusters
            cl_name_ver_dict = get_available_hosted_clusters_to_ocp_ver_dict()
            running_ocp_version = cl_name_ver_dict[cluster_name]
            if running_ocp_version:
                # update config.DEPLOYMENT["installer_version"] with ocp version
                def_client_config_dict.setdefault("DEPLOYMENT", {})[
                    "installer_version"
                ] = running_ocp_version

            cluster_path = create_cluster_dir(cluster_name)
            kubeconf_path = (
                hosted_clients_obj.download_hosted_clusters_kubeconfig_files(
                    {cluster_name: cluster_path}
                )
            )

            logger.info(f"Kubeconfig path: {kubeconf_path}")
            def_client_config_dict.setdefault("RUN", {}).update(
                {"kubeconfig": kubeconf_path}
            )
            cluster_config = Config()
            cluster_config.update(def_client_config_dict)

            logger.info(
                "Inserting new hosted cluster config to Multicluster Config "
                f"\n{json.dumps(vars(cluster_config), indent=4, cls=SetToListJSONEncoder)}"
            )
            ocsci_config.insert_cluster_config(ocsci_config.nclusters, cluster_config)
