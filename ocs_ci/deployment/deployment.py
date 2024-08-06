"""
This module provides base class for different deployment
platforms like AWS, VMWare, Baremetal etc.
"""

from copy import deepcopy
import json
import logging
import os
from subprocess import PIPE, Popen
import tempfile
import time
from pathlib import Path
import base64

import boto3
import yaml

from botocore.exceptions import EndpointConnectionError, BotoCoreError

from ocs_ci.deployment.ocp import OCPDeployment as BaseOCPDeployment
from ocs_ci.deployment.helpers.external_cluster_helpers import (
    ExternalCluster,
    get_external_cluster_client,
)
from ocs_ci.deployment.helpers.mcg_helpers import (
    mcg_only_deployment,
    mcg_only_post_deployment_checks,
)
from ocs_ci.deployment.helpers.odf_deployment_helpers import get_required_csvs
from ocs_ci.deployment.acm import Submariner
from ocs_ci.deployment.helpers.lso_helpers import setup_local_storage
from ocs_ci.deployment.disconnected import prepare_disconnected_ocs_deployment
from ocs_ci.framework import config, merge_dict
from ocs_ci.helpers.dr_helpers import (
    configure_drcluster_for_fencing,
)
from ocs_ci.ocs import constants, ocp, defaults, registry
from ocs_ci.ocs.cluster import (
    validate_cluster_on_pvc,
    validate_pdb_creation,
    CephClusterExternal,
    get_lvm_full_version,
    check_cephcluster_status,
)
from ocs_ci.ocs.exceptions import (
    CephHealthException,
    ChannelNotFound,
    CommandFailed,
    PodNotCreated,
    RBDSideCarContainerException,
    ResourceNameNotSpecifiedException,
    ResourceWrongStatusException,
    TimeoutExpiredError,
    UnavailableResourceException,
    UnsupportedFeatureError,
    UnexpectedDeploymentConfiguration,
    ResourceNotFoundError,
    ACMClusterConfigurationException,
    ACMObservabilityNotEnabled,
)
from ocs_ci.deployment.cert_manager import deploy_cert_manager
from ocs_ci.deployment.zones import create_dummy_zone_labels
from ocs_ci.deployment.netsplit import get_netsplit_mc
from ocs_ci.ocs.monitoring import (
    create_configmap_cluster_monitoring_pod,
    validate_pvc_created_and_bound_on_monitoring_pods,
    validate_pvc_are_mounted_on_monitoring_pods,
)
from ocs_ci.ocs.node import get_worker_nodes, verify_all_nodes_created
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources import machineconfig
from ocs_ci.ocs.resources import packagemanifest
from ocs_ci.ocs.resources.catalog_source import (
    CatalogSource,
    disable_specific_source,
)
from ocs_ci.ocs.resources.csv import CSV
from ocs_ci.ocs.resources.install_plan import wait_for_install_plan_and_approve
from ocs_ci.ocs.resources.packagemanifest import (
    get_selector_for_ocs_operator,
    PackageManifest,
)
from ocs_ci.ocs.resources.pod import (
    get_all_pods,
    validate_pods_are_respinned_and_running_state,
    get_pods_having_label,
    get_pod_count,
)
from ocs_ci.ocs.resources.storage_cluster import (
    ocs_install_verification,
    setup_ceph_debug,
    get_osd_count,
    StorageCluster,
    validate_serviceexport,
)
from ocs_ci.ocs.uninstall import uninstall_ocs
from ocs_ci.ocs.utils import (
    get_non_acm_cluster_config,
    get_primary_cluster_config,
    setup_ceph_toolbox,
    collect_ocs_logs,
    enable_console_plugin,
    get_all_acm_indexes,
    get_active_acm_index,
    enable_mco_console_plugin,
    label_pod_security_admission,
)
from ocs_ci.utility.deployment import (
    create_external_secret,
    get_and_apply_icsp_from_catalog,
)
from ocs_ci.utility.flexy import load_cluster_info
from ocs_ci.utility import (
    templating,
    ibmcloud,
    kms as KMS,
    pgsql,
    version,
)
from ocs_ci.utility.aws import update_config_from_s3, create_and_attach_sts_role
from ocs_ci.utility.retry import retry
from ocs_ci.utility.secret import link_all_sa_and_secret_and_delete_pods
from ocs_ci.utility.ssl_certs import (
    configure_custom_ingress_cert,
    configure_custom_api_cert,
)
from ocs_ci.utility.utils import (
    ceph_health_check,
    clone_repo,
    enable_huge_pages,
    exec_cmd,
    get_latest_ds_olm_tag,
    is_cluster_running,
    run_cmd,
    run_cmd_multicluster,
    set_selinux_permissions,
    set_registry_to_managed_state,
    add_stage_cert,
    modify_csv,
    wait_for_machineconfigpool_status,
    load_auth_config,
    TimeoutSampler,
    get_latest_acm_tag_unreleased,
    get_oadp_version,
    ceph_health_check_multi_storagecluster_external,
    get_acm_version,
)
from ocs_ci.utility.vsphere_nodes import update_ntp_compute_nodes
from ocs_ci.helpers import helpers
from ocs_ci.helpers.helpers import (
    set_configmap_log_level_rook_ceph_operator,
    get_default_storage_class,
)
from ocs_ci.ocs.ui.helpers_ui import ui_deployment_conditions
from ocs_ci.utility.utils import get_az_count
from ocs_ci.utility.ibmcloud import run_ibmcloud_cmd
from ocs_ci.deployment.cnv import CNVInstaller

logger = logging.getLogger(__name__)


class Deployment(object):
    """
    Base for all deployment platforms
    """

    # Default storage class for StorageCluster CRD,
    # every platform specific class which extending this base class should
    # define it
    DEFAULT_STORAGECLASS = None

    # Default storage class for LSO deployments. While each platform specific
    # subclass can redefine it, there is a well established platform
    # independent default value (based on OCS Installation guide), and it's
    # redefinition is not necessary in normal cases.
    DEFAULT_STORAGECLASS_LSO = "localblock"

    CUSTOM_STORAGE_CLASS_PATH = None
    """str: filepath of yaml file with custom storage class if necessary

    For some platforms, one have to create custom storage class for OCS to make
    sure ceph uses disks of expected type and parameters (eg. OCS requires
    ssd). This variable is either None (meaning that such custom storage class
    is not needed), or point to a yaml file with custom storage class.
    """

    def __init__(self):
        self.platform = config.ENV_DATA["platform"]
        self.ocp_deployment_type = config.ENV_DATA["deployment_type"]
        self.cluster_path = config.ENV_DATA["cluster_path"]
        self.namespace = config.ENV_DATA["cluster_namespace"]
        self.sts_role_arn = None

    class OCPDeployment(BaseOCPDeployment):
        """
        This class has to be implemented in child class and should overload
        methods for platform specific config.
        """

        pass

    def do_deploy_ocp(self, log_cli_level):
        """
        Deploy OCP
        Args:
            log_cli_level (str): log level for the installer

        """
        if not config.ENV_DATA["skip_ocp_deployment"]:
            if is_cluster_running(self.cluster_path):
                logger.warning("OCP cluster is already running, skipping installation")
            else:
                try:
                    self.deploy_ocp(log_cli_level)
                    self.post_ocp_deploy()
                except Exception as e:
                    config.RUN["is_ocp_deployment_failed"] = True
                    logger.error(e)
                    if config.REPORTING["gather_on_deploy_failure"]:
                        collect_ocs_logs("deployment", ocs=False)
                    raise

    def do_deploy_submariner(self):
        """
        Deploy Submariner operator

        """
        if config.ENV_DATA.get("skip_submariner_deployment", False):
            return

        # Multicluster operations
        if config.multicluster:
            # Configure submariner only on non-ACM clusters
            submariner = Submariner()
            submariner.deploy()

    def deploy_gitops_operator(self, switch_ctx=None):
        """
        Deploy GitOps operator

        Args:
            switch_ctx (int): The cluster index by the cluster name

        """
        config.switch_ctx(switch_ctx) if switch_ctx else config.switch_acm_ctx()

        logger.info("Creating GitOps Operator Subscription")

        run_cmd(f"oc create -f {constants.GITOPS_SUBSCRIPTION_YAML}")

        self.wait_for_subscription(
            constants.GITOPS_OPERATOR_NAME, namespace=constants.OPENSHIFT_OPERATORS
        )
        logger.info("Sleeping for 120 seconds after subscribing to GitOps Operator")
        time.sleep(120)
        subscriptions = ocp.OCP(
            kind=constants.SUBSCRIPTION_WITH_ACM,
            resource_name=constants.GITOPS_OPERATOR_NAME,
            namespace=constants.OPENSHIFT_OPERATORS,
        ).get()
        gitops_csv_name = subscriptions["status"]["currentCSV"]
        csv = CSV(resource_name=gitops_csv_name, namespace=constants.GITOPS_NAMESPACE)
        csv.wait_for_phase("Succeeded", timeout=720)
        logger.info("GitOps Operator Deployment Succeeded")

    def do_gitops_deploy(self):
        """
        Deploy GitOps operator

        Returns:

        """

        # Multicluster operations
        if config.multicluster:
            # Gitops operator is needed on all clusters for appset type workload deployment using pull model
            for cluster_index in range(config.nclusters):
                self.deploy_gitops_operator(switch_ctx=cluster_index)

            # Switching back context to ACM as below configs are specific to hub cluster
            config.switch_ctx(get_active_acm_index())

            logger.info("Creating GitOps CLuster Resource")
            run_cmd(f"oc create -f {constants.GITOPS_CLUSTER_YAML}")

            logger.info("Creating GitOps CLuster Placement Resource")
            run_cmd(f"oc create -f {constants.GITOPS_PLACEMENT_YAML}")

            logger.info("Creating ManagedClusterSetBinding")
            cluster_set = []
            managed_clusters = (
                ocp.OCP(kind=constants.ACM_MANAGEDCLUSTER).get().get("items", [])
            )
            # ignore local-cluster here
            for i in managed_clusters:
                if i["metadata"]["name"] != constants.ACM_LOCAL_CLUSTER:
                    cluster_set.append(
                        i["metadata"]["labels"][constants.ACM_CLUSTERSET_LABEL]
                    )
            if all(x == cluster_set[0] for x in cluster_set):
                logger.info(f"Found the uniq clusterset {cluster_set[0]}")
            else:
                raise UnexpectedDeploymentConfiguration(
                    "There are more then one clusterset added to multiple managedcluters"
                )

            managedclustersetbinding_obj = templating.load_yaml(
                constants.GITOPS_MANAGEDCLUSTER_SETBINDING_YAML
            )
            managedclustersetbinding_obj["metadata"]["name"] = cluster_set[0]
            managedclustersetbinding_obj["spec"]["clusterSet"] = cluster_set[0]
            managedclustersetbinding = tempfile.NamedTemporaryFile(
                mode="w+", prefix="managedcluster_setbinding", delete=False
            )
            templating.dump_data_to_temp_yaml(
                managedclustersetbinding_obj, managedclustersetbinding.name
            )
            run_cmd(f"oc create -f {managedclustersetbinding.name}")

            gitops_obj = ocp.OCP(
                resource_name=constants.GITOPS_CLUSTER_NAME,
                namespace=constants.GITOPS_CLUSTER_NAMESPACE,
                kind=constants.GITOPS_CLUSTER,
            )
            gitops_obj._has_phase = True
            gitops_obj.wait_for_phase("successful", timeout=720)

            logger.info(
                "Create clusterrolebinding on both the managed clusters, needed "
                "for appset pull model gitops deployment"
            )
            for cluster in managed_clusters:
                if cluster["metadata"]["name"] != constants.ACM_LOCAL_CLUSTER:
                    config.switch_to_cluster_by_name(cluster["metadata"]["name"])
                    exec_cmd(
                        f"oc create -f {constants.CLUSTERROLEBINDING_APPSET_PULLMODEL_PATH}"
                    )

    def do_deploy_ocs(self):
        """
        Deploy OCS/ODF and run verification as well

        """
        if config.ENV_DATA.get("odf_provider_mode_deployment", False):
            logger.warning(
                "Skipping normal ODF deployment because ODF deployment in Provider mode will be performed"
            )
            return
        if not config.ENV_DATA["skip_ocs_deployment"]:
            for i in range(config.nclusters):
                if config.multicluster and (i in get_all_acm_indexes()):
                    continue
                config.switch_ctx(i)
                try:
                    self.deploy_ocs()

                    if config.REPORTING["collect_logs_on_success_run"]:
                        collect_ocs_logs("deployment", ocp=False, status_failure=False)
                except Exception as e:
                    logger.error(e)
                    if config.REPORTING["gather_on_deploy_failure"]:
                        # Let's do the collections separately to guard against one
                        # of them failing
                        collect_ocs_logs("deployment", ocs=False)
                        collect_ocs_logs("deployment", ocp=False)
                    raise
            config.reset_ctx()
            # Run ocs_install_verification here only in case of multicluster.
            # For single cluster, test_deployment will take care.
            if config.multicluster:
                for i in range(config.multicluster):
                    if i in get_all_acm_indexes():
                        continue
                    else:
                        config.switch_ctx(i)
                        ocs_registry_image = config.DEPLOYMENT.get(
                            "ocs_registry_image", None
                        )
                        ocs_install_verification(ocs_registry_image=ocs_registry_image)
                # if we have Globalnet enabled in case of submariner with RDR
                # we need to add a flag to storagecluster
                if (
                    config.ENV_DATA.get("enable_globalnet", True)
                    and config.MULTICLUSTER["multicluster_mode"] == "regional-dr"
                ):
                    for cluster in get_non_acm_cluster_config():
                        config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
                        storage_cluster_name = config.ENV_DATA["storage_cluster_name"]
                        logger.info(
                            "Updating the StorageCluster resource for globalnet"
                        )
                        storage_cluster = StorageCluster(
                            resource_name=storage_cluster_name,
                            namespace=config.ENV_DATA["cluster_namespace"],
                        )
                        storage_cluster.reload_data()
                        storage_cluster.wait_for_phase(phase="Ready", timeout=1000)
                        ptch = (
                            f'\'{{"spec": {{"network": {{"multiClusterService": '
                            f"{{\"clusterID\": \"{config.ENV_DATA['cluster_name']}\", \"enabled\": true}}}}}}}}'"
                        )
                        ptch_cmd = (
                            f"oc patch storagecluster/{storage_cluster.data.get('metadata').get('name')} "
                            f"-n openshift-storage  --type merge --patch {ptch}"
                        )
                        run_cmd(ptch_cmd)
                        ocs_registry_image = config.DEPLOYMENT.get(
                            "ocs_registry_image", None
                        )
                        storage_cluster.reload_data()
                        assert (
                            storage_cluster.data.get("spec")
                            .get("network")
                            .get("multiClusterService")
                            .get("enabled")
                        ), "Failed to update StorageCluster globalnet"
                        validate_serviceexport()
                        ocs_install_verification(
                            timeout=2000, ocs_registry_image=ocs_registry_image
                        )
                config.reset_ctx()
        else:
            logger.warning("OCS deployment will be skipped")

    def do_deploy_oadp(self):
        """
        Deploy OADP Operator

        """
        if config.ENV_DATA.get("skip_dr_deployment", False):
            return
        if config.multicluster:
            managed_clusters = get_non_acm_cluster_config()
            for cluster in managed_clusters:
                index = cluster.MULTICLUSTER["multicluster_index"]
                config.switch_ctx(index)
                logger.info("Creating Namespace")
                # creating Namespace and operator group for cert-manager
                logger.info("Creating namespace and operator group for Openshift-oadp")
                run_cmd(f"oc create -f {constants.OADP_NS_YAML}")
                logger.info("Creating OADP Operator Subscription")
                oadp_subscription_yaml_data = templating.load_yaml(
                    constants.OADP_SUBSCRIPTION_YAML
                )
                package_manifest = PackageManifest(
                    resource_name=constants.OADP_OPERATOR_NAME,
                )
                oadp_default_channel = package_manifest.get_default_channel()
                oadp_subscription_yaml_data["spec"]["channel"] = oadp_default_channel
                oadp_subscription_manifest = tempfile.NamedTemporaryFile(
                    mode="w+", prefix="oadp_subscription_manifest", delete=False
                )
                templating.dump_data_to_temp_yaml(
                    oadp_subscription_yaml_data, oadp_subscription_manifest.name
                )
                run_cmd(f"oc create -f {oadp_subscription_manifest.name}")
                self.wait_for_subscription(
                    constants.OADP_OPERATOR_NAME, namespace=constants.OADP_NAMESPACE
                )
                logger.info(
                    "Sleeping for 120 seconds after subscribing to OADP Operator"
                )
                time.sleep(120)
                oadp_subscriptions = ocp.OCP(
                    kind=constants.SUBSCRIPTION_WITH_ACM,
                    resource_name=constants.OADP_OPERATOR_NAME,
                    namespace=constants.OADP_NAMESPACE,
                ).get()
                oadp_csv_name = oadp_subscriptions["status"]["currentCSV"]
                csv = CSV(
                    resource_name=oadp_csv_name, namespace=constants.OADP_NAMESPACE
                )
                csv.wait_for_phase("Succeeded", timeout=720)
                logger.info("OADP Operator Deployment Succeeded")

    def do_deploy_rdr(self):
        """
        Call Regional DR deploy

        """
        # Multicluster: Handle all ODF multicluster DR ops
        if config.ENV_DATA.get("skip_dr_deployment", False):
            return
        if config.multicluster:
            dr_conf = self.get_rdr_conf()
            deploy_dr = get_multicluster_dr_deployment()(dr_conf)
            deploy_dr.deploy()

    def do_deploy_lvmo(self):
        """
        call lvm deploy

        """
        self.deploy_lvmo()

    def do_deploy_cert_manager(self):
        """
        Installs cert-manager operator

        """
        if not config.ENV_DATA["skip_ocp_deployment"]:
            cert_manager_operator = defaults.CERT_MANAGER_OPERATOR_NAME
            cert_manager_namespace = defaults.CERT_MANAGER_NAMESPACE
            cert_manager_operator_csv = f"openshift-{cert_manager_operator}"

            # creating Namespace and operator group for cert-manager
            logger.info("Creating namespace and operator group for cert-manager")
            run_cmd(f"oc create -f {constants.CERT_MANAGER_NS_YAML}")

            deploy_cert_manager()
            self.wait_for_subscription(cert_manager_operator, cert_manager_namespace)
            self.wait_for_csv(cert_manager_operator, cert_manager_namespace)
            logger.info(
                f"Sleeping for 30 seconds after {cert_manager_operator} created"
            )
            time.sleep(30)
            package_manifest = PackageManifest(resource_name=cert_manager_operator_csv)
            package_manifest.wait_for_resource(timeout=120)
            csv_name = package_manifest.get_current_csv()
            csv = CSV(resource_name=csv_name, namespace=cert_manager_namespace)
            csv.wait_for_phase("Succeeded", timeout=300)

    def do_deploy_fusion(self):
        """
        Install IBM Fusion operator
        """
        if (
            config.DEPLOYMENT.get("fusion_deployment")
            and not config.ENV_DATA["skip_ocs_deployment"]
        ):
            # create catalog source
            create_fusion_catalog_source()

            # create namespace and operator group
            logger.info("Creating namespace for IBM Fusion.")
            run_cmd(f"oc create -f {constants.FUSION_NS_YAML}")

            # deploy fusion
            from ocs_ci.deployment.fusion import deploy_fusion

            deploy_fusion()

            # wait for subscription and csv to found
            fusion_operator = defaults.FUSION_OPERATOR_NAME
            fusion_namespace = defaults.FUSION_NAMESPACE
            self.wait_for_subscription(fusion_operator, fusion_namespace)
            self.wait_for_csv(fusion_operator, fusion_namespace)
            logger.info(f"Sleeping for 30 seconds after {fusion_operator} created")
            time.sleep(30)

            # wait for package manifest to found and csv in Succeeded state
            package_manifest = PackageManifest(resource_name=fusion_operator)
            package_manifest.wait_for_resource(timeout=120)
            csv_name = package_manifest.get_current_csv()
            csv = CSV(resource_name=csv_name, namespace=fusion_namespace)
            csv.wait_for_phase("Succeeded", timeout=300)

            # delete catalog source of IBM
            run_cmd(
                f"oc delete catalogsource {defaults.FUSION_CATALOG_NAME} -n {constants.MARKETPLACE_NAMESPACE}"
            )
            logger.info(
                f"Sleeping for 30 seconds after deleting catalogsource {defaults.FUSION_CATALOG_NAME}"
            )
            time.sleep(30)

    def do_deploy_odf_provider_mode(self):
        """
        Deploy ODF in provider mode and setup native client
        """
        # deploy provider-client deployment
        from ocs_ci.deployment.provider_client.storage_client_deployment import (
            ODFAndNativeStorageClientDeploymentOnProvider,
        )

        storage_client_deployment_obj = ODFAndNativeStorageClientDeploymentOnProvider()

        # Provider-client deployment if odf_provider_mode_deployment: True
        if (
            config.ENV_DATA.get("odf_provider_mode_deployment", False)
            and not config.ENV_DATA["skip_ocs_deployment"]
        ):
            storage_client_deployment_obj.provider_and_native_client_installation()

    def do_deploy_cnv(self):
        """
        Deploy CNV
        We run it in OCP deployment stage, hence `ship_ocs_deployment` is set True.
        When we run it in OCS deployment stage, the `skip_ocs_deployment` is set to False automatically and
        second installation does not happen.
        """
        if (
            config.DEPLOYMENT.get("cnv_deployment")
            and config.ENV_DATA["skip_ocs_deployment"]
        ):
            CNVInstaller().deploy_cnv()

    def do_deploy_hosted_clusters(self):
        """
        Deploy Hosted cluster(s)
        """
        if (
            config.ENV_DATA.get("clusters", False)
            and not config.ENV_DATA["skip_ocs_deployment"]
        ):
            # imported locally due to a circular dependency
            from ocs_ci.deployment.hosted_cluster import HostedClients

            HostedClients().do_deploy()

    def deploy_cluster(self, log_cli_level="DEBUG"):
        """
        We are handling both OCP and OCS deployment here based on flags

        Args:
            log_cli_level (str): log level for installer (default: DEBUG)
        """
        self.do_deploy_ocp(log_cli_level)

        # TODO: use temporary directory for all temporary files of
        # ocs-deployment, not just here in this particular case
        tmp_path = Path(tempfile.mkdtemp(prefix="ocs-ci-deployment-"))
        logger.debug("created temporary directory %s", tmp_path)

        if config.DEPLOYMENT.get("install_cert_manager"):
            self.do_deploy_cert_manager()

        # Deployment of network split and or extra latency scripts via
        # machineconfig API happens after OCP but before OCS deployment.
        if (
            config.DEPLOYMENT.get("network_split_setup")
            or config.DEPLOYMENT.get("network_zone_latency")
        ) and not config.ENV_DATA["skip_ocp_deployment"]:
            master_zones = config.ENV_DATA.get("master_availability_zones")
            worker_zones = config.ENV_DATA.get("worker_availability_zones")
            # special external zone, which is directly defined by ip addr list,
            # such zone could represent external services, which we could block
            # access to via ax-bx-cx network split
            if config.DEPLOYMENT.get("network_split_zonex_addrs") is not None:
                x_addr_list = config.DEPLOYMENT["network_split_zonex_addrs"].split(",")
            else:
                x_addr_list = None
            if config.DEPLOYMENT.get("arbiter_deployment"):
                arbiter_zone = self.get_arbiter_location()
                logger.debug("detected arbiter zone: %s", arbiter_zone)
            else:
                arbiter_zone = None
            mc_dict = get_netsplit_mc(
                tmp_path,
                master_zones,
                worker_zones,
                enable_split=config.DEPLOYMENT.get("network_split_setup"),
                x_addr_list=x_addr_list,
                arbiter_zone=arbiter_zone,
                latency=config.DEPLOYMENT.get("network_zone_latency"),
            )
            machineconfig.deploy_machineconfig(
                tmp_path, "network-split", mc_dict, mcp_num=2
            )
        ocp_version = version.get_semantic_ocp_version_from_config()
        if (
            config.ENV_DATA.get("deploy_acm_hub_cluster")
            and ocp_version >= version.VERSION_4_9
        ):
            self.deploy_acm_hub()
        self.do_deploy_lvmo()
        self.do_deploy_submariner()
        self.do_gitops_deploy()
        self.do_deploy_oadp()
        self.do_deploy_ocs()
        self.do_deploy_rdr()
        self.do_deploy_fusion()
        self.do_deploy_odf_provider_mode()
        self.do_deploy_cnv()
        self.do_deploy_hosted_clusters()

    def get_rdr_conf(self):
        """
        Aggregate important Regional DR parameters in the dictionary

        Returns:
            dict: of Regional DR config parameters

        """
        dr_conf = dict()
        dr_conf["rbd_dr_scenario"] = config.ENV_DATA.get("rbd_dr_scenario", False)
        dr_conf["dr_metadata_store"] = config.ENV_DATA.get("dr_metadata_store", "awss3")
        return dr_conf

    def deploy_ocp(self, log_cli_level="DEBUG"):
        """
        Base deployment steps, the rest should be implemented in the child
        class.

        Args:
            log_cli_level (str): log level for installer (default: DEBUG)
        """
        self.ocp_deployment = self.OCPDeployment()
        self.ocp_deployment.deploy_prereq()
        self.ocp_deployment.deploy(log_cli_level)
        # logging the cluster UUID so that we can ask for it's telemetry data
        cluster_id = run_cmd(
            "oc get clusterversion version -o jsonpath='{.spec.clusterID}'"
        )
        logger.info(f"clusterID (UUID): {cluster_id}")

    def post_ocp_deploy(self):
        """
        Function does post OCP deployment stuff we need to do.
        """
        managed_azure = (
            config.ENV_DATA["platform"] == constants.AZURE_PLATFORM
            and config.ENV_DATA["deployment_type"] == "managed"
        )
        # In the case of ARO deployment we are handling it much earlier in deployment
        # itself as it's needed sooner.
        if not managed_azure:
            if config.DEPLOYMENT.get("use_custom_ingress_ssl_cert"):
                configure_custom_ingress_cert()
            if config.DEPLOYMENT.get("use_custom_api_ssl_cert"):
                configure_custom_api_cert()
        verify_all_nodes_created()
        set_selinux_permissions()
        set_registry_to_managed_state()
        add_stage_cert()
        if config.ENV_DATA.get("huge_pages"):
            enable_huge_pages()
        if config.DEPLOYMENT.get("dummy_zone_node_labels"):
            create_dummy_zone_labels()
        ibmcloud_ipi = (
            config.ENV_DATA["platform"] == constants.IBMCLOUD_PLATFORM
            and config.ENV_DATA["deployment_type"] == "ipi"
        )
        if ibmcloud_ipi:
            ibmcloud.label_nodes_region()

    def label_and_taint_nodes(self):
        """
        Label and taint worker nodes to be used by OCS operator
        """

        # TODO: remove this "heuristics", it doesn't belong there, the process
        # should be explicit and simple, this is asking for trouble, bugs and
        # silently invalid deployments ...
        # See https://github.com/red-hat-storage/ocs-ci/issues/4470
        arbiter_deployment = config.DEPLOYMENT.get("arbiter_deployment")

        nodes = ocp.OCP(kind="node").get().get("items", [])

        worker_nodes = [
            node
            for node in nodes
            if constants.WORKER_LABEL in node["metadata"]["labels"]
        ]
        if not worker_nodes:
            raise UnavailableResourceException("No worker node found!")
        az_worker_nodes = {}
        for node in worker_nodes:
            az = node["metadata"]["labels"].get(constants.ZONE_LABEL)
            az_node_list = az_worker_nodes.get(az, [])
            az_node_list.append(node["metadata"]["name"])
            az_worker_nodes[az] = az_node_list
        logger.debug(f"Found the worker nodes in AZ: {az_worker_nodes}")

        if arbiter_deployment:
            to_label = config.DEPLOYMENT.get("ocs_operator_nodes_to_label", 4)
        else:
            to_label = config.DEPLOYMENT.get("ocs_operator_nodes_to_label")

        distributed_worker_nodes = []
        if arbiter_deployment and config.DEPLOYMENT.get("arbiter_autodetect"):
            for az in list(az_worker_nodes.keys()):
                az_node_list = az_worker_nodes.get(az)
                if az_node_list and len(az_node_list) > 1:
                    node_names = az_node_list[:2]
                    distributed_worker_nodes += node_names
        elif arbiter_deployment and not config.DEPLOYMENT.get("arbiter_autodetect"):
            to_label_per_az = int(
                to_label / len(config.ENV_DATA.get("worker_availability_zones"))
            )
            for az in list(config.ENV_DATA.get("worker_availability_zones")):
                az_node_list = az_worker_nodes.get(az)
                if az_node_list and len(az_node_list) > 1:
                    node_names = az_node_list[:to_label_per_az]
                    distributed_worker_nodes += node_names
                else:
                    raise UnavailableResourceException(
                        "Atleast 2 worker nodes required for arbiter cluster in zone %s",
                        az,
                    )
        else:
            while az_worker_nodes:
                for az in list(az_worker_nodes.keys()):
                    az_node_list = az_worker_nodes.get(az)
                    if az_node_list:
                        node_name = az_node_list.pop(0)
                        distributed_worker_nodes.append(node_name)
                    else:
                        del az_worker_nodes[az]
        logger.info(f"Distributed worker nodes for AZ: {distributed_worker_nodes}")

        to_taint = config.DEPLOYMENT.get("ocs_operator_nodes_to_taint", 0)

        distributed_worker_count = len(distributed_worker_nodes)
        if distributed_worker_count < to_label or distributed_worker_count < to_taint:
            logger.info(f"All nodes: {nodes}")
            logger.info(f"Distributed worker nodes: {distributed_worker_nodes}")
            raise UnavailableResourceException(
                f"Not enough distributed worker nodes: {distributed_worker_count} to label: "
                f"{to_label} or taint: {to_taint}!"
            )

        _ocp = ocp.OCP(kind="node")
        workers_to_label = " ".join(distributed_worker_nodes[:to_label])
        if workers_to_label:
            logger.info(
                f"Label nodes: {workers_to_label} with label: "
                f"{constants.OPERATOR_NODE_LABEL}"
            )
            label_cmds = [
                (
                    f"label nodes {workers_to_label} "
                    f"{constants.OPERATOR_NODE_LABEL} --overwrite"
                )
            ]
            if config.DEPLOYMENT.get("infra_nodes") and not config.ENV_DATA.get(
                "infra_replicas"
            ):
                logger.info(
                    f"Label nodes: {workers_to_label} with label: "
                    f"{constants.INFRA_NODE_LABEL}"
                )
                label_cmds.append(
                    f"label nodes {workers_to_label} "
                    f"{constants.INFRA_NODE_LABEL} --overwrite"
                )

            for cmd in label_cmds:
                _ocp.exec_oc_cmd(command=cmd)

        workers_to_taint = " ".join(distributed_worker_nodes[:to_taint])
        if workers_to_taint:
            logger.info(
                f"Taint nodes: {workers_to_taint} with taint: "
                f"{constants.OPERATOR_NODE_TAINT}"
            )
            taint_cmd = (
                f"adm taint nodes {workers_to_taint} {constants.OPERATOR_NODE_TAINT}"
            )
            _ocp.exec_oc_cmd(command=taint_cmd)

    def subscribe_ocs(self):
        """
        This method subscription manifest and subscribe to OCS operator.

        """
        live_deployment = config.DEPLOYMENT.get("live_deployment")
        managed_ibmcloud = (
            config.ENV_DATA["platform"] == constants.IBMCLOUD_PLATFORM
            and config.ENV_DATA["deployment_type"] == "managed"
        )
        if managed_ibmcloud and not live_deployment:
            link_all_sa_and_secret_and_delete_pods(constants.OCS_SECRET, self.namespace)
        operator_selector = get_selector_for_ocs_operator()
        # wait for package manifest
        # For OCS version >= 4.9, we have odf-operator
        ocs_version = version.get_semantic_ocs_version_from_config()
        if ocs_version >= version.VERSION_4_9:
            ocs_operator_name = defaults.ODF_OPERATOR_NAME
            subscription_file = constants.SUBSCRIPTION_ODF_YAML
        else:
            ocs_operator_name = defaults.OCS_OPERATOR_NAME
            subscription_file = constants.SUBSCRIPTION_YAML

        package_manifest = PackageManifest(
            resource_name=ocs_operator_name,
            selector=operator_selector,
        )
        # Wait for package manifest is ready
        package_manifest.wait_for_resource(timeout=300)
        default_channel = package_manifest.get_default_channel()
        subscription_yaml_data = templating.load_yaml(subscription_file)
        subscription_plan_approval = config.DEPLOYMENT.get("subscription_plan_approval")
        if subscription_plan_approval:
            subscription_yaml_data["spec"][
                "installPlanApproval"
            ] = subscription_plan_approval
        custom_channel = config.DEPLOYMENT.get("ocs_csv_channel")
        if custom_channel:
            logger.info(f"Custom channel will be used: {custom_channel}")
            subscription_yaml_data["spec"]["channel"] = custom_channel
        else:
            logger.info(f"Default channel will be used: {default_channel}")
            subscription_yaml_data["spec"]["channel"] = default_channel
        if config.DEPLOYMENT.get("stage"):
            subscription_yaml_data["spec"]["source"] = constants.OPERATOR_SOURCE_NAME
        if config.DEPLOYMENT.get("live_deployment"):
            subscription_yaml_data["spec"]["source"] = config.DEPLOYMENT.get(
                "live_content_source", defaults.LIVE_CONTENT_SOURCE
            )
        if config.DEPLOYMENT.get("sts_enabled"):
            if "config" not in subscription_yaml_data["spec"]:
                subscription_yaml_data["spec"]["config"] = {}
            role_arn_data = {"name": "ROLEARN", "value": self.sts_role_arn}
            if "env" not in subscription_yaml_data["spec"]["config"]:
                subscription_yaml_data["spec"]["config"]["env"] = [role_arn_data]
            else:
                subscription_yaml_data["spec"]["config"]["env"].append([role_arn_data])
        subscription_manifest = tempfile.NamedTemporaryFile(
            mode="w+", prefix="subscription_manifest", delete=False
        )
        templating.dump_data_to_temp_yaml(
            subscription_yaml_data, subscription_manifest.name
        )
        run_cmd(f"oc create -f {subscription_manifest.name}")
        self.wait_for_subscription(ocs_operator_name)
        if subscription_plan_approval == "Manual":
            wait_for_install_plan_and_approve(self.namespace)
            csv_name = package_manifest.get_current_csv(channel=custom_channel)
            csv = CSV(resource_name=csv_name, namespace=self.namespace)
            csv.wait_for_phase("Installing", timeout=60)
        self.wait_for_csv(ocs_operator_name)
        logger.info("Sleeping for 30 seconds after CSV created")
        time.sleep(30)

    def wait_for_subscription(self, subscription_name, namespace=None):
        """
        Wait for the subscription to appear

        Args:
            subscription_name (str): Subscription name pattern
            namespace (str): Namespace name for checking subscription if None then default from ENV_data

        """
        if not namespace:
            namespace = self.namespace

        ocp.OCP(kind=constants.SUBSCRIPTION_COREOS, namespace=namespace)
        for sample in TimeoutSampler(
            300, 10, ocp.OCP, kind=constants.SUBSCRIPTION_COREOS, namespace=namespace
        ):
            subscriptions = sample.get().get("items", [])
            for subscription in subscriptions:
                found_subscription_name = subscription.get("metadata", {}).get(
                    "name", ""
                )
                if subscription_name in found_subscription_name:
                    logger.info(f"Subscription found: {found_subscription_name}")
                    return
                logger.debug(f"Still waiting for the subscription: {subscription_name}")

    def wait_for_csv(self, csv_name, namespace=None):
        """
        Wait for the CSV to appear

        Args:
            csv_name (str): CSV name pattern
            namespace (str): Namespace where CSV exists

        """
        namespace = namespace or self.namespace
        ocp.OCP(kind="subscription", namespace=namespace)
        for sample in TimeoutSampler(300, 10, ocp.OCP, kind="csv", namespace=namespace):
            csvs = sample.get().get("items", [])
            for csv in csvs:
                found_csv_name = csv.get("metadata", {}).get("name", "")
                if csv_name in found_csv_name:
                    logger.info(f"CSV found: {found_csv_name}")
                    return
                logger.debug(f"Still waiting for the CSV: {csv_name}")

    def get_arbiter_location(self):
        """
        Get arbiter mon location for storage cluster
        """
        if config.DEPLOYMENT.get("arbiter_deployment") and not config.DEPLOYMENT.get(
            "arbiter_autodetect"
        ):
            return config.DEPLOYMENT.get("arbiter_zone")

        # below logic will autodetect arbiter_zone
        nodes = ocp.OCP(kind="node").get().get("items", [])

        worker_nodes_zones = {
            node["metadata"]["labels"].get(constants.ZONE_LABEL)
            for node in nodes
            if constants.WORKER_LABEL in node["metadata"]["labels"]
            and str(constants.OPERATOR_NODE_LABEL)[:-3] in node["metadata"]["labels"]
        }

        master_nodes_zones = {
            node["metadata"]["labels"].get(constants.ZONE_LABEL)
            for node in nodes
            if constants.MASTER_LABEL in node["metadata"]["labels"]
        }

        arbiter_locations = list(master_nodes_zones - worker_nodes_zones)

        if len(arbiter_locations) < 1:
            raise UnavailableResourceException(
                "Atleast 1 different zone required than storage nodes in master nodes to host arbiter mon"
            )

        return arbiter_locations[0]

    def deploy_ocs_via_operator(self, image=None):
        """
        Method for deploy OCS via OCS operator

        Args:
            image (str): Image of ocs registry.

        """
        ui_deployment = config.DEPLOYMENT.get("ui_deployment")
        live_deployment = config.DEPLOYMENT.get("live_deployment")
        arbiter_deployment = config.DEPLOYMENT.get("arbiter_deployment")

        if ui_deployment and ui_deployment_conditions():
            self.deployment_with_ui()
            # Skip the rest of the deployment when deploy via UI
            return
        else:
            logger.info("Deployment of OCS via OCS operator")
            self.label_and_taint_nodes()

        if config.DEPLOYMENT.get("sts_enabled"):
            role_data = create_and_attach_sts_role()
            self.sts_role_arn = role_data["Role"]["Arn"]

        if not live_deployment:
            create_catalog_source(image)

        if config.DEPLOYMENT.get("local_storage"):
            setup_local_storage(storageclass=self.DEFAULT_STORAGECLASS_LSO)

        logger.info("Creating namespace and operator group.")
        run_cmd(f"oc create -f {constants.OLM_YAML}")

        # Create Multus Networks
        if config.ENV_DATA.get("is_multus_enabled"):
            ocs_version = version.get_semantic_ocs_version_from_config()
            if (
                config.ENV_DATA.get("multus_create_public_net")
                and ocs_version >= version.VERSION_4_16
            ):
                from ocs_ci.deployment.nmstate import NMStateInstaller

                logger.info("Install NMState operator and create an instance")
                nmstate_obj = NMStateInstaller()
                nmstate_obj.running_nmstate()
                from ocs_ci.helpers.helpers import (
                    configure_node_network_configuration_policy_on_all_worker_nodes,
                )

                configure_node_network_configuration_policy_on_all_worker_nodes()

            create_public_net = config.ENV_DATA["multus_create_public_net"]
            create_cluster_net = config.ENV_DATA["multus_create_cluster_net"]
            interfaces = set()
            if create_public_net:
                interfaces.add(config.ENV_DATA["multus_public_net_interface"])
            if create_cluster_net:
                interfaces.add(config.ENV_DATA["multus_cluster_net_interface"])
            worker_nodes = get_worker_nodes()
            node_obj = ocp.OCP(kind="node")
            platform = config.ENV_DATA.get("platform").lower()
            if platform not in [constants.BAREMETAL_PLATFORM, constants.HCI_BAREMETAL]:
                for node in worker_nodes:
                    for interface in interfaces:
                        ip_link_cmd = f"ip link set promisc on {interface}"
                        node_obj.exec_oc_debug_cmd(node=node, cmd_list=[ip_link_cmd])

            if create_public_net:
                logger.info("Creating Multus public network")
                public_net_data = templating.load_yaml(constants.MULTUS_PUBLIC_NET_YAML)
                public_net_data["metadata"]["name"] = config.ENV_DATA.get(
                    "multus_public_net_name"
                )
                public_net_data["metadata"]["namespace"] = config.ENV_DATA.get(
                    "multus_public_net_namespace"
                )
                public_net_config_str = public_net_data["spec"]["config"]
                public_net_config_dict = json.loads(public_net_config_str)
                public_net_config_dict["master"] = config.ENV_DATA.get(
                    "multus_public_net_interface"
                )
                public_net_config_dict["ipam"]["range"] = config.ENV_DATA.get(
                    "multus_public_net_range"
                )
                public_net_config_dict["type"] = config.ENV_DATA.get(
                    "multus_public_net_type"
                )
                public_net_config_dict["mode"] = config.ENV_DATA.get(
                    "multus_public_net_mode"
                )
                public_net_data["spec"]["config"] = json.dumps(public_net_config_dict)
                public_net_yaml = tempfile.NamedTemporaryFile(
                    mode="w+", prefix="multus_public", delete=False
                )
                templating.dump_data_to_temp_yaml(public_net_data, public_net_yaml.name)
                run_cmd(f"oc create -f {public_net_yaml.name}")

            if create_cluster_net:
                logger.info("Creating Multus cluster network")
                cluster_net_data = templating.load_yaml(
                    constants.MULTUS_CLUSTER_NET_YAML
                )
                cluster_net_data["metadata"]["name"] = config.ENV_DATA.get(
                    "multus_cluster_net_name"
                )
                cluster_net_data["metadata"]["namespace"] = config.ENV_DATA.get(
                    "multus_cluster_net_namespace"
                )
                cluster_net_config_str = cluster_net_data["spec"]["config"]
                cluster_net_config_dict = json.loads(cluster_net_config_str)
                cluster_net_config_dict["master"] = config.ENV_DATA.get(
                    "multus_cluster_net_interface"
                )
                cluster_net_config_dict["ipam"]["range"] = config.ENV_DATA.get(
                    "multus_cluster_net_range"
                )
                cluster_net_config_dict["type"] = config.ENV_DATA.get(
                    "multus_cluster_net_type"
                )
                cluster_net_config_dict["mode"] = config.ENV_DATA.get(
                    "multus_cluster_net_mode"
                )
                cluster_net_data["spec"]["config"] = json.dumps(cluster_net_config_dict)
                cluster_net_yaml = tempfile.NamedTemporaryFile(
                    mode="w+", prefix="multus_public", delete=False
                )
                templating.dump_data_to_temp_yaml(
                    cluster_net_data, cluster_net_yaml.name
                )
                run_cmd(f"oc create -f {cluster_net_yaml.name}")

        disable_addon = config.DEPLOYMENT.get("ibmcloud_disable_addon")
        managed_ibmcloud = (
            config.ENV_DATA["platform"] == constants.IBMCLOUD_PLATFORM
            and config.ENV_DATA["deployment_type"] == "managed"
        )
        if managed_ibmcloud:
            ibmcloud.add_deployment_dependencies()
            if not live_deployment:
                create_ocs_secret(self.namespace)
        if config.ENV_DATA["platform"] == constants.IBMCLOUD_PLATFORM:
            if config.DEPLOYMENT.get("create_ibm_cos_secret", True):
                logger.info("Creating secret for IBM Cloud Object Storage")
                with open(constants.IBM_COS_SECRET_YAML, "r") as cos_secret_fd:
                    cos_secret_data = yaml.load(cos_secret_fd, Loader=yaml.SafeLoader)
                key_id = config.AUTH["ibmcloud"]["ibm_cos_access_key_id"]
                key_secret = config.AUTH["ibmcloud"]["ibm_cos_secret_access_key"]
                cos_secret_data["data"]["IBM_COS_ACCESS_KEY_ID"] = key_id
                cos_secret_data["data"]["IBM_COS_SECRET_ACCESS_KEY"] = key_secret
                cos_secret_data_yaml = tempfile.NamedTemporaryFile(
                    mode="w+", prefix="cos_secret", delete=False
                )
                templating.dump_data_to_temp_yaml(
                    cos_secret_data, cos_secret_data_yaml.name
                )
                exec_cmd(f"oc create -f {cos_secret_data_yaml.name}")
        if managed_ibmcloud and live_deployment and not disable_addon:
            self.deploy_odf_addon()
            return
        self.subscribe_ocs()
        operator_selector = get_selector_for_ocs_operator()
        subscription_plan_approval = config.DEPLOYMENT.get("subscription_plan_approval")
        ocs_version = version.get_semantic_ocs_version_from_config()
        ocs_operator_names = get_required_csvs()

        channel = config.DEPLOYMENT.get("ocs_csv_channel")
        is_ibm_sa_linked = False

        for ocs_operator_name in ocs_operator_names:
            package_manifest = PackageManifest(
                resource_name=ocs_operator_name,
                selector=operator_selector,
                subscription_plan_approval=subscription_plan_approval,
            )
            package_manifest.wait_for_resource(timeout=300)
            csv_name = package_manifest.get_current_csv(channel=channel)
            csv = CSV(resource_name=csv_name, namespace=self.namespace)
            if managed_ibmcloud and not live_deployment:
                if not is_ibm_sa_linked:
                    logger.info("Sleeping for 60 seconds before applying SA")
                    time.sleep(60)
                    link_all_sa_and_secret_and_delete_pods(
                        constants.OCS_SECRET, self.namespace
                    )
                    is_ibm_sa_linked = True
            csv.wait_for_phase("Succeeded", timeout=720)
            # Modify the CSV with custom values if required
            if all(
                key in config.DEPLOYMENT for key in ("csv_change_from", "csv_change_to")
            ):
                modify_csv(
                    csv=csv_name,
                    replace_from=config.DEPLOYMENT["csv_change_from"],
                    replace_to=config.DEPLOYMENT["csv_change_to"],
                )

        # create storage system
        if ocs_version >= version.VERSION_4_9:
            exec_cmd(f"oc apply -f {constants.STORAGE_SYSTEM_ODF_YAML}")

        ocp_version = version.get_semantic_ocp_version_from_config()
        if managed_ibmcloud:
            config_map = ocp.OCP(
                kind="configmap",
                namespace=self.namespace,
                resource_name=constants.ROOK_OPERATOR_CONFIGMAP,
            )
            config_map.get(retry=10, wait=5)
            config_map_patch = (
                '\'{"data": {"ROOK_CSI_KUBELET_DIR_PATH": "/var/data/kubelet"}}\''
            )
            logger.info("Patching config map to change KUBLET DIR PATH")
            exec_cmd(
                f"oc patch configmap -n {self.namespace} "
                f"{constants.ROOK_OPERATOR_CONFIGMAP} -p {config_map_patch}"
            )

        # create custom storage class for StorageCluster CR if necessary
        if self.CUSTOM_STORAGE_CLASS_PATH is not None:
            with open(self.CUSTOM_STORAGE_CLASS_PATH, "r") as custom_sc_fo:
                custom_sc = yaml.load(custom_sc_fo, Loader=yaml.SafeLoader)
            # set value of DEFAULT_STORAGECLASS to mach the custom storage cls
            self.DEFAULT_STORAGECLASS = custom_sc["metadata"]["name"]
            run_cmd(f"oc create -f {self.CUSTOM_STORAGE_CLASS_PATH}")

        # Set rook log level
        self.set_rook_log_level()

        # creating StorageCluster
        if config.DEPLOYMENT.get("kms_deployment"):
            kms = KMS.get_kms_deployment()
            kms.deploy()

        if config.ENV_DATA["mcg_only_deployment"]:
            mcg_only_deployment()
            return

        cluster_data = templating.load_yaml(constants.STORAGE_CLUSTER_YAML)
        # Figure out all the OCS modules enabled/disabled
        # CLI parameter --disable-components takes the precedence over
        # anything which comes from config file
        if config.ENV_DATA.get("disable_components"):
            for component in config.ENV_DATA["disable_components"]:
                config.COMPONENTS[f"disable_{component}"] = True
                logger.warning(f"disabling: {component}")

        # Update cluster_data with respective component enable/disable
        for key in config.COMPONENTS.keys():
            comp_name = constants.OCS_COMPONENTS_MAP[key.split("_")[1]]
            if config.COMPONENTS[key]:
                if "noobaa" in key:
                    merge_dict(
                        cluster_data,
                        {
                            "spec": {
                                "multiCloudGateway": {"reconcileStrategy": "ignore"}
                            }
                        },
                    )
                else:
                    merge_dict(
                        cluster_data,
                        {
                            "spec": {
                                "managedResources": {
                                    f"{comp_name}": {"reconcileStrategy": "ignore"}
                                }
                            }
                        },
                    )

        device_class = config.ENV_DATA.get("device_class")
        if arbiter_deployment:
            cluster_data["spec"]["arbiter"] = {}
            cluster_data["spec"]["nodeTopologies"] = {}
            cluster_data["spec"]["arbiter"]["enable"] = True
            cluster_data["spec"]["nodeTopologies"][
                "arbiterLocation"
            ] = self.get_arbiter_location()
            cluster_data["spec"]["storageDeviceSets"][0]["replica"] = 4

        cluster_data["metadata"]["name"] = config.ENV_DATA["storage_cluster_name"]

        deviceset_data = cluster_data["spec"]["storageDeviceSets"][0]
        device_size = int(config.ENV_DATA.get("device_size", defaults.DEVICE_SIZE))
        if device_class:
            deviceset_data["deviceClass"] = device_class

        logger.info(
            "Flexible scaling is available from version 4.7 on LSO cluster with less than 3 zones"
        )
        zone_num = get_az_count()
        if (
            config.DEPLOYMENT.get("local_storage")
            and ocs_version >= version.VERSION_4_7
            and zone_num < 3
            and not config.DEPLOYMENT.get("arbiter_deployment")
        ):
            cluster_data["spec"]["flexibleScaling"] = True
            # https://bugzilla.redhat.com/show_bug.cgi?id=1921023
            cluster_data["spec"]["storageDeviceSets"][0]["count"] = 3
            cluster_data["spec"]["storageDeviceSets"][0]["replica"] = 1

        # set size of request for storage
        if self.platform.lower() in [
            constants.BAREMETAL_PLATFORM,
            constants.HCI_BAREMETAL,
        ]:
            pv_size_list = helpers.get_pv_size(
                storageclass=self.DEFAULT_STORAGECLASS_LSO
            )
            pv_size_list.sort()
            deviceset_data["dataPVCTemplate"]["spec"]["resources"]["requests"][
                "storage"
            ] = f"{pv_size_list[0]}"
        else:
            deviceset_data["dataPVCTemplate"]["spec"]["resources"]["requests"][
                "storage"
            ] = f"{device_size}Gi"

        # set storage class to OCS default on current platform
        if self.DEFAULT_STORAGECLASS:
            deviceset_data["dataPVCTemplate"]["spec"][
                "storageClassName"
            ] = self.DEFAULT_STORAGECLASS

        # StorageCluster tweaks for LSO
        if config.DEPLOYMENT.get("local_storage"):
            cluster_data["spec"]["manageNodes"] = False
            cluster_data["spec"]["monDataDirHostPath"] = "/var/lib/rook"
            deviceset_data["name"] = constants.DEFAULT_DEVICESET_LSO_PVC_NAME
            deviceset_data["portable"] = False
            deviceset_data["dataPVCTemplate"]["spec"][
                "storageClassName"
            ] = self.DEFAULT_STORAGECLASS_LSO
            lso_type = config.DEPLOYMENT.get("type")
            if (
                self.platform.lower() == constants.AWS_PLATFORM
                and not lso_type == constants.AWS_EBS
            ):
                deviceset_data["count"] = 2
            # setting resource limits for AWS i3
            # https://access.redhat.com/documentation/en-us/red_hat_openshift_container_storage/4.6/html-single/deploying_openshift_container_storage_using_amazon_web_services/index#creating-openshift-container-storage-cluster-on-amazon-ec2_local-storage
            if (
                ocs_version >= version.VERSION_4_5
                and config.ENV_DATA.get("worker_instance_type")
                == constants.AWS_LSO_WORKER_INSTANCE
            ):
                deviceset_data["resources"] = {
                    "limits": {"cpu": 2, "memory": "5Gi"},
                    "requests": {"cpu": 1, "memory": "5Gi"},
                }
            if (ocp_version >= version.VERSION_4_6) and (
                ocs_version >= version.VERSION_4_6
            ):
                cluster_data["metadata"]["annotations"] = {
                    "cluster.ocs.openshift.io/local-devices": "true"
                }
            count = config.DEPLOYMENT.get("local_storage_storagedeviceset_count")
            if count is not None:
                deviceset_data["count"] = count

        # Allow lower instance requests and limits for OCS deployment
        # The resources we need to change can be found here:
        # https://github.com/openshift/ocs-operator/blob/release-4.5/pkg/deploy-manager/storagecluster.go#L88-L116
        if config.DEPLOYMENT.get("allow_lower_instance_requirements"):
            none_resources = {"Requests": None, "Limits": None}
            deviceset_data["resources"] = deepcopy(none_resources)
            resources = [
                "mon",
                "mds",
                "rgw",
                "mgr",
                "noobaa-core",
                "noobaa-db",
            ]
            if ocs_version >= version.VERSION_4_5:
                resources.append("noobaa-endpoint")
            cluster_data["spec"]["resources"] = {
                resource: deepcopy(none_resources) for resource in resources
            }
            if ocs_version >= version.VERSION_4_5:
                cluster_data["spec"]["resources"]["noobaa-endpoint"] = {
                    "limits": {"cpu": 1, "memory": "500Mi"},
                    "requests": {"cpu": 1, "memory": "500Mi"},
                }
        else:
            local_storage = config.DEPLOYMENT.get("local_storage")
            platform = config.ENV_DATA.get("platform", "").lower()
            if local_storage and platform == "aws":
                resources = {
                    "mds": {
                        "limits": {"cpu": 3, "memory": "8Gi"},
                        "requests": {"cpu": 1, "memory": "8Gi"},
                    }
                }
                if ocs_version < version.VERSION_4_5:
                    resources["noobaa-core"] = {
                        "limits": {"cpu": 2, "memory": "8Gi"},
                        "requests": {"cpu": 1, "memory": "8Gi"},
                    }
                    resources["noobaa-db"] = {
                        "limits": {"cpu": 2, "memory": "8Gi"},
                        "requests": {"cpu": 1, "memory": "8Gi"},
                    }
                cluster_data["spec"]["resources"] = resources

        # Enable host network if enabled in config (this require all the
        # rules to be enabled on underlaying platform).
        if config.DEPLOYMENT.get("host_network"):
            cluster_data["spec"]["hostNetwork"] = True

        cluster_data["spec"]["storageDeviceSets"] = [deviceset_data]

        if managed_ibmcloud:
            mon_pvc_template = {
                "spec": {
                    "accessModes": ["ReadWriteOnce"],
                    "resources": {"requests": {"storage": "20Gi"}},
                    "storageClassName": self.DEFAULT_STORAGECLASS,
                    "volumeMode": "Filesystem",
                }
            }
            cluster_data["spec"]["monPVCTemplate"] = mon_pvc_template
            # Need to check if it's needed for ibm cloud to set manageNodes
            cluster_data["spec"]["manageNodes"] = False

        if config.ENV_DATA.get("encryption_at_rest"):
            if ocs_version < version.VERSION_4_6:
                error_message = "Encryption at REST can be enabled only on OCS >= 4.6!"
                logger.error(error_message)
                raise UnsupportedFeatureError(error_message)
            logger.info("Enabling encryption at REST!")
            cluster_data["spec"]["encryption"] = {
                "enable": True,
            }
            if ocs_version >= version.VERSION_4_10:
                cluster_data["spec"]["encryption"] = {
                    "clusterWide": True,
                }
            if config.DEPLOYMENT.get("kms_deployment"):
                cluster_data["spec"]["encryption"]["kms"] = {
                    "enable": True,
                }
            if config.DEPLOYMENT.get("sc_encryption"):
                if not config.DEPLOYMENT.get("kms_deployment"):
                    raise UnsupportedFeatureError(
                        "StorageClass encryption can be enabled only when KMS is enabled!"
                    )
                cluster_data["spec"]["encryption"]["storageClass"] = True

        if config.DEPLOYMENT.get("ceph_debug"):
            setup_ceph_debug()
            cluster_data["spec"]["managedResources"] = {
                "cephConfig": {"reconcileStrategy": "ignore"}
            }
        if config.ENV_DATA.get("is_multus_enabled"):
            public_net_name = config.ENV_DATA["multus_public_net_name"]
            public_net_namespace = config.ENV_DATA["multus_public_net_namespace"]
            cluster_net_name = config.ENV_DATA["multus_cluster_net_name"]
            cluster_net_namespace = config.ENV_DATA["multus_cluster_net_namespace"]
            selector_data = {}
            if create_public_net:
                public_selector_data = {
                    "public": f"{public_net_namespace}/{public_net_name}"
                }
                selector_data.update(public_selector_data)
            if create_cluster_net:
                cluster_selector_data = {
                    "cluster": f"{cluster_net_namespace}/{cluster_net_name}"
                }
                selector_data.update(cluster_selector_data)
            cluster_data["spec"]["network"] = {
                "provider": "multus",
                "selectors": selector_data,
            }

        # Enable in-transit encryption.
        if config.ENV_DATA.get("in_transit_encryption"):
            if "network" not in cluster_data["spec"]:
                cluster_data["spec"]["network"] = {}

            if "connections" not in cluster_data["spec"]["network"]:
                cluster_data["spec"]["network"]["connections"] = {}

            cluster_data["spec"]["network"]["connections"] = {
                "encryption": {"enabled": True}
            }

        # Use Custom Storageclass Names
        if config.ENV_DATA.get("custom_default_storageclass_names"):
            storageclassnames = config.ENV_DATA.get("storageclassnames")

            keys_to_update = [
                constants.OCS_COMPONENTS_MAP["cephfs"],
                constants.OCS_COMPONENTS_MAP["rgw"],
                constants.OCS_COMPONENTS_MAP["blockpools"],
                constants.OCS_COMPONENTS_MAP["cephnonresilentpools"],
            ]

            cluster_data.setdefault("spec", {}).setdefault("managedResources", {})

            for key in keys_to_update:
                if storageclassnames.get(key):
                    cluster_data["spec"]["managedResources"][key] = {
                        "storageClassName": storageclassnames[key]
                    }

            if cluster_data["spec"].get("nfs"):
                cluster_data["spec"]["nfs"] = {
                    "storageClassName": storageclassnames["nfs"]
                }

            if cluster_data["spec"].get("encryption"):
                cluster_data["spec"]["encryption"] = {
                    "storageClassName": storageclassnames["encryption"]
                }
        performance_profile = config.ENV_DATA.get("performance_profile")
        if performance_profile:
            cluster_data["spec"]["resourceProfile"] = performance_profile
        # Bluestore-rdr for RDR greenfield deployments: 4.14 onwards
        if (
            (version.get_semantic_ocs_version_from_config() >= version.VERSION_4_14)
            and config.multicluster
            and (config.MULTICLUSTER.get("multicluster_mode") == "regional-dr")
            and config.ENV_DATA.get("rdr_osd_deployment_mode")
            == constants.RDR_OSD_MODE_GREENFIELD
        ):
            rdr_bluestore_annotation = {
                "ocs.openshift.io/clusterIsDisasterRecoveryTarget": "true"
            }
            merge_dict(
                cluster_data, {"metadata": {"annotations": rdr_bluestore_annotation}}
            )
        if config.ENV_DATA.get("noobaa_external_pgsql"):
            pgsql_data = config.AUTH["pgsql"]
            user = pgsql_data["username"]
            password = pgsql_data["password"]
            host = pgsql_data["host"]
            port = pgsql_data["port"]
            pgsql_manager = pgsql.PgsqlManager(
                username=user,
                password=password,
                host=host,
                port=port,
            )
            cluster_name = config.ENV_DATA["cluster_name"]
            db_name = f"nbcore_{cluster_name.replace('-', '_')}"
            pgsql_manager.create_database(
                db_name=db_name, extra_params="WITH LC_COLLATE = 'C' TEMPLATE template0"
            )
            create_external_pgsql_secret()
            cluster_data["spec"]["multiCloudGateway"] = {
                "externalPgConfig": {"pgSecretName": constants.NOOBAA_POSTGRES_SECRET}
            }
        # To be able to verify: https://bugzilla.redhat.com/show_bug.cgi?id=2276694
        wait_timeout_for_healthy_osd_in_minutes = config.ENV_DATA.get(
            "wait_timeout_for_healthy_osd_in_minutes"
        )
        # For testing: https://issues.redhat.com/browse/RHSTOR-5929
        ceph_threshold_backfill_full_ratio = config.ENV_DATA.get(
            "ceph_threshold_backfill_full_ratio"
        )
        ceph_threshold_full_ratio = config.ENV_DATA.get("ceph_threshold_full_ratio")
        ceph_threshold_near_full_ratio = config.ENV_DATA.get(
            "ceph_threshold_near_full_ratio"
        )
        set_managed_resources_ceph_cluster = (
            wait_timeout_for_healthy_osd_in_minutes
            or ceph_threshold_backfill_full_ratio
            or ceph_threshold_full_ratio
            or ceph_threshold_near_full_ratio
        )
        if set_managed_resources_ceph_cluster:
            cluster_data.setdefault("spec", {}).setdefault(
                "managedResources", {}
            ).setdefault("cephCluster", {})
            managed_resources_ceph_cluster = cluster_data["spec"]["managedResources"][
                "cephCluster"
            ]
            if wait_timeout_for_healthy_osd_in_minutes:
                managed_resources_ceph_cluster[
                    "waitTimeoutForHealthyOSDInMinutes"
                ] = wait_timeout_for_healthy_osd_in_minutes
            if ceph_threshold_backfill_full_ratio:
                managed_resources_ceph_cluster[
                    "backfillFullRatio"
                ] = ceph_threshold_backfill_full_ratio
            if ceph_threshold_full_ratio:
                managed_resources_ceph_cluster["fullRatio"] = ceph_threshold_full_ratio
            if ceph_threshold_near_full_ratio:
                managed_resources_ceph_cluster[
                    "nearFullRatio"
                ] = ceph_threshold_near_full_ratio

        cluster_data_yaml = tempfile.NamedTemporaryFile(
            mode="w+", prefix="cluster_storage", delete=False
        )
        templating.dump_data_to_temp_yaml(cluster_data, cluster_data_yaml.name)
        run_cmd(f"oc create -f {cluster_data_yaml.name}", timeout=1200)
        if config.DEPLOYMENT["infra_nodes"]:
            _ocp = ocp.OCP(kind="node")
            _ocp.exec_oc_cmd(
                command=f"annotate namespace {config.ENV_DATA['cluster_namespace']} "
                f"{constants.NODE_SELECTOR_ANNOTATION}"
            )

    def cleanup_pgsql_db(self):
        """
        Perform cleanup for noobaa external pgsql DB in case external pgsq is enabled.
        """
        if config.ENV_DATA.get("noobaa_external_pgsql"):
            pgsql_data = config.AUTH["pgsql"]
            user = pgsql_data["username"]
            password = pgsql_data["password"]
            host = pgsql_data["host"]
            port = pgsql_data["port"]
            pgsql_manager = pgsql.PgsqlManager(
                username=user,
                password=password,
                host=host,
                port=port,
            )
            cluster_name = config.ENV_DATA["cluster_name"]
            db_name = f"nbcore_{cluster_name.replace('-', '_')}"
            pgsql_manager.delete_database(db_name=db_name)

    def deploy_odf_addon(self):
        """
        This method deploy ODF addon.

        """
        logger.info("Deploying odf with ocs addon.")
        clustername = config.ENV_DATA.get("cluster_name")
        ocs_version = version.get_semantic_ocs_version_from_config()
        disable_noobaa = config.COMPONENTS.get("disable_noobaa", False)
        noobaa_cmd_arg = f"--param ignoreNoobaa={str(disable_noobaa).lower()}"
        cmd = (
            f"ibmcloud ks cluster addon enable openshift-data-foundation --cluster {clustername} -f --version "
            f"{ocs_version}.0 {noobaa_cmd_arg}"
        )
        run_ibmcloud_cmd(cmd)
        time.sleep(120)
        logger.info("Ocs addon started enabling.")

    def deployment_with_ui(self):
        """
        Deployment OCS Operator via OpenShift Console

        """
        from ocs_ci.ocs.ui.base_ui import login_ui, close_browser
        from ocs_ci.ocs.ui.deployment_ui import DeploymentUI

        live_deployment = config.DEPLOYMENT.get("live_deployment")
        if not live_deployment:
            create_catalog_source()
        login_ui()
        deployment_obj = DeploymentUI()
        deployment_obj.install_ocs_ui()
        close_browser()

    def deploy_with_external_mode(self):
        """
        This function handles the deployment of OCS on
        external/indpendent RHCS cluster

        """

        if not config.DEPLOYMENT.get("multi_storagecluster"):
            live_deployment = config.DEPLOYMENT.get("live_deployment")
            logger.info("Deploying OCS with external mode RHCS")
            ui_deployment = config.DEPLOYMENT.get("ui_deployment")
            if not ui_deployment:
                logger.info("Creating namespace and operator group.")
                run_cmd(f"oc create -f {constants.OLM_YAML}")
            if not live_deployment:
                create_catalog_source()
            self.subscribe_ocs()
            operator_selector = get_selector_for_ocs_operator()
            subscription_plan_approval = config.DEPLOYMENT.get(
                "subscription_plan_approval"
            )
            ocs_operator_names = get_required_csvs()
            channel = config.DEPLOYMENT.get("ocs_csv_channel")
            for ocs_operator_name in ocs_operator_names:
                package_manifest = PackageManifest(
                    resource_name=ocs_operator_name,
                    selector=operator_selector,
                    subscription_plan_approval=subscription_plan_approval,
                )
                package_manifest.wait_for_resource(timeout=300)
                csv_name = package_manifest.get_current_csv(channel=channel)
                csv = CSV(resource_name=csv_name, namespace=self.namespace)
                csv.wait_for_phase("Succeeded", timeout=720)
        # Set rook log level
        self.set_rook_log_level()

        # get external cluster details
        host, user, password, ssh_key = get_external_cluster_client()
        external_cluster = ExternalCluster(host, user, password, ssh_key)
        external_cluster.get_external_cluster_details()

        # get admin keyring
        external_cluster.get_admin_keyring()

        cluster_data = templating.load_yaml(constants.EXTERNAL_STORAGE_CLUSTER_YAML)

        if config.DEPLOYMENT.get("multi_storagecluster"):
            cluster_data["metadata"]["namespace"] = config.ENV_DATA[
                "external_storage_cluster_namespace"
            ]
            cluster_data["metadata"]["name"] = config.ENV_DATA[
                "external_storage_cluster_name"
            ]
            exec_cmd(
                f"oc create -f {constants.MULTI_STORAGECLUSTER_EXTERNAL_NAMESPACE}"
            )
            label_pod_security_admission(
                namespace=constants.OPENSHIFT_STORAGE_EXTENDED_NAMESPACE
            )
            exec_cmd(f"oc create -f {constants.STORAGE_SYSTEM_ODF_EXTERNAL}")
        else:
            cluster_data["metadata"]["name"] = config.ENV_DATA["storage_cluster_name"]

        # Create secret for external cluster
        create_external_secret()
        # Use Custom Storageclass Names
        if config.ENV_DATA.get("custom_default_storageclass_names"):
            storageclassnames = config.ENV_DATA.get("storageclassnames")

            keys_to_update = [
                constants.OCS_COMPONENTS_MAP["cephfs"],
                constants.OCS_COMPONENTS_MAP["rgw"],
                constants.OCS_COMPONENTS_MAP["blockpools"],
            ]

            cluster_data.setdefault("spec", {}).setdefault("managedResources", {})

            for key in keys_to_update:
                if storageclassnames.get(key):
                    cluster_data["spec"]["managedResources"][key] = {
                        "storageClassName": storageclassnames[key]
                    }

            # Setting up nonResilientPools custome storageclass names
            non_resilient_pool_key = constants.OCS_COMPONENTS_MAP[
                "cephnonresilentpools"
            ]
            non_resilient_pool_data = cluster_data["spec"]["managedResources"].get(
                non_resilient_pool_key, {}
            )

            if non_resilient_pool_data.get("enable"):
                non_resilient_pool_data = {
                    "enable": True,
                    "storageClassName": storageclassnames[non_resilient_pool_key],
                }
            cluster_data["spec"]["managedResources"][
                non_resilient_pool_key
            ] = non_resilient_pool_data

            # Setting up custom storageclass names for 'nfs' service
            if cluster_data["spec"].get("nfs", {}).get("enable"):
                cluster_data["spec"]["nfs"]["storageClassName"] = storageclassnames[
                    "nfs"
                ]

            # Setting up custom storageclass names for 'encryption' service
            if cluster_data["spec"].get("encryption", {}).get("enable"):
                cluster_data["spec"]["encryption"][
                    "storageClassName"
                ] = storageclassnames["encryption"]

        # Enable in-transit encryption.
        if config.ENV_DATA.get("in_transit_encryption"):
            cluster_data["spec"]["network"] = {
                "connections": {"encryption": {"enabled": True}},
            }
        cluster_data_yaml = tempfile.NamedTemporaryFile(
            mode="w+", prefix="external_cluster_storage", delete=False
        )
        templating.dump_data_to_temp_yaml(cluster_data, cluster_data_yaml.name)
        run_cmd(f"oc create -f {cluster_data_yaml.name}", timeout=2400)
        self.external_post_deploy_validation()

        # enable secure connection mode for in-transit encryption
        if config.ENV_DATA.get("in_transit_encryption"):
            external_cluster.enable_secure_connection_mode()
        if config.DEPLOYMENT.get("multi_storagecluster"):
            logger.info("not setting toolbox in multi-storagecluster")
        else:
            setup_ceph_toolbox()
        logger.info("Checking ceph health for external cluster")
        if not config.DEPLOYMENT.get("multi_storagecluster"):
            try:
                ceph_health_check(
                    tries=30,
                    delay=10,
                )
            except CephHealthException:
                raise CephHealthException("External ceph cluster not healthy")
        else:
            try:
                ceph_health_check_multi_storagecluster_external()
            except CephHealthException:
                raise CephHealthException(
                    "External multi-storagecluster external ceph cluster not healthy"
                )

    def set_rook_log_level(self):
        rook_log_level = config.DEPLOYMENT.get("rook_log_level")
        if rook_log_level:
            set_configmap_log_level_rook_ceph_operator(rook_log_level)

    def external_post_deploy_validation(self):
        """
        This function validates successful deployment of OCS
        in external mode, some of the steps overlaps with
        converged mode

        """
        cephcluster = CephClusterExternal()
        cephcluster.cluster_health_check(timeout=300)

    def deploy_ocs(self):
        """
        Handle OCS deployment, since OCS deployment steps are common to any
        platform, implementing OCS deployment here in base class.
        """
        set_registry_to_managed_state()
        image = None
        ceph_cluster = None
        ceph_cluster = ocp.OCP(kind="CephCluster", namespace=self.namespace)
        try:
            ceph_cluster.get().get("items")[0]
            logger.warning("OCS cluster already exists")
            return
        except (IndexError, CommandFailed):
            logger.info("Running OCS basic installation")

        # disconnected installation?
        load_cluster_info()
        if config.DEPLOYMENT.get("disconnected") and not config.DEPLOYMENT.get(
            "disconnected_env_skip_image_mirroring"
        ):
            image = prepare_disconnected_ocs_deployment()

        if config.DEPLOYMENT["external_mode"]:
            self.deploy_with_external_mode()
        else:
            self.deploy_ocs_via_operator(image)
            if config.ENV_DATA["mcg_only_deployment"]:
                mcg_only_post_deployment_checks()
                return

            # get ODF version and set MGR count based on ODF version
            ocs_version = version.get_semantic_ocs_version_from_config()
            mgr_count = constants.MGR_COUNT_415
            if ocs_version < version.VERSION_4_15:
                mgr_count = constants.MGR_COUNT

            pod = ocp.OCP(kind=constants.POD, namespace=self.namespace)
            cfs = ocp.OCP(kind=constants.CEPHFILESYSTEM, namespace=self.namespace)
            # Check for Ceph pods
            mon_pod_timeout = 900
            assert pod.wait_for_resource(
                condition="Running",
                selector="app=rook-ceph-mon",
                resource_count=3,
                timeout=mon_pod_timeout,
            )
            assert pod.wait_for_resource(
                condition="Running",
                selector="app=rook-ceph-mgr",
                resource_count=mgr_count,
                timeout=600,
            )
            assert pod.wait_for_resource(
                condition="Running",
                selector="app=rook-ceph-osd",
                resource_count=3,
                timeout=600,
            )

            # validate ceph mon/osd volumes are backed by pvc
            validate_cluster_on_pvc()

            # check for odf-console
            if ocs_version >= version.VERSION_4_9:
                assert pod.wait_for_resource(
                    condition="Running", selector="app=odf-console", timeout=600
                )

            # Creating toolbox pod
            setup_ceph_toolbox()

            assert pod.wait_for_resource(
                condition=constants.STATUS_RUNNING,
                selector="app=rook-ceph-tools",
                resource_count=1,
                timeout=600,
            )

            if not config.COMPONENTS["disable_cephfs"]:
                # Check for CephFilesystem creation in ocp
                cfs_data = cfs.get()
                cfs_name = cfs_data["items"][0]["metadata"]["name"]

                if helpers.validate_cephfilesystem(cfs_name):
                    logger.info("MDS deployment is successful!")
                    defaults.CEPHFILESYSTEM_NAME = cfs_name
                else:
                    logger.error("MDS deployment Failed! Please check logs!")
            if config.DEPLOYMENT.get("multi_storagecluster"):
                self.deploy_with_external_mode()
                # Checking external cephcluster health
                retry((CephHealthException, CommandFailed), tries=5, delay=20,)(
                    check_cephcluster_status(
                        desired_phase="Connected",
                        desired_health="HEALTH_OK",
                        name=constants.EXTERNAL_CEPHCLUSTER_NAME,
                        namespace=constants.OPENSHIFT_STORAGE_EXTENDED_NAMESPACE,
                    )
                )

        wait_timeout_for_healthy_osd_in_minutes = config.ENV_DATA.get(
            "wait_timeout_for_healthy_osd_in_minutes"
        )
        # This is a W/A because of the issue mentioned in
        # https://github.com/red-hat-storage/ocs-ci/issues/9901
        if wait_timeout_for_healthy_osd_in_minutes:
            run_cmd(
                f"oc patch storagecluster ocs-storagecluster -n {self.namespace}  --type merge -p '"
                '{"spec": {"managedResources": {"cephCluster": '
                f'{{"waitTimeoutForHealthyOSDInMinutes": {wait_timeout_for_healthy_osd_in_minutes}'
                "}}}}'"
            )

        # Change monitoring backend to OCS
        if config.ENV_DATA.get("monitoring_enabled") and config.ENV_DATA.get(
            "persistent-monitoring"
        ):
            setup_persistent_monitoring()
        elif config.ENV_DATA.get("monitoring_enabled") and config.ENV_DATA.get(
            "telemeter_server_url"
        ):
            # Create configmap cluster-monitoring-config to reconfigure
            # telemeter server url when 'persistent-monitoring' is False
            create_configmap_cluster_monitoring_pod(
                telemeter_server_url=config.ENV_DATA["telemeter_server_url"]
            )

        if not config.COMPONENTS["disable_cephfs"]:
            # Change registry backend to OCS CEPHFS RWX PVC
            registry.change_registry_backend_to_ocs()

        # Enable console plugin
        enable_console_plugin()

        # validate PDB creation of MON, MDS, OSD pods
        if not config.DEPLOYMENT["external_mode"]:
            validate_pdb_creation()

        # Verify health of ceph cluster
        logger.info("Done creating rook resources, waiting for HEALTH_OK")
        try:
            ceph_health_check(namespace=self.namespace, tries=30, delay=10)
        except CephHealthException as ex:
            err = str(ex)
            logger.warning(f"Ceph health check failed with {err}")
            if "clock skew detected" in err:
                logger.info(
                    f"Changing NTP on compute nodes to" f" {constants.RH_NTP_CLOCK}"
                )
                if self.platform == constants.VSPHERE_PLATFORM:
                    update_ntp_compute_nodes()
                assert ceph_health_check(namespace=self.namespace, tries=60, delay=10)

        # In case of RDR, check for bluestore-rdr on osds: 4.14 onwards
        if (
            (version.get_semantic_ocs_version_from_config() >= version.VERSION_4_14)
            and config.multicluster
            and (config.MULTICLUSTER.get("multicluster_mode") == "regional-dr")
            and config.ENV_DATA.get("rdr_osd_deployment_mode")
            == constants.RDR_OSD_MODE_GREENFIELD
        ):
            if not ceph_cluster:
                ceph_cluster = ocp.OCP(kind="CephCluster", namespace=self.namespace)
            store_type = ceph_cluster.get().get("items")[0]["status"]["storage"]["osd"][
                "storeType"
            ]
            if "bluestore-rdr" in store_type.keys():
                logger.info("OSDs with bluestore-rdr found ")
            else:
                raise UnexpectedDeploymentConfiguration(
                    f"OSDs were not brought up with Regional DR bluestore! instead we have {store_type} "
                )

            if store_type["bluestore-rdr"] == get_osd_count():
                logger.info(
                    f"OSDs found matching with bluestore-rdr count {store_type['bluestore-rdr']}"
                )
            else:
                raise UnexpectedDeploymentConfiguration(
                    f"OSDs count mismatch! bluestore-rdr count = {store_type['bluestore-rdr']} "
                    f"actual osd count = {get_osd_count()}"
                )

        # patch gp2/thin storage class as 'non-default'
        self.patch_default_sc_to_non_default()

    def deploy_lvmo(self):
        """
        deploy lvmo for platform specific (for now only vsphere)
        """
        if not config.DEPLOYMENT["install_lvmo"]:
            logger.warning("LVMO deployment will be skipped")
            return

        logger.info(f"Installing lvmo version {config.ENV_DATA['ocs_version']}")
        lvmo_version = config.ENV_DATA["ocs_version"]
        lvmo_version_without_period = lvmo_version.replace(".", "")
        label_version = constants.LVMO_POD_LABEL
        create_catalog_source()
        # this is a workaround for 2103818
        lvm_full_version = get_lvm_full_version()
        major, minor = lvm_full_version.split("-")
        if int(minor) > 105 and major == "4.11.0":
            lvmo_version_without_period = "411"
        elif int(minor) < 105 and major == "4.11.0":
            lvmo_version_without_period = "411-old"

        file_version = lvmo_version_without_period
        if "old" in file_version:
            file_version = file_version.split("-")[0]

        if "lvms" in config.DEPLOYMENT["ocs_registry_image"]:
            cluster_config_file = os.path.join(
                constants.TEMPLATE_DEPLOYMENT_DIR_LVMO,
                "lvms-cluster.yaml",
            )
        else:
            cluster_config_file = os.path.join(
                constants.TEMPLATE_DEPLOYMENT_DIR_LVMO,
                f"lvm-cluster-{file_version}.yaml",
            )

        if version.get_semantic_ocs_version_from_config() >= version.VERSION_4_11:
            lvmo_version_without_period = "default"

        # this is a workaround for 2101343
        if 110 > int(minor) > 98 and major == "4.11.0":
            rolebinding_config_file = os.path.join(
                constants.TEMPLATE_DEPLOYMENT_DIR_LVMO, "role_rolebinding.yaml"
            )
            run_cmd(f"oc create -f {rolebinding_config_file} -n default")
        # end of workaround
        lvm_bundle_filename = (
            "lvms-bundle.yaml"
            if "lvms" in config.DEPLOYMENT["ocs_registry_image"]
            else "lvm-bundle.yaml"
        )

        bundle_config_file = os.path.join(
            constants.TEMPLATE_DEPLOYMENT_DIR_LVMO, lvm_bundle_filename
        )
        run_cmd(f"oc create -f {bundle_config_file} -n {self.namespace}")
        pod = ocp.OCP(kind=constants.POD, namespace=self.namespace)
        assert pod.wait_for_resource(
            condition="Running",
            selector=label_version[lvmo_version_without_period][
                "controller_manager_label"
            ],
            resource_count=1,
            timeout=300,
        )
        time.sleep(30)
        run_cmd(f"oc create -f {cluster_config_file} -n {self.namespace}")
        assert pod.wait_for_resource(
            condition="Running",
            selector=label_version[lvmo_version_without_period][
                "topolvm-controller_label"
            ],
            resource_count=1,
            timeout=300,
        )
        assert pod.wait_for_resource(
            condition="Running",
            selector=label_version[lvmo_version_without_period]["topolvm-node_label"],
            resource_count=1,
            timeout=300,
        )
        assert pod.wait_for_resource(
            condition="Running",
            selector=label_version[lvmo_version_without_period]["vg-manager_label"],
            resource_count=1,
            timeout=300,
        )
        catalgesource = run_cmd(
            "oc -n openshift-marketplace get  "
            "catalogsources.operators.coreos.com redhat-operators -o json"
        )
        json_cts = json.loads(catalgesource)
        logger.info(
            f"LVMO installed successfully from image {json_cts['spec']['image']}"
        )

    def destroy_cluster(self, log_level="DEBUG"):
        """
        Base destroy cluster method, for more platform specific stuff please
        overload this method in child class.

        Args:
            log_level (str): log level for installer (default: DEBUG)
        """
        if config.DEPLOYMENT.get("skip_ocp_installer_destroy"):
            logger.info(
                "OCP Destroy is skipped because skip_ocp_installer_destroy was enabled!"
            )
            return
        if self.platform == constants.IBM_POWER_PLATFORM:
            if not config.ENV_DATA["skip_ocs_deployment"]:
                self.destroy_ocs()

            if not config.ENV_DATA["skip_ocp_deployment"]:
                logger.info("Destroy of OCP not implemented yet.")
        else:
            self.ocp_deployment = self.OCPDeployment()
            try:
                uninstall_ocs()
                # TODO - add ocs uninstall validation function call
                logger.info("OCS uninstalled successfully")
            except Exception as ex:
                logger.error(f"Failed to uninstall OCS. Exception is: {ex}")
                logger.info("resuming teardown")
            try:
                self.ocp_deployment.destroy(log_level)
            finally:
                self.cleanup_pgsql_db()

    def add_node(self):
        """
        Implement platform-specific add_node in child class
        """
        raise NotImplementedError("add node functionality not implemented")

    def patch_default_sc_to_non_default(self):
        """
        Patch storage class which comes as default with installation to non-default
        """
        if not self.DEFAULT_STORAGECLASS:
            logger.info(
                "Default StorageClass is not set for this class: "
                f"{self.__class__.__name__}"
            )
            return

        sc_to_patch = self.DEFAULT_STORAGECLASS
        if (
            config.ENV_DATA.get("use_custom_sc_in_deployment")
            and self.platform.lower() == constants.VSPHERE_PLATFORM
        ):
            sc_to_patch = "thin-csi"
        logger.info(f"Patch {sc_to_patch} storageclass as non-default")
        patch = ' \'{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"false"}}}\' '
        run_cmd(
            f"oc patch storageclass {sc_to_patch} "
            f"-p {patch} "
            f"--request-timeout=120s"
        )

    def acm_operator_installed(self):
        """
        Check if ACM HUB is already installed
        Returns:
             bool: True if ACM HUB operator is installed, False otherwise
        """
        ocp_obj = OCP(kind=constants.ROOK_OPERATOR, namespace=self.namespace)
        return ocp_obj.check_resource_existence(
            timeout=6,
            should_exist=True,
            resource_name=constants.ACM_HUB_OPERATOR_NAME_WITH_NS,
        )

    def deploy_acm_hub(self):
        """
        Handle ACM HUB deployment
        """
        if self.acm_operator_installed():
            logger.info("ACM Operator is already installed")
            self.deploy_multicluster_hub()
            return

        if config.ENV_DATA.get("acm_hub_unreleased"):
            self.deploy_acm_hub_unreleased()
        else:
            self.deploy_acm_hub_released()
            self.deploy_multicluster_hub()

    def deploy_acm_hub_unreleased(self):
        """
        Handle ACM HUB unreleased image deployment
        """
        logger.info("Cloning open-cluster-management deploy repository")
        acm_hub_deploy_dir = os.path.join(
            constants.EXTERNAL_DIR, "acm_hub_unreleased_deploy"
        )
        clone_repo(constants.ACM_HUB_UNRELEASED_DEPLOY_REPO, acm_hub_deploy_dir)

        logger.info("Retrieving quay token")
        docker_config = load_auth_config().get("quay", {}).get("cli_password", {})
        pw = base64.b64decode(docker_config)
        pw = pw.decode().replace("quay.io", "quay.io:443").encode()
        quay_token = base64.b64encode(pw).decode()

        kubeconfig_location = os.path.join(self.cluster_path, "auth", "kubeconfig")

        logger.info("Setting env vars")
        env_vars = {
            "QUAY_TOKEN": quay_token,
            "COMPOSITE_BUNDLE": "true",
            "CUSTOM_REGISTRY_REPO": "quay.io:443/acm-d",
            "DOWNSTREAM": "true",
            "DEBUG": "true",
            "KUBECONFIG": kubeconfig_location,
        }
        for key, value in env_vars.items():
            if value:
                os.environ[key] = value

        logger.info("Writing pull-secret")
        _templating = templating.Templating(
            os.path.join(constants.TEMPLATE_DIR, "acm-deployment")
        )
        template_data = {"docker_config": docker_config}
        data = _templating.render_template(
            constants.ACM_HUB_UNRELEASED_PULL_SECRET_TEMPLATE,
            template_data,
        )
        pull_secret_path = os.path.join(
            acm_hub_deploy_dir, "prereqs", "pull-secret.yaml"
        )
        with open(pull_secret_path, "w") as f:
            f.write(data)

        logger.info("Creating ImageContentSourcePolicy")
        run_cmd(f"oc create -f {constants.ACM_HUB_UNRELEASED_ICSP_YAML}")

        logger.info("Writing tag data to snapshot.ver")
        acm_version = config.ENV_DATA.get("acm_version")

        image_tag = config.ENV_DATA.get(
            "acm_unreleased_image"
        ) or get_latest_acm_tag_unreleased(version=acm_version)

        with open(os.path.join(acm_hub_deploy_dir, "snapshot.ver"), "w") as f:
            f.write(image_tag)

        logger.info("Running open-cluster-management deploy")
        cmd = ["./start.sh", "--silent"]
        logger.info("Running cmd: %s", " ".join(cmd))
        proc = Popen(
            cmd,
            cwd=acm_hub_deploy_dir,
            stdout=PIPE,
            stderr=PIPE,
            encoding="utf-8",
        )
        stdout, stderr = proc.communicate()
        logger.info(stdout)
        if proc.returncode:
            logger.error(stderr)
            raise CommandFailed("open-cluster-management deploy script error")

        validate_acm_hub_install()

    def deploy_acm_hub_released(self):
        """
        Handle ACM HUB released image deployment
        """
        channel = config.ENV_DATA.get("acm_hub_channel")
        logger.info("Creating ACM HUB namespace")
        acm_hub_namespace_yaml_data = templating.load_yaml(constants.NAMESPACE_TEMPLATE)
        acm_hub_namespace_yaml_data["metadata"]["name"] = constants.ACM_HUB_NAMESPACE
        acm_hub_namespace_manifest = tempfile.NamedTemporaryFile(
            mode="w+", prefix="acm_hub_namespace_manifest", delete=False
        )
        templating.dump_data_to_temp_yaml(
            acm_hub_namespace_yaml_data, acm_hub_namespace_manifest.name
        )
        run_cmd(f"oc create -f {acm_hub_namespace_manifest.name}")

        logger.info("Creating OperationGroup for ACM deployment")
        package_manifest = PackageManifest(
            resource_name=constants.ACM_HUB_OPERATOR_NAME,
        )

        run_cmd(
            f"oc create -f {constants.ACM_HUB_OPERATORGROUP_YAML} -n {constants.ACM_HUB_NAMESPACE}"
        )

        logger.info("Creating ACM HUB Subscription")
        acm_hub_subscription_yaml_data = templating.load_yaml(
            constants.ACM_HUB_SUBSCRIPTION_YAML
        )
        acm_hub_subscription_yaml_data["spec"]["channel"] = channel
        retry(
            (ResourceNameNotSpecifiedException, ChannelNotFound, CommandFailed),
            tries=10,
            delay=2,
        )(package_manifest.get_current_csv)(channel, constants.ACM_HUB_OPERATOR_NAME)
        acm_hub_subscription_yaml_data["spec"][
            "startingCSV"
        ] = package_manifest.get_current_csv(
            channel=channel, csv_pattern=constants.ACM_HUB_OPERATOR_NAME
        )

        acm_hub_subscription_manifest = tempfile.NamedTemporaryFile(
            mode="w+", prefix="acm_hub_subscription_manifest", delete=False
        )
        templating.dump_data_to_temp_yaml(
            acm_hub_subscription_yaml_data, acm_hub_subscription_manifest.name
        )
        run_cmd(f"oc create -f {acm_hub_subscription_manifest.name}")
        logger.info("Sleeping for 90 seconds after subscribing to ACM")
        time.sleep(90)
        csv_name = package_manifest.get_current_csv(channel=channel)
        csv = CSV(resource_name=csv_name, namespace=constants.ACM_HUB_NAMESPACE)
        csv.wait_for_phase("Succeeded", timeout=720)
        logger.info("ACM HUB Operator Deployment Succeeded")

    def deploy_multicluster_hub(self):
        """
        Handle Multicluster HUB creation
        Returns:
            bool: True if ACM HUB is installed, False otherwise
        """
        logger.info("Creating MultiCluster Hub")

        # check if MCH is already installed
        if OCP(
            kind=constants.ACM_MULTICLUSTER_HUB, namespace=constants.ACM_HUB_NAMESPACE
        ).check_resource_existence(
            should_exist=True,
            resource_name=constants.ACM_MULTICLUSTER_RESOURCE,
            timeout=6,
        ):
            logger.info("MultiClusterHub already installed")
            return True

        exec_cmd(
            f"oc create -f {constants.ACM_HUB_MULTICLUSTERHUB_YAML} -n {constants.ACM_HUB_NAMESPACE}"
        )
        try:
            validate_acm_hub_install()
        except Exception as ex:
            logger.error(f"Failed to install MultiClusterHub. Exception is: {ex}")
            return False

    def muliclusterhub_running(self):
        """
        Check if MultiCluster Hub is running

        Returns:
            bool: True if MultiCluster Hub is running, False otherwise
        """
        ocp_obj = OCP(
            kind=constants.ACM_MULTICLUSTER_HUB, namespace=constants.ACM_HUB_NAMESPACE
        )
        try:
            mch_running = ocp_obj.wait_for_resource(
                condition=constants.STATUS_RUNNING,
                resource_name=constants.ACM_MULTICLUSTER_RESOURCE,
                column="STATUS",
                timeout=6,
                sleep=3,
            )
        except CommandFailed:
            mch_running = False
        return mch_running


def create_external_pgsql_secret():
    """
    Creates secret for external PgSQL to be used by Noobaa
    """
    secret_data = templating.load_yaml(constants.EXTERNAL_PGSQL_NOOBAA_SECRET_YAML)
    secret_data["metadata"]["namespace"] = config.ENV_DATA["cluster_namespace"]
    pgsql_data = config.AUTH["pgsql"]
    user = pgsql_data["username"]
    password = pgsql_data["password"]
    host = pgsql_data["host"]
    port = pgsql_data["port"]
    cluster_name = config.ENV_DATA["cluster_name"].replace("-", "_")
    secret_data["stringData"][
        "db_url"
    ] = f"postgres://{user}:{password}@{host}:{port}/nbcore_{cluster_name}"

    secret_data_yaml = tempfile.NamedTemporaryFile(
        mode="w+", prefix="external_pgsql_noobaa_secret", delete=False
    )
    templating.dump_data_to_temp_yaml(secret_data, secret_data_yaml.name)
    logger.info("Creating external PgSQL Noobaa secret")
    run_cmd(f"oc create -f {secret_data_yaml.name}")


def validate_acm_hub_install():
    """
    Verify the ACM MultiClusterHub installation was successful.
    """
    logger.info("Verify ACM MultiClusterHub Installation")
    acm_mch = ocp.OCP(
        kind=constants.ACM_MULTICLUSTER_HUB,
        namespace=constants.ACM_HUB_NAMESPACE,
    )
    acm_mch.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        resource_name=constants.ACM_MULTICLUSTER_RESOURCE,
        column="STATUS",
        timeout=1200,
        sleep=30,
    )
    logger.info("MultiClusterHub Deployment Succeeded")


def create_ocs_secret(namespace):
    """
    Function for creation of pull secret for OCS. (Mostly for ibmcloud purpose)

    Args:
        namespace (str): namespace where to create the secret

    """
    secret_data = templating.load_yaml(constants.OCS_SECRET_YAML)
    docker_config_json = config.DEPLOYMENT["ocs_secret_dockerconfigjson"]
    secret_data["data"][".dockerconfigjson"] = docker_config_json
    secret_manifest = tempfile.NamedTemporaryFile(
        mode="w+", prefix="ocs_secret", delete=False
    )
    templating.dump_data_to_temp_yaml(secret_data, secret_manifest.name)
    exec_cmd(f"oc apply -f {secret_manifest.name} -n {namespace}", timeout=2400)


def create_catalog_source(image=None, ignore_upgrade=False):
    """
    This prepare catalog source manifest for deploy OCS operator from
    quay registry.

    Args:
        image (str): Image of ocs registry.
        ignore_upgrade (bool): Ignore upgrade parameter.

    """
    # Because custom catalog source will be called: redhat-operators, we need to disable
    # default sources. This should not be an issue as OCS internal registry images
    # are now based on OCP registry image
    disable_specific_source(constants.OPERATOR_CATALOG_SOURCE_NAME)
    logger.info("Adding CatalogSource")
    if not image:
        image = config.DEPLOYMENT.get("ocs_registry_image", "")
    if config.DEPLOYMENT.get("stage_rh_osbs"):
        image = config.DEPLOYMENT.get("stage_index_image", constants.OSBS_BOUNDLE_IMAGE)
        ocp_version = version.get_semantic_ocp_version_from_config()
        osbs_image_tag = config.DEPLOYMENT.get(
            "stage_index_image_tag", f"v{ocp_version}"
        )
        image += f":{osbs_image_tag}"
        run_cmd(
            "oc patch image.config.openshift.io/cluster --type merge -p '"
            '{"spec": {"registrySources": {"insecureRegistries": '
            '["registry-proxy.engineering.redhat.com", "registry.stage.redhat.io"]'
            "}}}'"
        )
        run_cmd(f"oc apply -f {constants.STAGE_IMAGE_CONTENT_SOURCE_POLICY_YAML}")
        wait_for_machineconfigpool_status("all", timeout=1800)
    if not ignore_upgrade:
        upgrade = config.UPGRADE.get("upgrade", False)
    else:
        upgrade = False
    image_and_tag = image.rsplit(":", 1)
    image = image_and_tag[0]
    image_tag = image_and_tag[1] if len(image_and_tag) == 2 else None
    if not image_tag and config.REPORTING.get("us_ds") == "DS":
        image_tag = get_latest_ds_olm_tag(
            upgrade, latest_tag=config.DEPLOYMENT.get("default_latest_tag", "latest")
        )
    catalog_source_data = templating.load_yaml(constants.CATALOG_SOURCE_YAML)
    managed_ibmcloud = (
        config.ENV_DATA["platform"] == constants.IBMCLOUD_PLATFORM
        and config.ENV_DATA["deployment_type"] == "managed"
    )
    if managed_ibmcloud:
        create_ocs_secret(constants.MARKETPLACE_NAMESPACE)
        catalog_source_data["spec"]["secrets"] = [constants.OCS_SECRET]
    cs_name = constants.OPERATOR_CATALOG_SOURCE_NAME
    change_cs_condition = (
        (image or image_tag)
        and catalog_source_data["kind"] == "CatalogSource"
        and catalog_source_data["metadata"]["name"] == cs_name
    )
    if change_cs_condition:
        default_image = config.DEPLOYMENT["default_ocs_registry_image"]
        image = image if image else default_image.rsplit(":", 1)[0]
        catalog_source_data["spec"][
            "image"
        ] = f"{image}:{image_tag if image_tag else 'latest'}"
    # apply icsp if present in the catalog image
    image = f"{image}:{image_tag if image_tag else 'latest'}"
    insecure_mode = True if config.DEPLOYMENT.get("disconnected") else False

    get_and_apply_icsp_from_catalog(image=image, insecure=insecure_mode)

    catalog_source_manifest = tempfile.NamedTemporaryFile(
        mode="w+", prefix="catalog_source_manifest", delete=False
    )
    templating.dump_data_to_temp_yaml(catalog_source_data, catalog_source_manifest.name)
    run_cmd(f"oc apply -f {catalog_source_manifest.name}", timeout=2400)
    catalog_source = CatalogSource(
        resource_name=constants.OPERATOR_CATALOG_SOURCE_NAME,
        namespace=constants.MARKETPLACE_NAMESPACE,
    )
    # Wait for catalog source is ready
    catalog_source.wait_for_state("READY")


def create_fusion_catalog_source():
    """
    Create catalog source for fusion operator
    """
    logger.info("Adding CatalogSource for IBM Fusion")
    fusion_catalog_source_data = templating.load_yaml(
        constants.FUSION_CATALOG_SOURCE_YAML
    )
    fusion_catalog_source_manifest = tempfile.NamedTemporaryFile(
        mode="w+", prefix="fusion_catalog_source_manifest", delete=False
    )
    templating.dump_data_to_temp_yaml(
        fusion_catalog_source_data, fusion_catalog_source_manifest.name
    )
    run_cmd(f"oc apply -f {fusion_catalog_source_manifest.name}")
    ibm_catalog_source = CatalogSource(
        resource_name=constants.IBM_OPERATOR_CATALOG_SOURCE_NAME,
        namespace=constants.MARKETPLACE_NAMESPACE,
    )

    # Wait for catalog source is ready
    ibm_catalog_source.wait_for_state("READY")


@retry(CommandFailed, tries=8, delay=3)
def setup_persistent_monitoring():
    """
    Change monitoring backend to OCS
    """
    sc = helpers.default_storage_class(interface_type=constants.CEPHBLOCKPOOL)

    # Get the list of monitoring pods
    pods_list = get_all_pods(
        namespace=defaults.OCS_MONITORING_NAMESPACE,
        selector=["prometheus", "alertmanager"],
    )

    # Create configmap cluster-monitoring-config and reconfigure
    # storage class and telemeter server (if the url is specified in a
    # config file)
    create_configmap_cluster_monitoring_pod(
        sc_name=sc.name,
        telemeter_server_url=config.ENV_DATA.get("telemeter_server_url"),
    )

    # Take some time to respin the pod
    waiting_time = 45
    logger.info(f"Waiting {waiting_time} seconds...")
    time.sleep(waiting_time)

    # Validate the pods are respinned and in running state
    retry((CommandFailed, ResourceWrongStatusException), tries=3, delay=15)(
        validate_pods_are_respinned_and_running_state
    )(pods_list)

    # Validate the pvc is created on monitoring pods
    validate_pvc_created_and_bound_on_monitoring_pods()

    # Validate the pvc are mounted on pods
    retry((CommandFailed, AssertionError), tries=3, delay=15)(
        validate_pvc_are_mounted_on_monitoring_pods
    )(pods_list)


class RBDDRDeployOps(object):
    """
    All RBD specific DR deployment operations

    """

    def deploy(self):
        self.configure_rbd()

    @retry(ResourceWrongStatusException, tries=10, delay=5)
    def configure_rbd(self):
        st_string = '{.items[?(@.metadata.ownerReferences[*].kind=="StorageCluster")].spec.mirroring.enabled}'
        query_mirroring = (
            f"oc get CephBlockPool -n {config.ENV_DATA['cluster_namespace']}"
            f" -o=jsonpath='{st_string}'"
        )
        out_list = run_cmd_multicluster(
            query_mirroring, skip_index=get_all_acm_indexes()
        )
        index = 0
        for out in out_list:
            if not out:
                continue
            logger.info(out.stdout)
            if out.stdout.decode() != "true":
                logger.error(
                    f"On cluster {config.clusters[index].ENV_DATA['cluster_name']}"
                )
                raise ResourceWrongStatusException(
                    "CephBlockPool", expected="true", got=out
                )
            index = +1

        # Check for RBD mirroring pods
        @retry(PodNotCreated, tries=1000, delay=5)
        def _get_mirror_pod_count():
            mirror_pod = get_pod_count(label="app=rook-ceph-rbd-mirror")
            if not mirror_pod:
                raise PodNotCreated(
                    f"RBD mirror pod not found on cluster: "
                    f"{cluster.ENV_DATA['cluster_name']}"
                )

        for cluster in get_non_acm_cluster_config():
            config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
            _get_mirror_pod_count()
            self.validate_csi_sidecar()

        # Reset CTX back to ACM
        config.switch_acm_ctx()

    def validate_csi_sidecar(self):
        """
        validate sidecar containers for rbd mirroring on each of the
        ODF cluster

        """
        # Number of containers should be 8/8 from 2 pods now which makes total 16 containers
        rbd_pods = (
            f"oc get pods -n {config.ENV_DATA['cluster_namespace']} "
            f"-l app=csi-rbdplugin-provisioner -o jsonpath={{.items[*].spec.containers[*].name}}"
        )
        timeout = 10
        ocs_version = version.get_ocs_version_from_csv(only_major_minor=True)
        if ocs_version <= version.get_semantic_version("4.11"):
            rbd_sidecar_count = constants.RBD_SIDECAR_COUNT
        else:
            rbd_sidecar_count = constants.RBD_SIDECAR_COUNT_4_12
        while timeout:
            out = run_cmd(rbd_pods)
            logger.info(out)
            logger.info(len(out.split(" ")))
            if rbd_sidecar_count != len(out.split(" ")):
                time.sleep(2)
            else:
                break
            timeout -= 1
        if not timeout:
            raise RBDSideCarContainerException("RBD Sidecar container count mismatch")

    def validate_mirror_peer(self, resource_name):
        """
        Validate mirror peer,
        Begins with CTX: ACM

        1. Check initial phase of 'ExchangingSecret'
        2. Check token-exchange-agent pod in 'Running' phase

        Raises:
            ResourceWrongStatusException: If pod is not in expected state

        """
        # Check mirror peer status only on HUB
        mirror_peer = ocp.OCP(
            kind="MirrorPeer",
            namespace=constants.DR_DEFAULT_NAMESPACE,
            resource_name=resource_name,
        )
        mirror_peer._has_phase = True
        mirror_peer.get()
        try:
            mirror_peer.wait_for_phase(phase="ExchangedSecret", timeout=1200)
            logger.info("Mirror peer is in expected phase 'ExchangedSecret'")
        except ResourceWrongStatusException:
            logger.exception("Mirror peer couldn't attain expected phase")
            raise

        # Check for token-exchange-agent pod and its status has to be running
        # on all participating clusters except HUB
        # We will switch config ctx to Participating clusters
        for cluster in config.clusters:
            if (
                cluster.MULTICLUSTER["multicluster_index"]
                == config.get_active_acm_index()
            ):
                continue
            else:
                config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
                token_xchange_agent = get_pods_having_label(
                    constants.TOKEN_EXCHANGE_AGENT_LABEL,
                    config.ENV_DATA["cluster_namespace"],
                )
                pod_status = token_xchange_agent[0]["status"]["phase"]
                pod_name = token_xchange_agent[0]["metadata"]["name"]
                if pod_status != "Running":
                    logger.error(f"On cluster {cluster.ENV_DATA['cluster_name']}")
                    ResourceWrongStatusException(
                        pod_name, expected="Running", got=pod_status
                    )
        # Switching back CTX to ACM
        config.switch_acm_ctx()


def get_multicluster_dr_deployment():
    return MULTICLUSTER_DR_MAP[config.MULTICLUSTER["multicluster_mode"]]


class MultiClusterDROperatorsDeploy(object):
    """
    Implement Multicluster DR operators deploy part here, mainly
    1. ODF Multicluster Orchestrator operator
    2. Metadata object stores (s3 OR MCG)
    3. ODF Hub operator
    4. ODF Cluster operator

    """

    def __init__(self, dr_conf):
        self.meta_map = {
            "awss3": self.s3_meta_obj_store,
            "mcg": self.mcg_meta_obj_store,
        }
        # Default to s3 for metadata store
        self.meta_obj_store = dr_conf.get("dr_metadata_store", "awss3")
        self.meta_obj = self.meta_map[self.meta_obj_store]()
        self.channel = config.DEPLOYMENT.get("ocs_csv_channel")

    def deploy(self):
        """
        deploy ODF multicluster orchestrator operator

        """

        # Create openshift-dr-system namespace
        run_cmd_multicluster(
            f"oc create -f {constants.OPENSHIFT_DR_SYSTEM_NAMESPACE_YAML} ",
        )
        self.deploy_dr_multicluster_orchestrator()
        # create this only on ACM
        run_cmd(
            f"oc create -f {constants.OPENSHIFT_DR_SYSTEM_OPERATORGROUP}",
        )
        # HUB operator will be deployed by multicluster orechestrator
        self.verify_dr_hub_operator()

    def deploy_dr_multicluster_orchestrator(self):
        """
        Deploy multicluster orchestrator
        """
        live_deployment = config.DEPLOYMENT.get("live_deployment")
        current_csv = None

        if not live_deployment:
            create_catalog_source()
        odf_multicluster_orchestrator_data = templating.load_yaml(
            constants.ODF_MULTICLUSTER_ORCHESTRATOR
        )
        package_manifest = packagemanifest.PackageManifest(
            resource_name=constants.ACM_ODF_MULTICLUSTER_ORCHESTRATOR_RESOURCE
        )

        retry(
            (ResourceNameNotSpecifiedException, ChannelNotFound, CommandFailed),
            tries=50,
            delay=20,
        )(package_manifest.get_current_csv)(
            self.channel, constants.ACM_ODF_MULTICLUSTER_ORCHESTRATOR_RESOURCE
        )

        current_csv = package_manifest.get_current_csv(
            channel=self.channel,
            csv_pattern=constants.ACM_ODF_MULTICLUSTER_ORCHESTRATOR_RESOURCE,
        )

        logger.info(f"CurrentCSV={current_csv}")
        odf_multicluster_orchestrator_data["spec"]["channel"] = self.channel
        odf_multicluster_orchestrator_data["spec"]["startingCSV"] = current_csv
        odf_multicluster_orchestrator = tempfile.NamedTemporaryFile(
            mode="w+", prefix="odf_multicluster_orchestrator", delete=False
        )
        templating.dump_data_to_temp_yaml(
            odf_multicluster_orchestrator_data, odf_multicluster_orchestrator.name
        )
        run_cmd(f"oc create -f {odf_multicluster_orchestrator.name}")
        orchestrator_controller = ocp.OCP(
            kind="Deployment",
            resource_name=constants.ODF_MULTICLUSTER_ORCHESTRATOR_CONTROLLER_MANAGER,
            namespace=constants.OPENSHIFT_OPERATORS,
        )
        orchestrator_controller.wait_for_resource(
            condition="1", column="AVAILABLE", resource_count=1, timeout=600
        )

    def configure_mirror_peer(self):
        # Current CTX: ACM
        # Create mirror peer
        if config.MULTICLUSTER["multicluster_mode"] == "metro-dr":
            mirror_peer = constants.MIRROR_PEER_MDR
        else:
            mirror_peer = constants.MIRROR_PEER_RDR
        mirror_peer_data = templating.load_yaml(mirror_peer)
        mirror_peer_yaml = tempfile.NamedTemporaryFile(
            mode="w+", prefix="mirror_peer", delete=False
        )
        # Update all the participating clusters in mirror_peer_yaml
        non_acm_clusters = get_non_acm_cluster_config()
        primary = get_primary_cluster_config()
        non_acm_clusters.remove(primary)
        for cluster in non_acm_clusters:
            logger.info(f"{cluster.ENV_DATA['cluster_name']}")
        index = -1
        # First entry should be the primary cluster
        # in the mirror peer
        for cluster_entry in mirror_peer_data["spec"]["items"]:
            if index == -1:
                cluster_entry["clusterName"] = primary.ENV_DATA["cluster_name"]
            else:
                cluster_entry["clusterName"] = non_acm_clusters[index].ENV_DATA[
                    "cluster_name"
                ]
            index += 1
        templating.dump_data_to_temp_yaml(mirror_peer_data, mirror_peer_yaml.name)
        # Current CTX: ACM
        # Just being explicit here to make code more readable
        config.switch_acm_ctx()
        run_cmd(f"oc create -f {mirror_peer_yaml.name}")
        self.validate_mirror_peer(mirror_peer_data["metadata"]["name"])

    def validate_mirror_peer(self, resource_name):
        """
        Validate mirror peer,
        Begins with CTX: ACM

        1. Check phase: if RDR then state =  'ExchangedSecret'
                        if MDR then state = 'S3ProfileSynced'
        2. Check token-exchange-agent pod in 'Running' phase

        Raises:
            ResourceWrongStatusException: If pod is not in expected state

        """
        # Check mirror peer status only on HUB
        mirror_peer = ocp.OCP(
            kind="MirrorPeer",
            namespace=constants.DR_DEFAULT_NAMESPACE,
            resource_name=resource_name,
        )
        mirror_peer._has_phase = True
        mirror_peer.get()
        if config.MULTICLUSTER["multicluster_mode"] == "regional-dr":
            expected_phase = "ExchangedSecret"
        elif config.MULTICLUSTER["multicluster_mode"] == "metro-dr":
            expected_phase = "S3ProfileSynced"

        try:
            # Need high timeout in case of MDR
            mirror_peer.wait_for_phase(phase=expected_phase, timeout=2400)
            logger.info(f"Mirror peer is in expected phase {expected_phase}")
        except ResourceWrongStatusException:
            logger.exception("Mirror peer couldn't attain expected phase")
            raise

        # Check for token-exchange-agent pod and its status has to be running
        # on all participating clusters except HUB
        # We will switch config ctx to Participating clusters
        for cluster in config.clusters:
            if cluster.MULTICLUSTER["multicluster_index"] in get_all_acm_indexes():
                continue
            else:
                config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
                token_xchange_agent = get_pods_having_label(
                    constants.TOKEN_EXCHANGE_AGENT_LABEL,
                    config.ENV_DATA["cluster_namespace"],
                )
                pod_status = token_xchange_agent[0]["status"]["phase"]
                pod_name = token_xchange_agent[0]["metadata"]["name"]
                if pod_status != "Running":
                    logger.error(f"On cluster {cluster.ENV_DATA['cluster_name']}")
                    ResourceWrongStatusException(
                        pod_name, expected="Running", got=pod_status
                    )
        # Switching back CTX to ACM
        config.switch_acm_ctx()

    def update_ramen_config_misc(self):
        config_map_data = self.meta_obj.get_ramen_resource()
        self.update_config_map_commit(config_map_data.data)

    def update_config_map_commit(self, config_map_data, prefix=None):
        """
        merge the config and update the resource

        Args:
            config_map_data (dict): base dictionary which will be later converted to yaml content
            prefix (str): Used to identify temp yaml

        """
        logger.debug(
            "Converting Ramen section (which is string) to dict and updating "
            "config_map_data with the same dict"
        )
        ramen_section = {
            f"{constants.DR_RAMEN_CONFIG_MANAGER_KEY}": yaml.safe_load(
                config_map_data["data"].pop(f"{constants.DR_RAMEN_CONFIG_MANAGER_KEY}")
            )
        }
        ramen_section[constants.DR_RAMEN_CONFIG_MANAGER_KEY][
            "drClusterOperator"
        ].update({"deploymentAutomationEnabled": True})
        logger.debug("Merge back the ramen_section with config_map_data")
        config_map_data["data"].update(ramen_section)
        for key in ["annotations", "creationTimestamp", "resourceVersion", "uid"]:
            if config_map_data["metadata"].get(key):
                config_map_data["metadata"].pop(key)

        dr_ramen_configmap_yaml = tempfile.NamedTemporaryFile(
            mode="w+", prefix=prefix, delete=False
        )
        yaml_serialized = yaml.dump(config_map_data)
        logger.debug(
            "Update yaml stream with a '|' for literal interpretation"
            " which comes exactly right after the key 'ramen_manager_config.yaml'"
        )
        yaml_serialized = yaml_serialized.replace(
            f"{constants.DR_RAMEN_CONFIG_MANAGER_KEY}:",
            f"{constants.DR_RAMEN_CONFIG_MANAGER_KEY}: |",
        )
        logger.info(f"after serialize {yaml_serialized}")
        dr_ramen_configmap_yaml.write(yaml_serialized)
        dr_ramen_configmap_yaml.flush()
        run_cmd(f"oc apply -f {dr_ramen_configmap_yaml.name}")

    def verify_dr_hub_operator(self):
        # ODF HUB operator only on ACM HUB
        package_manifest = PackageManifest(
            resource_name=constants.ACM_ODR_HUB_OPERATOR_RESOURCE
        )
        current_csv = package_manifest.get_current_csv(
            channel=self.channel, csv_pattern=constants.ACM_ODR_HUB_OPERATOR_RESOURCE
        )
        logger.info("Sleeping for 90 seconds after subscribing ")
        time.sleep(90)
        dr_hub_csv = CSV(
            resource_name=current_csv,
            namespace=constants.OPENSHIFT_DR_SYSTEM_NAMESPACE,
        )
        dr_hub_csv.wait_for_phase("Succeeded")

    def deploy_dr_policy(self):
        # Create DR policy on ACM hub cluster
        dr_policy_hub_data = templating.load_yaml(constants.DR_POLICY_ACM_HUB)
        # Update DR cluster name and s3profile name
        dr_policy_hub_data["spec"]["drClusters"][
            0
        ] = get_primary_cluster_config().ENV_DATA["cluster_name"]
        # Fill in for the rest of the non-acm clusters
        # index 0 is filled by primary
        index = 1
        for cluster in get_non_acm_cluster_config():
            if (
                cluster.ENV_DATA["cluster_name"]
                == get_primary_cluster_config().ENV_DATA["cluster_name"]
            ):
                continue
            dr_policy_hub_data["spec"]["drClusters"][index] = cluster.ENV_DATA[
                "cluster_name"
            ]

        if config.MULTICLUSTER["multicluster_mode"] == "metro-dr":
            dr_policy_hub_data["metadata"]["name"] = constants.MDR_DR_POLICY
            dr_policy_hub_data["spec"]["schedulingInterval"] = "0m"

        dr_policy_hub_yaml = tempfile.NamedTemporaryFile(
            mode="w+", prefix="dr_policy_hub_", delete=False
        )
        templating.dump_data_to_temp_yaml(dr_policy_hub_data, dr_policy_hub_yaml.name)
        self.dr_policy_name = dr_policy_hub_data["metadata"]["name"]
        run_cmd(f"oc create -f {dr_policy_hub_yaml.name}")
        # Check the status of DRPolicy and wait for 'Reason' field to be set to 'Succeeded'
        dr_policy_resource = ocp.OCP(
            kind="DRPolicy",
            resource_name=self.dr_policy_name,
            namespace=constants.OPENSHIFT_DR_SYSTEM_NAMESPACE,
        )
        dr_policy_resource.get()
        sample = TimeoutSampler(
            timeout=600,
            sleep=3,
            func=self.meta_obj._get_status,
            resource_data=dr_policy_resource,
        )
        if not sample.wait_for_func_status(True):
            raise TimeoutExpiredError("DR Policy failed to reach Succeeded state")

    def enable_cluster_backup(self):
        """
        set cluster-backup to True in mch resource
        Note: changing this flag automatically installs OADP operator
        """
        mch_resource = ocp.OCP(
            kind=constants.ACM_MULTICLUSTER_HUB,
            resource_name=constants.ACM_MULTICLUSTER_RESOURCE,
            namespace=constants.ACM_HUB_NAMESPACE,
        )
        mch_resource._has_phase = True
        resource_dict = mch_resource.get()
        for components in resource_dict["spec"]["overrides"]["components"]:
            if components["name"] == "cluster-backup":
                components["enabled"] = True
        mch_resource_yaml = tempfile.NamedTemporaryFile(
            mode="w+", prefix="mch", delete=False
        )
        yaml_serialized = yaml.dump(resource_dict)
        mch_resource_yaml.write(yaml_serialized)
        mch_resource_yaml.flush()
        run_cmd(f"oc apply -f {mch_resource_yaml.name}")
        mch_resource.wait_for_phase("Running")
        self.backup_pod_status_check()

    def create_s3_bucket(self, access_key, secret_key, bucket_name):
        """
        Create s3 bucket
        Args:
            access_key (str): S3 access key
            secret_key (str): S3 secret key
            acm_indexes (list): List of acm indexes
        """
        client = boto3.resource(
            "s3",
            verify=True,
            endpoint_url="https://s3.amazonaws.com",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
        try:
            client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": constants.AWS_REGION},
            )
            logger.info(f"Successfully created backup bucket: {bucket_name}")
        except BotoCoreError as e:
            logger.error(f"Failed to create s3 bucket {e}")
            raise

    def build_bucket_name(self, acm_indexes):
        """
        Create backupname from cluster names
        Args:
            acm_indexes (list): List of acm indexes
        """
        bucket_name = "dr-"
        for index in acm_indexes:
            bucket_name += config.clusters[index].ENV_DATA["cluster_name"]
        return bucket_name

    @retry((TimeoutExpiredError, ACMClusterConfigurationException), tries=20, delay=10)
    def backup_pod_status_check(self):
        pods_list = get_all_pods(namespace=constants.ACM_HUB_BACKUP_NAMESPACE)
        if len(pods_list) != 3:
            raise ACMClusterConfigurationException("backup pod count mismatch ")
        for pod in pods_list:
            # check pod status Running
            if not pod.data["status"]["phase"] == "Running":
                raise ACMClusterConfigurationException(
                    "backup pods not in Running state"
                )

    def create_generic_credentials(self, access_key, secret_key, acm_indexes):
        """
        Create s3 secret for backup and restore
        Args:
            access_key (str): S3 access key
            secret_key (str): S3 secret key
            acm_indexes (list): List of acm indexes
        """
        s3_cred_str = (
            "[default]\n"
            f"aws_access_key_id={access_key}\n"
            f"aws_secret_access_key={secret_key}\n"
        )
        cred_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="s3_creds", delete=False
        )
        cred_file.write(s3_cred_str)
        cred_file.flush()

        cmd = (
            f"oc create secret generic cloud-credentials --namespace {constants.ACM_HUB_BACKUP_NAMESPACE} "
            f"--from-file cloud={cred_file.name}"
        )
        old_index = config.cur_index
        # Create on all ACM clusters
        for index in acm_indexes:
            config.switch_ctx(index)
            try:
                run_cmd(f"oc create namespace {constants.ACM_HUB_BACKUP_NAMESPACE}")
            except CommandFailed as ex:
                if "already exists" in str(ex):
                    logger.warning("Namespace already exists!")
                else:
                    raise
            try:
                run_cmd(cmd)
            except CommandFailed:
                logger.error("Failed to create generic secrets cloud-credentials")

        config.switch_ctx(old_index)

    def enable_managed_serviceaccount(self):
        """
        update MultiClusterEngine

        """
        old_ctx = config.cur_index
        config.switch_ctx(get_active_acm_index())

        multicluster_engine = ocp.OCP(
            kind="MultiClusterEngine",
            resource_name=constants.MULTICLUSTER_ENGINE,
        )
        multicluster_engine._has_phase = True
        resource = multicluster_engine.get()
        for item in resource["spec"]["overrides"]["components"]:
            if item["name"] == "managedserviceaccount":
                item["enabled"] = True
        multicluster_engine_yaml = tempfile.NamedTemporaryFile(
            mode="w+", prefix="multiengine", delete=False
        )
        yaml_serialized = yaml.dump(resource)
        multicluster_engine_yaml.write(yaml_serialized)
        multicluster_engine_yaml.flush()
        run_cmd(f"oc apply -f {multicluster_engine_yaml.name}")
        multicluster_engine.wait_for_phase("Available")
        config.switch_ctx(old_ctx)

    def create_dpa(self, bucket_name):
        """
        create DPA
        OADP will be already installed when we enable backup flag
        Here we will create dataprotection application and
        update bucket name and s3 storage link
        Args:
            bucket_name (str): Name of the Bucket
        """
        oadp_data = templating.load_yaml(constants.ACM_DPA)
        oadp_data["spec"]["backupLocations"][0]["velero"]["objectStorage"][
            "bucket"
        ] = bucket_name
        oadp_yaml = tempfile.NamedTemporaryFile(mode="w+", prefix="oadp", delete=False)
        templating.dump_data_to_temp_yaml(oadp_data, oadp_yaml.name)
        run_cmd(f"oc create -f {oadp_yaml.name}")
        # Validation
        self.validate_dpa()

    @retry((CommandFailed, ACMClusterConfigurationException), tries=10, delay=10)
    def validate_dpa(self):
        """
        Validate
        1. 3 restic / Node-agent pods
        2. 1 velero pod
        3. backupstoragelocation resource in "Available" phase
        """
        # Restic pods have been renamed to node-agent after oadp 1.2
        oadp_version = get_oadp_version()

        if version.compare_versions(f"{oadp_version} >= 1.2"):
            restic_or_node_agent_pod_prefix = "node-agent"
        else:
            restic_or_node_agent_pod_prefix = "restic"
        restic_or_node_agent_list = get_pods_having_label(
            f"name={restic_or_node_agent_pod_prefix}",
            constants.ACM_HUB_BACKUP_NAMESPACE,
        )
        if len(restic_or_node_agent_list) != constants.RESTIC_OR_NODE_AGENT_POD_COUNT:
            raise ACMClusterConfigurationException("restic/node pod count mismatch")
        for pod in restic_or_node_agent_list:
            if pod["status"]["phase"] != "Running":
                raise ACMClusterConfigurationException(
                    "restic/node-agent pod not in 'Running' phase"
                )

        # Check velero pod
        veleropod = get_pods_having_label(
            "app.kubernetes.io/name=velero", constants.ACM_HUB_BACKUP_NAMESPACE
        )
        if len(veleropod) != constants.VELERO_POD_COUNT:
            raise ACMClusterConfigurationException("Velero pod count mismatch")
        if veleropod[0]["status"]["phase"] != "Running":
            raise ACMClusterConfigurationException("Velero pod not in 'Running' phase")

        # Check backupstoragelocation resource is in "Available" phase
        backupstorage = ocp.OCP(
            kind="BackupStorageLocation",
            resource_name="default",
            namespace=constants.ACM_HUB_BACKUP_NAMESPACE,
        )
        resource = backupstorage.get()
        if resource["status"].get("phase") != "Available":
            raise ACMClusterConfigurationException(
                "Backupstoragelocation resource is not in 'Available' phase"
            )
        logger.info("Dataprotection application successful")

    def validate_secret_creation_oadp(self):
        """
        Verify Secret are created

        Raises:
            ResourceNotFoundError: raised when secret not found

        """
        try:
            secret = ocp.OCP(
                kind=constants.SECRET,
                resource_name="cloud-credentials",
                namespace=constants.ACM_HUB_BACKUP_NAMESPACE,
            )
            secret.get()
            logger.info("Secret found")
        except CommandFailed:
            raise ResourceNotFoundError("Secret Not found")

    def validate_policy_compliance_status(
        self, resource_name, resource_namespace, compliance_state
    ):
        """
        Validate policy status for given resource

        Raises:
            ResourceWrongStatusException: Raised when resource state does not match

        """

        compliance_output = ocp.OCP(
            kind=constants.ACM_POLICY,
            resource_name=resource_name,
            namespace=resource_namespace,
        )
        compliance_status = compliance_output.get()
        if compliance_status["status"]["compliant"] == compliance_state:
            logger.info("Compliance status Matches ")
        else:
            raise ResourceWrongStatusException("Compliance status does not match")

    class s3_meta_obj_store:
        """
        Internal class to handle aws s3 metadata obj store

        """

        def __init__(self, conf=None):
            self.dr_regions = self.get_participating_regions()
            self.conf = conf if conf else dict()
            self.access_key = None
            self.secret_key = None
            self.bucket_name = None

        def deploy_and_configure(self):
            self.s3_configure()

        def s3_configure(self):
            # Configure s3secret on both primary and secondary clusters
            secret_yaml_files = []
            secret_names = self.get_s3_secret_names()
            for secret in secret_names:
                secret_data = ocp.OCP(
                    kind="Secret",
                    resource_name=secret,
                    namespace=constants.OPENSHIFT_DR_SYSTEM_NAMESPACE,
                )
                secret_data.get()
                for key in ["creationTimestamp", "resourceVersion", "uid"]:
                    secret_data.data["metadata"].pop(key)
                secret_temp_file = tempfile.NamedTemporaryFile(
                    mode="w+", prefix=secret, delete=False
                )
                templating.dump_data_to_temp_yaml(
                    secret_data.data, secret_temp_file.name
                )
                secret_yaml_files.append(secret_temp_file.name)

            # Create s3 secret on all clusters except ACM
            for secret_yaml in secret_yaml_files:
                cmd = f"oc create -f {secret_yaml}"
                run_cmd_multicluster(cmd, skip_index=config.get_active_acm_index())

        def get_participating_regions(self):
            """
            Get all the participating regions in the DR scenario

            Returns:
                list of str: List of participating regions

            """
            # For first cut just returning east and west
            return ["east", "west"]

        def get_s3_secret_names(self):
            """
            Get secret resource names for s3

            """
            s3_secrets = []
            dr_ramen_hub_configmap_data = self.get_ramen_resource()
            ramen_config = yaml.safe_load(
                dr_ramen_hub_configmap_data.data["data"]["ramen_manager_config.yaml"]
            )
            for s3profile in ramen_config["s3StoreProfiles"]:
                s3_secrets.append(s3profile["s3SecretRef"]["name"])
            return s3_secrets

        def get_s3_profiles(self):
            """
            Get names of s3 profiles from hub configmap resource

            """
            s3_profiles = []
            dr_ramen_hub_configmap_data = self.get_ramen_resource()
            ramen_config = yaml.safe_load(
                dr_ramen_hub_configmap_data.data["data"]["ramen_manager_config.yaml"]
            )
            for s3profile in ramen_config["s3StoreProfiles"]:
                s3_profiles.append(s3profile["s3ProfileName"])

            return s3_profiles

        def get_ramen_resource(self):
            dr_ramen_hub_configmap_data = ocp.OCP(
                kind="ConfigMap",
                resource_name=constants.DR_RAMEN_HUB_OPERATOR_CONFIG,
                namespace=constants.OPENSHIFT_DR_SYSTEM_NAMESPACE,
            )
            dr_ramen_hub_configmap_data.get()
            return dr_ramen_hub_configmap_data

        def _get_status(self, resource_data):
            resource_data.reload_data()
            reason = resource_data.data.get("status").get("conditions")[0].get("reason")
            if reason == "Succeeded":
                return True
            return False

        def get_meta_access_secret_keys(self):
            """
            Get aws_access_key_id and aws_secret_access_key
            by default we go with AWS, in case of noobaa it should be
            implemented in mcg_meta_obj_store class

            """
            try:
                logger.info("Trying to load AWS credentials")
                secret_dict = update_config_from_s3().get("AUTH")
            except (AttributeError, EndpointConnectionError):
                logger.warning(
                    "Failed to load credentials from ocs-ci-data.\n"
                    "Your local AWS credentials might be misconfigured.\n"
                    "Trying to load credentials from local auth.yaml instead"
                )
                secret_dict = load_auth_config().get("AUTH", {})
            self.access_key = secret_dict["AWS"]["AWS_ACCESS_KEY_ID"]
            self.secret_key = secret_dict["AWS"]["AWS_SECRET_ACCESS_KEY"]

    class mcg_meta_obj_store:
        def __init__(self):
            raise NotImplementedError("MCG metadata store support not implemented")


class RDRMultiClusterDROperatorsDeploy(MultiClusterDROperatorsDeploy):
    """
    A class for Regional-DR deployments
    """

    def __init__(self, dr_conf):
        super().__init__(dr_conf)
        # DR use case could be RBD or CephFS or Both
        self.rbd = dr_conf.get("rbd_dr_scenario", False)
        # CephFS For future usecase
        self.cephfs = dr_conf.get("cephfs_dr_scenario", False)

    def deploy(self):
        """
        RDR specific steps for deploy
        """
        # current CTX: ACM
        acm_indexes = get_all_acm_indexes()
        config.switch_acm_ctx()
        super().deploy()
        # RBD specific dr deployment
        if self.rbd:
            rbddops = RBDDRDeployOps()
            self.configure_mirror_peer()
            rbddops.deploy()
        self.enable_acm_observability()
        self.deploy_dr_policy()

        # Enable cluster backup on both ACMs
        for i in acm_indexes:
            config.switch_ctx(i)
            self.enable_cluster_backup()
        # Configuring s3 bucket
        self.meta_obj.get_meta_access_secret_keys()
        # bucket name formed like '{acm_active_cluster}-{acm_passive_cluster}'
        self.meta_obj.bucket_name = self.build_bucket_name(acm_indexes)
        # create s3 bucket
        self.create_s3_bucket(
            self.meta_obj.access_key,
            self.meta_obj.secret_key,
            self.meta_obj.bucket_name,
        )
        self.create_generic_credentials(
            self.meta_obj.access_key, self.meta_obj.secret_key, acm_indexes
        )
        self.validate_secret_creation_oadp()
        # Reconfigure OADP on all ACM clusters
        for i in acm_indexes:
            config.switch_ctx(i)
            self.create_dpa(self.meta_obj.bucket_name)
        # Only on the active hub enable managedserviceaccount-preview
        config.switch_acm_ctx()
        acm_version = get_acm_version()

        if version.compare_versions(f"{acm_version} >= 2.10"):
            logger.info("Skipping Enabling Managed ServiceAccount")
        else:
            self.enable_managed_serviceaccount()

    @retry(ACMObservabilityNotEnabled, tries=10, delay=30)
    def check_observability_status(self):
        """
        Check observability status

        Raises:
             ACMObservabilityNotEnabled: if the cmd returns False, ACM observability is not enabled

        """

        acm_observability_status = bool(
            exec_cmd(
                "oc get MultiClusterObservability observability -o jsonpath='{.status.conditions[1].status}'"
            )
        )

        if acm_observability_status:
            logger.info("ACM observability is successfully enabled")
        else:
            logger.error("ACM observability could not be enabled, re-trying...")
            raise ACMObservabilityNotEnabled

    def thanos_secret(self):
        """
        Create thanos secret yaml by using Noobaa or AWS bucket (AWS bucket is used in this function)

        """
        acm_indexes = get_all_acm_indexes()
        self.meta_obj.get_meta_access_secret_keys()
        thanos_secret_data = templating.load_yaml(constants.THANOS_PATH)
        thanos_bucket_name = (
            f"dr-thanos-bucket-{config.clusters[0].ENV_DATA['cluster_name']}"
        )
        self.create_s3_bucket(
            self.meta_obj.access_key,
            self.meta_obj.secret_key,
            thanos_bucket_name,
        )
        logger.info(f"ACM indexes {acm_indexes}")
        navigate_thanos_yaml = thanos_secret_data["stringData"]["thanos.yaml"]
        navigate_thanos_yaml = yaml.safe_load(navigate_thanos_yaml)
        navigate_thanos_yaml["config"]["bucket"] = thanos_bucket_name
        navigate_thanos_yaml["config"]["endpoint"] = "s3.amazonaws.com"
        navigate_thanos_yaml["config"]["access_key"] = self.meta_obj.access_key
        navigate_thanos_yaml["config"]["secret_key"] = self.meta_obj.secret_key
        thanos_secret_data["stringData"]["thanos.yaml"] = str(navigate_thanos_yaml)
        thanos_data_yaml = tempfile.NamedTemporaryFile(
            mode="w+", prefix="thanos", delete=False
        )
        templating.dump_data_to_temp_yaml(thanos_secret_data, thanos_data_yaml.name)

        logger.info(
            "Creating thanos.yaml needed for ACM observability after passing required params"
        )
        exec_cmd(f"oc create -f {thanos_data_yaml.name}")

        self.check_observability_status()

    def enable_acm_observability(self):
        """
        Function to enable ACM observability for enabling DR monitoring dashboard for Regional DR on the RHACM console.

        """
        config.switch_acm_ctx()

        defaultstorageclass = get_default_storage_class()

        logger.info(
            "Enabling ACM MultiClusterObservability for DR monitoring dashboard"
        )

        # load multiclusterobservability.yaml
        multiclusterobservability_yaml_data = templating.load_yaml(
            constants.MULTICLUSTEROBSERVABILITY_PATH
        )
        multiclusterobservability_yaml_data["spec"]["storageConfig"][
            "storageClass"
        ] = defaultstorageclass[0]
        multiclusterobservability_data_yaml = tempfile.NamedTemporaryFile(
            mode="w+", prefix="multiclusterobservability", delete=False
        )
        templating.dump_data_to_temp_yaml(
            multiclusterobservability_yaml_data,
            multiclusterobservability_data_yaml.name,
        )

        exec_cmd(f"oc create -f {multiclusterobservability_data_yaml.name}")

        logger.info("Create thanos secret yaml")
        self.thanos_secret()

        logger.info("Whitelist RBD metrics by creating configmap")
        exec_cmd(f"oc create -f {constants.OBSERVABILITYMETRICSCONFIGMAP_PATH}")

        logger.info(
            "Add label for cluster-monitoring needed to fire VolumeSyncronizationDelayAlert on the Hub cluster"
        )
        exec_cmd(
            "oc label namespace openshift-operators openshift.io/cluster-monitoring='true'"
        )


class MDRMultiClusterDROperatorsDeploy(MultiClusterDROperatorsDeploy):
    """
    A class for Metro-DR deployments
    """

    def __init__(self, dr_conf):
        super().__init__(dr_conf)

    def deploy(self):
        # We need multicluster orchestrator on both the active/passive ACM clusters
        acm_indexes = get_all_acm_indexes()
        for i in acm_indexes:
            config.switch_ctx(i)
            self.deploy_dr_multicluster_orchestrator()
            # Enable MCO console plugin
            enable_mco_console_plugin()
        # Configure mirror peer
        self.configure_mirror_peer()
        # Deploy dr policy
        self.deploy_dr_policy()
        # Configure DRClusters for fencing automation
        configure_drcluster_for_fencing()

        # Enable cluster backup on both ACMs
        for i in acm_indexes:
            config.switch_ctx(i)
            self.enable_cluster_backup()
        # Configuring s3 bucket
        self.meta_obj.get_meta_access_secret_keys()
        # bucket name formed like '{acm_active_cluster}-{acm_passive_cluster}'
        self.meta_obj.bucket_name = self.build_bucket_name(acm_indexes)
        # create s3 bucket
        self.create_s3_bucket(
            self.meta_obj.access_key,
            self.meta_obj.secret_key,
            self.meta_obj.bucket_name,
        )
        self.create_generic_credentials(
            self.meta_obj.access_key, self.meta_obj.secret_key, acm_indexes
        )
        self.validate_secret_creation_oadp()
        # Reconfigure OADP on all ACM clusters
        old_ctx = config.cur_index
        for i in acm_indexes:
            config.switch_ctx(i)
            self.create_dpa(self.meta_obj.bucket_name)
        config.switch_ctx(old_ctx)
        # Only on the active hub enable managedserviceaccount-preview
        acm_version = get_acm_version()

        if version.compare_versions(f"{acm_version} >= 2.10"):
            logger.info("Skipping Enabling Managed ServiceAccount")
        else:
            self.enable_managed_serviceaccount()

    def deploy_multicluster_orchestrator(self):
        super().deploy()

    def deploy_dr_policy(self):
        """
        Deploy dr policy with MDR perspective, only on active ACM
        """
        old_ctx = config.cur_index
        active_acm_index = get_active_acm_index()
        config.switch_ctx(active_acm_index)
        super().deploy_dr_policy()
        config.switch_ctx(old_ctx)


MULTICLUSTER_DR_MAP = {
    "regional-dr": RDRMultiClusterDROperatorsDeploy,
    "metro-dr": MDRMultiClusterDROperatorsDeploy,
}
