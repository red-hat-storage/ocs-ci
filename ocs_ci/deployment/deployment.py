"""
This module provides base class for different deployment
platforms like AWS, VMWare, Baremetal etc.
"""

from copy import deepcopy
import json
import logging
import tempfile
import time

import requests
import yaml

from ocs_ci.deployment.ocp import OCPDeployment as BaseOCPDeployment
from ocs_ci.deployment.helpers.lso_helpers import setup_local_storage
from ocs_ci.deployment.disconnected import prepare_disconnected_ocs_deployment
from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp, defaults, registry
from ocs_ci.ocs.cluster import (
    validate_cluster_on_pvc,
    validate_pdb_creation,
    CephClusterExternal,
)
from ocs_ci.ocs.exceptions import (
    CephHealthException,
    CommandFailed,
    ResourceWrongStatusException,
    UnavailableResourceException,
    ExternalClusterDetailsException,
    UnsupportedFeatureError,
)
from ocs_ci.ocs.monitoring import (
    create_configmap_cluster_monitoring_pod,
    validate_pvc_created_and_bound_on_monitoring_pods,
    validate_pvc_are_mounted_on_monitoring_pods,
)
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs.resources.csv import CSV
from ocs_ci.ocs.resources.install_plan import wait_for_install_plan_and_approve
from ocs_ci.ocs.resources.packagemanifest import (
    get_selector_for_ocs_operator,
    PackageManifest,
)
from ocs_ci.ocs.resources.pod import (
    get_all_pods,
    validate_pods_are_respinned_and_running_state,
)
from ocs_ci.ocs.uninstall import uninstall_ocs
from ocs_ci.ocs.utils import setup_ceph_toolbox, collect_ocs_logs
from ocs_ci.utility import templating, ibmcloud
from ocs_ci.utility.flexy import load_cluster_info
from ocs_ci.utility.openshift_console import OpenshiftConsole
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import (
    ceph_health_check,
    exec_cmd,
    get_latest_ds_olm_tag,
    get_ocp_version,
    is_cluster_running,
    run_cmd,
    set_selinux_permissions,
    set_registry_to_managed_state,
    add_stage_cert,
    modify_csv,
    wait_for_machineconfigpool_status,
    apply_ssd_tuning_azure,
)
from ocs_ci.utility.vsphere_nodes import update_ntp_compute_nodes
from ocs_ci.helpers import helpers

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

    class OCPDeployment(BaseOCPDeployment):
        """
        This class has to be implemented in child class and should overload
        methods for platform specific config.
        """

        pass

    def deploy_cluster(self, log_cli_level="DEBUG"):
        """
        We are handling both OCP and OCS deployment here based on flags

        Args:
            log_cli_level (str): log level for installer (default: DEBUG)
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

        if not config.ENV_DATA["skip_ocs_deployment"]:
            try:
                self.deploy_ocs()

                # WA for Bug 1909793
                ocs_version = float(config.ENV_DATA["ocs_version"])
                if self.platform == constants.AZURE_PLATFORM and ocs_version < 4.7:
                    apply_ssd_tuning_azure()

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
        else:
            logger.warning("OCS deployment will be skipped")

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
        set_selinux_permissions()
        set_registry_to_managed_state()
        add_stage_cert()

    def label_and_taint_nodes(self):
        """
        Label and taint worker nodes to be used by OCS operator
        """

        nodes = ocp.OCP(kind="node").get().get("items", [])
        worker_nodes = [
            node
            for node in nodes
            if "node-role.kubernetes.io/worker" in node["metadata"]["labels"]
        ]
        if not worker_nodes:
            raise UnavailableResourceException("No worker node found!")
        az_worker_nodes = {}
        for node in worker_nodes:
            az = node["metadata"]["labels"].get(
                "failure-domain.beta.kubernetes.io/zone"
            )
            az_node_list = az_worker_nodes.get(az, [])
            az_node_list.append(node)
            az_worker_nodes[az] = az_node_list
        logger.debug(f"Found the worker nodes in AZ: {az_worker_nodes}")
        distributed_worker_nodes = []
        while az_worker_nodes:
            for az in list(az_worker_nodes.keys()):
                az_node_list = az_worker_nodes.get(az)
                if az_node_list:
                    node_name = az_node_list.pop(0)["metadata"]["name"]
                    distributed_worker_nodes.append(node_name)
                else:
                    del az_worker_nodes[az]
        logger.info(f"Distributed worker nodes for AZ: {distributed_worker_nodes}")
        to_label = config.DEPLOYMENT.get("ocs_operator_nodes_to_label", 3)
        to_taint = config.DEPLOYMENT.get("ocs_operator_nodes_to_taint", 0)
        worker_count = len(worker_nodes)
        if worker_count < to_label or worker_count < to_taint:
            logger.info(f"All nodes: {nodes}")
            logger.info(f"Worker nodes: {worker_nodes}")
            raise UnavailableResourceException(
                f"Not enough worker nodes: {worker_count} to label: "
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

    def create_stage_operator_source(self):
        """
        This prepare operator source for OCS deployment from stage.
        """
        logger.info("Adding Stage Secret")
        # generate quay token
        credentials = {
            "user": {
                "username": config.DEPLOYMENT["stage_quay_username"],
                "password": config.DEPLOYMENT["stage_quay_password"],
            }
        }
        token = requests.post(
            url="https://quay.io/cnr/api/v1/users/login",
            data=json.dumps(credentials),
            headers={"Content-Type": "application/json"},
        ).json()["token"]
        stage_ns = config.DEPLOYMENT["stage_namespace"]

        # create Secret
        stage_os_secret = templating.load_yaml(constants.OPERATOR_SOURCE_SECRET_YAML)
        stage_os_secret["metadata"]["name"] = constants.OPERATOR_SOURCE_SECRET_NAME
        stage_os_secret["stringData"]["token"] = token
        stage_secret_data_yaml = tempfile.NamedTemporaryFile(
            mode="w+",
            prefix=constants.OPERATOR_SOURCE_SECRET_NAME,
            delete=False,
        )
        templating.dump_data_to_temp_yaml(stage_os_secret, stage_secret_data_yaml.name)
        run_cmd(f"oc create -f {stage_secret_data_yaml.name}")
        logger.info("Waiting 10 secs after secret is created")
        time.sleep(10)

        logger.info("Adding Stage Operator Source")
        # create Operator Source
        stage_os = templating.load_yaml(constants.OPERATOR_SOURCE_YAML)
        stage_os["spec"]["registryNamespace"] = stage_ns
        stage_os["spec"]["authorizationToken"][
            "secretName"
        ] = constants.OPERATOR_SOURCE_SECRET_NAME
        stage_os_data_yaml = tempfile.NamedTemporaryFile(
            mode="w+", prefix=constants.OPERATOR_SOURCE_NAME, delete=False
        )
        templating.dump_data_to_temp_yaml(stage_os, stage_os_data_yaml.name)
        run_cmd(f"oc create -f {stage_os_data_yaml.name}")
        catalog_source = CatalogSource(
            resource_name=constants.OPERATOR_SOURCE_NAME,
            namespace=constants.MARKETPLACE_NAMESPACE,
        )
        # Wait for catalog source is ready
        catalog_source.wait_for_state("READY")

    def create_ocs_operator_source(self, image=None):
        """
        This prepare catalog or operator source for OCS deployment.

        Args:
            image (str): Image of ocs registry.

        """
        if config.DEPLOYMENT.get("stage"):
            # deployment from stage
            self.create_stage_operator_source()
        else:
            create_catalog_source(image)

    def subscribe_ocs(self):
        """
        This method subscription manifest and subscribe to OCS operator.

        """
        live_deployment = config.DEPLOYMENT.get("live_deployment")
        if (
            config.ENV_DATA["platform"] == constants.IBMCLOUD_PLATFORM
            and not live_deployment
        ):
            link_all_sa_and_secret(constants.OCS_SECRET, self.namespace)
        operator_selector = get_selector_for_ocs_operator()
        # wait for package manifest
        package_manifest = PackageManifest(
            resource_name=defaults.OCS_OPERATOR_NAME,
            selector=operator_selector,
        )
        # Wait for package manifest is ready
        package_manifest.wait_for_resource(timeout=300)
        default_channel = package_manifest.get_default_channel()
        subscription_yaml_data = templating.load_yaml(constants.SUBSCRIPTION_YAML)
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
        subscription_manifest = tempfile.NamedTemporaryFile(
            mode="w+", prefix="subscription_manifest", delete=False
        )
        templating.dump_data_to_temp_yaml(
            subscription_yaml_data, subscription_manifest.name
        )
        run_cmd(f"oc create -f {subscription_manifest.name}")
        logger.info("Sleeping for 15 seconds after subscribing OCS")
        subscription_plan_approval = config.DEPLOYMENT.get("subscription_plan_approval")
        if subscription_plan_approval == "Manual":
            wait_for_install_plan_and_approve(self.namespace)

    def deploy_ocs_via_operator(self, image=None):
        """
        Method for deploy OCS via OCS operator

        Args:
            image (str): Image of ocs registry.

        """
        ui_deployment = config.DEPLOYMENT.get("ui_deployment")
        live_deployment = config.DEPLOYMENT.get("live_deployment")

        if ui_deployment:
            if not live_deployment:
                self.create_ocs_operator_source()
            self.deployment_with_ui()
            # Skip the rest of the deployment when deploy via UI
            return
        else:
            logger.info("Deployment of OCS via OCS operator")
            self.label_and_taint_nodes()

        if config.DEPLOYMENT.get("local_storage"):
            setup_local_storage(storageclass=self.DEFAULT_STORAGECLASS_LSO)

        logger.info("Creating namespace and operator group.")
        run_cmd(f"oc create -f {constants.OLM_YAML}")
        if config.ENV_DATA["platform"] == constants.IBMCLOUD_PLATFORM:
            ibmcloud.add_deployment_dependencies()
            if not live_deployment:
                create_ocs_secret(self.namespace)
                create_ocs_secret(constants.MARKETPLACE_NAMESPACE)
        if not live_deployment:
            self.create_ocs_operator_source(image)
        self.subscribe_ocs()
        operator_selector = get_selector_for_ocs_operator()
        subscription_plan_approval = config.DEPLOYMENT.get("subscription_plan_approval")
        package_manifest = PackageManifest(
            resource_name=defaults.OCS_OPERATOR_NAME,
            selector=operator_selector,
            subscription_plan_approval=subscription_plan_approval,
        )
        package_manifest.wait_for_resource(timeout=300)
        channel = config.DEPLOYMENT.get("ocs_csv_channel")
        csv_name = package_manifest.get_current_csv(channel=channel)
        csv = CSV(resource_name=csv_name, namespace=self.namespace)
        if (
            config.ENV_DATA["platform"] == constants.IBMCLOUD_PLATFORM
            and not live_deployment
        ):
            csv.wait_for_phase("Installing", timeout=720)
            logger.info("Sleeping for 30 seconds before applying SA")
            time.sleep(30)
            link_all_sa_and_secret(constants.OCS_SECRET, self.namespace)
            logger.info("Deleting all pods in openshift-storage namespace")
            exec_cmd(f"oc delete pod --all -n {self.namespace}")
        csv.wait_for_phase("Succeeded", timeout=720)
        if config.ENV_DATA["platform"] == constants.IBMCLOUD_PLATFORM:
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

        # Modify the CSV with custom values if required
        if all(
            key in config.DEPLOYMENT for key in ("csv_change_from", "csv_change_to")
        ):
            modify_csv(
                csv=csv_name,
                replace_from=config.DEPLOYMENT["csv_change_from"],
                replace_to=config.DEPLOYMENT["csv_change_to"],
            )

        # create custom storage class for StorageCluster CR if necessary
        if self.CUSTOM_STORAGE_CLASS_PATH is not None:
            with open(self.CUSTOM_STORAGE_CLASS_PATH, "r") as custom_sc_fo:
                custom_sc = yaml.load(custom_sc_fo, Loader=yaml.SafeLoader)
            # set value of DEFAULT_STORAGECLASS to mach the custom storage cls
            self.DEFAULT_STORAGECLASS = custom_sc["metadata"]["name"]
            run_cmd(f"oc create -f {self.CUSTOM_STORAGE_CLASS_PATH}")

        # creating StorageCluster
        if self.platform == constants.IBM_POWER_PLATFORM:
            cluster_data = templating.load_yaml(constants.IBM_STORAGE_CLUSTER_YAML)
        else:
            cluster_data = templating.load_yaml(constants.STORAGE_CLUSTER_YAML)

        cluster_data["metadata"]["name"] = config.ENV_DATA["storage_cluster_name"]

        if self.platform == constants.IBM_POWER_PLATFORM:
            numberofstoragenodes = config.ENV_DATA["number_of_storage_nodes"]
            deviceset = [None] * numberofstoragenodes

            for i in range(numberofstoragenodes):
                deviceset_data = cluster_data["spec"]["storageDeviceSets"][i]
                device_size = int(
                    config.ENV_DATA.get("device_size", defaults.DEVICE_SIZE)
                )

                # set size of request for storage
                if self.platform.lower() == "powervs":
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
                if self.DEFAULT_STORAGECLASS_LSO:
                    deviceset_data["dataPVCTemplate"]["spec"][
                        "storageClassName"
                    ] = self.DEFAULT_STORAGECLASS_LSO

                # StorageCluster tweaks for LSO
                if config.DEPLOYMENT.get("local_storage"):
                    cluster_data["spec"]["manageNodes"] = False
                    cluster_data["spec"]["monDataDirHostPath"] = "/var/lib/rook"
                    deviceset_data["portable"] = False
                    deviceset_data["dataPVCTemplate"]["spec"][
                        "storageClassName"
                    ] = self.DEFAULT_STORAGECLASS_LSO

                deviceset[i] = deviceset_data
        else:
            deviceset_data = cluster_data["spec"]["storageDeviceSets"][0]
            device_size = int(config.ENV_DATA.get("device_size", defaults.DEVICE_SIZE))

            # set size of request for storage
            if self.platform.lower() == constants.BAREMETAL_PLATFORM:
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

            ocs_version = float(config.ENV_DATA["ocs_version"])
            ocp_version = float(get_ocp_version())

            # StorageCluster tweaks for LSO
            if config.DEPLOYMENT.get("local_storage"):
                cluster_data["spec"]["manageNodes"] = False
                cluster_data["spec"]["monDataDirHostPath"] = "/var/lib/rook"
                deviceset_data["portable"] = False
                deviceset_data["dataPVCTemplate"]["spec"][
                    "storageClassName"
                ] = self.DEFAULT_STORAGECLASS_LSO
                if self.platform.lower() == constants.AWS_PLATFORM:
                    deviceset_data["count"] = 2
                if ocs_version >= 4.5:
                    deviceset_data["resources"] = {
                        "limits": {"cpu": 2, "memory": "5Gi"},
                        "requests": {"cpu": 1, "memory": "5Gi"},
                    }
                if (ocp_version >= 4.6) and (ocs_version >= 4.6):
                    cluster_data["metadata"]["annotations"] = {
                        "cluster.ocs.openshift.io/local-devices": "true"
                    }

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
                if ocs_version >= 4.5:
                    resources.append("noobaa-endpoint")
                cluster_data["spec"]["resources"] = {
                    resource: deepcopy(none_resources) for resource in resources
                }
                if ocs_version >= 4.5:
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
                    if ocs_version < 4.5:
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

        if self.platform == constants.IBM_POWER_PLATFORM:
            cluster_data["spec"]["storageDeviceSets"] = deviceset
        else:
            cluster_data["spec"]["storageDeviceSets"] = [deviceset_data]

        if self.platform == constants.IBMCLOUD_PLATFORM:
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
            if ocs_version < 4.6:
                error_message = "Encryption at REST can be enabled only on OCS >= 4.6!"
                logger.error(error_message)
                raise UnsupportedFeatureError(error_message)
            logger.info("Enabling encryption at REST!")
            cluster_data["spec"]["encryption"] = {
                "enable": True,
            }

        cluster_data_yaml = tempfile.NamedTemporaryFile(
            mode="w+", prefix="cluster_storage", delete=False
        )
        templating.dump_data_to_temp_yaml(cluster_data, cluster_data_yaml.name)
        run_cmd(f"oc create -f {cluster_data_yaml.name}", timeout=1200)
        if config.DEPLOYMENT["infra_nodes"]:
            _ocp = ocp.OCP(kind="node")
            _ocp.exec_oc_cmd(
                command=f"annotate namespace {defaults.ROOK_CLUSTER_NAMESPACE} "
                f"{constants.NODE_SELECTOR_ANNOTATION}"
            )

    def deployment_with_ui(self):
        """
        This method will deploy OCS with openshift-console UI test.
        """
        logger.info("Deployment of OCS will be done by openshift-console")
        ocp_console = OpenshiftConsole(
            config.DEPLOYMENT.get("deployment_browser", constants.CHROME_BROWSER)
        )
        live_deploy = "1" if config.DEPLOYMENT.get("live_deployment") else "0"
        env_vars = {
            "OCS_LIVE": live_deploy,
        }
        ocp_console.run_openshift_console(
            suite="ceph-storage-install", env_vars=env_vars, log_suffix="ui-deployment"
        )

    def deploy_with_external_mode(self):
        """
        This function handles the deployment of OCS on
        external/indpendent RHCS cluster

        """
        live_deployment = config.DEPLOYMENT.get("live_deployment")
        logger.info("Deploying OCS with external mode RHCS")
        ui_deployment = config.DEPLOYMENT.get("ui_deployment")
        if not ui_deployment:
            logger.info("Creating namespace and operator group.")
            run_cmd(f"oc create -f {constants.OLM_YAML}")
        if not live_deployment:
            self.create_ocs_operator_source()
        self.subscribe_ocs()
        operator_selector = get_selector_for_ocs_operator()
        subscription_plan_approval = config.DEPLOYMENT.get("subscription_plan_approval")
        package_manifest = PackageManifest(
            resource_name=defaults.OCS_OPERATOR_NAME,
            selector=operator_selector,
            subscription_plan_approval=subscription_plan_approval,
        )
        package_manifest.wait_for_resource(timeout=300)
        channel = config.DEPLOYMENT.get("ocs_csv_channel")
        csv_name = package_manifest.get_current_csv(channel=channel)
        csv = CSV(resource_name=csv_name, namespace=self.namespace)
        csv.wait_for_phase("Succeeded", timeout=720)

        # Create secret for external cluster
        secret_data = templating.load_yaml(constants.EXTERNAL_CLUSTER_SECRET_YAML)
        external_cluster_details = config.EXTERNAL_MODE.get(
            "external_cluster_details", ""
        )
        if not external_cluster_details:
            raise ExternalClusterDetailsException("No external cluster data found")
        secret_data["data"]["external_cluster_details"] = external_cluster_details
        secret_data_yaml = tempfile.NamedTemporaryFile(
            mode="w+", prefix="external_cluster_secret", delete=False
        )
        templating.dump_data_to_temp_yaml(secret_data, secret_data_yaml.name)
        logger.info("Creating external cluster secret")
        run_cmd(f"oc create -f {secret_data_yaml.name}")

        cluster_data = templating.load_yaml(constants.EXTERNAL_STORAGE_CLUSTER_YAML)
        cluster_data["metadata"]["name"] = config.ENV_DATA["storage_cluster_name"]
        cluster_data_yaml = tempfile.NamedTemporaryFile(
            mode="w+", prefix="external_cluster_storage", delete=False
        )
        templating.dump_data_to_temp_yaml(cluster_data, cluster_data_yaml.name)
        run_cmd(f"oc create -f {cluster_data_yaml.name}", timeout=2400)
        self.external_post_deploy_validation()
        setup_ceph_toolbox()

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
        image = None
        ceph_cluster = ocp.OCP(kind="CephCluster", namespace=self.namespace)
        try:
            ceph_cluster.get().get("items")[0]
            logger.warning("OCS cluster already exists")
            return
        except (IndexError, CommandFailed):
            logger.info("Running OCS basic installation")

        # disconnected installation?
        load_cluster_info()
        if config.DEPLOYMENT.get("disconnected"):
            image = prepare_disconnected_ocs_deployment()

        if config.DEPLOYMENT["external_mode"]:
            logger.info("Deploying OCS on external mode RHCS")
            return self.deploy_with_external_mode()
        self.deploy_ocs_via_operator(image)
        pod = ocp.OCP(kind=constants.POD, namespace=self.namespace)
        cfs = ocp.OCP(kind=constants.CEPHFILESYSTEM, namespace=self.namespace)
        # Check for Ceph pods
        assert pod.wait_for_resource(
            condition="Running",
            selector="app=rook-ceph-mon",
            resource_count=3,
            timeout=600,
        )
        assert pod.wait_for_resource(
            condition="Running", selector="app=rook-ceph-mgr", timeout=600
        )
        assert pod.wait_for_resource(
            condition="Running",
            selector="app=rook-ceph-osd",
            resource_count=3,
            timeout=600,
        )

        # validate ceph mon/osd volumes are backed by pvc
        validate_cluster_on_pvc()

        # validate PDB creation of MON, MDS, OSD pods
        validate_pdb_creation()

        # Creating toolbox pod
        setup_ceph_toolbox()

        assert pod.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector="app=rook-ceph-tools",
            resource_count=1,
            timeout=600,
        )

        # Check for CephFilesystem creation in ocp
        cfs_data = cfs.get()
        cfs_name = cfs_data["items"][0]["metadata"]["name"]

        if helpers.validate_cephfilesystem(cfs_name):
            logger.info("MDS deployment is successful!")
            defaults.CEPHFILESYSTEM_NAME = cfs_name
        else:
            logger.error("MDS deployment Failed! Please check logs!")

        # Change monitoring backend to OCS
        if config.ENV_DATA.get("monitoring_enabled") and config.ENV_DATA.get(
            "persistent-monitoring"
        ):

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
        elif config.ENV_DATA.get("monitoring_enabled") and config.ENV_DATA.get(
            "telemeter_server_url"
        ):
            # Create configmap cluster-monitoring-config to reconfigure
            # telemeter server url when 'persistent-monitoring' is False
            create_configmap_cluster_monitoring_pod(
                telemeter_server_url=config.ENV_DATA["telemeter_server_url"]
            )

        # Change registry backend to OCS CEPHFS RWX PVC
        registry.change_registry_backend_to_ocs()

        # Verify health of ceph cluster
        # TODO: move destroy cluster logic to new CLI usage pattern?
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

        # patch gp2/thin storage class as 'non-default'
        self.patch_default_sc_to_non_default()

    def destroy_cluster(self, log_level="DEBUG"):
        """
        Base destroy cluster method, for more platform specific stuff please
        overload this method in child class.

        Args:
            log_level (str): log level for installer (default: DEBUG)
        """
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
                logger.info("OCS uninstalled succesfully")
            except Exception as ex:
                logger.error(f"Failed to uninstall OCS. Exception is: {ex}")
                logger.info("resuming teardown")
            self.ocp_deployment.destroy(log_level)

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
        logger.info(f"Patch {self.DEFAULT_STORAGECLASS} storageclass as non-default")
        patch = ' \'{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"false"}}}\' '
        run_cmd(
            f"oc patch storageclass {self.DEFAULT_STORAGECLASS} "
            f"-p {patch} "
            f"--request-timeout=120s"
        )


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


def link_sa_and_secret(sa_name, secret_name, namespace):
    """
    Link service account and secret for pulling of images.

    Args:
        sa_name (str): service account name
        secret_name (str): secret name
        namespace (str): namespace name

    """
    exec_cmd(f"oc secrets link {sa_name} {secret_name} --for=pull -n {namespace}")


def link_all_sa_and_secret(secret_name, namespace):
    """
    Link all service accounts in specified namespace with the secret for pulling
    of images.

    Args:
        secret_name (str): secret name
        namespace (str): namespace name

    """
    service_account = ocp.OCP(kind="serviceAccount", namespace=namespace)
    service_accounts = service_account.get()
    for sa in service_accounts.get("items", []):
        sa_name = sa["metadata"]["name"]
        logger.info(f"Linking secret: {secret_name} with SA: {sa_name}")
        link_sa_and_secret(sa_name, secret_name, namespace)


def create_catalog_source(image=None, ignore_upgrade=False):
    """
    This prepare catalog source manifest for deploy OCS operator from
    quay registry.

    Args:
        image (str): Image of ocs registry.
        ignore_upgrade (bool): Ignore upgrade parameter.

    """
    if config.ENV_DATA["platform"] == constants.IBMCLOUD_PLATFORM:
        link_all_sa_and_secret(constants.OCS_SECRET, constants.MARKETPLACE_NAMESPACE)
    logger.info("Adding CatalogSource")
    if not image:
        image = config.DEPLOYMENT.get("ocs_registry_image", "")
    if config.DEPLOYMENT.get("stage_rh_osbs"):
        image = config.DEPLOYMENT.get("stage_index_image", constants.OSBS_BOUNDLE_IMAGE)
        osbs_image_tag = config.DEPLOYMENT.get(
            "stage_index_image_tag", f"v{get_ocp_version()}"
        )
        image += f":{osbs_image_tag}"
        run_cmd(
            "oc patch image.config.openshift.io/cluster --type merge -p '"
            '{"spec": {"registrySources": {"insecureRegistries": '
            '["registry-proxy.engineering.redhat.com"]}}}\''
        )
        run_cmd(f"oc apply -f {constants.STAGE_IMAGE_CONTENT_SOURCE_POLICY_YAML}")
        logger.info("Sleeping for 60 sec to start update machineconfigpool status")
        time.sleep(60)
        wait_for_machineconfigpool_status("all", timeout=1800)
    if not ignore_upgrade:
        upgrade = config.UPGRADE.get("upgrade", False)
    else:
        upgrade = False
    image_and_tag = image.split(":")
    image = image_and_tag[0]
    image_tag = image_and_tag[1] if len(image_and_tag) == 2 else None
    if not image_tag and config.REPORTING.get("us_ds") == "DS":
        image_tag = get_latest_ds_olm_tag(
            upgrade, latest_tag=config.DEPLOYMENT.get("default_latest_tag", "latest")
        )

    catalog_source_data = templating.load_yaml(constants.CATALOG_SOURCE_YAML)
    cs_name = constants.OPERATOR_CATALOG_SOURCE_NAME
    change_cs_condition = (
        (image or image_tag)
        and catalog_source_data["kind"] == "CatalogSource"
        and catalog_source_data["metadata"]["name"] == cs_name
    )
    if change_cs_condition:
        default_image = config.DEPLOYMENT["default_ocs_registry_image"]
        image = image if image else default_image.split(":")[0]
        catalog_source_data["spec"][
            "image"
        ] = f"{image}:{image_tag if image_tag else 'latest'}"
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
