"""
This module provides base class for different deployment
platforms like AWS, VMWare, Baremetal etc.
"""
import logging
import tempfile
import time

import json
import requests
from copy import deepcopy

from ocs_ci.deployment.ocp import OCPDeployment as BaseOCPDeployment

from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp, defaults, registry
from ocs_ci.ocs.cluster import (
    validate_cluster_on_pvc,
    validate_pdb_creation,
    CephClusterExternal
)
from ocs_ci.ocs.exceptions import (
    CephHealthException,
    CommandFailed,
    ResourceWrongStatusException,
    UnavailableResourceException,
    UnsupportedPlatformError,
    ExternalClusterDetailsException
)
from ocs_ci.ocs.monitoring import (
    create_configmap_cluster_monitoring_pod,
    validate_pvc_created_and_bound_on_monitoring_pods,
    validate_pvc_are_mounted_on_monitoring_pods
)
from ocs_ci.ocs.node import get_typed_nodes, check_nodes_specs
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs.resources.csv import CSV
from ocs_ci.ocs.resources.install_plan import wait_for_install_plan_and_approve
from ocs_ci.ocs.resources.packagemanifest import (
    get_selector_for_ocs_operator,
    PackageManifest,
)
from ocs_ci.ocs.resources.storage_cluster import change_noobaa_endpoints_count
from ocs_ci.ocs.resources.pod import (
    get_all_pods,
    validate_pods_are_respinned_and_running_state
)
from ocs_ci.ocs.utils import (
    setup_ceph_toolbox, collect_ocs_logs
)
from ocs_ci.utility import templating
from ocs_ci.utility.localstorage import get_lso_channel
from ocs_ci.utility.openshift_console import OpenshiftConsole
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import (
    ceph_health_check,
    get_latest_ds_olm_tag,
    is_cluster_running,
    run_cmd,
    set_selinux_permissions,
    set_registry_to_managed_state,
    add_stage_cert,
    modify_csv
)
from ocs_ci.utility.vsphere_nodes import update_ntp_compute_nodes
from tests import helpers

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
    DEFAULT_STORAGECLASS_LSO = 'localblock'

    def __init__(self):
        self.platform = config.ENV_DATA['platform']
        self.ocp_deployment_type = config.ENV_DATA['deployment_type']
        self.cluster_path = config.ENV_DATA['cluster_path']
        self.namespace = config.ENV_DATA["cluster_namespace"]

    class OCPDeployment(BaseOCPDeployment):
        """
        This class has to be implemented in child class and should overload
        methods for platform specific config.
        """
        pass

    def deploy_cluster(self, log_cli_level='DEBUG'):
        """
        We are handling both OCP and OCS deployment here based on flags

        Args:
            log_cli_level (str): log level for installer (default: DEBUG)
        """
        if not config.ENV_DATA['skip_ocp_deployment']:
            if is_cluster_running(self.cluster_path):
                logger.warning(
                    "OCP cluster is already running, skipping installation"
                )
            else:
                try:
                    self.deploy_ocp(log_cli_level)
                    self.post_ocp_deploy()
                except Exception as e:
                    logger.error(e)
                    if config.REPORTING['gather_on_deploy_failure']:
                        collect_ocs_logs('deployment', ocs=False)
                    raise

        if not config.ENV_DATA['skip_ocs_deployment']:
            try:
                self.deploy_ocs()
            except Exception as e:
                logger.error(e)
                if config.REPORTING['gather_on_deploy_failure']:
                    # Let's do the collections separately to guard against one
                    # of them failing
                    collect_ocs_logs('deployment', ocs=False)
                    collect_ocs_logs('deployment', ocp=False)
                raise
        else:
            logger.warning("OCS deployment will be skipped")

    def deploy_ocp(self, log_cli_level='DEBUG'):
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
        cluster_id = run_cmd("oc get clusterversion version -o jsonpath='{.spec.clusterID}'")
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

        nodes = ocp.OCP(kind='node').get().get('items', [])
        worker_nodes = [
            node for node in nodes if "node-role.kubernetes.io/worker"
            in node['metadata']['labels']
        ]
        if not worker_nodes:
            raise UnavailableResourceException("No worker node found!")
        az_worker_nodes = {}
        for node in worker_nodes:
            az = node['metadata']['labels'].get(
                'failure-domain.beta.kubernetes.io/zone'
            )
            az_node_list = az_worker_nodes.get(az, [])
            az_node_list.append(node)
            az_worker_nodes[az] = az_node_list
        logger.info(f"Found worker nodes in AZ: {az_worker_nodes}")
        distributed_worker_nodes = []
        while az_worker_nodes:
            for az in list(az_worker_nodes.keys()):
                az_node_list = az_worker_nodes.get(az)
                if az_node_list:
                    node_name = az_node_list.pop(0)['metadata']['name']
                    distributed_worker_nodes.append(node_name)
                else:
                    del az_worker_nodes[az]
        logger.info(
            f"Distributed worker nodes for AZ: {distributed_worker_nodes}"
        )
        to_label = config.DEPLOYMENT.get('ocs_operator_nodes_to_label', 3)
        to_taint = config.DEPLOYMENT.get('ocs_operator_nodes_to_taint', 0)
        worker_count = len(worker_nodes)
        if worker_count < to_label or worker_count < to_taint:
            logger.info(f"All nodes: {nodes}")
            logger.info(f"Worker nodes: {worker_nodes}")
            raise UnavailableResourceException(
                f"Not enough worker nodes: {worker_count} to label: "
                f"{to_label} or taint: {to_taint}!"
            )

        _ocp = ocp.OCP(kind='node')
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
            if config.DEPLOYMENT["infra_nodes"]:
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
            url='https://quay.io/cnr/api/v1/users/login',
            data=json.dumps(credentials),
            headers={'Content-Type': 'application/json'},
        ).json()['token']
        stage_ns = config.DEPLOYMENT["stage_namespace"]

        # create Secret
        stage_os_secret = templating.load_yaml(
            constants.OPERATOR_SOURCE_SECRET_YAML
        )
        stage_os_secret['metadata']['name'] = (
            constants.OPERATOR_SOURCE_SECRET_NAME
        )
        stage_os_secret['stringData']['token'] = token
        stage_secret_data_yaml = tempfile.NamedTemporaryFile(
            mode='w+', prefix=constants.OPERATOR_SOURCE_SECRET_NAME,
            delete=False,
        )
        templating.dump_data_to_temp_yaml(
            stage_os_secret, stage_secret_data_yaml.name
        )
        run_cmd(f"oc create -f {stage_secret_data_yaml.name}")
        logger.info("Waiting 10 secs after secret is created")
        time.sleep(10)

        logger.info("Adding Stage Operator Source")
        # create Operator Source
        stage_os = templating.load_yaml(
            constants.OPERATOR_SOURCE_YAML
        )
        stage_os['spec']['registryNamespace'] = stage_ns
        stage_os['spec']['authorizationToken']['secretName'] = (
            constants.OPERATOR_SOURCE_SECRET_NAME
        )
        stage_os_data_yaml = tempfile.NamedTemporaryFile(
            mode='w+', prefix=constants.OPERATOR_SOURCE_NAME, delete=False
        )
        templating.dump_data_to_temp_yaml(
            stage_os, stage_os_data_yaml.name
        )
        run_cmd(f"oc create -f {stage_os_data_yaml.name}")
        catalog_source = CatalogSource(
            resource_name=constants.OPERATOR_SOURCE_NAME,
            namespace=constants.MARKETPLACE_NAMESPACE,
        )
        # Wait for catalog source is ready
        catalog_source.wait_for_state("READY")

    def create_ocs_operator_source(self):
        """
        This prepare catalog or operator source for OCS deployment.
        """
        if config.DEPLOYMENT.get('stage'):
            # deployment from stage
            self.create_stage_operator_source()
        else:
            create_catalog_source()

    def subscribe_ocs(self):
        """
        This method subscription manifest and subscribe to OCS operator.

        """
        operator_selector = get_selector_for_ocs_operator()
        # wait for package manifest
        package_manifest = PackageManifest(
            resource_name=defaults.OCS_OPERATOR_NAME,
            selector=operator_selector,
        )
        # Wait for package manifest is ready
        package_manifest.wait_for_resource(timeout=300)
        default_channel = package_manifest.get_default_channel()
        subscription_yaml_data = templating.load_yaml(
            constants.SUBSCRIPTION_YAML
        )
        subscription_plan_approval = config.DEPLOYMENT.get(
            'subscription_plan_approval'
        )
        if subscription_plan_approval:
            subscription_yaml_data['spec']['installPlanApproval'] = (
                subscription_plan_approval
            )
        custom_channel = config.DEPLOYMENT.get('ocs_csv_channel')
        if custom_channel:
            logger.info(f"Custom channel will be used: {custom_channel}")
            subscription_yaml_data['spec']['channel'] = custom_channel
        else:
            logger.info(f"Default channel will be used: {default_channel}")
            subscription_yaml_data['spec']['channel'] = default_channel
        if config.DEPLOYMENT.get('stage'):
            subscription_yaml_data['spec']['source'] = (
                constants.OPERATOR_SOURCE_NAME
            )
        if config.DEPLOYMENT.get('live_deployment'):
            subscription_yaml_data['spec']['source'] = (
                config.DEPLOYMENT.get(
                    'live_content_source', defaults.LIVE_CONTENT_SOURCE
                )
            )
        subscription_manifest = tempfile.NamedTemporaryFile(
            mode='w+', prefix='subscription_manifest', delete=False
        )
        templating.dump_data_to_temp_yaml(
            subscription_yaml_data, subscription_manifest.name
        )
        run_cmd(f"oc create -f {subscription_manifest.name}")
        subscription_plan_approval = config.DEPLOYMENT.get(
            'subscription_plan_approval'
        )
        if subscription_plan_approval == 'Manual':
            wait_for_install_plan_and_approve(self.namespace)

    def deploy_ocs_via_operator(self):
        """
        Method for deploy OCS via OCS operator
        """
        ui_deployment = config.DEPLOYMENT.get('ui_deployment')
        live_deployment = config.DEPLOYMENT.get('live_deployment')

        if config.DEPLOYMENT.get('local_storage'):
            setup_local_storage(storageclass=self.DEFAULT_STORAGECLASS_LSO)

        if ui_deployment:
            if not live_deployment:
                self.create_ocs_operator_source()
            self.deployment_with_ui()
            # Skip the rest of the deployment when deploy via UI
            return
        else:
            logger.info("Deployment of OCS via OCS operator")
            self.label_and_taint_nodes()
        logger.info("Creating namespace and operator group.")
        run_cmd(f"oc create -f {constants.OLM_YAML}")
        if not live_deployment:
            self.create_ocs_operator_source()
        self.subscribe_ocs()
        operator_selector = get_selector_for_ocs_operator()
        package_manifest = PackageManifest(
            resource_name=defaults.OCS_OPERATOR_NAME,
            selector=operator_selector,
        )
        package_manifest.wait_for_resource(timeout=300)
        channel = config.DEPLOYMENT.get('ocs_csv_channel')
        csv_name = package_manifest.get_current_csv(channel=channel)
        csv = CSV(resource_name=csv_name, namespace=self.namespace)
        csv.wait_for_phase("Succeeded", timeout=720)

        # Modify the CSV with custom values if required
        if all(key in config.DEPLOYMENT for key in ('csv_change_from', 'csv_change_to')):
            modify_csv(
                csv=csv_name,
                replace_from=config.DEPLOYMENT['csv_change_from'],
                replace_to=config.DEPLOYMENT['csv_change_to']
            )

        # creating StorageCluster
        cluster_data = templating.load_yaml(constants.STORAGE_CLUSTER_YAML)
        cluster_data['metadata']['name'] = config.ENV_DATA[
            'storage_cluster_name'
        ]
        deviceset_data = cluster_data['spec']['storageDeviceSets'][0]
        device_size = int(
            config.ENV_DATA.get('device_size', defaults.DEVICE_SIZE)
        )

        # set size of request for storage
        if self.platform.lower() == constants.BAREMETAL_PLATFORM:
            pv_size_list = helpers.get_pv_size(storageclass=self.DEFAULT_STORAGECLASS_LSO)
            pv_size_list.sort()
            deviceset_data['dataPVCTemplate']['spec']['resources']['requests'][
                'storage'
            ] = f"{pv_size_list[0]}"
        else:
            deviceset_data['dataPVCTemplate']['spec']['resources']['requests'][
                'storage'
            ] = f"{device_size}Gi"

        # set storage class to OCS default on current platform
        if self.DEFAULT_STORAGECLASS:
            deviceset_data['dataPVCTemplate']['spec'][
                'storageClassName'
            ] = self.DEFAULT_STORAGECLASS

        ocs_version = float(config.ENV_DATA['ocs_version'])

        # StorageCluster tweaks for LSO
        if config.DEPLOYMENT.get('local_storage'):
            cluster_data['spec']['manageNodes'] = False
            cluster_data['spec']['monDataDirHostPath'] = '/var/lib/rook'
            deviceset_data['portable'] = False
            deviceset_data['dataPVCTemplate']['spec']['storageClassName'] = \
                self.DEFAULT_STORAGECLASS_LSO
            if self.platform.lower() == constants.AWS_PLATFORM:
                deviceset_data['count'] = 2
            if ocs_version >= 4.5:
                deviceset_data['resources'] = {
                    'limits': {
                        'cpu': 2,
                        'memory': '5Gi'
                    },
                    'requests': {
                        'cpu': 1,
                        'memory': '5Gi'
                    }
                }

        # Allow lower instance requests and limits for OCS deployment
        # The resources we need to change can be found here:
        # https://github.com/openshift/ocs-operator/blob/release-4.5/pkg/deploy-manager/storagecluster.go#L88-L116
        if config.DEPLOYMENT.get('allow_lower_instance_requirements'):
            none_resources = {'Requests': None, 'Limits': None}
            deviceset_data["resources"] = deepcopy(none_resources)
            resources = [
                'mon', 'mds', 'rgw', 'mgr', 'noobaa-core', 'noobaa-db',
            ]
            if ocs_version >= 4.5:
                resources.append('noobaa-endpoint')
            cluster_data['spec']['resources'] = {
                resource: deepcopy(none_resources) for resource in resources
            }
        else:
            local_storage = config.DEPLOYMENT.get('local_storage')
            platform = config.ENV_DATA.get('platform', '').lower()
            if local_storage and platform == 'aws':
                resources = {
                    'mds': {
                        'limits': {'cpu': 3, 'memory': '8Gi'},
                        'requests': {'cpu': 1, 'memory': '8Gi'}
                    }
                }
                if ocs_version < 4.5:
                    resources['noobaa-core'] = {
                        'limits': {'cpu': 2, 'memory': '8Gi'},
                        'requests': {'cpu': 1, 'memory': '8Gi'}
                    }
                    resources['noobaa-db'] = {
                        'limits': {'cpu': 2, 'memory': '8Gi'},
                        'requests': {'cpu': 1, 'memory': '8Gi'}
                    }
                cluster_data['spec']['resources'] = resources

        # Enable host network if enabled in config (this require all the
        # rules to be enabled on underlaying platform).
        if config.DEPLOYMENT.get('host_network'):
            cluster_data['spec']['hostNetwork'] = True

        cluster_data['spec']['storageDeviceSets'] = [deviceset_data]
        cluster_data_yaml = tempfile.NamedTemporaryFile(
            mode='w+', prefix='cluster_storage', delete=False
        )
        templating.dump_data_to_temp_yaml(
            cluster_data, cluster_data_yaml.name
        )
        run_cmd(f"oc create -f {cluster_data_yaml.name}", timeout=2400)
        if config.DEPLOYMENT["infra_nodes"]:
            _ocp = ocp.OCP(kind='node')
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
            config.DEPLOYMENT.get(
                'deployment_browser', constants.CHROME_BROWSER
            )
        )
        live_deploy = '1' if config.DEPLOYMENT.get('live_deployment') else '0'
        env_vars = {
            "OCS_LIVE": live_deploy,
        }
        ocp_console.run_openshift_console(
            suite="ceph-storage-install", env_vars=env_vars,
            log_suffix="ui-deployment"
        )

    def deploy_with_external_mode(self):
        """
        This function handles the deployment of OCS on
        external/indpendent RHCS cluster

        """
        live_deployment = config.DEPLOYMENT.get('live_deployment')
        logger.info("Deploying OCS with external mode RHCS")
        logger.info("Creating namespace and operator group")
        run_cmd(f"oc create -f {constants.OLM_YAML}")
        if not live_deployment:
            self.create_ocs_operator_source()
        self.subscribe_ocs()
        operator_selector = get_selector_for_ocs_operator()
        package_manifest = PackageManifest(
            resource_name=defaults.OCS_OPERATOR_NAME,
            selector=operator_selector,
        )
        package_manifest.wait_for_resource(timeout=300)
        channel = config.DEPLOYMENT.get('ocs_csv_channel')
        csv_name = package_manifest.get_current_csv(channel=channel)
        csv = CSV(resource_name=csv_name, namespace=self.namespace)
        csv.wait_for_phase("Succeeded", timeout=720)

        # Create secret for external cluster
        secret_data = templating.load_yaml(
            constants.EXTERNAL_CLUSTER_SECRET_YAML
        )
        external_cluster_details = config.EXTERNAL_MODE.get(
            'external_cluster_details',
            ''
        )
        if not external_cluster_details:
            raise ExternalClusterDetailsException(
                "No external cluster data found"
            )
        secret_data['data']['external_cluster_details'] = (
            external_cluster_details
        )
        secret_data_yaml = tempfile.NamedTemporaryFile(
            mode='w+', prefix='external_cluster_secret', delete=False
        )
        templating.dump_data_to_temp_yaml(
            secret_data, secret_data_yaml.name
        )
        logger.info("Creating external cluster secret")
        run_cmd(f"oc create -f {secret_data_yaml.name}")

        cluster_data = templating.load_yaml(
            constants.EXTERNAL_STORAGE_CLUSTER_YAML
        )
        cluster_data['metadata']['name'] = config.ENV_DATA[
            'storage_cluster_name'
        ]
        cluster_data_yaml = tempfile.NamedTemporaryFile(
            mode='w+', prefix='external_cluster_storage', delete=False
        )
        templating.dump_data_to_temp_yaml(
            cluster_data, cluster_data_yaml.name
        )
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
        ceph_cluster = ocp.OCP(
            kind='CephCluster', namespace=self.namespace
        )
        try:
            ceph_cluster.get().get('items')[0]
            logger.warning("OCS cluster already exists")
            return
        except (IndexError, CommandFailed):
            logger.info("Running OCS basic installation")

        if config.DEPLOYMENT['external_mode']:
            logger.info("Deploying OCS on external mode RHCS")
            return self.deploy_with_external_mode()

        self.deploy_ocs_via_operator()
        pod = ocp.OCP(
            kind=constants.POD, namespace=self.namespace
        )
        cfs = ocp.OCP(
            kind=constants.CEPHFILESYSTEM,
            namespace=self.namespace
        )
        # Check for Ceph pods
        assert pod.wait_for_resource(
            condition='Running', selector='app=rook-ceph-mon',
            resource_count=3, timeout=600
        )
        assert pod.wait_for_resource(
            condition='Running', selector='app=rook-ceph-mgr',
            timeout=600
        )
        assert pod.wait_for_resource(
            condition='Running', selector='app=rook-ceph-osd',
            resource_count=3, timeout=600
        )

        # validate ceph mon/osd volumes are backed by pvc
        validate_cluster_on_pvc()

        # validate PDB creation of MON, MDS, OSD pods
        validate_pdb_creation()

        # Creating toolbox pod
        setup_ceph_toolbox()

        assert pod.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector='app=rook-ceph-tools', resource_count=1, timeout=600
        )

        # Check for CephFilesystem creation in ocp
        cfs_data = cfs.get()
        cfs_name = cfs_data['items'][0]['metadata']['name']

        if helpers.validate_cephfilesystem(cfs_name):
            logger.info("MDS deployment is successful!")
            defaults.CEPHFILESYSTEM_NAME = cfs_name
        else:
            logger.error(
                "MDS deployment Failed! Please check logs!"
            )

        # Change monitoring backend to OCS
        if config.ENV_DATA.get('monitoring_enabled') and config.ENV_DATA.get('persistent-monitoring'):

            sc = helpers.default_storage_class(interface_type=constants.CEPHBLOCKPOOL)

            # Get the list of monitoring pods
            pods_list = get_all_pods(
                namespace=defaults.OCS_MONITORING_NAMESPACE,
                selector=['prometheus', 'alertmanager']
            )

            # Create configmap cluster-monitoring-config and reconfigure
            # storage class and telemeter server (if the url is specified in a
            # config file)
            create_configmap_cluster_monitoring_pod(
                sc_name=sc.name,
                telemeter_server_url=config.ENV_DATA.get("telemeter_server_url"))

            # Take some time to respin the pod
            waiting_time = 45
            logger.info(f"Waiting {waiting_time} seconds...")
            time.sleep(waiting_time)

            # Validate the pods are respinned and in running state
            retry(
                (CommandFailed, ResourceWrongStatusException),
                tries=3,
                delay=15)(
                validate_pods_are_respinned_and_running_state
            )(
                pods_list
            )

            # Validate the pvc is created on monitoring pods
            validate_pvc_created_and_bound_on_monitoring_pods()

            # Validate the pvc are mounted on pods
            retry((CommandFailed, AssertionError), tries=3, delay=15)(
                validate_pvc_are_mounted_on_monitoring_pods
            )(pods_list)
        elif config.ENV_DATA.get('monitoring_enabled') and config.ENV_DATA.get("telemeter_server_url"):
            # Create configmap cluster-monitoring-config to reconfigure
            # telemeter server url when 'persistent-monitoring' is False
            create_configmap_cluster_monitoring_pod(
                telemeter_server_url=config.ENV_DATA["telemeter_server_url"])

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
                    f"Changing NTP on compute nodes to"
                    f" {constants.RH_NTP_CLOCK}"
                )
                update_ntp_compute_nodes()
                assert ceph_health_check(
                    namespace=self.namespace, tries=60, delay=10
                )

        # patch gp2/thin storage class as 'non-default'
        self.patch_default_sc_to_non_default()

        # Modify Noobaa endpoint auto scale values according to the cluster specs
        if check_nodes_specs(min_cpu=constants.MIN_NODE_CPU, min_memory=constants.MIN_NODE_MEMORY):
            logger.info(
                "The cluster specs meet the minimum requirements and "
                "therefore, NooBaa auto scale will be enabled"
            )
            min_nb_eps = config.DEPLOYMENT.get('min_noobaa_endpoints')
            max_nb_eps = config.DEPLOYMENT.get('max_noobaa_endpoints')
            change_noobaa_endpoints_count(min_nb_eps=min_nb_eps, max_nb_eps=max_nb_eps)
        else:
            logger.warning(
                "The cluster specs do not meet the minimum requirements and "
                "therefore, NooBaa auto scale will remain with its default values"
            )
            min_eps = 1
            max_eps = 1 if float(config.ENV_DATA['ocs_version']) < 4.6 else 2
            logger.info(
                f"The Noobaa endpoint auto scale values: min: {min_eps}, max: {max_eps}"
            )

    def destroy_cluster(self, log_level="DEBUG"):
        """
        Base destroy cluster method, for more platform specific stuff please
        overload this method in child class.

        Args:
            log_level (str): log level for installer (default: DEBUG)
        """
        self.ocp_deployment = self.OCPDeployment()
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
        patch = " '{\"metadata\": {\"annotations\":{\"storageclass.kubernetes.io/is-default-class\":\"false\"}}}' "
        run_cmd(
            f"oc patch storageclass {self.DEFAULT_STORAGECLASS} "
            f"-p {patch} "
            f"--request-timeout=120s"
        )


def create_catalog_source(image=None, ignore_upgrade=False):
    """
    This prepare catalog source manifest for deploy OCS operator from
    quay registry.

    Args:
        image (str): Image of ocs registry.
        ignore_upgrade (bool): Ignore upgrade parameter.

    """
    logger.info("Adding CatalogSource")
    if not image:
        image = config.DEPLOYMENT.get('ocs_registry_image', '')
    if not ignore_upgrade:
        upgrade = config.UPGRADE.get('upgrade', False)
    else:
        upgrade = False
    image_and_tag = image.split(':')
    image = image_and_tag[0]
    image_tag = image_and_tag[1] if len(image_and_tag) == 2 else None
    if not image_tag and config.REPORTING.get("us_ds") == 'DS':
        image_tag = get_latest_ds_olm_tag(
            upgrade, latest_tag=config.DEPLOYMENT.get(
                'default_latest_tag', 'latest'
            )
        )
    catalog_source_data = templating.load_yaml(
        constants.CATALOG_SOURCE_YAML
    )
    cs_name = constants.OPERATOR_CATALOG_SOURCE_NAME
    change_cs_condition = (
        (image or image_tag) and catalog_source_data['kind'] == 'CatalogSource'
        and catalog_source_data['metadata']['name'] == cs_name
    )
    if change_cs_condition:
        default_image = config.DEPLOYMENT['default_ocs_registry_image']
        image = image if image else default_image.split(':')[0]
        catalog_source_data['spec']['image'] = (
            f"{image}:{image_tag if image_tag else 'latest'}"
        )
    catalog_source_manifest = tempfile.NamedTemporaryFile(
        mode='w+', prefix='catalog_source_manifest', delete=False
    )
    templating.dump_data_to_temp_yaml(
        catalog_source_data, catalog_source_manifest.name
    )
    run_cmd(f"oc create -f {catalog_source_manifest.name}", timeout=2400)
    catalog_source = CatalogSource(
        resource_name=constants.OPERATOR_CATALOG_SOURCE_NAME,
        namespace=constants.MARKETPLACE_NAMESPACE,
    )
    # Wait for catalog source is ready
    catalog_source.wait_for_state("READY")


def setup_local_storage(storageclass):
    """
    Setup the necessary resources for enabling local storage.

    Args:
        storageclass (string): storageClassName value to be used in LocalVolume CR
            based on LOCAL_VOLUME_YAML

    """
    # Get the worker nodes
    workers = get_typed_nodes(node_type='worker')
    worker_names = [worker.name for worker in workers]
    worker_names_str = " ".join(worker_names)
    logger.debug("Workers: %s", workers)

    # Label the worker nodes
    logger.info("Labeling worker nodes")
    run_cmd(
        f"oc label nodes {worker_names_str} "
        f"{constants.OPERATOR_NODE_LABEL}"
    )

    logger.info("Retrieving local-storage-operator data from yaml")
    lso_data = list(templating.load_yaml(
        constants.LOCAL_STORAGE_OPERATOR, multi_document=True
    ))
    # Update local-storage-operator subscription data with channel
    for data in lso_data:
        if data['kind'] == 'Subscription':
            data['spec']['channel'] = get_lso_channel()

    # Create temp yaml file and create local storage operator
    logger.info(
        "Creating temp yaml file with local-storage-operator data:\n %s",
        lso_data
    )
    lso_data_yaml = tempfile.NamedTemporaryFile(
        mode='w+', prefix='local_storage_operator', delete=False
    )
    templating.dump_data_to_temp_yaml(
        lso_data, lso_data_yaml.name
    )
    with open(lso_data_yaml.name, 'r') as f:
        logger.info(f.read())
    logger.info("Creating local-storage-operator")
    run_cmd(f"oc create -f {lso_data_yaml.name}")

    local_storage_operator = ocp.OCP(
        kind=constants.POD, namespace='local-storage'
    )
    assert local_storage_operator.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.LOCAL_STORAGE_OPERATOR_LABEL,
        timeout=600
    ), "Local storage operator did not reach running phase"

    # Add RDM disk for vSphere platform
    platform = config.ENV_DATA.get('platform').lower()
    lso_type = config.DEPLOYMENT.get('type')
    if platform == constants.VSPHERE_PLATFORM:
        # Types of LSO Deployment
        # Importing here to avoid circular dependancy
        from ocs_ci.deployment.vmware import VSPHEREBASE
        vsphere_base = VSPHEREBASE()

        if lso_type == constants.RDM:
            logger.info(f"LSO Deployment type: {constants.RDM}")
            vsphere_base.add_rdm_disks()

        if lso_type == constants.VMDK:
            logger.info(f"LSO Deployment type: {constants.VMDK}")
            vsphere_base.attach_disk(
                config.ENV_DATA.get('device_size', defaults.DEVICE_SIZE),
                config.DEPLOYMENT.get('provision_type', constants.VM_DISK_TYPE)
            )

        if lso_type == constants.DIRECTPATH:
            raise NotImplementedError(
                "LSO Deployment for VMDirectPath is not implemented"
            )

    # Retrieve NVME device path ID for each worker node
    device_paths = get_device_paths(worker_names)

    # Pull local volume yaml data
    logger.info("Pulling LocalVolume CR data from yaml")
    lv_data = templating.load_yaml(
        constants.LOCAL_VOLUME_YAML
    )

    # Set storage class
    logger.info(
        "Updating LocalVolume CR data with LSO storageclass: %s", storageclass)
    for scd in lv_data['spec']['storageClassDevices']:
        scd['storageClassName'] = storageclass

    # Update local volume data with NVME IDs
    logger.info(
        "Updating LocalVolume CR data with device paths: %s",
        device_paths
    )
    lv_data['spec']['storageClassDevices'][0][
        'devicePaths'
    ] = device_paths

    # Create temp yaml file and create local volume
    lv_data_yaml = tempfile.NamedTemporaryFile(
        mode='w+', prefix='local_volume', delete=False
    )
    templating.dump_data_to_temp_yaml(
        lv_data, lv_data_yaml.name
    )
    logger.info("Creating LocalVolume CR")
    run_cmd(f"oc create -f {lv_data_yaml.name}")
    logger.info("Waiting 30 seconds for PVs to create")
    storage_class_device_count = 1
    if platform == constants.AWS_PLATFORM:
        storage_class_device_count = 2
    verify_pvs_created(len(worker_names) * storage_class_device_count)


@retry(AssertionError, 120, 10, 1)
def verify_pvs_created(expected_pvs):
    """
    Verify that PVs were created and are in the Available state

    Args:
        expected_pvs (int): number of PVs to verify

    Raises:
        AssertionError: if any PVs are not in the Available state or if the
            number of PVs does not match the given parameter.

    """
    logger.info("Verifying PVs are created")
    out = run_cmd("oc get pv -o json")
    pv_json = json.loads(out)
    assert pv_json['items'], (
        f"No PVs created but we are expecting {expected_pvs}"
    )

    # check number of PVs created
    num_pvs = len(pv_json['items'])
    assert num_pvs == expected_pvs, (
        f"{num_pvs} PVs created but we are expecting {expected_pvs}"
    )

    # checks the state of PV
    for pv in pv_json['items']:
        pv_state = pv['status']['phase']
        pv_name = pv['metadata']['name']
        logger.info(f"{pv_name} is in {pv_state} state")
        assert pv_state == 'Available', (
            f"{pv_name} not in 'Available' state. Current state is {pv_state}"
        )

    logger.debug("PVs, Workers: %s, %s", num_pvs, expected_pvs)


def get_device_paths(worker_names):
    """
    Retrieve a list of the device paths for each worker node

    Args:
        worker_names (list): worker node names

    Returns:
        list: device path ids
    """
    device_paths = []
    platform = config.ENV_DATA.get('platform').lower()
    if platform == 'aws':
        pattern = 'nvme-Amazon_EC2_NVMe_Instance_Storage'
    elif platform == 'vsphere':
        pattern = 'wwn'
    elif platform == 'baremetal':
        pattern = config.ENV_DATA.get('disk_pattern')
    elif platform == 'baremetalpsi':
        pattern = 'virtio'
    # TODO: add patterns bare metal
    else:
        raise UnsupportedPlatformError(
            'LSO deployment is not supported for platform: %s', platform
        )
    for worker in worker_names:
        logger.info("Retrieving device path for node: %s", worker)
        out = _get_disk_by_id(worker)
        out_lines = out.split('\n')
        nvme_lines = [
            line for line in out_lines if (
                pattern in line and constants.ROOT_DISK_NAME not in line
            )
        ]
        for nvme_line in nvme_lines:
            device_path = [
                part for part in nvme_line.split(' ') if pattern in part
            ][0]
            logger.info("Adding %s to device paths", device_path)
            device_paths.append(f'/dev/disk/by-id/{device_path}')

    return device_paths


@retry(CommandFailed)
def _get_disk_by_id(worker):
    """
    Retrieve disk by-id on a worker node using the debug pod

    Args:
        worker: worker node to get disks by-id for

    Returns:
        str: stdout of disk by-id command

    """
    cmd = (
        f"oc debug nodes/{worker} "
        f"-- chroot /host ls -la /dev/disk/by-id/"
    )
    return run_cmd(cmd)
