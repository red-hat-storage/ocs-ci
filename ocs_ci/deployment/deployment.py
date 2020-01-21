"""
This module provides base class for different deployment
platforms like AWS, VMWare, Baremetal etc.
"""
import logging
import os
import tempfile
import time

import json
import requests
from copy import deepcopy

from ocs_ci.deployment.ocp import OCPDeployment as BaseOCPDeployment
from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp, defaults, registry
from ocs_ci.ocs.cluster import validate_cluster_on_pvc, validate_pdb_creation
from ocs_ci.ocs.exceptions import CommandFailed, UnavailableResourceException
from ocs_ci.ocs.monitoring import (
    create_configmap_cluster_monitoring_pod,
    validate_pvc_created_and_bound_on_monitoring_pods,
    validate_pvc_are_mounted_on_monitoring_pods
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
    validate_pods_are_respinned_and_running_state
)
from ocs_ci.ocs.utils import (
    setup_ceph_toolbox, collect_ocs_logs
)
from ocs_ci.utility import templating
from ocs_ci.utility.utils import (
    run_cmd, ceph_health_check, is_cluster_running, get_kubeadmin_password,
    get_latest_ds_olm_tag,
)
from tests import helpers

logger = logging.getLogger(__name__)


class Deployment(object):
    """
    Base for all deployment platforms
    """
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

    def add_volume(self):
        """
        Implement add_volume in child class which is specific to
        platform
        """
        raise NotImplementedError("add_volume functionality not implemented")

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
                except Exception:
                    if config.REPORTING['gather_on_deploy_failure']:
                        collect_ocs_logs('deployment', ocs=False)
                    raise

        if not config.ENV_DATA['skip_ocs_deployment']:
            try:
                self.deploy_ocs()
            except Exception:
                if config.REPORTING['gather_on_deploy_failure']:
                    # Let's do the collections separately to guard against one
                    # of them failing
                    collect_ocs_logs('deployment', ocs=False)
                    collect_ocs_logs('deployment', ocp=False)
                raise
        else:
            logger.warning("OCS deployment will be skipped")

    def add_stage_cert(self):
        """
        Deploy stage certificate to the cluster.
        """
        logger.info("Create configmap stage-registry-config with stage CA.")
        run_cmd(
            f"oc -n openshift-config create configmap stage-registry-config"
            f" --from-file=registry.stage.redhat.io={constants.STAGE_CA_FILE}"
        )

        logger.info("Add stage-registry-config to additionalTrustedCA.")
        additional_trusted_ca_patch = (
            '{"spec":{"additionalTrustedCA":{"name":"stage-registry-config"}}}'
        )
        run_cmd(
            f"oc patch image.config.openshift.io cluster --type=merge"
            f" -p '{additional_trusted_ca_patch}'"
        )

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
        self.add_stage_cert()
        # logging the cluster UUID so that we can ask for it's telemetry data
        cluster_id = run_cmd("oc get clusterversion version -o jsonpath='{.spec.clusterID}'")
        logger.info(f"clusterID (UUID): {cluster_id}")

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

        workers_to_label = " ".join(distributed_worker_nodes[:to_label])
        if workers_to_label:
            _ocp = ocp.OCP(kind='node')
            logger.info(
                f"Label nodes: {workers_to_label} with label: "
                f"{constants.OPERATOR_NODE_LABEL}"
            )
            label_cmd = (
                f"label nodes {workers_to_label} {constants.OPERATOR_NODE_LABEL}"
            )
            _ocp.exec_oc_cmd(command=label_cmd)

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

    def create_catalog_source(self):
        """
        This prepare catalog source manifest for deploy OCS operator from
        quay registry.
        """
        logger.info("Adding CatalogSource")
        image = config.DEPLOYMENT.get('ocs_registry_image', '')
        upgrade = config.DEPLOYMENT.get('upgrade', False)
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
        # TODO: Once needed we can also set the channel for the subscription
        # from config.DEPLOYMENT.get('ocs_csv_channel')
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
            self.create_catalog_source()

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
        channel = config.DEPLOYMENT.get('ocs_csv_channel')
        csv_name = package_manifest.get_current_csv(channel=channel)
        csv = CSV(resource_name=csv_name, namespace=self.namespace)
        csv.wait_for_phase("Succeeded", timeout=720)
        cluster_data = templating.load_yaml(constants.STORAGE_CLUSTER_YAML)
        cluster_data['metadata']['name'] = config.ENV_DATA[
            'storage_cluster_name'
        ]
        deviceset_data = cluster_data['spec']['storageDeviceSets'][0]
        device_size = int(
            config.ENV_DATA.get('device_size', defaults.DEVICE_SIZE)
        )
        deviceset_data['dataPVCTemplate']['spec']['resources']['requests'][
            'storage'
        ] = f"{device_size}Gi"

        # Allow lower instance requests and limits for OCS deployment
        if config.DEPLOYMENT.get('allow_lower_instance_requirements'):
            none_resources = {'Requests': None, 'Limits': None}
            deviceset_data["resources"] = deepcopy(none_resources)
            cluster_data['spec']['resources'] = {
                resource: deepcopy(none_resources) for resource
                in [
                    'mon', 'mds', 'rgw', 'mgr', 'noobaa-core', 'noobaa-db',
                ]
            }

        if self.platform.lower() == constants.VSPHERE_PLATFORM:
            deviceset_data['dataPVCTemplate']['spec'][
                'storageClassName'
            ] = constants.DEFAULT_SC_VSPHERE

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

    def deployment_with_ui(self):
        """
        This method will deploy OCS with openshift-console UI test.
        """
        # TODO: add support for other browsers
        logger.info("Deployment of OCS will be done by openshift-console")
        console_path = config.RUN['openshift_console_path']
        password_secret_yaml = os.path.join(
            console_path, constants.HTPASSWD_SECRET_YAML
        )
        patch_htpasswd_yaml = os.path.join(
            console_path, constants.HTPASSWD_PATCH_YAML
        )
        with open(patch_htpasswd_yaml) as fd_patch_htpasswd:
            content_patch_htpasswd_yaml = fd_patch_htpasswd.read()
        run_cmd(f"oc apply -f {password_secret_yaml}", cwd=console_path)
        run_cmd(
            f"oc patch oauths cluster --patch "
            f"\"{content_patch_htpasswd_yaml}\" --type=merge",
            cwd=console_path
        )
        bridge_base_address = run_cmd(
            "oc get consoles.config.openshift.io cluster -o"
            "jsonpath='{.status.consoleURL}'"
        )
        chrome_branch_base = config.RUN.get("force_chrome_branch_base")
        chrome_branch_sha = config.RUN.get("force_chrome_branch_sha256sum")
        openshift_console_env = {
            "BRIDGE_KUBEADMIN_PASSWORD": get_kubeadmin_password(),
            "BRIDGE_BASE_ADDRESS": bridge_base_address,
            "FORCE_CHROME_BRANCH_BASE": chrome_branch_base,
            "FORCE_CHROME_BRANCH_SHA256SUM": chrome_branch_sha,
            "OCS_LIVE": int(config.DEPLOYMENT.get('live_deployment', 0)),
        }
        openshift_console_env.update(os.environ)
        ui_deploy_output = run_cmd(
            "./test-gui.sh ceph-storage-install", cwd=console_path,
            env=openshift_console_env, timeout=1500,
        )
        ui_deploy_log_file = os.path.expanduser(
            os.path.join(config.RUN['log_dir'], "ui_deployment.log")
        )
        with open(ui_deploy_log_file, "w+") as log_fd:
            log_fd.write(ui_deploy_output)

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
            logger.info(f"MDS deployment is successful!")
            defaults.CEPHFILESYSTEM_NAME = cfs_name
        else:
            logger.error(
                f"MDS deployment Failed! Please check logs!"
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
            validate_pods_are_respinned_and_running_state(
                pods_list
            )

            # Validate the pvc is created on monitoring pods
            validate_pvc_created_and_bound_on_monitoring_pods()

            # Validate the pvc are mounted on pods
            validate_pvc_are_mounted_on_monitoring_pods(pods_list)
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
        assert ceph_health_check(
            namespace=self.namespace
        )
        # patch gp2/thin storage class as 'non-default'
        self.patch_default_sc_to_non_default()

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
        sc_to_patch = None
        if self.platform.lower() == constants.AWS_PLATFORM:
            sc_to_patch = constants.DEFAULT_SC_AWS
        elif self.platform.lower() == constants.VSPHERE_PLATFORM:
            sc_to_patch = constants.DEFAULT_SC_VSPHERE
        else:
            logger.info(f"Unsupported platform {self.platform} to patch")
        if sc_to_patch:
            logger.info(f"Patch {sc_to_patch} storageclass as non-default")
            patch = " '{\"metadata\": {\"annotations\":{\"storageclass.kubernetes.io/is-default-class\":\"false\"}}}' "
            run_cmd(
                f"oc patch storageclass {sc_to_patch} "
                f"-p {patch} "
                f"--request-timeout=120s"
            )
