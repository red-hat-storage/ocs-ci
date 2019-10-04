"""
This module provides base class for different deployment
platforms like AWS, VMWare, Baremetal etc.
"""
import logging
import tempfile
import time

from ocs_ci.deployment.ocp import OCPDeployment as BaseOCPDeployment
from ocs_ci.framework import config
from ocs_ci.utility import templating
from ocs_ci.utility.utils import (
    run_cmd, ceph_health_check, is_cluster_running, get_latest_ds_olm_tag
)
from ocs_ci.ocs.exceptions import CommandFailed, UnavailableResourceException
from ocs_ci.ocs import constants, ocp, defaults
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs.resources.csv import CSV
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.resources.packagemanifest import PackageManifest
from tests import helpers
from ocs_ci.ocs.monitoring import (
    create_configmap_cluster_monitoring_pod,
    validate_pvc_created_and_bound_on_monitoring_pods,
    validate_pvc_are_mounted_on_monitoring_pods
)
from ocs_ci.ocs.utils import (
    create_oc_resource, apply_oc_resource, setup_ceph_toolbox, collect_ocs_logs
)
from ocs_ci.ocs.resources.pod import (
    get_all_pods,
    validate_pods_are_respinned_and_running_state
)
from ocs_ci.ocs.cluster import validate_cluster_on_pvc


logger = logging.getLogger(__name__)


class Deployment(object):
    """
    Base for all deployment platforms
    """
    def __init__(self):
        self.platform = config.ENV_DATA['platform']
        self.ocp_deployment_type = config.ENV_DATA['deployment_type']
        self.cluster_path = config.ENV_DATA['cluster_path']
        self.ocs_operator_deployment = config.DEPLOYMENT.get(
            'ocs_operator_deployment', True
        )
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
        to_label = config.DEPLOYMENT.get('ocs_operator_nodes_to_label', 3)
        to_taint = config.DEPLOYMENT.get('ocs_operator_nodes_to_tain', 0)
        worker_count = len(worker_nodes)
        if worker_count < to_label or worker_count < to_taint:
            logger.info(f"All nodes: {nodes}")
            logger.info(f"Worker nodes: {worker_nodes}")
            raise UnavailableResourceException(
                f"Not enough worker nodes: {worker_count} to label: "
                f"{to_label} or taint: {to_taint}!"
            )

        workers_to_label = " ".join(
            [node['metadata']['name'] for node in worker_nodes[:to_label]]
        )
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

        workers_to_taint = " ".join(
            [node['metadata']['name'] for node in worker_nodes[:to_taint]]
        )
        if workers_to_taint:
            logger.info(
                f"Taint nodes: {workers_to_taint} with taint: "
                f"{constants.OPERATOR_NODE_TAINT}"
            )
            taint_cmd = (
                f"adm taint nodes {workers_to_taint} {constants.OPERATOR_NODE_TAINT}"
            )
            _ocp.exec_oc_cmd(command=taint_cmd)

    def get_olm_and_subscription_manifest(self):
        """
        This method prepare manifest for deploy OCS operator and subscription.

        Returns:
            tuple: Path to olm deploy and subscription manifest

        """
        image = config.DEPLOYMENT.get('ocs_operator_image', '')
        image_and_tag = image.split(':')
        image = image_and_tag[0]
        image_tag = image_and_tag[1] if len(image_and_tag) == 2 else None
        if not image_tag and config.REPORTING.get("us_ds") == 'DS':
            image_tag = get_latest_ds_olm_tag()
        ocs_operator_olm = config.DEPLOYMENT['ocs_operator_olm']
        olm_data_generator = templating.load_yaml(
            ocs_operator_olm, multi_document=True
        )
        olm_yaml_data = []
        subscription_yaml_data = []
        cs_name = constants.OPERATOR_CATALOG_SOURCE_NAME
        # TODO: Once needed we can also set the channel for the subscription
        # from config.DEPLOYMENT.get('ocs_csv_channel')
        for yaml_doc in olm_data_generator:
            change_cs_condition = (
                (image or image_tag) and yaml_doc['kind'] == 'CatalogSource'
                and yaml_doc['metadata']['name'] == cs_name
            )
            if change_cs_condition:
                image_from_spec = yaml_doc['spec']['image']
                image = image if image else image_from_spec.split(':')[0]
                yaml_doc['spec']['image'] = (
                    f"{image}:{image_tag if image_tag else 'latest'}"
                )
            if yaml_doc.get('kind') == 'Subscription':
                subscription_yaml_data.append(yaml_doc)
                continue
            olm_yaml_data.append(yaml_doc)
        olm_manifest = tempfile.NamedTemporaryFile(
            mode='w+', prefix='olm_manifest', delete=False
        )
        templating.dump_data_to_temp_yaml(
            olm_yaml_data, olm_manifest.name
        )
        subscription_manifest = tempfile.NamedTemporaryFile(
            mode='w+', prefix='subscription_manifest', delete=False
        )
        templating.dump_data_to_temp_yaml(
            subscription_yaml_data, subscription_manifest.name
        )
        return olm_manifest.name, subscription_manifest.name

    def deploy_ocs_via_operator(self):
        """
        Method for deploy OCS via OCS operator
        """
        logger.info("Deployment of OCS via OCS operator")
        olm_manifest, subscription_manifest = (
            self.get_olm_and_subscription_manifest()
        )
        self.label_and_taint_nodes()
        run_cmd(f"oc create -f {olm_manifest}")
        catalog_source = CatalogSource(
            resource_name='ocs-catalogsource',
            namespace='openshift-marketplace',
        )
        # Wait for catalog source is ready
        catalog_source.wait_for_state("READY")
        run_cmd(f"oc create -f {subscription_manifest}")
        package_manifest = PackageManifest(
            resource_name=defaults.OCS_OPERATOR_NAME
        )
        # Wait for package manifest is ready
        package_manifest.wait_for_resource()
        channel = config.DEPLOYMENT.get('ocs_csv_channel')
        csv_name = package_manifest.get_current_csv(channel=channel)
        csv = CSV(
            resource_name=csv_name, kind="csv",
            namespace=self.namespace
        )
        csv.wait_for_phase("Succeeded", timeout=400)
        ocs_operator_storage_cluster_cr = config.DEPLOYMENT.get(
            'ocs_operator_storage_cluster_cr'
        )
        cluster_data = templating.load_yaml(
            ocs_operator_storage_cluster_cr
        )
        cluster_data['metadata']['name'] = config.ENV_DATA[
            'storage_cluster_name'
        ]
        deviceset_data = templating.load_yaml(
            constants.DEVICESET_YAML
        )
        device_size = int(
            config.ENV_DATA.get('device_size', defaults.DEVICE_SIZE)
        )
        deviceset_data['dataPVCTemplate']['spec']['resources']['requests'][
            'storage'
        ] = f"{device_size}Gi"

        if self.platform.lower() == constants.VSPHERE_PLATFORM:
            cluster_data['spec']['monPVCTemplate']['spec'][
                'storageClassName'
            ] = constants.DEFAULT_SC_VSPHERE
            deviceset_data['dataPVCTemplate']['spec'][
                'storageClassName'
            ] = constants.DEFAULT_SC_VSPHERE

        cluster_data['spec']['storageDeviceSets'] = [deviceset_data]
        cluster_data_yaml = tempfile.NamedTemporaryFile(
            mode='w+', prefix='cluster_storage', delete=False
        )
        templating.dump_data_to_temp_yaml(
            cluster_data, cluster_data_yaml.name
        )
        run_cmd(f"oc create -f {cluster_data_yaml.name}")

    def deploy_ocs(self):
        """
        Handle OCS deployment, since OCS deployment steps are common to any
        platform, implementing OCS deployment here in base class.
        """
        _templating = templating.Templating()

        ceph_cluster = ocp.OCP(
            kind='CephCluster', namespace=self.namespace
        )
        try:
            ceph_cluster.get().get('items')[0]
            logger.warning("OCS cluster already exists")
            return
        except (IndexError, CommandFailed):
            logger.info("Running OCS basic installation")

        if not self.ocs_operator_deployment:
            create_oc_resource(
                'common.yaml', self.cluster_path, _templating, config.ENV_DATA
            )
            run_cmd(
                f'oc label namespace {config.ENV_DATA["cluster_namespace"]} '
                f'"openshift.io/cluster-monitoring=true"'
            )
            run_cmd(
                f"oc policy add-role-to-user view "
                f"system:serviceaccount:openshift-monitoring:prometheus-k8s "
                f"-n {self.namespace}"
            )
            # HACK: If you would like to drop this hack, make sure that you
            # also updated docs and write appropriate unit/integration tests
            # for config processing.
            if config.ENV_DATA.get('monitoring_enabled') in (
                "true", "True", True
            ):
                # RBAC rules for monitoring, based on documentation change in
                # rook:
                # https://github.com/rook/rook/commit/1b6fe840f6ae7372a9675ba727ecc65326708aa8
                # HACK: This should be dropped when OCS is managed by OLM
                apply_oc_resource(
                    'rbac.yaml',
                    self.cluster_path,
                    _templating,
                    config.ENV_DATA,
                    template_dir="monitoring"
                )
            # Increased to 15 seconds as 10 is not enough
            # TODO: do the sampler function and check if resource exist
            wait_time = 15
            logger.info(f"Waiting {wait_time} seconds...")
            time.sleep(wait_time)
            create_oc_resource(
                'operator-openshift.yaml', self.cluster_path,
                _templating, config.ENV_DATA
            )
            logger.info(f"Waiting {wait_time} seconds...")
            time.sleep(wait_time)
            run_cmd(
                f"oc wait --for condition=ready pod "
                f"-l app=rook-ceph-operator "
                f"-n {self.namespace} "
                f"--timeout=120s"
            )
            run_cmd(
                f"oc wait --for condition=ready pod "
                f"-l app=rook-discover "
                f"-n {self.namespace} "
                f"--timeout=120s"
            )
            create_oc_resource(
                'cluster.yaml', self.cluster_path, _templating, config.ENV_DATA
            )
        else:
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
        # Validation for cluster on pvc
        logger.info("Validate mon and OSD are backed by PVCs")
        validate_cluster_on_pvc(label=constants.MON_APP_LABEL)
        validate_cluster_on_pvc(label=constants.DEFAULT_DEVICESET_LABEL)

        # Creating toolbox pod
        setup_ceph_toolbox()

        assert pod.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector='app=rook-ceph-tools', resource_count=1, timeout=600
        )

        if not self.ocs_operator_deployment:
            logger.info(f"Waiting {wait_time} seconds...")
            time.sleep(wait_time)
            # HACK: This should be dropped (including service-monitor.yaml and
            # prometheus-rules.yaml files) when OCS is managed by OLM
            if config.ENV_DATA.get('monitoring_enabled') not in (
                "true", "True", True
            ):
                # HACK: skip creation of rook-ceph-mgr service monitor when
                # monitoring is enabled (if this were not skipped, the step
                # would fail because rook would create the service monitor at
                # this point already)
                create_oc_resource(
                    "service-monitor.yaml", self.cluster_path, _templating,
                    config.ENV_DATA
                )
                # HACK: skip creation of prometheus-rules, rook-ceph is
                # concerned with it's setup now, based on clarification from
                # Umanga Chapagain
                create_oc_resource(
                    "prometheus-rules.yaml", self.cluster_path, _templating,
                    config.ENV_DATA
                )
            logger.info(f"Waiting {wait_time} seconds...")
            time.sleep(wait_time)

            # Create MDS pods for CephFileSystem
            fs_data = templating.load_yaml(constants.CEPHFILESYSTEM_YAML)
            fs_data['metadata']['namespace'] = self.namespace

            ceph_obj = OCS(**fs_data)
            ceph_obj.create()
            assert pod.wait_for_resource(
                condition=constants.STATUS_RUNNING, selector='app=rook-ceph-mds',
                resource_count=2, timeout=600
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

        if config.ENV_DATA.get('monitoring_enabled') and config.ENV_DATA.get('persistent-monitoring'):
            # Create a pool, secrets and sc
            secret_obj = helpers.create_secret(interface_type=constants.CEPHBLOCKPOOL)
            cbj_obj = helpers.create_ceph_block_pool()
            sc_obj = helpers.create_storage_class(
                interface_type=constants.CEPHBLOCKPOOL,
                interface_name=cbj_obj.name,
                secret_name=secret_obj.name
            )

            # Get the list of monitoring pods
            pods_list = get_all_pods(
                namespace=defaults.OCS_MONITORING_NAMESPACE,
                selector=['prometheus', 'alertmanager']
            )

            # Create configmap cluster-monitoring-config
            create_configmap_cluster_monitoring_pod(sc_obj.name)

            # Take some time to respin the pod
            waiting_time = 30
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
