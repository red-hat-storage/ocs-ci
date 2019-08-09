"""
This module provides base class for different deployment
platforms like AWS, VMWare, Baremetal etc.
"""
import logging
import time

from ocs_ci.deployment.ocp import OCPDeployment as BaseOCPDeployment
from ocs_ci.framework import config
from ocs_ci.ocs.utils import create_oc_resource
from ocs_ci.utility import templating
from ocs_ci.utility.utils import (
    run_cmd, ceph_health_check, is_cluster_running
)
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs import constants, ocp, defaults
from ocs_ci.ocs.resources.ocs import OCS
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
                self.deploy_ocp(log_cli_level)

        if not config.ENV_DATA['skip_ocs_deployment']:
            self.deploy_ocs()
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

    def deploy_ocs(self):
        """
        Handle OCS deployment, since OCS deployment steps are common to any
        platform, implementing OCS deployment here in base class.
        """
        _templating = templating.Templating()

        ceph_cluster = ocp.OCP(
            kind='CephCluster', namespace=config.ENV_DATA['cluster_namespace']
        )
        try:
            ceph_cluster.get().get('items')[0]
            logger.warning("OCS cluster already exists")
            return
        except (IndexError, CommandFailed):
            logger.info("Running OCS basic installation")

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
            f"-n {config.ENV_DATA['cluster_namespace']}"
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
            f"-n {config.ENV_DATA['cluster_namespace']} "
            f"--timeout=120s"
        )
        run_cmd(
            f"oc wait --for condition=ready pod "
            f"-l app=rook-discover "
            f"-n {config.ENV_DATA['cluster_namespace']} "
            f"--timeout=120s"
        )
        create_oc_resource(
            'cluster.yaml', self.cluster_path, _templating, config.ENV_DATA
        )

        pod = ocp.OCP(
            kind=constants.POD, namespace=config.ENV_DATA['cluster_namespace']
        )
        cfs = ocp.OCP(
            kind=constants.CEPHFILESYSTEM,
            namespace=config.ENV_DATA['cluster_namespace']
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

        create_oc_resource(
            'toolbox.yaml', self.cluster_path, _templating, config.ENV_DATA
        )
        logger.info(f"Waiting {wait_time} seconds...")
        time.sleep(wait_time)
        create_oc_resource(
            "service-monitor.yaml", self.cluster_path, _templating,
            config.ENV_DATA
        )
        create_oc_resource(
            "prometheus-rules.yaml", self.cluster_path, _templating,
            config.ENV_DATA
        )
        logger.info(f"Waiting {wait_time} seconds...")
        time.sleep(wait_time)

        # Create MDS pods for CephFileSystem
        fs_data = templating.load_yaml_to_dict(constants.CEPHFILESYSTEM_YAML)
        fs_data['metadata']['namespace'] = config.ENV_DATA['cluster_namespace']

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

        # Verify health of ceph cluster
        # TODO: move destroy cluster logic to new CLI usage pattern?
        logger.info("Done creating rook resources, waiting for HEALTH_OK")
        assert ceph_health_check(
            namespace=config.ENV_DATA['cluster_namespace']
        )
        # patch gp2 (EBS) storage class as 'non-default'
        logger.info("Patch gp2 storageclass as non-default")
        patch = " '{\"metadata\": {\"annotations\":{\"storageclass.kubernetes.io/is-default-class\":\"false\"}}}' "
        run_cmd(
            f"oc patch storageclass gp2 "
            f"-p {patch} "
            f"--request-timeout=120s"
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
        Implement platform specif add_node in child class
        """
        raise NotImplementedError("add node functionality node implemented")
