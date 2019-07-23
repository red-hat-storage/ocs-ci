"""
This module provides base class for different deployment
platforms like AWS, VMWare, Baremetal etc.
"""

import os
import logging
import time
import json

import pytest
import yaml

from ocs_ci.framework import config
from ocs_ci.utility import templating, system
from ocs_ci.ocs.utils import create_oc_resource, apply_oc_resource
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility.utils import (
    run_cmd, ceph_health_check, is_cluster_running
)
from ocs_ci.ocs import constants, ocp, defaults
from ocs_ci.ocs.resources.ocs import OCS
from tests import helpers


logger = logging.getLogger(name=__file__)


class Deployment(object):
    """
    Base for all deployment platforms
    """
    def __init__(self):
        self.platform = config.ENV_DATA['platform']
        self.ocp_deployment_type = config.ENV_DATA['deployment_type']
        self.cluster_path = config.ENV_DATA['cluster_path']
        self.deploy = config.RUN['cli_params']['deploy']
        self.teardown = config.RUN['cli_params']['teardown']

    def add_volume(self):
        """
        Implement add_volume in child class which is specific to
        platform
        """
        raise NotImplementedError("add_volume functionality not implemented")

    def deploy_cluster(self):
        """
        Implement deploy in child class
        """
        raise NotImplementedError("deploy functionality not implemented")

    def deploy_ocp(self):
        """
        Implement ocp deploy in specific child class
        """
        raise NotImplementedError("deploy_ocp functionality not implemented")

    def deploy_ocp_prereq(self):
        """
        Perform generic prereq before calling openshift-installer
        This method performs all the basic steps necessary before invoking the
        installer
        """
        # Test cluster access and if exist just skip the deployment.
        if is_cluster_running(self.cluster_path):
            logger.info(
                "The OCP installation is skipped because the cluster is "
                "running"
            )
            return False
        elif self.teardown and not self.deploy:
            logger.info(
                f"Attempting teardown of non-accessible cluster: "
                f"{self.cluster_path}"
            )
            return False
        elif not self.deploy and not self.teardown:
            msg = "The given cluster can not be connected to: {}. ".format(
                self.cluster_path)
            msg += (
                f"Provide a valid --cluster-path or use --deploy to "
                f"deploy a new cluster"
            )
            pytest.fail(msg)
        elif not system.is_path_empty(self.cluster_path) and self.deploy:
            msg = "The given cluster path is not empty: {}. ".format(
                self.cluster_path)
            msg += (
                f"Provide an empty --cluster-path and --deploy to deploy "
                f"a new cluster"
            )
            pytest.fail(msg)
        else:
            logger.info(
                f"A testing cluster will be deployed and cluster information "
                f"stored at: %s",
                self.cluster_path
            )

        # Generate install-config from template
        logger.info("Generating install-config")
        pull_secret_path = os.path.join(
            constants.TOP_DIR,
            "data",
            "pull-secret"
        )

        # TODO: check for supported platform and raise the exception if not
        # supported. Currently we support just AWS.

        _templating = templating.Templating()
        install_config_str = _templating.render_template(
            "install-config.yaml.j2", config.ENV_DATA
        )
        # Log the install config *before* adding the pull secret,
        # so we don't leak sensitive data.
        logger.info(f"Install config: \n{install_config_str}")
        # Parse the rendered YAML so that we can manipulate the object directly
        install_config_obj = yaml.safe_load(install_config_str)
        with open(pull_secret_path, "r") as f:
            # Parse, then unparse, the JSON file.
            # We do this for two reasons: to ensure it is well-formatted, and
            # also to ensure it ends up as a single line.
            install_config_obj['pullSecret'] = json.dumps(json.loads(f.read()))
        install_config_str = yaml.safe_dump(install_config_obj)
        install_config = os.path.join(self.cluster_path, "install-config.yaml")
        with open(install_config, "w") as f:
            f.write(install_config_str)

        return True

    def deploy_ocs(self, ):
        """
        Handle OCS deployment, since OCS deployment steps are common to any
        platform, implementing OCS deployment here in base class.
        """
        _templating = templating.Templating()

        try:
            create_oc_resource(
                'common.yaml', self.cluster_path, _templating, config.ENV_DATA
            )
        except CommandFailed:
            # TODO: This can't be a solid reasoning to tell that
            # ocs cluster doesn't exist, find efficient method
            logger.warning("OCS cluster already exists")
            return

        run_cmd(
            f'oc label namespace {config.ENV_DATA["cluster_namespace"]} '
            f'"openshift.io/cluster-monitoring=true"'
        )
        run_cmd(
            f"oc policy add-role-to-user view "
            f"system:serviceaccount:openshift-monitoring:prometheus-k8s "
            f"-n {config.ENV_DATA['cluster_namespace']}"
        )
        apply_oc_resource(
            'csi-nodeplugin-rbac_rbd.yaml',
            self.cluster_path,
            _templating,
            config.ENV_DATA,
            template_dir="ocs-deployment/csi/rbd/"
        )
        apply_oc_resource(
            'csi-provisioner-rbac_rbd.yaml',
            self.cluster_path,
            _templating,
            config.ENV_DATA,
            template_dir="ocs-deployment/csi/rbd/"
        )
        apply_oc_resource(
            'csi-nodeplugin-rbac_cephfs.yaml',
            self.cluster_path,
            _templating,
            config.ENV_DATA,
            template_dir="ocs-deployment/csi/cephfs/"
        )
        apply_oc_resource(
            'csi-provisioner-rbac_cephfs.yaml',
            self.cluster_path,
            _templating,
            config.ENV_DATA,
            template_dir="ocs-deployment/csi/cephfs/"
        )
        # Increased to 15 seconds as 10 is not enough
        # TODO: do the sampler function and check if resource exist
        wait_time = 15
        logger.info(f"Waiting {wait_time} seconds...")
        time.sleep(wait_time)
        create_oc_resource(
            'operator-openshift-with-csi.yaml', self.cluster_path,
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

        POD = ocp.OCP(
            kind=constants.POD, namespace=config.ENV_DATA['cluster_namespace']
        )
        CFS = ocp.OCP(
            kind=constants.CEPHFILESYSTEM,
            namespace=config.ENV_DATA['cluster_namespace']
        )

        # Check for the Running status of Ceph Pods
        run_cmd(
            f"oc wait --for condition=ready pod "
            f"-l app=rook-ceph-agent "
            f"-n {config.ENV_DATA['cluster_namespace']} "
            f"--timeout=120s"
        )
        assert POD.wait_for_resource(
            condition='Running', selector='app=rook-ceph-mon',
            resource_count=3, timeout=600
        )
        assert POD.wait_for_resource(
            condition='Running', selector='app=rook-ceph-mgr',
            timeout=600
        )
        assert POD.wait_for_resource(
            condition='Running', selector='app=rook-ceph-osd',
            resource_count=3, timeout=600
        )

        create_oc_resource(
            'toolbox.yaml', self.cluster_path, _templating, config.ENV_DATA
        )
        logger.info(f"Waiting {wait_time} seconds...")
        time.sleep(wait_time)
        create_oc_resource(
            'storage-manifest.yaml', self.cluster_path, _templating,
            config.ENV_DATA
        )
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
        assert POD.wait_for_resource(
            condition=constants.STATUS_RUNNING, selector='app=rook-ceph-mds',
            resource_count=2, timeout=600
        )

        # Check for CephFilesystem creation in ocp
        cfs_data = CFS.get()
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

    def destroy_cluster(self):
        """
        Implement platform specific destroy method in child class
        """
        raise NotImplementedError("destroy functionality not implemented")

    def add_node(self):
        """
        Implement platform specif add_node in child class
        """
        raise NotImplementedError("add node functionality node implemented")
