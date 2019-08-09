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
from ocs_ci.ocs.utils import create_oc_resource
from ocs_ci.utility.utils import (
    run_cmd, ceph_health_check,
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
        if self.teardown and not self.deploy:
            msg = f"Attempting teardown of non-accessible cluster: "
            msg += f"{self.cluster_path}"
            pytest.fail(msg)
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
