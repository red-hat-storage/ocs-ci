"""
This module contains platform specific methods and classes for deployment
on AWS platform
"""
import os
import logging
import pytest
import json
import traceback

from .deployment import Deployment
from ocs_ci.utility.utils import run_cmd, is_cluster_running
from ocs_ci.ocs.openshift_ops import OCP
from ocs_ci.framework import config
from ocs_ci.ocs.parallel import parallel
from ocs_ci.utility.aws import AWS as AWSUtil
from ocs_ci.utility import utils


logger = logging.getLogger(name=__file__)


# As of now only IPI
# TODO: Introduce UPI once we have proper doc
__all__ = ['AWSIPI']


class AWSBase(Deployment):
    def __init__(self):
        """
        This would be base for both IPI and UPI deployment
        """
        super(AWSBase, self).__init__()
        self.region = config.ENV_DATA['region']
        self.aws = AWSUtil(self.region)

    def create_ebs_volumes(self, worker_pattern, size=100):
        """
        Add new ebs volumes to the workers

        Args:
            worker_pattern (str):  Worker name pattern e.g.:
                cluster-55jx2-worker*
            size (int): Size in GB (default: 100)
        """
        worker_instances = self.aws.get_instances_by_name_pattern(
            worker_pattern
        )
        with parallel() as p:
            for worker in worker_instances:
                logger.info(
                    f"Creating and attaching {size} GB "
                    f"volume to {worker['name']}"
                )
                p.spawn(
                    self.aws.create_volume_and_attach,
                    availability_zone=worker['avz'],
                    instance_id=worker['id'],
                    name=f"{worker['name']}_extra_volume",
                    size=size,
                )

    def add_volume(self, size=100):
        """
        Add a new volume to all the workers

        Args:
            size (int): Size of volume in GB (default: 100)
        """
        with open(os.path.join(self.cluster_path, "terraform.tfvars")) as f:
            tfvars = json.load(f)

        cluster_id = tfvars['cluster_id']
        worker_pattern = f'{cluster_id}-worker*'
        logger.info(f'Worker pattern: {worker_pattern}')
        self.create_ebs_volumes(worker_pattern, size)

    def add_node(self):
        # TODO: Implement later
        super(AWSBase, self).add_node()


class AWSIPI(AWSBase):
    """
    A class to handle AWS IPI specific deployment
    """
    def __init__(self):
        self.name = self.__class__.__name__
        super(AWSIPI, self).__init__()
        self.installer = utils.get_openshift_installer(
            config.DEPLOYMENT['installer_version']
        )

    def deploy_cluster(self, log_cli_level='DEBUG'):
        """
        Deployment method specific to AWS IPI
        We are handling both OCP and OCS deployment here based on flags

        Args:
            log_cli_level (str): log level for installer (default: DEBUG)
        """
        if not config.ENV_DATA['skip_ocp_deployment']:
            if is_cluster_running(self.cluster_path):
                logger.warning(
                    "OCP cluster is already running, skipping installation"
                )
            elif self.deploy_ocp_prereq():
                self.deploy_ocp(log_cli_level)
            else:
                return

        if not config.ENV_DATA['skip_ocs_deployment']:
            self.deploy_ocs()
        else:
            logger.warning("OCS deployment will be skipped")

    def deploy_ocp(self, log_cli_level='DEBUG'):
        """
        Deployment specific to OCP cluster on this platform

        Args:
            log_cli_level (str): openshift installer's log level
                (default: "DEBUG")
        """
        logger.info("Deploying OCP cluster")
        logger.info(
            f"Openshift-installer will be using loglevel:{log_cli_level}"
        )
        run_cmd(
            f"{self.installer} create cluster "
            f"--dir {self.cluster_path} "
            f"--log-level {log_cli_level}"
        )
        # Test cluster access
        if not OCP.set_kubeconfig(
            os.path.join(
                self.cluster_path, config.RUN.get('kubeconfig_location'),
            )
        ):
            pytest.fail("Cluster is not available!")

        volume_size = config.ENV_DATA.get('DEFAULT_EBS_VOLUME_SIZE', 100)
        self.add_volume(volume_size)

    def destroy_cluster(self, log_level="DEBUG"):
        """
        Destroy OCP cluster specific to AWS IPI

        Args:
            log_level (str): log level openshift-installer (default: DEBUG)
        """

        logger.info("Destroying the cluster")

        destroy_cmd = (
            f"{self.installer} destroy cluster "
            f"--dir {self.cluster_path} "
            f"--log-level {log_level}"
        )

        try:

            # Retrieve cluster name and AWS region from metadata
            metadata_file = os.path.join(self.cluster_path, "metadata.json")
            with open(metadata_file) as f:
                metadata = json.loads(f.read())
            cluster_name = metadata.get("clusterName")

            # Execute destroy cluster using OpenShift installer
            logger.info(f"Destroying cluster defined in {self.cluster_path}")
            run_cmd(destroy_cmd)

            # Find and delete volumes
            volume_pattern = f"{cluster_name}*"
            logger.debug(f"Finding volumes with pattern: {volume_pattern}")
            volumes = self.aws.get_volumes_by_name_pattern(volume_pattern)
            logger.debug(f"Found volumes: \n {volumes}")
            for volume in volumes:
                self.aws.detach_and_delete_volume(volume)

            # Remove installer
            utils.delete_file(self.installer)

        except Exception:
            logger.error(traceback.format_exc())
