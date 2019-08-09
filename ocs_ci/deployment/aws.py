"""
This module contains platform specific methods and classes for deployment
on AWS platform
"""
import os
import logging
import json
import traceback

from .deployment import Deployment
from ocs_ci.deployment.ocp import OCPDeployment as BaseOCPDeployment
from ocs_ci.utility.utils import run_cmd
from ocs_ci.framework import config
from ocs_ci.ocs.parallel import parallel
from ocs_ci.utility.aws import AWS as AWSUtil


logger = logging.getLogger(__name__)


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
        tfvars_file = "terraform.tfvars.json"
        with open(os.path.join(self.cluster_path, tfvars_file)) as f:
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

    class OCPDeployment(BaseOCPDeployment):
        def __init__(self):
            super(AWSIPI.OCPDeployment, self).__init__()

        def deploy(self, log_cli_level='DEBUG'):
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
            self.test_cluster()

    def deploy_ocp(self, log_cli_level='DEBUG'):
        """
        Deployment specific to OCP cluster on this platform

        Args:
            log_cli_level (str): openshift installer's log level
                (default: "DEBUG")
        """
        super(AWSIPI, self).deploy_ocp(log_cli_level)
        volume_size = config.ENV_DATA.get('DEFAULT_EBS_VOLUME_SIZE', 100)
        self.add_volume(volume_size)

    def destroy_cluster(self, log_level="DEBUG"):
        """
        Destroy OCP cluster specific to AWS IPI

        Args:
            log_level (str): log level openshift-installer (default: DEBUG)
        """
        super(AWSIPI, self).destroy_cluster(log_level)

        try:
            # Retrieve cluster name and AWS region from metadata
            cluster_name = self.ocp_deployment.metadata.get("clusterName")
            # Find and delete volumes
            volume_pattern = f"{cluster_name}*"
            logger.debug(f"Finding volumes with pattern: {volume_pattern}")
            volumes = self.aws.get_volumes_by_name_pattern(volume_pattern)
            logger.debug(f"Found volumes: \n {volumes}")
            for volume in volumes:
                self.aws.detach_and_delete_volume(volume)
        except Exception:
            logger.error(traceback.format_exc())
