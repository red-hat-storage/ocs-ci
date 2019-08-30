"""
This module contains platform specific methods and classes for deployment
on AWS platform
"""
import json
import logging
import os
import sys
import traceback
import shutil
from subprocess import Popen, PIPE

import pytest

from ocs_ci.deployment.ocp import OCPDeployment as BaseOCPDeployment
from ocs_ci.framework import config
from ocs_ci.ocs import defaults, constants
from ocs_ci.ocs.parallel import parallel
from ocs_ci.utility.aws import AWS as AWSUtil
from ocs_ci.ocs.exceptions import SameNamePrefixClusterAlreadyExistsException

from ocs_ci.utility.utils import run_cmd, clone_repo
from .deployment import Deployment
from ocs_ci.ocs import exceptions

logger = logging.getLogger(__name__)


# As of now only IPI
# TODO: Introduce UPI once we have proper doc
__all__ = ['AWSIPI', 'AWSUPI']


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

    def check_cluster_existence(self, cluster_name_prefix):
        """
        Check cluster existence according to cluster name prefix

        Returns:
            bool: True in case a cluster with the same name prefix already exists,
                False otherwise

        """
        instances = self.aws.get_instances_by_name_pattern(cluster_name_prefix)
        instance_objs = [self.aws.get_ec2_instance(ins.get('id')) for ins in instances]
        non_terminated_instances = [
            ins for ins in instance_objs if ins.state
            .get('Code') != constants.INSTANCE_TERMINATED
        ]
        if non_terminated_instances:
            logger.error(
                f"Non terminated EC2 instances with the same name prefix were"
                f" found: {[ins.id for ins in non_terminated_instances]}"
            )
            return True
        return False


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
        if not config.DEPLOYMENT.get('force_deploy_multiple_clusters'):
            cluster_name = config.ENV_DATA['cluster_name']
            prefix = cluster_name.split("-")[0] + '*'
            if self.check_cluster_existence(prefix):
                raise SameNamePrefixClusterAlreadyExistsException(
                    f"Cluster with name prefix {prefix} already exists. "
                    f"Please destroy the existing cluster for a new cluster deployment"
                )
        super(AWSIPI, self).deploy_ocp(log_cli_level)
        if not self.ocs_operator_deployment:
            volume_size = int(
                config.ENV_DATA.get('device_size', defaults.DEVICE_SIZE)
            )
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
                self.aws.detach_and_delete_volume(
                    self.aws.ec2_resource.Volume(volume['id'])
                )
        except Exception:
            logger.error(traceback.format_exc())


class AWSUPI(AWSBase):
    """
    A class to handle AWS UPI specific deployment
    """
    def __init__(self):
        self.name = self.__class__.__name__
        super(AWSUPI, self).__init__()

    class OCPDeployment(BaseOCPDeployment):
        def __init__(self):
            super(AWSUPI.OCPDeployment, self).__init__()
            self.upi_repo_path = os.path.join(
                constants.EXTERNAL_DIR, 'openshift-misc',
            )
            self.upi_unique_path = os.path.join(
                constants.EXTERNAL_DIR,
                f"aws_upi_deploy_{config.RUN['run_id']}"
            )
            try:
                os.mkdir(self.upi_unique_path)
            except OSError:
                logger.error("Already a cluster exists with same runid")
                pytest.fail("Cluster with same runid exists")

            self.upi_script_path = os.path.join(
                self.upi_unique_path,
                f'openshift-misc/v3-launch-templates/functionality-testing'
                f'/aos-4_2/hosts/'
            )

        def deploy_prereq(self):
            """
            Overriding deploy_prereq from parent. Perform all necessary
            prerequisites for AWSUPI here.
            """
            super(AWSUPI.OCPDeployment, self).deploy_prereq()

            # setup necessary env variables
            upi_env_vars = {
                'INSTANCE_NAME_PREFIX': config.ENV_DATA['cluster_name'],
                'AWS_REGION': config.ENV_DATA['region'],
                'rhcos_ami': 'ami-06c85f9d106577272',
                'route53_domain_name': config.ENV_DATA['base_domain'],
                'vm_type_masters': config.ENV_DATA['master_instance_type'],
                'vm_type_workers': config.ENV_DATA['worker_instance_type'],
                'num_workers': "3"
            }
            for key, value in upi_env_vars.items():
                os.environ[key] = value

            # ensure environment variables have been set correctly
            for key, value in upi_env_vars.items():
                assert os.getenv(key) == value, f"{os.getenv(key)} != {value}"

            # git clone repo from openshift-qe repo
            clone_repo(
                constants.OCP_QE_MISC_REPO, self.upi_repo_path
            )

            # copy openshift-misc cloned repos to this run dir
            shutil.copytree(
                self.upi_repo_path,
                os.path.join(self.upi_unique_path, "openshift-misc")
            )

            # create install-dir inside upi_script_path
            os.mkdir(os.path.join(self.upi_script_path, "install-dir"))

            # NOT A CLEAN APPROACH: copy openshift-install and oc binary to
            # script path because upi script expectes it to be present in
            # script dir
            bindir = os.path.join(os.getcwd(), 'bin')
            shutil.copy2(
                os.path.join(bindir, 'openshift-install'),
                self.upi_script_path,
            )
            shutil.copy2(
                os.path.join(bindir, 'oc'), self.upi_script_path
            )
            # copy install-config.yaml from cluster_path to
            # upi_script_path/install-dir
            shutil.copy2(
                os.path.join(self.cluster_path, "install-config.yaml"),
                os.path.join(self.upi_script_path, "install-dir/")
            )
            # Remove current cluster_path dir and make it as symlink to
            # upi_script_path/install-dir/ so that further operations
            # operates transparently
            shutil.rmtree(self.cluster_path, ignore_errors=True)
            os.symlink(
                os.path.join(self.upi_script_path, "install-dir/"),
                self.cluster_path.rstrip('/'),
            )

        def deploy(self, log_cli_level='DEBUG'):
            """
            Exact deployment will happen here

            Args:
                log_cli_level (str): openshift installer's log level
                    (default: "DEBUG")
            """
            logger.info("Deploying OCP cluster")
            logger.info(
                f"Openshift-installer will be using loglevel:{log_cli_level}"
            )

            # Invoke upi_on_aws-install.sh
            cidir = os.getcwd()
            logger.info("Changing CWD")
            try:
                os.chdir(self.upi_script_path)
            except OSError:
                logger.exception(
                    f"Failed to change CWD to {self.upi_script_path} "
                )
            logger.info(f"CWD changed to {self.upi_script_path}")

            with open("./upi_on_aws-install.sh", "r") as fd:
                buf = fd.read()
            data = buf.replace("openshift-qe-upi", "ocs-qe-upi")
            with open("./upi_on_aws-install.sh", "w") as fd:
                fd.write(data)

            sys.path.append(self.upi_script_path)
            proc = Popen(
                [f'{self.upi_script_path}/upi_on_aws-install.sh'],
                stdout=PIPE, stderr=PIPE,
            )
            stdout, stderr = proc.communicate()

            # Change dir back to ocs-ci dir
            os.chdir(cidir)

            if proc.returncode:
                logger.error(stderr)
                raise exceptions.CommandFailed("upi install script failed")
            logger.info(stdout)

    def deploy_ocp(self, log_cli_level='DEBUG'):
        """
        OCP deployment specific to AWS UPI

        Args:
             log_cli_level (str): openshift installer's log level
                (default: 'DEBUG')
        """
        super(AWSUPI, self).deploy_ocp(log_cli_level)
        volume_size = config.ENV_DATA.get('DEFAULT_EBS_VOLUME_SIZE', 100)
        # TODO: Implement add_volume for UPI
        # existing function looks for terraform files
        self.add_volume(volume_size)

    def destroy_cluster(self, log_level="DEBUG"):
        """
        Destroy OCP cluster for AWS UPI

        Args:
            log_level (str): log level for openshift-installer (
                default:DEBUG)
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
                self.aws.detach_and_delete_volume(
                    self.aws.ec2_resource.Volume(volume['id'])
                )
        except Exception:
            logger.error(traceback.format_exc())

        # TODO: Delete the cloudformation stacks
