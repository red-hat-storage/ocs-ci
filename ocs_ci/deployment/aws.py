"""
This module contains platform specific methods and classes for deployment
on AWS platform
"""
import json
import logging
import os
import shutil
import traceback
from subprocess import Popen, PIPE

import boto3
from botocore.exceptions import ClientError
import yaml

from ocs_ci.deployment.ocp import OCPDeployment as BaseOCPDeployment
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs import exceptions
from ocs_ci.ocs.exceptions import SameNamePrefixClusterAlreadyExistsException
from ocs_ci.ocs.parallel import parallel
from ocs_ci.utility.aws import AWS as AWSUtil
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import run_cmd, clone_repo
from .deployment import Deployment
from tests import helpers
from ocs_ci.ocs.resources import pod
from ocs_ci.utility import utils
from ocs_ci.utility import templating
from ocs_ci.ocs import ocp

logger = logging.getLogger(__name__)


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
        cluster_id = get_infra_id(self.cluster_path)
        worker_pattern = f'{cluster_id}-worker*'
        logger.info(f'Worker pattern: {worker_pattern}')
        self.create_ebs_volumes(worker_pattern, size)

    def host_network_update(self):
        """
        Update security group rules for HostNetwork
        """
        cluster_id = get_infra_id(self.cluster_path)
        worker_pattern = f'{cluster_id}-worker*'
        worker_instances = self.aws.get_instances_by_name_pattern(
            worker_pattern
        )
        security_groups = worker_instances[0]['security_groups']
        sg_id = security_groups[0]['GroupId']
        security_group = self.aws.ec2_resource.SecurityGroup(sg_id)
        # The ports are not 100 % clear yet. Taken from doc:
        # https://docs.google.com/document/d/1c23ooTkW7cdbHNRbCTztprVU6leDqJxcvFZ1ZvK2qtU/edit#
        security_group.authorize_ingress(
            DryRun=False,
            IpPermissions=[
                {
                    'FromPort': 6800,
                    'ToPort': 7300,
                    'IpProtocol': 'tcp',
                    'UserIdGroupPairs': [
                        {
                            'Description': 'Ceph OSDs',
                            'GroupId': sg_id,
                        },
                    ],
                },
                {
                    'FromPort': 3300,
                    'ToPort': 3300,
                    'IpProtocol': 'tcp',
                    'UserIdGroupPairs': [
                        {
                            'Description': 'Ceph MONs rule1',
                            'GroupId': sg_id,
                        },
                    ],
                },
                {
                    'FromPort': 6789,
                    'ToPort': 6789,
                    'IpProtocol': 'tcp',
                    'UserIdGroupPairs': [
                        {
                            'Description': 'Ceph MONs rule2',
                            'GroupId': sg_id,
                        },
                    ],
                },
                {
                    'FromPort': 8443,
                    'ToPort': 8443,
                    'IpProtocol': 'tcp',
                    'UserIdGroupPairs': [
                        {
                            'Description': 'Ceph Dashboard rule1',
                            'GroupId': sg_id,
                        },
                    ],
                },
                {
                    'FromPort': 8080,
                    'ToPort': 8080,
                    'IpProtocol': 'tcp',
                    'UserIdGroupPairs': [
                        {
                            'Description': 'Ceph Dashboard rule2',
                            'GroupId': sg_id,
                        },
                    ],
                },
            ]
        )

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

    def destroy_volumes(self):
        try:
            # Retrieve cluster name and AWS region from metadata
            cluster_name = self.ocp_deployment.metadata.get('clusterName')
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
                f"--log-level {log_cli_level}",
                timeout=3600
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
        if config.DEPLOYMENT.get('host_network'):
            self.host_network_update()

    def destroy_cluster(self, log_level="DEBUG"):
        """
        Destroy OCP cluster specific to AWS IPI

        Args:
            log_level (str): log level openshift-installer (default: DEBUG)
        """
        super(AWSIPI, self).destroy_cluster(log_level)
        self.destroy_volumes()


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
            upi_repo_name = f'openshift-misc-{config.RUN["run_id"]}'
            self.upi_repo_path = os.path.join(
                constants.EXTERNAL_DIR, upi_repo_name,
            )

            self.upi_script_path = os.path.join(
                self.upi_repo_path,
                'v3-launch-templates/functionality-testing'
                '/aos-4_2/hosts/'
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
                'rhcos_ami': config.ENV_DATA.get('rhcos_ami'),
                'route53_domain_name': config.ENV_DATA['base_domain'],
                'vm_type_masters': config.ENV_DATA['master_instance_type'],
                'vm_type_workers': config.ENV_DATA['worker_instance_type'],
                'num_workers': str(config.ENV_DATA['worker_replicas']),
                'AVAILABILITY_ZONE_COUNT': str(config.ENV_DATA.get(
                    'availability_zone_count', ''
                ))
            }
            for key, value in upi_env_vars.items():
                if value:
                    os.environ[key] = value

            # ensure environment variables have been set correctly
            for key, value in upi_env_vars.items():
                if value:
                    assert os.getenv(key) == value

            # git clone repo from openshift-qe repo
            clone_repo(
                constants.OCP_QE_MISC_REPO, self.upi_repo_path
            )

            # Sym link install-dir to cluster_path
            install_dir = os.path.join(self.upi_script_path, "install-dir")
            absolute_cluster_path = os.path.abspath(self.cluster_path)
            logger.info(
                "Sym linking %s to %s", install_dir, absolute_cluster_path
            )
            os.symlink(absolute_cluster_path, install_dir)

            # NOT A CLEAN APPROACH: copy openshift-install and oc binary to
            # script path because upi script expects it to be present in
            # script dir
            bindir = os.path.abspath(os.path.expanduser(config.RUN['bin_dir']))
            shutil.copy2(
                os.path.join(bindir, 'openshift-install'),
                self.upi_script_path,
            )
            shutil.copy2(
                os.path.join(bindir, 'oc'), self.upi_script_path
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

            # Invoke UPI on AWS install script
            cidir = os.getcwd()
            logger.info("Changing CWD")
            try:
                os.chdir(self.upi_script_path)
            except OSError:
                logger.exception(
                    f"Failed to change CWD to {self.upi_script_path} "
                )
            logger.info(f"CWD changed to {self.upi_script_path}")

            with open(f"./{constants.UPI_INSTALL_SCRIPT}", "r") as fd:
                buf = fd.read()
            data = buf.replace("openshift-qe-upi-1", "ocs-qe-upi")
            with open(f"./{constants.UPI_INSTALL_SCRIPT}", "w") as fd:
                fd.write(data)

            logger.info("Executing UPI install script")
            proc = Popen(
                [os.path.join(
                    self.upi_script_path, constants.UPI_INSTALL_SCRIPT
                )],
                stdout=PIPE, stderr=PIPE
            )
            stdout, stderr = proc.communicate()

            # Change dir back to ocs-ci dir
            os.chdir(cidir)

            if proc.returncode:
                logger.error(stderr)
                raise exceptions.CommandFailed("upi install script failed")
            logger.info(stdout)

            self.test_cluster()

            # Delete openshift-misc repository
            logger.info(
                "Removing openshift-misc directory located at %s",
                self.upi_repo_path
            )
            shutil.rmtree(self.upi_repo_path)

    def deploy_ocp(self, log_cli_level='DEBUG'):
        """
        OCP deployment specific to AWS UPI

        Args:
             log_cli_level (str): openshift installer's log level
                (default: 'DEBUG')
        """
        super(AWSUPI, self).deploy_ocp(log_cli_level)
        if config.DEPLOYMENT.get('host_network'):
            self.host_network_update()

    def destroy_cluster(self, log_level="DEBUG"):
        """
        Destroy OCP cluster for AWS UPI

        Args:
            log_level (str): log level for openshift-installer (
                default:DEBUG)
        """
        cluster_name = get_cluster_name(self.cluster_path)
        # Destroy extra volumes
        self.destroy_volumes()

        # Create cloudformation client
        cf = boto3.client('cloudformation')

        # Delete master, bootstrap, security group, and worker stacks
        suffixes = ['ma', 'bs', 'sg']
        # TODO: read in num_workers in a better way
        num_workers = int(os.environ.get('num_workers', 3))
        for i in range(num_workers - 1, -1, -1):
            suffixes.insert(0, f'no{i}')
        stack_names = [f'{cluster_name}-{suffix}' for suffix in suffixes]
        for stack_name in stack_names:
            logger.info("Destroying stack: %s", stack_name)
            cf.delete_stack(StackName=stack_name)
            verify_stack_deleted(stack_name)

        # Call openshift-installer destroy cluster
        super(AWSUPI, self).destroy_cluster(log_level)

        # Delete inf and vpc stacks
        suffixes = ['inf', 'vpc']
        stack_names = [f'{cluster_name}-{suffix}' for suffix in suffixes]
        for stack_name in stack_names:
            logger.info("Destroying stack: %s", stack_name)
            cf.delete_stack(StackName=stack_name)
            verify_stack_deleted(stack_name)


class AWSUPIRHELWORKERS(AWSUPI):
    """
    A class to handle AWS UPI with RHEL worker nodes
    """
    def __init__(self):
        self.name = self.__class__.__name__
        super(AWSUPIRHELWORKERS, self).__init__()
        self.worker_vpc = None
        self.worker_iam_role = None
        self.worker_subnet = None
        self.worker_security_group = None
        self.worker_tag = None
        self.cf = None
        self.cluster_name = get_cluster_name(self.cluster_path)
        # A dict for holding instance Name to instance object mapping
        self.rhel_worker_list = {}
        self.rhel_worker_user = "ec2-user"

    def deploy_cluster(self, log_cli_level='DEBUG'):
        """
        Deploy cluster overridden function from parent

        Args:
            log_cli_level (str): log level
        """
        if config.ENV_DATA['skip_ocp_deployment']:
            super(AWSUPIRHELWORKERS, self).deploy_cluster(log_cli_level)
        else:
            prev_ocs_flag = config.ENV_DATA['skip_ocs_deployment']
            # deploy only OCP
            config.ENV_DATA['skip_ocs_deployment'] = 'true'
            #super(AWSUPIRHELWORKERS, self).deploy_cluster(log_cli_level)
            self.add_rhel_workers()
            config.ENV_DATA['skip_ocs_deployment'] = prev_ocs_flag
            config.ENV_DATA['skip_ocp_deployment'] = 'true'
            super(AWSUPIRHELWORKERS, self).deploy_cluster(log_cli_level)
            config.ENV_DATA['skip_ocp_deployment'] = 'false'

    def get_worker_resource_id(self, resource):
        """
        Get the resource ID

        Args:
            resource (dict): a dictionary of stack resource

        Returns:
            resource_id (str): ID of worker stack resource
        """
        return resource['StackResourceSummaries'][0]['PhysicalResourceId']

    def gather_worker_data(self):
        """
        Gather various info like vpc, iam role, subnet,security group,
        cluster tag from existing RHCOS workers
        """
        suffix = 'no1'
        self.cf = boto3.client('cloudformation')
        stack_name = f'{self.cluster_name}-{suffix}'
        resource = self.cf.list_stack_resources(StackName=stack_name)
        worker_id = self.get_worker_resource_id(resource)
        ec2 = boto3.resource('ec2')
        worker_instance = ec2.Instance(worker_id)

        self.worker_vpc = worker_instance.vpc.id
        self.worker_subnet = worker_instance.subnet.id
        self.worker_security_group = worker_instance.security_groups
        self.worker_iam_role = worker_instance.iam_instance_profile
        self.worker_tag = self.get_kube_tag(worker_instance.tags)
        del self.worker_iam_role['Id']

    def get_kube_tag(self, tags):
        for each in tags:
            if 'kubernetes' in each['Key']:
                return each['Key'], each['Value']

    def create_rhel_instance(self):
        num_workers = int(os.environ.get('num_workers', 3))
        for i in range(num_workers):
            client = boto3.client('ec2', region_name=config.ENV_DATA['region'])
            response = client.run_instances(
                BlockDeviceMappings=[
                    {
                        'DeviceName': '/dev/xvda',
                        'Ebs': {

                            'DeleteOnTermination': True,
                            'VolumeSize': 50,
                            'VolumeType': 'gp2'
                        },
                    },
                ],
                ImageId=config.ENV_DATA['rhel_worker_ami'],
                SubnetId=self.worker_subnet,
                InstanceType=config.ENV_DATA['RHEL_INSTANCE_TYPE'],
                MaxCount=num_workers,
                MinCount=num_workers,
                Monitoring={
                    'Enabled': False
                },
                SecurityGroupIds=[
                    self.worker_security_group[0]['GroupId'],
                ],
                KeyName='openshift-dev'
            )
            inst_id = response['Instances'][0]['InstanceId']
            worker_ec2 = boto3.resource('ec2')
            worker_instance = worker_ec2.Instance(inst_id)
            worker_instance.wait_until_running()
            worker_name = f'{self.cluster_name}-RHEL-WORKER-{i}'
            self.rhel_worker_list[worker_name] = worker_instance
            worker_ec2.create_tags(
                Resources=[inst_id],
                Tags=[
                    {'Key': 'Name', 'Value': f'{worker_name}'},
                    {'Key': self.worker_tag[0], 'Value': self.worker_tag[1]}
                ]
            )
            logging.info(self.worker_iam_role)
            client.associate_iam_instance_profile(
                IamInstanceProfile=self.worker_iam_role,
                InstanceId=inst_id,
            )

    def run_ansible_playbook(self):
        """
        Bring up a helper pod (RHEL) to run openshift-ansible
        playbook
        """
        rhel_pod_name = "rhel-ansible"
        rhel_pod_obj = utils.create_rhelpod(
            constants.DEFAULT_NAMESPACE, rhel_pod_name
        )

        # copy openshift-dev.pem to RHEL ansible pod
        pem_src_path = "~/.ssh/openshift-dev.pem"
        pem_dst_path = "/openshift-dev.pem"
        pod.upload(rhel_pod_obj.name, pem_src_path, pem_dst_path)
        repo_dst_path = "/etc/yum.repos.d/"
        repo_file = os.path.basename(constants.OCP4_2_REPO)
        pod.upload(
            rhel_pod_obj.name, constants.OCP4_2_REPO, repo_dst_path
        )
        # copy the .pem file for our internal repo on all nodes
        # including ansible pod
        # get it from URL
        mirror_pem_file = "ops-mirror.pem"
        tmp_path = f"/tmp/{mirror_pem_file}"
        utils.download_file(
            constants.INTERNAL_MIRROR_PEM_URL, tmp_path
        )
        dst = "/etc/pki/ca-trust/source/anchors/"
        pod.upload(rhel_pod_obj.name, tmp_path, dst)
        # Install scp on pod
        rhel_pod_obj.install_packages("openssh-clients")
        # distribute repo file to all RHEL workers
        hosts = [inst.private_dns_name for node, inst in
                 self.rhel_worker_list.items()]
        for host in hosts:
            rhel_pod_obj.copy_to_server(
                host, pem_dst_path, f'{repo_dst_path}/{repo_file}',
                f'/tmp/{repo_file}', user=self.rhel_worker_user
            )
            rhel_pod_obj.exec_cmd_on_node(
                host, pem_dst_path,
                f'sudo mv /tmp/{repo_file} {repo_dst_path}'
            )
#            rhel_pod_obj.exec_cmd_on_pod(f"chmod 777 {repo_dst_path}/{
            #            repo_file}")
#            rhel_pod_obj.exec_cmd_on_node(
#                host, pem_dst_path, f'sudo chmod 777 {dst}'
#            )
            rhel_pod_obj.copy_to_server(
                host, pem_dst_path, f'{dst}/{mirror_pem_file}',
                f'/tmp/{mirror_pem_file}',
                user=self.rhel_worker_user,
            )
            rhel_pod_obj.exec_cmd_on_node(
                host, pem_dst_path,
                f'sudo mv /tmp/{mirror_pem_file} {dst}'
            )
        # copy kubeconfig to pod
        kubeconfig = os.path.join(
            self.cluster_path, config.RUN.get('kubeconfig_location')
        )
        pod.upload(rhel_pod_obj.name, kubeconfig, "/")
        pull_secret_path = os.path.join(
            constants.TOP_DIR,
            "data",
            "pull-secret"
        )
        pod.upload(rhel_pod_obj.name, pull_secret_path, "/tmp/")
        host_file = self.build_ansible_inventory(hosts)
        pod.upload(rhel_pod_obj.name, host_file, "/")
        # install pod packages
        rhel_pod_obj.install_packages(constants.RHEL_POD_PACKAGES)
        # run ansible
        openshift_ansible_path = "/usr/share/ansible/openshift-ansible"
        cmd = (
            f"ansible-playbook -i /hosts --private-key={pem_dst_path} "
            f"{openshift_ansible_path}/playbooks/scaleup.yml"
        )

        rhel_pod_obj.exec_cmd_on_pod(cmd)
        self.verify_nodes_added(hosts)
        # remove rhcos workers
        self.remove_rhcos_workers()

    def remove_rhcos_workers(self):
        """
        After RHEL workers are added remove rhcos workers from the cluster

        Raise:
            FailedToRemoveNodeException: if rhcos removal is failed
        """
        rhcos_workers = self.get_rhcos_workers()
        for node in rhcos_workers:
            cordon = f"oc adm cordon {node}"
            run_cmd(cordon)
            drain = (
                f"oc adm drain {node} --force --delete-local-data "
                f"--ignore-daemonsets"
            )
            run_cmd(drain)
            delete = f"oc delete nodes {node}"
            run_cmd(delete)
        if len(self.get_rhcos_workers()):
            raise exceptions.FailedToAddNodeException()

    def get_rhcos_workers(self):
        """
        Returns a list of rhcos worker names

        rhcos_workers (list): list of rhcos worker nodes
        """
        rhcos_workers = []
        ocp_obj = ocp.OCP(kind='node')
        node_info = ocp_obj.get()
        for each in node_info['items']:
            labels = each['metadata']['labels']
            if(
                labels['node.openshift.io/os_id'] == 'rhcos'
                and 'node-role.kubernetes.io/worker' in labels
            ):
                for every in each['status']['addresses']:
                    if every['type'] == 'Hostname':
                        rhcos_workers.append(every['address'])
        return rhcos_workers

    def verify_nodes_added(self, hosts):
        """
        Verify RHEL workers are added

        Args:
             hosts (list): list of aws private hostnames

        Returns:
            diff_list (list): hosts which are present in 'hosts' list but
            not in oc get nodes. If diff_list is null then all the nodes are
            added
        """
        ocp_obj = ocp.OCP(kind='node')
        node_info = ocp_obj.get()
        for host in hosts:
            for entry in node_info['items']:
                for each in entry['status']['addresses']:
                    if each['type'] == 'Hostname':
                        if each['address'] in hosts:
                            if not self.get_ready_status(each):
                                raise exceptions.FailedToAddNodeException()

    def get_ready_status(self, node_ent):
        for cond in node_ent['status']['conditions']:
            if cond['type'] == 'Ready':
                if not cond['status'] == "True":
                    raise exceptions.FailedToAddNodeException

    def build_ansible_inventory(self, hosts):
        """
        Build the ansible hosts file from jinja template

        Args:
            hosts (list): list of private host names

        Returns:
            path (str): path of the ansible file created

        """
        _templating = templating.Templating()
        ansible_host_file = dict()
        ansible_host_file['ansible_user'] = 'ec2-user'
        ansible_host_file['ansible_become'] = 'True'
        ansible_host_file['ansible_python_interpreter'] = 'auto_silent'
        ansible_host_file['pod_kubeconfig'] = '/kubeconfig'
        ansible_host_file['pod_pull_secret'] = '/tmp/pull-secret'
        ansible_host_file['rhel_worker_nodes'] = hosts


        logging.info(ansible_host_file)
        data = _templating.render_template(
            constants.ANSIBLE_INVENTORY_YAML,
            ansible_host_file,
        )
        logging.debug("Ansible hosts file:", data)
        host_file_path = "/tmp/hosts"
        with open(host_file_path, 'w') as f:
            f.write(data)
        return host_file_path

    def copy_repo_file(self, pod_obj):
        repo_path = "openshift.repo"
        dst_path = "/etc/yum.repos.d/"
        helpers.copy_file_to_pod(pod_obj, repo_path, dst_path)

    def add_rhel_workers(self):
        """
        Add RHEL worker nodes to the existing cluster
        """
        self.gather_worker_data()
        self.create_rhel_instance()
        self.run_ansible_playbook()

    def destroy_cluster(self, log_level="DEBUG"):
        super(AWSUPIRHELWORKERS, self).destroy_cluster()


def get_infra_id(cluster_path):
    """
    Get infraID from metadata.json in given cluster_path

    Args:
        cluster_path: path to cluster install directory

    Returns:
        str: metadata.json['infraID']

    """
    metadata_file = os.path.join(cluster_path, "metadata.json")
    with open(metadata_file) as f:
        metadata = json.load(f)
    return metadata["infraID"]


def get_cluster_name(cluster_path):
    """
    Get clusterName from metadata.json in given cluster_path

    Args:
        cluster_path: path to cluster install directory

    Returns:
        str: metadata.json['clusterName']

    """
    metadata_file = os.path.join(cluster_path, "metadata.json")
    with open(metadata_file) as f:
        metadata = json.load(f)
    return metadata["clusterName"]


class StackStatusError(Exception):
    pass


@retry(StackStatusError, tries=12, delay=30, backoff=1)
def verify_stack_deleted(stack_name):
    try:
        cf = boto3.client('cloudformation')
        result = cf.describe_stacks(StackName=stack_name)
        stacks = result['Stacks']
        for stack in stacks:
            status = stack['StackStatus']
            raise StackStatusError(
                f'{stack_name} not deleted yet, current status: {status}.'
            )
    except ClientError as e:
        assert f"Stack with id {stack_name} does not exist" in str(e)
        logger.info(
            "Received expected ClientError, stack successfully deleted"
        )
