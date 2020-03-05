"""
This module contains classes which provides support
for addition, maintenance of nodes
"""
import os
import logging

import boto3
import pytest


from ocs_ci.framework import config
from ocs_ci.ocs import ocp, constants, exceptions
from .node import AWSNode, VMWareNode
from ocs_ci.ocs.resources import pod
from ocs_ci.utility.utils import (
    get_infra_id, create_rhelpod, get_ocp_version,
    TimeoutSampler
)
from ocs_ci.utility.retry import retry
from ocs_ci.utility import templating

logger = logging.getLogger(__name__)

node_cls_map = {'AWS': AWSNode, 'vmware': VMWareNode}


class NodeUtils(object):
    def __init__(self):
        self.cluster_name = config.ENV_DATA['cluster_name']
        self.platform = config.ENV_DATA['platform']
        self.cluster_path = config.ENV_DATA['cluster_path']
        self.cluster_id = get_infra_id(self.cluster_path)
        if 'AWS' in self.platform:
            self.region = config.ENV_DATA['region']

    def prepare_nodes(self, node_config, node_t=None, num_nodes=1):
        """
        Prepare multiple node instances

        Args:
            node_config (dict): node config
            node_t (str): type of node eg: RHEL/RHCOS etc
            num_nodes (int): Number of nodes to be created

        Returns:
            list: of node objects

        """
        node = node_cls_map[config.ENV_DATA['platform']]
        nlist = []
        num_workers = int(os.environ.get('num_workers', 3))
        for i in num_nodes:
            if not node_config['zone_worker_id']:
                node_config['zone_worker_id'] = (i % num_workers)
            nlist[i] = node(node_config, node_t)
            nlist[i].prepare_node()
        return nlist

    def attach_nodes_to_cluster(self, node_list):
        """
        Prepare aws nodes with given node_config

        Args:
            node_list(list): of Node objects
        """
        if not self.same_node_type(node_list):
            pytest.fail("All the nodes should be of same type")
        if self.platform == 'AWS':
            self.attach_nodes_to_aws_cluster(node_list)
        elif self.platform == 'vmware':
            self.attach_nodes_to_vmware_cluster(node_list)

    def attach_nodes_to_aws_cluster(self, node_list):
        """
        Attaches nodes to aws cluster, this function will take care
        of calling appropriate deployment specific functions

        Args:
            node_list (list): of Node objects
        """
        if node_list[0].deployment_t == 'upi':
            self.attach_nodes_to_aws_upi_cluster(node_list)

    def attach_nodes_to_aws_upi_cluster(self, node_list):
        """
        Attaches the node aws upi cluster, this function will take
        care of calling appropriate node type specific functions

        Args:
            node_list (list): of Node objects
        """
        if node_list[0].node_t == 'RHEL':
            self.attach_rhel_nodes_to_aws_upi_cluster(node_list)
        elif node_list[0].node_t == 'RHCOS':
            pass

    def attach_rhel_nodes_to_aws_upi_cluster(self, node_list):
        """
        Attaches the RHEL nodes to aws upi cluster,also
        Brings up a helper pod (RHEL) to run openshift-ansible
        playbook

        Args:
            node_list(list): of Node objects
        """
        rhel_pod_name = "rhel-ansible"
        rhel_pod_obj = create_rhelpod(
            constants.DEFAULT_NAMESPACE, rhel_pod_name
        )
        timeout = 4000  # For ansible-playbook

        # copy openshift-dev.pem to RHEL ansible pod
        pem_src_path = "~/.ssh/openshift-dev.pem"
        pem_dst_path = "/openshift-dev.pem"
        pod.upload(rhel_pod_obj.name, pem_src_path, pem_dst_path)
        repo_dst_path = constants.YUM_REPOS_PATH
        repo = os.path.join(
            constants.REPO_DIR, f"ocp_{get_ocp_version('_')}.repo"
        )
        assert os.path.exists(repo), f"Required repo file {repo} doesn't exist!"
        repo_file = os.path.basename(repo)
        pod.upload(
            rhel_pod_obj.name, repo, repo_dst_path
        )
        # copy the .pem file for our internal repo on all nodes
        # including ansible pod
        # get it from URL
        mirror_pem_file_path = os.path.join(
            constants.DATA_DIR,
            constants.INTERNAL_MIRROR_PEM_FILE
        )
        dst = constants.PEM_PATH
        pod.upload(rhel_pod_obj.name, mirror_pem_file_path, dst)
        # Install scp on pod
        rhel_pod_obj.install_packages("openssh-clients")
        # distribute repo file to all RHEL workers
        hosts = [
            inst.private_dns_name for inst in
            node_list
        ]
        # Check whether every host is acceptin ssh connections
        for host in hosts:
            self.check_connection(rhel_pod_obj, host, pem_dst_path)

        for host in hosts:
            disable = "sudo yum-config-manager --disable *"
            rhel_pod_obj.exec_cmd_on_node(
                host, pem_dst_path, disable, user=constants.EC2_USER
            )
            rhel_pod_obj.copy_to_server(
                host, pem_dst_path,
                os.path.join(repo_dst_path, repo_file),
                os.path.join('/tmp', repo_file),
                user=constants.EC2_USER
            )
            rhel_pod_obj.exec_cmd_on_node(
                host, pem_dst_path,
                f'sudo mv {os.path.join("/tmp", repo_file)} {repo_dst_path}',
                user=constants.EC2_USER
            )
            rhel_pod_obj.copy_to_server(
                host, pem_dst_path,
                os.path.join(dst, constants.INTERNAL_MIRROR_PEM_FILE),
                os.path.join('/tmp', constants.INTERNAL_MIRROR_PEM_FILE),
                user=constants.EC2_USER,
            )
            cmd = (
                f'sudo mv '
                f'{os.path.join("/tmp/", constants.INTERNAL_MIRROR_PEM_FILE)} '
                f'{dst}'
            )
            rhel_pod_obj.exec_cmd_on_node(
                host, pem_dst_path,
                cmd, user=constants.EC2_USER
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
        openshift_ansible_path = '/usr/share/ansible/openshift-ansible'
        cmd = (
            f"ansible-playbook -i /hosts --private-key={pem_dst_path} "
            f"{os.path.join(openshift_ansible_path, 'playbooks/scaleup.yml')}"
        )

        rhel_pod_obj.exec_cmd_on_pod(
            cmd, out_yaml_format=False, timeout=timeout
        )
        self.verify_nodes_added(hosts)

    def verify_nodes_added(self, hosts):
        """
        Verify RHEL workers are added

        Args:
             hosts (list): list of aws private hostnames

        Raises:
            FailedToAddNodeException: if node addition failed

        """
        timeout = 600
        ocp_obj = ocp.OCP(kind='node')
        node_info = ocp_obj.get()
        for i in range(len(hosts)):
            for entry in node_info['items']:
                for each in entry['status']['addresses']:
                    if each['type'] == 'Hostname':
                        if each['address'] in hosts:
                            logging.info(
                                f"Checking status for {each['address']}"
                            )
                            sample = TimeoutSampler(
                                timeout, 3,
                                self.get_ready_status, entry
                            )
                            try:
                                assert sample.wait_for_func_status(result=True)
                            except AssertionError:
                                raise exceptions.FailedToAddNodeException(
                                    "Failed to add RHEL node"
                                )

    def get_ready_status(self, node_ent):
        """
        Get the node 'Ready' status

        Args:
            node_ent (dict): Node info which includes details

        Returns:
            bool: True if node is Ready else False

        """
        for cond in node_ent['status']['conditions']:
            if cond['type'] == 'Ready':
                if not cond['status'] == "True":
                    return False
                else:
                    return True

    def build_ansible_inventory(self, hosts):
        """
        Build the ansible hosts file from jinja template

        Args:
            hosts (list): list of private host names

        Returns:
            str: path of the ansible file created

        """
        _templating = templating.Templating()
        ansible_host_file = dict()
        ansible_host_file['ansible_user'] = constants.EC2_USER
        ansible_host_file['ansible_become'] = 'True'
        ansible_host_file['pod_kubeconfig'] = '/kubeconfig'
        ansible_host_file['pod_pull_secret'] = '/tmp/pull-secret'
        ansible_host_file['rhel_worker_nodes'] = hosts

        logging.info(ansible_host_file)
        data = _templating.render_template(
            constants.ANSIBLE_INVENTORY_YAML,
            ansible_host_file,
        )
        logging.debug("Ansible hosts file:%s", data)
        host_file_path = "/tmp/hosts"
        with open(host_file_path, 'w') as f:
            f.write(data)
        return host_file_path

    @retry(exceptions.CommandFailed, tries=15, delay=30, backoff=1)
    def check_connection(self, rhel_pod_obj, host, pem_dst_path):
        """
        Check whether newly brought up RHEL instances are accepting
        ssh connections

        Args:
            rhel_pod_obj (Pod): object for handling ansible pod
            host (str): Node to which we want to try ssh
            pem_dst_path (str): path to private key for ssh

        """
        cmd = 'ls'
        rhel_pod_obj.exec_cmd_on_node(
            host, pem_dst_path, cmd, user=constants.EC2_USER
        )

    def attach_nodes_to_vmware_cluster(self, node_list):
        pass

    def same_node_type(self, node_list):
        """
        Make sure that nodes in the list are of same type

        Args:
            node_list(list): of Node objects

        Returns:
            True if all the nodes are of same type, else False

        """
        reference = node_list[0].node_t
        for node in node_list:
            if node.node_t != reference:
                return False
        return True

    def create_aws_rhel_instance(self, node_config):
        """
        This function does the following:
        1. Create RHEL worker instances, copy required AWS tags from existing
        2. worker instances to new RHEL instances
        3. Copy  IAM role from existing worker to new RHEL workers

        Args:
            node_config (dict): node configuration for creating instance
        """
        self.boto_client = boto3.client(
            'ec2', region_name=self.region
        )
        self.cf = boto3.client(
            'cloudformation', region_name=self.region
        )
        rhel_worker_count = len(self.get_rhel_workers())
        rhcos_worker_count = len(self.get_rhcos_workers())
        total_workers = rhel_worker_count + rhcos_worker_count

        node_suffix = [
            node_config['node_name_suffix']
            if node_config['node_name_suffix'] else total_workers
        ]

        zone_worker_id = [
            node_config['zone_worker_id']
            if node_config['zone_worker_id'] else 0
        ]
        self.gather_aws_worker_data(f'no{zone_worker_id}')
        logger.info(f"Creating  worker")
        response = self.boto_client.run_instances(
            BlockDeviceMappings=[
                {
                    'DeviceName': node_config['root_disk'],
                    'Ebs': {
                        'DeleteOnTermination': True,
                        'VolumeSize': node_config['root_disk_size'],
                        'VolumeType': 'gp2'
                    },
                },
            ],
            ImageId=node_config['rhel_worker_ami'],
            SubnetId=self.worker_subnet,
            InstanceType=node_config['rhel_worker_instance_type'],
            MaxCount=1,
            MinCount=1,
            Monitoring={
                'Enabled': False
            },
            SecurityGroupIds=[
                self.worker_security_group[0]['GroupId'],
            ],
            KeyName='openshift-dev'
        )
        inst_id = response['Instances'][0]['InstanceId']
        worker_ec2 = boto3.resource('ec2', region_name=self.region)
        worker_instance = worker_ec2.Instance(inst_id)
        worker_instance.wait_until_running()
        worker_name = f'{self.cluster_id}-rhel-worker-{node_suffix}'
        worker_ec2.create_tags(
            Resources=[inst_id],
            Tags=[
                {'Key': 'Name', 'Value': f'{worker_name}'},
                {'Key': self.worker_tag[0], 'Value': self.worker_tag[1]}
            ]
        )
        logger.info(self.worker_iam_role)
        self.boto_client.associate_iam_instance_profile(
            IamInstanceProfile=self.worker_iam_role,
            InstanceId=inst_id,
        )
        return worker_instance

    def gather_aws_worker_data(self, suffix='no0'):
        """
        Gather various info like vpc, iam role, subnet,security group,
        cluster tag from existing RHCOS workers

        Args:
            suffix (str): suffix to get resource of worker node, 'no0' by default

        """
        stack_name = f'{self.cluster_name}-{suffix}'
        resource = self.cf.list_stack_resources(StackName=stack_name)
        worker_id = self.get_aws_worker_resource_id(resource)
        ec2 = boto3.resource('ec2', region_name=self.region)
        worker_instance = ec2.Instance(worker_id)
        self.worker_vpc = worker_instance.vpc.id
        self.worker_subnet = worker_instance.subnet.id
        self.worker_security_group = worker_instance.security_groups
        self.worker_iam_role = worker_instance.iam_instance_profile
        self.worker_tag = self.get_kube_tag(worker_instance.tags)
        del self.worker_iam_role['Id']

    def get_aws_worker_resource_id(self, resource):
        """
        Get the resource ID

        Args:
            resource (dict): a dictionary of stack resource

        Returns:
            str: ID of worker stack resource

        """
        return resource['StackResourceSummaries'][0]['PhysicalResourceId']

    def get_kube_tag(self, tags):
        """
        Fetch kubernets.io tag from worker instance

        Args:
            tags (dict): AWS tags from existing worker

        Returns:
            tuple: key looks like
                "kubernetes.io/cluster/<cluster-name>" and value looks like
                "share" OR "owned"

        """
        for each in tags:
            if 'kubernetes' in each['Key']:
                return each['Key'], each['Value']

    def get_rhcos_workers(self):
        """
        Returns a list of rhcos worker names

        Returns:
            rhcos_workers (list): list of rhcos worker nodes

        """
        rhcos_workers = []
        ocp_obj = ocp.OCP(kind='node')
        node_info = ocp_obj.get()
        for each in node_info['items']:
            labels = each['metadata']['labels']
            if (
                labels['node.openshift.io/os_id'] == 'rhcos'
                and 'node-role.kubernetes.io/worker' in labels
            ):
                for every in each['status']['addresses']:
                    if every['type'] == 'Hostname':
                        rhcos_workers.append(every['address'])
        return rhcos_workers

    def get_rhel_workers(self):
        """
        Returns a list of rhel worker names

        Returns:
            rhel_workers (list): list of rhel worker nodes

        """
        rhel_workers = []
        ocp_obj = ocp.OCP(kind='node')
        node_info = ocp_obj.get()
        for each in node_info['items']:
            labels = each['metadata']['labels']
            if (
                labels['node.openshift.io/os_id'] == 'rhel'
                and 'node-role.kubernetes.io/worker' in labels
            ):
                for every in each['status']['addresses']:
                    if every['type'] == 'Hostname':
                        rhel_workers.append(every['address'])
        return rhel_workers
