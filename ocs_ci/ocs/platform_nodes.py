import logging
import os
import random

import boto3
import yaml

from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.framework import config, merge_dict
from ocs_ci.utility import aws, vsphere, templating
from ocs_ci.utility.retry import retry
from ocs_ci.ocs import constants, ocp, exceptions
from ocs_ci.ocs.node import get_node_objs, get_typed_worker_nodes
from ocs_ci.ocs.resources.pvc import get_deviceset_pvs
from ocs_ci.ocs.resources import pod
from ocs_ci.utility.utils import (
    get_cluster_name, get_infra_id, create_rhelpod,
    get_ocp_version, get_az_count, TimeoutSampler
)


logger = logging.getLogger(__name__)


class PlatformNodesFactory:
    """
    A factory class to get specific nodes platform object

    """
    def __init__(self):
        self.cls_map = {'AWS': AWSNodes, 'vsphere': VMWareNodes, 'aws': AWSNodes}

    def get_nodes_platform(self):
        platform = config.ENV_DATA['platform']
        return self.cls_map[platform]()


class NodesBase(object):
    """
    A base class for nodes related operations.
    Should be inherited by specific platform classes

    """
    def __init__(self):
        self.cluster_nodes = get_node_objs()
        self.cluster_path = config.ENV_DATA['cluster_path']
        self.platform = config.ENV_DATA['platform']
        self.deployment_type = config.ENV_DATA['deployment_type']
        self.nodes_map = {'AWSUPINode': AWSUPINode}

    def get_data_volumes(self):
        raise NotImplementedError(
            "Get data volume functionality is not implemented"
        )

    def get_node_by_attached_volume(self, volume):
        raise NotImplementedError(
            "Get node by attached volume functionality is not implemented"
        )

    def stop_nodes(self, nodes):
        raise NotImplementedError(
            "Stop nodes functionality is not implemented"
        )

    def start_nodes(self, nodes):
        raise NotImplementedError(
            "Start nodes functionality is not implemented"
        )

    def restart_nodes(self, nodes, force=True):
        raise NotImplementedError(
            "Restart nodes functionality is not implemented"
        )

    def detach_volume(self, node):
        raise NotImplementedError(
            "Detach volume functionality is not implemented"
        )

    def attach_volume(self, node, volume):
        raise NotImplementedError(
            "Attach volume functionality is not implemented"
        )

    def wait_for_volume_attach(self, volume):
        raise NotImplementedError(
            "Wait for volume attach functionality is not implemented"
        )

    def restart_nodes_teardown(self):
        raise NotImplementedError(
            "Restart nodes teardown functionality is not implemented"
        )

    def create_and_attach_nodes_to_cluster(self, node_conf, node_type, num_nodes):
        """
        Create nodes and attach them to cluster
        Use this function if you want to do both creation/attachment in
        a single call

        Args:
            node_conf (dict): of node configuration
            node_type (str): type of node to be created RHCOS/RHEL
            num_nodes (int): Number of node instances to be created

        """
        node_list = self.create_nodes(node_conf, node_type, num_nodes)
        self.attach_nodes_to_cluster(node_list)

    def create_nodes(self, node_conf, node_type, num_nodes):
        """
        create aws instances of nodes

        Args:
            node_conf (dict): of node configuration
            node_type (str): type of node to be created RHCOS/RHEL
            num_nodes (int): Number of node instances to be created

        Returns:
           list: of AWSUPINode/AWSIPINode objects

        """
        node_list = []
        node_cls = self.nodes_map[
            f'{self.platform.upper()}{self.deployment_type.upper()}Node'
        ]

        rhel_cnt = len(get_typed_worker_nodes('rhel'))
        rhcos_cnt = len(get_typed_worker_nodes('rhcos'))
        for i in range(num_nodes):
            node_id = rhel_cnt + rhcos_cnt + i
            node_list.append(node_cls(node_conf, node_type))
            node_list[i]._prepare_node(node_id)

        return node_list

    def attach_nodes_to_cluster(self, node_list):
        raise NotImplementedError(
            "attach nodes to cluster functionality is not implemented"
        )

    def read_default_config(self, default_config_path):
        """
        Commonly used function to read default config

        Args:
            default_config_path (str): Path to default config file

        Returns:
            dict: of default config loaded

        """
        assert os.path.exists(default_config_path), (
            f'Config file doesnt exists'
        )

        with open(default_config_path) as f:
            default_config_dict = yaml.safe_load(f)

        return default_config_dict


class VMWareNodes(NodesBase):
    """
    VMWare nodes class

    """
    def __init__(self):
        super(VMWareNodes, self).__init__()
        self.cluster_name = config.ENV_DATA.get("cluster_name")
        self.server = config.ENV_DATA['vsphere_server']
        self.user = config.ENV_DATA['vsphere_user']
        self.password = config.ENV_DATA['vsphere_password']
        self.cluster = config.ENV_DATA['vsphere_cluster']
        self.datacenter = config.ENV_DATA['vsphere_datacenter']
        self.vsphere = vsphere.VSPHERE(self.server, self.user, self.password)

    def get_vms(self, nodes):
        """
        Get vSphere vm objects list

        Args:
            nodes (list): The OCS objects of the nodes

        Returns:
            list: vSphere vm objects list

        """
        vms_in_pool = self.vsphere.get_all_vms_in_pool(
            self.cluster_name, self.datacenter, self.cluster
        )
        node_names = [node.get().get('metadata').get('name') for node in nodes]
        vms = []
        for node in node_names:
            node_vms = [vm for vm in vms_in_pool if vm.name in node]
            vms.extend(node_vms)
        return vms

    def get_data_volumes(self):
        raise NotImplementedError(
            "Get data volume functionality is not implemented for VMWare"
        )

    def get_node_by_attached_volume(self, volume):
        raise NotImplementedError(
            "Get node by attached volume functionality is not "
            "implemented for VMWare"
        )

    def stop_nodes(self, nodes, force=True):
        """
        Stop vSphere VMs

        Args:
            nodes (list): The OCS objects of the nodes
            force (bool): True for force VM stop, False otherwise

        """
        vms = self.get_vms(nodes)
        assert vms, (
            f"Failed to get VM objects for nodes {[n.name for n in nodes]}"
        )
        self.vsphere.stop_vms(vms, force=force)

    def start_nodes(self, nodes, wait=True):
        """
        Start vSphere VMs

        Args:
            nodes (list): The OCS objects of the nodes

        """
        vms = self.get_vms(nodes)
        assert vms, (
            f"Failed to get VM objects for nodes {[n.name for n in nodes]}"
        )
        self.vsphere.start_vms(vms)

    def restart_nodes(self, nodes, force=True):
        """
        Restart vSphere VMs

        Args:
            nodes (list): The OCS objects of the nodes
            force (bool): True for force VM stop, False otherwise

        """
        vms = self.get_vms(nodes)
        assert vms, (
            f"Failed to get VM objects for nodes {[n.name for n in nodes]}"
        )
        self.vsphere.restart_vms(vms, force=force)

    def detach_volume(self, node):
        raise NotImplementedError(
            "Detach volume functionality is not implemented for VMWare"
        )

    def attach_volume(self, node, volume):
        raise NotImplementedError(
            "Attach volume functionality is not implemented for VMWare"
        )

    def wait_for_volume_attach(self, volume):
        raise NotImplementedError(
            "Wait for volume attach functionality is not implemented for VMWare"
        )

    def restart_nodes_teardown(self):
        """
        Make sure all VMs are up by the end of the test

        """
        vms = self.get_vms(self.cluster_nodes)
        assert vms, (
            f"Failed to get VM objects for nodes {[n.name for n in self.cluster_nodes]}"
        )
        stopped_vms = [
            vm for vm in vms if self.vsphere.get_vm_power_status(vm) == constants.VM_POWERED_OFF
        ]
        # Start the VMs
        if stopped_vms:
            logger.info(f"The following VMs are powered off: {stopped_vms}")
            self.vsphere.start_vms(stopped_vms)


class AWSNodes(NodesBase):
    """
    AWS EC2 instances class

    """
    def __init__(self):
        super(AWSNodes, self).__init__()
        self.aws = aws.AWS()

    def get_ec2_instances(self, nodes):
        """
        Get the EC2 instances dicts

        Args:
            nodes (list): The OCS objects of the nodes

        Returns:
            dict: The EC2 instances dicts (IDs and names)

        """
        return aws.get_instances_ids_and_names(nodes)

    def get_data_volumes(self):
        """
        Get the data EBS volumes

        Returns:
            list: EBS Volume instances

        """
        pvs = get_deviceset_pvs()
        return aws.get_data_volumes(pvs)

    def get_node_by_attached_volume(self, volume):
        """
        Get node OCS object of the EC2 instance that has the volume attached to

        Args:
            volume (Volume): The volume to get the EC2 according to

        Returns:
            OCS: The OCS object of the EC2 instance

        """
        instance_ids = [
            at.get('InstanceId') for at in volume.attachments
        ]
        assert instance_ids, (
            f"EBS Volume {volume.id} is not attached to any EC2 instance"
        )
        instance_id = instance_ids[0]
        all_nodes = get_node_objs()
        nodes = [
            n for n in all_nodes if instance_id in n.get()
            .get('spec').get('providerID')
        ]
        assert nodes, (
            f"Failed to find the OCS object for EC2 instance {instance_id}"
        )
        return nodes[0]

    def stop_nodes(self, nodes, wait=True):
        """
        Stop EC2 instances

        Args:
            nodes (list): The OCS objects of the nodes
            wait (bool): True for waiting the instances to stop, False otherwise


        """
        instances = self.get_ec2_instances(nodes)
        assert instances, (
            f"Failed to get the EC2 instances for nodes {[n.name for n in nodes]}"
        )
        self.aws.stop_ec2_instances(instances=instances, wait=wait)

    def start_nodes(self, nodes, wait=True):
        """
        Start EC2 instances

        Args:
            nodes (list): The OCS objects of the nodes
            wait (bool): True for waiting the instances to start, False otherwise

        """
        instances = self.get_ec2_instances(nodes)
        assert instances, (
            f"Failed to get the EC2 instances for nodes {[n.name for n in nodes]}"
        )
        self.aws.start_ec2_instances(instances=instances, wait=wait)

    def restart_nodes(self, nodes, wait=True, force=True):
        """
        Restart EC2 instances

        Args:
            nodes (list): The OCS objects of the nodes
            wait (bool): True in case wait for status is needed,
                False otherwise
            force (bool): True for force instance stop, False otherwise

        Returns:

        """
        instances = self.get_ec2_instances(nodes)
        assert instances, (
            f"Failed to get the EC2 instances for nodes {[n.name for n in nodes]}"
        )
        self.aws.restart_ec2_instances(instances=instances, wait=wait, force=force)

    def terminate_nodes(self, nodes, wait=True):
        """
        Terminate EC2 instances

        Args:
            nodes (list): The OCS objects of the nodes
            wait (bool): True for waiting the instances to terminate,
            False otherwise

        """
        instances = self.get_ec2_instances(nodes)
        assert instances, (
            f"Failed to terminate the "
            f"EC2 instances for nodes {[n.name for n in nodes]}"
        )
        self.aws.terminate_ec2_instances(instances=instances, wait=wait)

    def detach_volume(self, volume):
        """
        Detach a volume from an EC2 instance

        Args:
            volume (Volume): The volume to delete

        """
        self.aws.detach_volume(volume)

    def attach_volume(self, node, volume):
        """
        Attach a data volume to an instance

        Args:
            node (OCS): The EC2 instance to attach the volume to
            volume (Volume): The volume to delete

        """
        volume.load()
        volume_attachments = [
            at.get('InstanceId') for at in volume.attachments
        ]
        if not volume_attachments:
            instance = self.get_ec2_instances([node])
            assert instance, f"Failed to get the EC2 instance for nodes {node.name}"
            self.aws.attach_volume(volume, [*instance][0])
        else:
            logger.warning(
                f"Volume {volume.id} is already attached to EC2 "
                f"instance/s {volume_attachments}"
            )

    def wait_for_volume_attach(self, volume):
        """
        Wait for an EBS volume to be attached to an EC2 instance.
        This is used as when detaching the EBS volume from the EC2 instance,
        re-attach should take place automatically

        Args:
            volume (Volume): The volume to wait for to be attached

        Returns:
            bool: True if the volume has been attached to the
                instance, False otherwise

        """
        def get_volume_attachments(ebs_volume):
            ebs_volume.reload()
            return ebs_volume.attachments

        try:
            for sample in TimeoutSampler(
                300, 3, get_volume_attachments, volume
            ):
                logger.info(
                    f"EBS volume {volume.id} attachments are: {sample}"
                )
                if sample:
                    return True
        except TimeoutExpiredError:
            logger.error(
                f"Volume {volume.id} failed to be attached to an EC2 instance"
            )
            return False

    def restart_nodes_teardown(self):
        """
        Make sure all EC2 instances are up. To be used in the test teardown

        """
        # Get the cluster nodes ec2 instances
        ec2_instances = self.get_ec2_instances(self.cluster_nodes)
        assert ec2_instances, (
            f"Failed to get ec2 instances for nodes {[n.name for n in self.cluster_nodes]}"
        )

        logger.info(
            "Getting the instances that are in status 'stopping' (if there are any), "
            "and wait for them to get to status 'stopped', "
            "so it will be possible to start them"
        )
        stopping_instances = {
            key: val for key, val in ec2_instances.items() if
            self.aws.get_instances_status_by_id(key) == constants.INSTANCE_STOPPING
        }

        logger.info(
            "Waiting fot the instances that are in status 'stopping' "
            "(if there are any) to reach 'stopped'"
        )
        if stopping_instances:
            for stopping_instance in stopping_instances:
                instance = self.aws.get_ec2_instance(stopping_instance.key())
                instance.wait_until_stopped()
        stopped_instances = {
            key: val for key, val in ec2_instances.items() if
            self.aws.get_instances_status_by_id(key) == constants.INSTANCE_STOPPED
        }

        # Start the instances
        if stopped_instances:
            self.aws.start_ec2_instances(instances=stopped_instances, wait=True)

    def attach_nodes_to_cluster(self, node_list):
        """
        Attach nodes in the list to the cluster

        Args:
            node_list (list): of AWSUPINode/AWSIPINode objects

        """
        if self.deployment_type.lower() == 'upi':
            self.attach_nodes_to_upi_cluster(node_list)

    def attach_nodes_to_upi_cluster(self, node_list):
        """
        Attach node to upi cluster

        Args:
            node_list (list): of AWSUPINode objects

        """
        if node_list[0].node_type == 'RHEL':
            self.attach_rhel_nodes_to_upi_cluster(node_list)

    def attach_rhel_nodes_to_upi_cluster(self, node_list):
        """
        Attach RHEL nodes to upi cluster

        Args:
            node_list (list): of AWSUPINode objects with RHEL os

        """
        rhel_pod_name = "rhel-ansible"
        rhel_pod_obj = create_rhelpod(
            constants.DEFAULT_NAMESPACE, rhel_pod_name, 600
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
            node.aws_instance_obj.private_dns_name for node in
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
        cmd = (
            f"ansible-playbook -i /hosts --private-key={pem_dst_path} "
            f"{constants.SCALEUP_ANSIBLE_PLAYBOOK}"
        )

        rhel_pod_obj.exec_cmd_on_pod(
            cmd, out_yaml_format=False, timeout=timeout
        )
        self.verify_nodes_added(hosts)
        rhel_pod_obj.delete()

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

    def get_ready_status(self, node_info):
        """
        Get the node 'Ready' status

        Args:
            node_info (dict): Node info which includes details

        Returns:
            bool: True if node is Ready else False

        """
        for cond in node_info['status']['conditions']:
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


class AWSUPINode(AWSNodes):
    """
    Node object representing AWS upi nodes

    """
    def __init__(self, node_conf, node_type):
        super(AWSUPINode, self).__init__()
        self.node_conf = node_conf
        #  RHEL/RHCOS
        self.node_type = node_type
        #  This variable will hold the AWS instance object
        self.aws_instance_obj = None
        self.region = config.ENV_DATA['region']
        self.cluster_name = get_cluster_name(self.cluster_path)
        self.client = boto3.client(
            'ec2', region_name=self.region
        )
        # cloudformation
        self.cf = boto3.client('cloudformation', region_name=self.region)

    def _prepare_node(self, node_id):
        """
        Create AWS instance of the node

        Args:
            node_id (int): Unique integer id for node

        """
        if self.node_type == 'RHEL':
            conf = self._prepare_rhel_node_conf()
            conf['node_id'] = node_id
            try:
                self.aws_instance_obj = self._prepare_upi_rhel_node(conf)
            except Exception:
                logger.error("Failed to create RHEL node")
                raise

    def _prepare_rhel_node_conf(self):
        """
        Merge default RHEL node config with the user provided
        config

        """
        conf = self.read_default_config(constants.RHEL_WORKERS_CONF)
        default_conf = conf.get('ENV_DATA')
        merge_dict(default_conf, self.node_conf)
        logger.info(f"Merged dict is {default_conf}")
        return default_conf

    def _prepare_upi_rhel_node(self, node_conf):
        """
        Handle RHEL worker instance creation
        1. Create RHEL worker instance , copy required AWS tags from existing
        worker instances to new RHEL instance
        2. Copy IAM role from existing worker to new RHEL worker

        """
        cluster_id = get_infra_id(self.cluster_path)
        node_id = node_conf['node_id']
        if not node_conf.get('zone'):
            num_zone = get_az_count()
            zone = random.randint(0, num_zone)
        else:
            zone = node_conf.get('zone')
        logger.info("Creating RHEL worker node")
        self.gather_worker_data(f'no{zone}')
        response = self.client.run_instances(
            BlockDeviceMappings=[
                {
                    'DeviceName': node_conf['root_disk'],
                    'Ebs': {
                        'DeleteOnTermination': True,
                        'VolumeSize': node_conf['root_disk_size'],
                        'VolumeType': 'gp2'
                    },
                },
            ],
            ImageId=node_conf['rhel_worker_ami'],
            SubnetId=self.worker_subnet,
            InstanceType=node_conf['rhel_worker_instance_type'],
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
        worker_name = f'{cluster_id}-rhel-worker-{node_id}'
        worker_ec2.create_tags(
            Resources=[inst_id],
            Tags=[
                {'Key': 'Name', 'Value': f'{worker_name}'},
                {'Key': self.worker_tag[0], 'Value': self.worker_tag[1]}
            ]
        )
        logging.info(self.worker_iam_role)
        self.client.associate_iam_instance_profile(
            IamInstanceProfile=self.worker_iam_role,
            InstanceId=inst_id,
        )
        return worker_instance

    def gather_worker_data(self, suffix='no0'):
        """
        Gather various info like vpc, iam role, subnet,security group,
        cluster tag from existing RHCOS workers

        Args:
            suffix (str): suffix to get resource of worker node, 'no0' by default

        """
        stack_name = f'{self.cluster_name}-{suffix}'
        resource = self.cf.list_stack_resources(StackName=stack_name)
        worker_id = self.get_worker_resource_id(resource)
        ec2 = boto3.resource('ec2', region_name=self.region)
        worker_instance = ec2.Instance(worker_id)
        self.worker_vpc = worker_instance.vpc.id
        self.worker_subnet = worker_instance.subnet.id
        self.worker_security_group = worker_instance.security_groups
        self.worker_iam_role = worker_instance.iam_instance_profile
        self.worker_tag = self.get_kube_tag(worker_instance.tags)
        del self.worker_iam_role['Id']

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

    def get_worker_resource_id(self, resource):
        """
        Get the resource ID

        Args:
            resource (dict): a dictionary of stack resource

        Returns:
            str: ID of worker stack resource

        """
        return resource['StackResourceSummaries'][0]['PhysicalResourceId']
