"""
TNF Hypervisor utility for managing EC2 bare-metal instances
as KVM hypervisors for two-node OCP cluster deployment via dev-scripts.
"""

import base64
import json
import logging
import os
import tempfile

import boto3

from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility.connection import Connection
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)

TNF_HYPERVISOR_PACKAGES = [
    "libvirt",
    "qemu-kvm",
    "libvirt-daemon-kvm",
    "virt-install",
    "podman",
    "firewalld",
    "git",
    "make",
    "golang",
    "jq",
    "lvm2",
    "bind-utils",
    "net-tools",
    "ipmitool",
    "python3",
    "squid",
    "cockpit",
    "cockpit-machines",
]


class TNFHypervisor:
    """
    Manages the lifecycle of an EC2 bare-metal instance used as
    a KVM hypervisor for TNF two-node OCP clusters via dev-scripts.
    """

    def __init__(self, hypervisor_config, dev_scripts_config, proxy_config=None):
        self.config = hypervisor_config
        self.dev_scripts_config = dev_scripts_config
        self.proxy_config = proxy_config or {}

        self.region = self.config.get("region", constants.DEFAULT_AWS_REGION)
        self.ec2_client = boto3.client("ec2", region_name=self.region)
        self.ec2_resource = boto3.resource("ec2", region_name=self.region)

        self.instance_id = None
        self.public_ip = None
        self.ssh_conn = None

        self.ssh_user = self.config.get(
            "ssh_user", constants.TNF_HYPERVISOR_DEFAULT_SSH_USER
        )
        self.ssh_key, self.ssh_pub_key = self._resolve_ssh_keys()
        self._created_sg_id = None
        self._created_vpc_stack = {}
        self._disk_resize_map = {}

    def _resolve_ssh_keys(self):
        """
        Resolve SSH key pair. If ssh_key_private is set in config, use it.
        Otherwise auto-detect from common locations (~/.ssh/).
        Returns (private_key_path, public_key_path) tuple.
        """
        configured_private = self.config.get("ssh_key_private")
        if configured_private:
            private_path = os.path.expanduser(configured_private)
            pub_path = private_path + ".pub"
            if not os.path.exists(private_path):
                raise FileNotFoundError(
                    f"Configured SSH private key not found: {private_path}"
                )
            if not os.path.exists(pub_path):
                pub_path = private_path.rsplit(".", 1)[0] + ".pub"
            return private_path, pub_path

        search_order = [
            "~/.ssh/id_ed25519_tnf",
            "~/.ssh/id_ed25519",
            "~/.ssh/id_rsa",
            "~/.ssh/id_ecdsa",
        ]
        for key_path in search_order:
            private_path = os.path.expanduser(key_path)
            pub_path = private_path + ".pub"
            if os.path.exists(private_path) and os.path.exists(pub_path):
                logger.info(f"Auto-detected SSH key: {private_path}")
                return private_path, pub_path

        raise FileNotFoundError(
            "No SSH key found. Provide ssh_key_private in config or "
            "ensure a key exists at ~/.ssh/id_ed25519, ~/.ssh/id_rsa, "
            "or ~/.ssh/id_ecdsa"
        )

    def _get_rhel_ami(self, architecture="x86_64", rhel_version="9"):
        """
        Auto-discover the latest RHEL AMI for the region,
        matching how two-node-toolbox does it via describe-images.

        Args:
            architecture (str): CPU architecture (x86_64 or arm64)
            rhel_version (str): RHEL major version (default "9")

        Returns:
            str: AMI ID of the latest matching RHEL image
        """
        ami_arch = "arm64" if architecture == "aarch64" else architecture
        name_filter = f"RHEL-{rhel_version}*{ami_arch}*"
        logger.info(
            f"Auto-discovering RHEL AMI: version={rhel_version}, "
            f"arch={ami_arch}, region={self.region}"
        )

        response = self.ec2_client.describe_images(
            Owners=["309956199498"],
            Filters=[
                {"Name": "name", "Values": [name_filter]},
                {"Name": "state", "Values": ["available"]},
            ],
        )

        images = response.get("Images", [])
        if not images:
            raise ValueError(
                f"No RHEL {rhel_version} AMI found for {ami_arch} " f"in {self.region}"
            )

        images.sort(key=lambda x: x["CreationDate"], reverse=True)
        ami_id = images[0]["ImageId"]
        ami_name = images[0].get("Name", "")
        logger.info(f"Auto-discovered AMI: {ami_id} ({ami_name})")
        return ami_id

    def _create_vpc_stack(self, name_prefix):
        """
        Create a full VPC networking stack (VPC, subnet, internet gateway,
        route table, security group) matching two-node-toolbox's
        CloudFormation network-stack.yaml.

        Returns:
            dict: {"vpc_id", "subnet_id", "igw_id", "rtb_id", "sg_id"}
        """
        tag_spec = [
            {"Key": "Name", "Value": f"{name_prefix}-vpc"},
            {"Key": "tnf-hypervisor", "Value": "true"},
        ]

        logger.info("Creating VPC networking stack...")

        vpc = self.ec2_client.create_vpc(CidrBlock="10.192.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        self.ec2_client.create_tags(Resources=[vpc_id], Tags=tag_spec)
        self.ec2_client.modify_vpc_attribute(
            VpcId=vpc_id, EnableDnsSupport={"Value": True}
        )
        self.ec2_client.modify_vpc_attribute(
            VpcId=vpc_id, EnableDnsHostnames={"Value": True}
        )
        logger.info(f"VPC created: {vpc_id}")

        igw = self.ec2_client.create_internet_gateway()
        igw_id = igw["InternetGateway"]["InternetGatewayId"]
        self.ec2_client.attach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)
        logger.info(f"Internet gateway created and attached: {igw_id}")

        subnet = self.ec2_client.create_subnet(
            VpcId=vpc_id,
            CidrBlock="10.192.10.0/24",
        )
        subnet_id = subnet["Subnet"]["SubnetId"]
        self.ec2_client.modify_subnet_attribute(
            SubnetId=subnet_id,
            MapPublicIpOnLaunch={"Value": True},
        )
        logger.info(f"Public subnet created: {subnet_id}")

        rtb = self.ec2_client.create_route_table(VpcId=vpc_id)
        rtb_id = rtb["RouteTable"]["RouteTableId"]
        self.ec2_client.create_route(
            RouteTableId=rtb_id,
            DestinationCidrBlock="0.0.0.0/0",
            GatewayId=igw_id,
        )
        self.ec2_client.associate_route_table(RouteTableId=rtb_id, SubnetId=subnet_id)
        logger.info(f"Route table created with internet route: {rtb_id}")

        sg_id = self._ensure_security_group(vpc_id)

        stack = {
            "vpc_id": vpc_id,
            "subnet_id": subnet_id,
            "igw_id": igw_id,
            "rtb_id": rtb_id,
            "sg_id": sg_id,
        }
        self._created_vpc_stack = stack
        logger.info("VPC networking stack created successfully")
        return stack

    def _wait_and_delete_resource(self, delete_fn, resource_name, retries=10, delay=30):
        """
        Retry a delete operation, waiting for dependencies to clear.
        Bare-metal instance ENIs take time to release after termination.
        """
        import time

        for attempt in range(retries):
            try:
                delete_fn()
                logger.info(f"{resource_name} deleted")
                return True
            except Exception as e:
                if "DependencyViolation" in str(e) and attempt < retries - 1:
                    logger.info(
                        f"{resource_name} has dependencies, "
                        f"retrying in {delay}s ({attempt + 1}/{retries})..."
                    )
                    time.sleep(delay)
                else:
                    logger.warning(f"Failed to delete {resource_name}: {e}")
                    return False

    def _delete_vpc_stack(self):
        """
        Delete all resources in the auto-created VPC stack.
        Order: VPC endpoints → SG → RTB → IGW → subnet → VPC.
        VPC endpoints (e.g. GuardDuty) are auto-created by AWS and
        hold ENIs that block subnet/VPC deletion.
        """
        stack = self._created_vpc_stack
        if not stack:
            return

        logger.info("Deleting auto-created VPC networking stack...")

        if stack.get("vpc_id"):
            self._delete_vpc_endpoints(stack["vpc_id"])

        if stack.get("sg_id"):
            self._delete_security_group(stack["sg_id"])

        if stack.get("rtb_id"):
            try:
                associations = self.ec2_client.describe_route_tables(
                    RouteTableIds=[stack["rtb_id"]]
                )["RouteTables"][0].get("Associations", [])
                for assoc in associations:
                    if not assoc.get("Main"):
                        self.ec2_client.disassociate_route_table(
                            AssociationId=assoc["RouteTableAssociationId"]
                        )
                self.ec2_client.delete_route_table(RouteTableId=stack["rtb_id"])
                logger.info(f"Route table deleted: {stack['rtb_id']}")
            except Exception as e:
                logger.warning(f"Failed to delete route table: {e}")

        if stack.get("igw_id") and stack.get("vpc_id"):
            try:
                self.ec2_client.detach_internet_gateway(
                    InternetGatewayId=stack["igw_id"],
                    VpcId=stack["vpc_id"],
                )
                self.ec2_client.delete_internet_gateway(
                    InternetGatewayId=stack["igw_id"]
                )
                logger.info(f"Internet gateway deleted: {stack['igw_id']}")
            except Exception as e:
                logger.warning(f"Failed to delete internet gateway: {e}")

        if stack.get("subnet_id"):
            self._wait_and_delete_resource(
                lambda: self.ec2_client.delete_subnet(SubnetId=stack["subnet_id"]),
                f"Subnet {stack['subnet_id']}",
            )

        if stack.get("vpc_id"):
            self._wait_and_delete_resource(
                lambda: self.ec2_client.delete_vpc(VpcId=stack["vpc_id"]),
                f"VPC {stack['vpc_id']}",
            )

        logger.info("VPC networking stack cleanup complete")

    def launch_instance(self):
        """
        Launch EC2 bare-metal instance. Creates full VPC networking
        if no subnet_id is provided (matching two-node-toolbox behavior).

        Returns:
            str: instance_id
        """
        instance_type = self.config.get(
            "instance_type", constants.TNF_HYPERVISOR_DEFAULT_INSTANCE_TYPE
        )
        ami = self.config.get("ami", "")
        if not ami:
            ami = self._get_rhel_ami()
        subnet_id = self.config.get("subnet_id")
        security_group_ids = self.config.get("security_group_ids", [])
        root_volume_size = self.config.get("root_volume_size", 200)
        root_volume_type = self.config.get("root_volume_type", "gp3")
        name_prefix = self.config.get("instance_name_prefix", "tnf-hypervisor")

        with open(self.ssh_pub_key, "r") as f:
            pub_key_content = f.read().strip()

        userdata_script = (
            "#!/bin/bash\n"
            f"mkdir -p /home/{self.ssh_user}/.ssh\n"
            f'echo "{pub_key_content}" >> '
            f"/home/{self.ssh_user}/.ssh/authorized_keys\n"
            f"chown -R {self.ssh_user}:{self.ssh_user} "
            f"/home/{self.ssh_user}/.ssh\n"
            f"chmod 700 /home/{self.ssh_user}/.ssh\n"
            f"chmod 600 /home/{self.ssh_user}/.ssh/authorized_keys\n"
        )

        logger.info(
            f"Launching EC2 bare-metal instance: type={instance_type}, "
            f"ami={ami}, region={self.region}"
        )

        if not subnet_id:
            stack = self._create_vpc_stack(name_prefix)
            subnet_id = stack["subnet_id"]
            security_group_ids = [stack["sg_id"]]
        elif not security_group_ids:
            subnet_info = self.ec2_client.describe_subnets(SubnetIds=[subnet_id])
            vpc_id = subnet_info["Subnets"][0]["VpcId"]
            sg_id = self._ensure_security_group(vpc_id)
            security_group_ids = [sg_id]

        run_kwargs = {
            "ImageId": ami,
            "InstanceType": instance_type,
            "UserData": base64.b64encode(userdata_script.encode()).decode(),
            "MaxCount": 1,
            "MinCount": 1,
            "SubnetId": subnet_id,
            "SecurityGroupIds": security_group_ids,
            "BlockDeviceMappings": [
                {
                    "DeviceName": "/dev/sda1",
                    "Ebs": {
                        "DeleteOnTermination": True,
                        "VolumeSize": root_volume_size,
                        "VolumeType": root_volume_type,
                    },
                }
            ],
            "TagSpecifications": [
                {
                    "ResourceType": "instance",
                    "Tags": [
                        {"Key": "Name", "Value": name_prefix},
                        {"Key": "tnf-hypervisor", "Value": "true"},
                    ],
                }
            ],
        }

        response = self.ec2_client.run_instances(**run_kwargs)
        self.instance_id = response["Instances"][0]["InstanceId"]
        logger.info(f"EC2 instance launched: {self.instance_id}")

        logger.info("Waiting for instance to pass status checks...")
        waiter = self.ec2_client.get_waiter("instance_status_ok")
        waiter.wait(
            InstanceIds=[self.instance_id],
            WaiterConfig={"Delay": 30, "MaxAttempts": 60},
        )
        instance = self.ec2_resource.Instance(self.instance_id)
        instance.reload()
        self.public_ip = instance.public_ip_address
        logger.info(f"EC2 instance ready: {self.instance_id}, IP: {self.public_ip}")
        return self.instance_id

    def save_instance_info(self, cluster_path):
        """
        Save instance ID to metadata.json for cross-session teardown.
        """
        metadata_path = os.path.join(cluster_path, "metadata.json")
        metadata = {}
        if os.path.exists(metadata_path):
            with open(metadata_path, "r") as f:
                metadata = json.load(f)

        metadata["tnf_hypervisor_instance_id"] = self.instance_id
        metadata["tnf_hypervisor_public_ip"] = self.public_ip
        metadata["tnf_hypervisor_region"] = self.region
        if self._created_sg_id:
            metadata["tnf_hypervisor_security_group_id"] = self._created_sg_id
        if self._created_vpc_stack:
            metadata["tnf_hypervisor_vpc_stack"] = self._created_vpc_stack

        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)
        logger.info(f"Saved hypervisor instance info to {metadata_path}")

    def load_instance_info(self, cluster_path):
        """
        Load instance ID from metadata.json for teardown.
        """
        metadata_path = os.path.join(cluster_path, "metadata.json")
        if not os.path.exists(metadata_path):
            return False

        with open(metadata_path, "r") as f:
            metadata = json.load(f)

        self.instance_id = metadata.get("tnf_hypervisor_instance_id")
        self.public_ip = metadata.get("tnf_hypervisor_public_ip")
        self._created_sg_id = metadata.get("tnf_hypervisor_security_group_id")
        self._created_vpc_stack = metadata.get("tnf_hypervisor_vpc_stack", {})
        if metadata.get("tnf_hypervisor_region"):
            self.region = metadata["tnf_hypervisor_region"]
            self.ec2_client = boto3.client("ec2", region_name=self.region)
            self.ec2_resource = boto3.resource("ec2", region_name=self.region)
        return bool(self.instance_id)

    def wait_for_ssh_ready(self, timeout=None):
        """
        Wait for SSH to become available on the hypervisor.
        Bare-metal instances can take 5-10 minutes to boot.
        """
        timeout = timeout or constants.TNF_HYPERVISOR_SSH_TIMEOUT
        logger.info(f"Waiting for SSH on {self.public_ip} (timeout {timeout}s)...")

        for sample in TimeoutSampler(
            timeout=timeout,
            sleep=30,
            func=self._try_ssh_connect,
        ):
            if sample:
                logger.info("SSH connection established to hypervisor")
                return

    def _try_ssh_connect(self):
        try:
            conn = Connection(
                host=self.public_ip,
                user=self.ssh_user,
                private_key=self.ssh_key,
            )
            retcode, stdout, _ = conn.exec_cmd("hostname")
            if retcode == 0:
                self.ssh_conn = conn
                logger.info(f"SSH ready, hostname: {stdout.strip()}")
                return True
        except Exception as e:
            logger.debug(f"SSH not ready yet: {e}")
        return False

    def terminate_instance(self):
        """
        Terminate the EC2 hypervisor instance.
        """
        if not self.instance_id:
            logger.warning("No instance ID to terminate")
            return

        logger.info(f"Terminating EC2 instance: {self.instance_id}")
        self.ec2_client.terminate_instances(InstanceIds=[self.instance_id])
        waiter = self.ec2_client.get_waiter("instance_terminated")
        waiter.wait(
            InstanceIds=[self.instance_id],
            WaiterConfig={"Delay": 30, "MaxAttempts": 40},
        )
        logger.info(f"EC2 instance terminated: {self.instance_id}")

        if self._created_vpc_stack:
            self._delete_vpc_stack()
        elif self._created_sg_id:
            self._delete_security_group(self._created_sg_id)

    def _ensure_security_group(self, vpc_id):
        """
        Create a security group with ports required for TNF hypervisor.

        Ports match two-node-toolbox CloudFormation: SSH, HTTP, HTTPS,
        squid proxy, OCP API, dev-scripts BMC, and NodePort range.

        Args:
            vpc_id (str): VPC ID to create the security group in

        Returns:
            str: security group ID
        """
        name_prefix = self.config.get("instance_name_prefix", "tnf-hypervisor")
        sg_name = f"{constants.TNF_HYPERVISOR_SG_NAME_PREFIX}-{name_prefix}"

        logger.info(f"Creating security group '{sg_name}' in VPC {vpc_id}...")
        response = self.ec2_client.create_security_group(
            GroupName=sg_name,
            Description=("TNF hypervisor security group (auto-created by ocs-ci)"),
            VpcId=vpc_id,
        )
        sg_id = response["GroupId"]

        ip_permissions = []
        for port_spec in constants.TNF_HYPERVISOR_SG_PORTS:
            rule = {
                "IpProtocol": port_spec["protocol"],
                "FromPort": port_spec["port"],
                "ToPort": port_spec.get("to_port", port_spec["port"]),
                "IpRanges": [
                    {
                        "CidrIp": "0.0.0.0/0",
                        "Description": port_spec["description"],
                    }
                ],
            }
            ip_permissions.append(rule)

        # ICMP for connectivity checks
        ip_permissions.append(
            {
                "IpProtocol": "icmp",
                "FromPort": -1,
                "ToPort": -1,
                "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "ICMP"}],
            }
        )

        self.ec2_client.authorize_security_group_ingress(
            GroupId=sg_id,
            IpPermissions=ip_permissions,
        )

        self.ec2_client.create_tags(
            Resources=[sg_id],
            Tags=[
                {"Key": "Name", "Value": sg_name},
                {"Key": "tnf-hypervisor", "Value": "true"},
            ],
        )

        logger.info(f"Security group created: {sg_id}")
        self._created_sg_id = sg_id
        return sg_id

    def _delete_vpc_endpoints(self, vpc_id):
        """
        Delete all VPC endpoints in the VPC. AWS auto-creates endpoints
        (e.g. GuardDuty) that hold ENIs blocking subnet/VPC deletion.
        """
        try:
            response = self.ec2_client.describe_vpc_endpoints(
                Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
            )
            endpoint_ids = [
                ep["VpcEndpointId"] for ep in response.get("VpcEndpoints", [])
            ]
            if endpoint_ids:
                logger.info(
                    f"Deleting {len(endpoint_ids)} VPC endpoint(s): " f"{endpoint_ids}"
                )
                self.ec2_client.delete_vpc_endpoints(VpcEndpointIds=endpoint_ids)
                import time

                time.sleep(15)
        except Exception as e:
            logger.warning(f"Failed to delete VPC endpoints: {e}")

    def _delete_security_group(self, sg_id):
        try:
            logger.info(f"Deleting auto-created security group: {sg_id}")
            self.ec2_client.delete_security_group(GroupId=sg_id)
            logger.info(f"Security group deleted: {sg_id}")
        except Exception as e:
            logger.warning(f"Failed to delete security group {sg_id}: {e}")

    @staticmethod
    def _shell_quote(s):
        return "'" + s.replace("'", "'\"'\"'") + "'"

    def _ssh_cmd(self, cmd, timeout=300, ignore_error=False, sudo=True):
        """
        Run a command on the hypervisor via SSH.

        Args:
            cmd (str): Command to execute
            timeout (int): Timeout in seconds
            ignore_error (bool): If True, don't raise on non-zero exit
            sudo (bool): Prepend sudo (ec2-user is not root)

        Returns:
            tuple: (retcode, stdout, stderr)

        Raises:
            CommandFailed: if command fails and ignore_error is False
        """
        if sudo and self.ssh_user != "root":
            cmd = f"sudo bash -c {self._shell_quote(cmd)}"
        retcode, stdout, stderr = self.ssh_conn.exec_cmd(cmd)
        if retcode != 0 and not ignore_error:
            raise CommandFailed(
                f"Command failed on hypervisor (exit {retcode}): {cmd}\n"
                f"stderr: {stderr}"
            )
        return retcode, stdout, stderr

    def configure_host(self):
        """
        Configure the bare-metal host as a KVM hypervisor.
        """
        logger.info("Configuring hypervisor host...")

        transport = self.ssh_conn.client.get_transport()
        if transport:
            transport.set_keepalive(60)

        self._set_hostname()
        self._enable_repos()
        self._install_packages()
        self._configure_libvirt()
        self._configure_networking()
        logger.info("Hypervisor host configuration complete")

    def _set_hostname(self):
        name_prefix = self.config.get("instance_name_prefix", "tnf-hypervisor")
        base_domain = self.dev_scripts_config.get("base_domain", "tnf.testing")
        hostname = f"{name_prefix}.{base_domain}"
        logger.info(f"Setting hostname to {hostname}...")
        self._ssh_cmd(f"hostnamectl set-hostname {hostname}")

    def _enable_repos(self):
        logger.info("Enabling CRB repository...")
        self._ssh_cmd(
            "dnf config-manager --set-enabled crb "
            "|| dnf config-manager --set-enabled codeready-builder-for-rhel-9-rhui-rpms "
            "|| subscription-manager repos "
            "--enable codeready-builder-for-rhel-9-x86_64-rpms",
            ignore_error=True,
        )

    def _install_packages(self):
        logger.info("Cleaning dnf cache and installing required packages...")
        self._ssh_cmd("dnf clean all", timeout=120)
        pkg_list = " ".join(TNF_HYPERVISOR_PACKAGES)
        self._ssh_cmd(f"dnf install -y {pkg_list}", timeout=600)
        self._ssh_cmd("systemctl enable --now libvirtd")

    def _configure_libvirt(self):
        logger.info("Configuring libvirt...")
        self._ssh_cmd("systemctl enable --now libvirtd")
        self._ssh_cmd(
            "virsh pool-define-as default dir --target /var/lib/libvirt/images",
            ignore_error=True,
        )
        self._ssh_cmd("virsh pool-start default", ignore_error=True)
        self._ssh_cmd("virsh pool-autostart default", ignore_error=True)

    def _configure_networking(self):
        logger.info("Configuring networking...")
        self._ssh_cmd("sysctl -w net.ipv4.ip_forward=1")
        self._ssh_cmd(
            "echo 'net.ipv4.ip_forward = 1' >> /etc/sysctl.conf",
            ignore_error=True,
        )
        self._ssh_cmd("systemctl enable --now firewalld", ignore_error=True)

    def clone_dev_scripts(self):
        """
        Clone dev-scripts repository on the hypervisor.
        """
        repo = self.dev_scripts_config.get("repo", constants.TNF_DEV_SCRIPTS_REPO)
        branch = self.dev_scripts_config.get("branch", constants.TNF_DEV_SCRIPTS_BRANCH)
        dev_scripts_dir = constants.TNF_DEV_SCRIPTS_DIR

        logger.info(f"Cloning dev-scripts from {repo} (branch: {branch})...")
        self._ssh_cmd(
            f"rm -rf {dev_scripts_dir} && "
            f"git clone {repo} -b {branch} {dev_scripts_dir}",
            timeout=300,
        )
        logger.info("dev-scripts cloned successfully")

    def _resolve_ocp_release_image(self, ocp_version):
        """
        Resolve OCP version to a GA release image from mirror.openshift.com.

        Args:
            ocp_version (str): OCP version like "4.22" or "4.22.8"

        Returns:
            str: Release image (quay.io/openshift-release-dev/ocp-release:X.Y.Z-x86_64)
                 or empty string if resolution fails
        """
        import urllib.request

        major_minor = ".".join(ocp_version.split(".")[:2])
        url = (
            f"https://mirror.openshift.com/pub/openshift-v4/clients/"
            f"ocp/stable-{major_minor}/release.txt"
        )
        logger.info(f"Resolving OCP release image from {url}")
        try:
            with urllib.request.urlopen(url, timeout=30) as resp:
                content = resp.read().decode()
            for line in content.splitlines():
                if "quay.io/openshift-release-dev/ocp-release" in line:
                    image = line.strip().split()[-1]
                    logger.info(f"Resolved OCP release image: {image}")
                    return image
        except Exception as e:
            logger.warning(f"Failed to resolve OCP release image: {e}")
        return ""

    def generate_dev_scripts_config(self, pull_secret_content, ocp_version=""):
        """
        Generate dev-scripts config file and upload pull secret.
        """
        dev_scripts_dir = constants.TNF_DEV_SCRIPTS_DIR
        cluster_name = self.dev_scripts_config.get("cluster_name", "tnf-cluster")
        remote_ps_path = f"{dev_scripts_dir}/pull_secret.json"

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write(pull_secret_content)
            local_ps_path = f.name

        tmp_ps_path = "/tmp/pull_secret.json"
        self.ssh_conn.upload_file(local_ps_path, tmp_ps_path)
        os.unlink(local_ps_path)
        self._ssh_cmd(f"cp {tmp_ps_path} {remote_ps_path}")

        extra_disks = self.dev_scripts_config.get("extra_disks", [])

        # dev-scripts VM_EXTRADISKS_SIZE applies one size to ALL extra disks.
        # Create all disks at the smallest size (for DRBD monitor), then
        # resize the larger disks (OSD) after dev-scripts via virsh blockresize.
        # This avoids PCI slot exhaustion from hotplugging additional disks.
        if extra_disks:
            sorted_disks = sorted(extra_disks, key=lambda d: d["size"])
            min_disk_size = sorted_disks[0]["size"]
            self._disk_resize_map = {}
            for i, disk in enumerate(sorted_disks):
                if disk["size"] > min_disk_size:
                    self._disk_resize_map[f"vd{chr(98 + i)}"] = disk["size"]
        else:
            min_disk_size = 8
            self._disk_resize_map = {}

        config_lines = [
            "export IP_STACK=v4",
            f"export NUM_MASTERS={self.dev_scripts_config.get('num_masters', 2)}",
            f"export NUM_WORKERS={self.dev_scripts_config.get('num_workers', 0)}",
            f"export MASTER_VCPU={self.dev_scripts_config.get('master_vcpu', 16)}",
            f"export MASTER_MEMORY={self.dev_scripts_config.get('master_memory', 65536)}",
            f"export MASTER_DISK={self.dev_scripts_config.get('master_disk', 120)}",
            f"export NETWORK_TYPE={self.dev_scripts_config.get('network_type', 'OVNKubernetes')}",
            f"export CLUSTER_NAME={cluster_name}",
            f"export BASE_DOMAIN={self.dev_scripts_config.get('base_domain', 'tnf.testing')}",
        ]

        if extra_disks:
            disk_names = " ".join([f"vd{chr(98 + i)}" for i in range(len(extra_disks))])
            config_lines.extend(
                [
                    "export VM_EXTRADISKS=true",
                    f'export VM_EXTRADISKS_LIST="{disk_names}"',
                    f"export VM_EXTRADISKS_SIZE={min_disk_size}G",
                ]
            )

        ocp_release = self.dev_scripts_config.get("ocp_release_image")
        if not ocp_release and ocp_version:
            ocp_release = self._resolve_ocp_release_image(ocp_version)
        if ocp_release:
            config_lines.append(f"export OPENSHIFT_RELEASE_IMAGE={ocp_release}")

        config_lines.append("export OPENSHIFT_CI=true")
        if ocp_version:
            major_minor = ".".join(ocp_version.split(".")[:2])
            config_lines.append(f"export OPENSHIFT_VERSION={major_minor}")

        topology = self.dev_scripts_config.get("topology", "fencing-ipi")
        if "fencing" in topology:
            config_lines.append("export FENCING=true")
            config_lines.append("export BMC_DRIVER=redfish")

        config_content = "\n".join(config_lines) + "\n"

        with tempfile.NamedTemporaryFile(mode="w", suffix=".sh", delete=False) as f:
            f.write(config_content)
            local_config_path = f.name

        remote_config_path = f"{dev_scripts_dir}/config_{cluster_name}.sh"
        tmp_config_path = "/tmp/dev-scripts-config.sh"
        self.ssh_conn.upload_file(local_config_path, tmp_config_path)
        os.unlink(local_config_path)
        self._ssh_cmd(f"cp {tmp_config_path} {remote_config_path}")

        logger.info(f"dev-scripts config uploaded to {remote_config_path}")

    def run_dev_scripts(self, timeout=None):
        """
        Execute 'make' in the dev-scripts directory.
        This is the long-running step (~45-90 minutes).
        """
        timeout = timeout or constants.TNF_DEV_SCRIPTS_TIMEOUT
        dev_scripts_dir = constants.TNF_DEV_SCRIPTS_DIR
        cluster_name = self.dev_scripts_config.get("cluster_name", "tnf-cluster")

        transport = self.ssh_conn.client.get_transport()
        if transport:
            transport.set_keepalive(60)

        logger.info(f"Running dev-scripts (timeout: {timeout}s, ~45-90 min)...")

        config_file = f"config_{cluster_name}.sh"
        completion_marker = "/tmp/dev-scripts-complete"
        self._ssh_cmd(
            f"nohup bash -c '"
            f"export PATH=/usr/local/bin:$PATH && "
            f"cd {dev_scripts_dir} && "
            f"export CONFIG={config_file} && "
            f"make >> /tmp/dev-scripts.log 2>&1; "
            f"echo $? > {completion_marker}"
            f"' &",
            ignore_error=True,
        )

        logger.info("dev-scripts started in background, polling for completion...")
        for sample in TimeoutSampler(
            timeout=timeout,
            sleep=60,
            func=self._check_dev_scripts_done,
            completion_marker=completion_marker,
        ):
            if sample is not None:
                if sample != 0:
                    retcode, log_tail, _ = self._ssh_cmd(
                        "tail -50 /tmp/dev-scripts.log",
                        ignore_error=True,
                    )
                    raise CommandFailed(
                        f"dev-scripts failed (exit {sample}).\n"
                        f"Last 50 lines:\n{log_tail}"
                    )
                logger.info("dev-scripts completed successfully")
                return

    def _check_dev_scripts_done(self, completion_marker):
        retcode, stdout, _ = self.ssh_conn.exec_cmd(
            f"cat {completion_marker} 2>/dev/null"
        )
        if retcode == 0 and stdout.strip():
            return int(stdout.strip())
        return None

    def resize_vm_disks(self):
        """
        Resize OSD disks from initial min size to their intended sizes.

        All extra disks are created at the smallest size (for DRBD monitor)
        via VM_EXTRADISKS_SIZE. After dev-scripts completes, OSD disks are
        grown to their configured size using virsh blockresize on the
        running VMs. This avoids PCI slot exhaustion from hotplugging.
        """
        if not self._disk_resize_map:
            logger.info("No disk resizing needed")
            return

        cluster_name = self.dev_scripts_config.get("cluster_name", "tnf-cluster")
        num_masters = self.dev_scripts_config.get("num_masters", 2)

        for i in range(num_masters):
            domain = f"{cluster_name}_master_{i}"
            for dev_name, target_size in self._disk_resize_map.items():
                logger.info(f"Resizing {dev_name} on {domain} to {target_size}G")
                self._ssh_cmd(f"virsh blockresize {domain} {dev_name} {target_size}G")

        logger.info("VM disk resize complete")

    def setup_proxy(self):
        """
        Set up squid HTTP proxy on the hypervisor for external
        access to the OCP cluster API on the private libvirt network.
        """
        port = self.proxy_config.get("port", constants.TNF_HYPERVISOR_PROXY_PORT)
        logger.info(f"Setting up squid proxy on port {port}...")

        commands = [
            "dnf install -y squid",
            "sed -i 's/http_access deny all/http_access allow all/' /etc/squid/squid.conf",
            f"sed -i 's/^http_port .*/http_port {port}/' /etc/squid/squid.conf",
            "sed -i '/^acl SSL_ports/a acl SSL_ports port 6443' /etc/squid/squid.conf",
            f"firewall-cmd --permanent --add-port={port}/tcp",
            "firewall-cmd --reload",
            "systemctl enable --now squid",
        ]
        for cmd in commands:
            self._ssh_cmd(cmd, ignore_error=True)

        logger.info(f"Squid proxy running on {self.public_ip}:{port}")

    def get_proxy_url(self):
        port = self.proxy_config.get("port", constants.TNF_HYPERVISOR_PROXY_PORT)
        return f"http://{self.public_ip}:{port}"

    def retrieve_kubeconfig(self, local_auth_dir):
        """
        Download kubeconfig and kubeadmin-password from hypervisor.
        """
        cluster_name = self.dev_scripts_config.get("cluster_name", "tnf-cluster")
        remote_base = f"{constants.TNF_DEV_SCRIPTS_OCP_DIR}/{cluster_name}"

        files_to_download = {
            f"{remote_base}/auth/kubeconfig": os.path.join(
                local_auth_dir, "kubeconfig"
            ),
            f"{remote_base}/auth/kubeadmin-password": os.path.join(
                local_auth_dir, "kubeadmin-password"
            ),
        }

        os.makedirs(local_auth_dir, exist_ok=True)
        for remote_path, local_path in files_to_download.items():
            tmp_path = f"/tmp/{os.path.basename(remote_path)}"
            self._ssh_cmd(f"cp {remote_path} {tmp_path} && chmod 644 {tmp_path}")
            self.ssh_conn.download_file(tmp_path, local_path)
            logger.info(f"Downloaded {remote_path} -> {local_path}")
