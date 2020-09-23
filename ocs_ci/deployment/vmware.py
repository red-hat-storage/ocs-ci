"""
This module contains platform specific methods and classes for deployment
on vSphere platform
"""
import json
import logging
import os
import time

import hcl
import yaml

from ocs_ci.deployment.helpers.vsphere_helpers import VSPHEREHELPERS
from ocs_ci.deployment.install_ocp_on_rhel import OCPINSTALLRHEL
from ocs_ci.deployment.ocp import OCPDeployment as BaseOCPDeployment
from ocs_ci.deployment.terraform import Terraform
from ocs_ci.framework import config
from ocs_ci.ocs import constants, exceptions
from ocs_ci.ocs.exceptions import CommandFailed, RDMDiskNotFound
from ocs_ci.ocs.node import (
    get_node_ips, get_typed_worker_nodes, remove_nodes, wait_for_nodes_status
)
from ocs_ci.ocs.openshift_ops import OCP
from ocs_ci.utility.bootstrap import gather_bootstrap
from ocs_ci.utility.csr import (
    approve_pending_csr, wait_for_all_nodes_csr_and_approve
)
from ocs_ci.utility.load_balancer import LoadBalancer
from ocs_ci.utility.templating import (
    dump_data_to_json,
    Templating,
    json_to_dict,
)
from ocs_ci.utility.utils import (
    clone_repo, convert_yaml2tfvars, create_directory_path, read_file_as_str,
    replace_content_in_file, run_cmd, upload_file, wait_for_co,
    get_ocp_version, get_terraform, set_aws_region,
    configure_chrony_and_wait_for_machineconfig_status,
    get_terraform_ignition_provider, get_ocp_upgrade_history,
)
from ocs_ci.utility.vsphere import VSPHERE as VSPHEREUtil
from semantic_version import Version
from .deployment import Deployment

logger = logging.getLogger(__name__)


# As of now only UPI
__all__ = ['VSPHEREUPI']


class VSPHEREBASE(Deployment):

    # default storage class for StorageCluster CRD on VmWare platform
    DEFAULT_STORAGECLASS = "thin"

    def __init__(self):
        """
        This would be base for both IPI and UPI deployment
        """
        super(VSPHEREBASE, self).__init__()
        self.region = config.ENV_DATA['region']
        self.server = config.ENV_DATA['vsphere_server']
        self.user = config.ENV_DATA['vsphere_user']
        self.password = config.ENV_DATA['vsphere_password']
        self.cluster = config.ENV_DATA['vsphere_cluster']
        self.datacenter = config.ENV_DATA['vsphere_datacenter']
        self.datastore = config.ENV_DATA['vsphere_datastore']
        self.vsphere = VSPHEREUtil(self.server, self.user, self.password)
        self.upi_repo_path = os.path.join(
            constants.EXTERNAL_DIR,
            'installer'
        )
        self.upi_scale_up_repo_path = os.path.join(
            constants.EXTERNAL_DIR,
            'openshift-misc'
        )
        self.cluster_launcer_repo_path = os.path.join(
            constants.EXTERNAL_DIR,
            'cluster-launcher'
        )
        os.environ['TF_LOG'] = config.ENV_DATA.get('TF_LOG_LEVEL', "TRACE")
        os.environ['TF_LOG_PATH'] = os.path.join(
            config.ENV_DATA.get('cluster_path'),
            config.ENV_DATA.get('TF_LOG_FILE')
        )
        self.ocp_version = get_ocp_version()

        self.wait_time = 90

    def attach_disk(self, size=100, disk_type=constants.VM_DISK_TYPE):
        """
        Add a new disk to all the workers nodes

        Args:
            size (int): Size of disk in GB (default: 100)

        """
        vms = self.vsphere.get_all_vms_in_pool(
            config.ENV_DATA.get("cluster_name"),
            self.datacenter,
            self.cluster
        )
        # Add disks to all worker nodes
        for vm in vms:
            if "compute" in vm.name:
                self.vsphere.add_disks(
                    config.ENV_DATA.get("extra_disks", 1),
                    vm,
                    size,
                    disk_type
                )

    def add_nodes(self):
        """
        Add new nodes to the cluster
        """
        # create separate directory for scale-up terraform data
        scaleup_terraform_data_dir = os.path.join(
            self.cluster_path,
            constants.TERRAFORM_DATA_DIR,
            constants.SCALEUP_TERRAFORM_DATA_DIR
        )
        create_directory_path(scaleup_terraform_data_dir)
        logger.info(
            f"scale-up terraform data directory: {scaleup_terraform_data_dir}"
        )

        # git clone repo from openshift-misc
        clone_repo(
            constants.VSPHERE_SCALEUP_REPO, self.upi_scale_up_repo_path
        )

        # git clone repo from cluster-launcher
        clone_repo(
            constants.VSPHERE_CLUSTER_LAUNCHER, self.cluster_launcer_repo_path
        )

        helpers = VSPHEREHELPERS()
        helpers.modify_scaleup_repo()

        config.ENV_DATA['vsphere_resource_pool'] = config.ENV_DATA.get(
            "cluster_name"
        )

        # sync guest time with host
        sync_time_with_host_file = constants.SCALEUP_VSPHERE_MACHINE_CONF
        if config.ENV_DATA['folder_structure']:
            sync_time_with_host_file = os.path.join(
                constants.CLUSTER_LAUNCHER_VSPHERE_DIR,
                f"aos-{get_ocp_version(seperator='_')}",
                constants.CLUSTER_LAUNCHER_MACHINE_CONF
            )
        if config.ENV_DATA.get('sync_time_with_host'):
            sync_time_with_host(sync_time_with_host_file, True)

        # get the RHCOS worker list
        rhcos_ips = get_node_ips()
        logger.info(f"RHCOS IP's: {json.dumps(rhcos_ips)}")

        # generate terraform variable for scaling nodes
        self.scale_up_terraform_var = (
            helpers.generate_terraform_vars_for_scaleup(rhcos_ips)
        )

        # choose the vsphere_dir based on OCP version
        # generate cluster_info and config yaml files
        # for OCP version greater than 4.4
        vsphere_dir = constants.SCALEUP_VSPHERE_DIR
        rhel_module = "rhel-worker"
        if Version.coerce(self.ocp_version) >= Version.coerce('4.5'):
            vsphere_dir = os.path.join(
                constants.CLUSTER_LAUNCHER_VSPHERE_DIR,
                f"aos-{get_ocp_version('_')}",
                "vsphere"
            )
            helpers.generate_cluster_info()
            helpers.generate_config_yaml()
            rhel_module = "RHEL_WORKER_LIST"

        # Add nodes using terraform
        scaleup_terraform = Terraform(vsphere_dir)
        previous_dir = os.getcwd()
        os.chdir(scaleup_terraform_data_dir)
        scaleup_terraform.initialize()
        scaleup_terraform.apply(self.scale_up_terraform_var)
        scaleup_terraform_tfstate = os.path.join(
            scaleup_terraform_data_dir,
            "terraform.tfstate"
        )
        out = scaleup_terraform.output(
            scaleup_terraform_tfstate,
            rhel_module
        )
        if config.ENV_DATA['folder_structure']:
            rhel_worker_nodes = out.strip().replace("\"", '').split(",")
        else:
            rhel_worker_nodes = json.loads(out)['value']

        logger.info(f"RHEL worker nodes: {rhel_worker_nodes}")
        os.chdir(previous_dir)

        # Install OCP on rhel nodes
        rhel_install = OCPINSTALLRHEL(rhel_worker_nodes)
        rhel_install.prepare_rhel_nodes()
        rhel_install.execute_ansible_playbook()

        # Giving some time to settle down the new nodes
        time.sleep(self.wait_time)

        # wait for nodes to be in READY state
        wait_for_nodes_status(timeout=300)

    def delete_disks(self):
        """
        Delete the extra disks from all the worker nodes
        """
        vms = self.get_compute_vms(self.datacenter, self.cluster)
        if vms:
            for vm in vms:
                self.vsphere.remove_disks(vm)
        else:
            logger.debug("NO Resource Pool or VMs exists")

    def get_compute_vms(self, dc, cluster):
        """
        Gets the compute VM's from resource pool

        Args:
            dc (str): Datacenter name
            cluster (str): Cluster name

        Returns:
            list: VM instance

        """
        if self.vsphere.is_resource_pool_exist(
            config.ENV_DATA['cluster_name'],
            self.datacenter,
            self.cluster
        ):
            vms = self.vsphere.get_all_vms_in_pool(
                config.ENV_DATA.get("cluster_name"),
                dc,
                cluster
            )
            return [
                vm for vm in vms if "compute" in vm.name or "rhel" in vm.name
            ]

    def add_rdm_disks(self):
        """
        Attaches RDM disk to the compute nodes

        Raises:
            RDMDiskNotFound: In case there is no disks found on host

        """
        logger.info("Adding RDM disk to all compute nodes")
        datastore_type = self.vsphere.get_datastore_type_by_name(
            self.datastore,
            self.datacenter
        )

        compute_vms = self.get_compute_vms(self.datacenter, self.cluster)
        for vm in compute_vms:
            host = self.vsphere.get_host(vm)
            logger.info(f"{vm.name} belongs to host {host.name}")
            devices_available = self.vsphere.available_storage_devices(
                host,
                datastore_type=datastore_type
            )
            if not devices_available:
                raise RDMDiskNotFound

            # Erase the partition on the disk before adding to node
            device = devices_available[0]
            self.vsphere.erase_partition(host, device)

            # Attach RDM disk to node
            self.attach_rdm_disk(vm, device)

    def attach_rdm_disk(self, vm, device_name):
        """
        Attaches RDM disk to host

        Args:
            vm (vim.VirtualMachine): VM instance
            device_name (str): Device name to add to VM.
                e.g:"/vmfs/devices/disks/naa.600304801b540c0125ef160f3048faba"

        """
        self.vsphere.add_rdm_disk(vm, device_name)

    def post_destroy_checks(self):
        """
        Post destroy checks on cluster
        """
        pool = config.ENV_DATA['cluster_name']
        if self.vsphere.is_resource_pool_exist(
                pool,
                self.datacenter,
                self.cluster
        ):
            logger.warning(
                f"Resource pool {pool} exists even after destroying cluster"
            )
            self.vsphere.destroy_pool(pool, self.datacenter, self.cluster)
        else:
            logger.info(
                f"Resource pool {pool} does not exist in "
                f"cluster {self.cluster}"
            )

        # destroy the folder in templates
        self.vsphere.destroy_folder(pool, self.cluster, self.datacenter)

    def check_cluster_existence(self, cluster_name_prefix):
        """
        Check cluster existence according to cluster name prefix

        Args:
            cluster_name_prefix (str): The cluster name prefix to look for

        Returns:
            bool: True if a cluster with the same name prefix already exists,
                False otherwise

        """
        cluster_name_pattern = cluster_name_prefix
        rp_exist = self.vsphere.is_resource_pool_prefix_exist(
            cluster_name_pattern, self.datacenter, self.cluster
        )
        if rp_exist:
            logger.error(f"Resource pool with the prefix of {cluster_name_prefix} was found")
            return True
        else:
            return False


class VSPHEREUPI(VSPHEREBASE):
    """
    A class to handle vSphere UPI specific deployment
    """
    def __init__(self):
        super(VSPHEREUPI, self).__init__()
        self.ipam = config.ENV_DATA.get('ipam')
        self.token = config.ENV_DATA.get('ipam_token')
        self.cidr = config.ENV_DATA.get('machine_cidr')
        self.vm_network = config.ENV_DATA.get('vm_network')

    class OCPDeployment(BaseOCPDeployment):
        def __init__(self):
            super(VSPHEREUPI.OCPDeployment, self).__init__()
            self.public_key = {}
            self.upi_repo_path = os.path.join(
                constants.EXTERNAL_DIR,
                'installer'
            )
            self.previous_dir = os.getcwd()

            # get OCP version
            ocp_version = get_ocp_version()

            # create terraform_data directory
            self.terraform_data_dir = os.path.join(
                self.cluster_path,
                constants.TERRAFORM_DATA_DIR
            )
            create_directory_path(self.terraform_data_dir)

            # Download terraform binary based on ocp version and
            # update the installer path in ENV_DATA
            # use "0.11.14" for releases below OCP 4.5
            terraform_version = config.DEPLOYMENT['terraform_version']
            terraform_installer = get_terraform(version=terraform_version)
            config.ENV_DATA['terraform_installer'] = terraform_installer

            # Download terraform ignition provider
            # ignition provider dependancy from OCP 4.6
            if Version.coerce(ocp_version) >= Version.coerce('4.6'):
                get_terraform_ignition_provider(self.terraform_data_dir)

            # Initialize Terraform
            self.terraform_work_dir = constants.VSPHERE_DIR
            self.terraform = Terraform(self.terraform_work_dir)

            self.folder_structure = False
            if Version.coerce(ocp_version) >= Version.coerce('4.5'):
                self.folder_structure = True
                config.ENV_DATA['folder_structure'] = self.folder_structure

        def deploy_prereq(self):
            """
            Pre-Requisites for vSphere UPI Deployment
            """
            super(VSPHEREUPI.OCPDeployment, self).deploy_prereq()
            # create ignitions
            self.create_ignitions()
            self.kubeconfig = os.path.join(
                self.cluster_path,
                config.RUN.get('kubeconfig_location')
            )
            self.terraform_var = os.path.join(
                config.ENV_DATA['cluster_path'],
                constants.TERRAFORM_DATA_DIR,
                "terraform.tfvars"
            )

            # git clone repo from openshift installer
            clone_openshift_installer()

            # generate terraform variable file
            generate_terraform_vars_and_update_machine_conf()

            # sync guest time with host
            vm_file = (
                constants.VM_MAIN if self.folder_structure
                else constants.INSTALLER_MACHINE_CONF
            )
            if config.ENV_DATA.get('sync_time_with_host'):
                sync_time_with_host(vm_file, True)

        def create_config(self):
            """
            Creates the OCP deploy config for the vSphere
            """
            # Generate install-config from template
            _templating = Templating()
            ocp_install_template = (
                f"install-config-{self.deployment_platform}-"
                f"{self.deployment_type}.yaml.j2"
            )
            ocp_install_template_path = os.path.join(
                "ocp-deployment", ocp_install_template
            )
            install_config_str = _templating.render_template(
                ocp_install_template_path, config.ENV_DATA
            )

            # Parse the rendered YAML so that we can manipulate the object directly
            install_config_obj = yaml.safe_load(install_config_str)
            install_config_obj['pullSecret'] = self.get_pull_secret()
            install_config_obj['sshKey'] = self.get_ssh_key()
            install_config_str = yaml.safe_dump(install_config_obj)
            install_config = os.path.join(self.cluster_path, "install-config.yaml")
            with open(install_config, "w") as f:
                f.write(install_config_str)

        def create_ignitions(self):
            """
            Creates the ignition files
            """
            logger.info("creating ignition files for the cluster")
            run_cmd(
                f"{self.installer} create ignition-configs "
                f"--dir {self.cluster_path} "
            )

        def configure_storage_for_image_registry(self, kubeconfig):
            """
            Configures storage for the image registry
            """
            logger.info("configuring storage for image registry")
            patch = " '{\"spec\":{\"storage\":{\"emptyDir\":{}}}}' "
            run_cmd(
                f"oc --kubeconfig {kubeconfig} patch "
                f"configs.imageregistry.operator.openshift.io "
                f"cluster --type merge --patch {patch}"
            )

        def deploy(self, log_cli_level='DEBUG'):
            """
            Deployment specific to OCP cluster on this platform

            Args:
                log_cli_level (str): openshift installer's log level
                    (default: "DEBUG")

            """
            logger.info("Deploying OCP cluster for vSphere platform")
            logger.info(
                f"Openshift-installer will be using loglevel:{log_cli_level}"
            )
            os.chdir(self.terraform_data_dir)
            self.terraform.initialize()
            self.terraform.apply(self.terraform_var)
            os.chdir(self.previous_dir)
            logger.info("waiting for bootstrap to complete")
            try:
                run_cmd(
                    f"{self.installer} wait-for bootstrap-complete "
                    f"--dir {self.cluster_path} "
                    f"--log-level {log_cli_level}",
                    timeout=3600
                )
            except CommandFailed as e:
                if constants.GATHER_BOOTSTRAP_PATTERN in str(e):
                    try:
                        gather_bootstrap()
                    except Exception as ex:
                        logger.error(ex)
                raise e

            if self.folder_structure:
                # comment bootstrap module
                comment_bootstrap_in_lb_module()

                # remove bootstrap IP in load balancer and
                # restart haproxy
                lb = LoadBalancer()
                lb.remove_boostrap_in_proxy()
                lb.restart_haproxy()

            # remove bootstrap node
            if not config.DEPLOYMENT['preserve_bootstrap_node']:
                logger.info("removing bootstrap node")
                os.chdir(self.terraform_data_dir)
                if self.folder_structure:
                    self.terraform.destroy_module(
                        self.terraform_var,
                        constants.BOOTSTRAP_MODULE
                    )
                else:
                    self.terraform.apply(
                        self.terraform_var, bootstrap_complete=True
                    )
                os.chdir(self.previous_dir)

            OCP.set_kubeconfig(self.kubeconfig)

            # wait for all nodes to generate CSR
            # From OCP version 4.4 and above, we have to approve CSR manually
            # for all the nodes
            ocp_version = get_ocp_version()
            if Version.coerce(ocp_version) >= Version.coerce('4.4'):
                wait_for_all_nodes_csr_and_approve(timeout=1200, sleep=30)

            # wait for image registry to show-up
            co = "image-registry"
            wait_for_co(co)

            # patch image registry to null
            self.configure_storage_for_image_registry(self.kubeconfig)

            # wait for install to complete
            logger.info("waiting for install to complete")
            run_cmd(
                f"{self.installer} wait-for install-complete "
                f"--dir {self.cluster_path} "
                f"--log-level {log_cli_level}",
                timeout=1800
            )

            # Approving CSRs here in-case if any exists
            approve_pending_csr()

            self.test_cluster()

    def deploy_ocp(self, log_cli_level='DEBUG'):
        """
        Deployment specific to OCP cluster on vSphere platform

        Args:
            log_cli_level (str): openshift installer's log level
                (default: "DEBUG")

        """
        cluster_name_parts = config.ENV_DATA.get("cluster_name").split("-")
        prefix = cluster_name_parts[0]
        if not prefix.startswith(tuple(constants.PRODUCTION_JOBS_PREFIX)):
            if self.check_cluster_existence(prefix):
                raise exceptions.SameNamePrefixClusterAlreadyExistsException(
                    f"Cluster with name prefix {prefix} already exists. "
                    f"Please destroy the existing cluster for a new cluster "
                    f"deployment"
                )
        super(VSPHEREUPI, self).deploy_ocp(log_cli_level)
        if config.ENV_DATA.get('scale_up'):
            logger.info("Adding extra nodes to cluster")
            self.add_nodes()

        # remove RHCOS compute nodes
        if (
                config.ENV_DATA.get('scale_up')
                and not config.ENV_DATA.get('mixed_cluster')
        ):
            rhcos_nodes = get_typed_worker_nodes()
            logger.info(
                f"RHCOS compute nodes to delete: "
                f"{[node.name for node in rhcos_nodes]}"
            )
            logger.info("Removing RHCOS compute nodes from a cluster")
            remove_nodes(rhcos_nodes)

        # get datastore type and configure chrony for all nodes ONLY if
        # datstore type is vsan
        datastore_type = self.vsphere.get_datastore_type_by_name(
            self.datastore,
            self.datacenter
        )
        if datastore_type != constants.VMFS:
            configure_chrony_and_wait_for_machineconfig_status(
                node_type="all", timeout=1800
            )

    def destroy_cluster(self, log_level="DEBUG"):
        """
        Destroy OCP cluster specific to vSphere UPI

        Args:
            log_level (str): log level openshift-installer (default: DEBUG)

        """
        previous_dir = os.getcwd()

        # Download terraform binary based on terraform version
        # in terraform.log
        terraform_log_path = os.path.join(
            config.ENV_DATA.get('cluster_path'),
            config.ENV_DATA.get('TF_LOG_FILE')
        )

        # check for terraform.log, this check is for partially
        # deployed clusters
        try:
            with open(terraform_log_path, 'r') as fd:
                logger.debug(
                    f"Reading terraform version from {terraform_log_path}"
                )
                version_line = fd.readline()
                terraform_version = version_line.split()[-1]
        except FileNotFoundError:
            logger.debug(f"{terraform_log_path} file not found")
            terraform_version = config.DEPLOYMENT['terraform_version']

        terraform_installer = get_terraform(version=terraform_version)
        config.ENV_DATA['terraform_installer'] = terraform_installer

        # getting OCP version here since we run destroy job as
        # separate job in jenkins
        ocp_version = get_ocp_version()
        self.folder_structure = False
        if Version.coerce(ocp_version) >= Version.coerce('4.5'):
            set_aws_region()
            self.folder_structure = True
            config.ENV_DATA['folder_structure'] = self.folder_structure

        # delete the extra disks
        self.delete_disks()

        # check whether cluster has scale-up nodes
        scale_up_terraform_data_dir = os.path.join(
            self.cluster_path,
            constants.TERRAFORM_DATA_DIR,
            constants.SCALEUP_TERRAFORM_DATA_DIR
        )
        scale_up_terraform_var = os.path.join(
            scale_up_terraform_data_dir,
            "scale_up_terraform.tfvars"
        )
        if os.path.exists(scale_up_terraform_var):
            os.chdir(scale_up_terraform_data_dir)
            self.destroy_scaleup_nodes(
                scale_up_terraform_data_dir,
                scale_up_terraform_var
            )
            os.chdir(previous_dir)

        terraform_data_dir = os.path.join(self.cluster_path, constants.TERRAFORM_DATA_DIR)
        upi_repo_path = os.path.join(
            constants.EXTERNAL_DIR, 'installer',
        )
        tfvars = os.path.join(
            config.ENV_DATA.get('cluster_path'),
            constants.TERRAFORM_DATA_DIR,
            constants.TERRAFORM_VARS
        )

        clone_openshift_installer()
        if (
            os.path.exists(f"{constants.VSPHERE_MAIN}.backup")
            and os.path.exists(f"{constants.VSPHERE_MAIN}.json")
        ):
            os.rename(f"{constants.VSPHERE_MAIN}.json", f"{constants.VSPHERE_MAIN}.json.backup")

        # terraform initialization and destroy cluster
        terraform = Terraform(os.path.join(upi_repo_path, "upi/vsphere/"))
        os.chdir(terraform_data_dir)
        if Version.coerce(ocp_version) >= Version.coerce('4.6'):
            # Download terraform ignition provider. For OCP upgrade clusters,
            # ignition provider doesn't exist, so downloading in destroy job
            # as well
            terraform_plugins_path = ".terraform/plugins/linux_amd64/"
            terraform_ignition_provider_path = os.path.join(
                terraform_data_dir,
                terraform_plugins_path,
                "terraform-provider-ignition"
            )

            # check the upgrade history of cluster and checkout to the
            # original installer release. This is due to the issue of not
            # supporting terraform state of OCP 4.5 in installer
            # release of 4.6 branch. More details in
            # https://github.com/red-hat-storage/ocs-ci/issues/2941
            is_cluster_upgraded = False
            try:
                upgrade_history = get_ocp_upgrade_history()
                if len(upgrade_history) > 1:
                    is_cluster_upgraded = True
                    original_installed_ocp_version = upgrade_history[-1]
                    installer_release_branch = (
                        f"release-{original_installed_ocp_version[0:3]}"
                    )
                    clone_repo(
                        constants.VSPHERE_INSTALLER_REPO, upi_repo_path,
                        installer_release_branch
                    )
            except Exception as ex:
                logger.error(ex)

            if not (
                os.path.exists(terraform_ignition_provider_path)
                or is_cluster_upgraded
            ):
                get_terraform_ignition_provider(terraform_data_dir)
            terraform.initialize()
        else:
            terraform.initialize(upgrade=True)
        terraform.destroy(tfvars, refresh=(not self.folder_structure))
        os.chdir(previous_dir)

        # post destroy checks
        self.post_destroy_checks()

    def destroy_scaleup_nodes(self, scale_up_terraform_data_dir, scale_up_terraform_var):
        """
        Destroy the scale-up nodes

        Args:
            scale_up_terraform_data_dir (str): Path to scale-up terraform
                data directory
            scale_up_terraform_var (str): Path to scale-up
                terraform.tfvars file

        """
        clone_repo(
            constants.VSPHERE_SCALEUP_REPO, self.upi_scale_up_repo_path
        )
        # git clone repo from cluster-launcher
        clone_repo(
            constants.VSPHERE_CLUSTER_LAUNCHER, self.cluster_launcer_repo_path
        )

        # modify scale-up repo
        helpers = VSPHEREHELPERS()
        helpers.modify_scaleup_repo()

        vsphere_dir = constants.SCALEUP_VSPHERE_DIR
        if Version.coerce(self.ocp_version) >= Version.coerce('4.5'):
            vsphere_dir = os.path.join(
                constants.CLUSTER_LAUNCHER_VSPHERE_DIR,
                f"aos-{get_ocp_version('_')}",
                "vsphere"
            )

        terraform_scale_up = Terraform(vsphere_dir)
        os.chdir(scale_up_terraform_data_dir)
        terraform_scale_up.initialize(upgrade=True)
        terraform_scale_up.destroy(scale_up_terraform_var)


def change_vm_root_disk_size(machine_file):
    """
    Change the root disk size of VM from constants.CURRENT_VM_ROOT_DISK_SIZE
    to constants.VM_ROOT_DISK_SIZE

    Args:
         machine_file (str): machine file to change the disk size
    """
    disk_size_prefix = "size             = "
    current_vm_root_disk_size = f"{disk_size_prefix}{constants.CURRENT_VM_ROOT_DISK_SIZE}"
    vm_root_disk_size = f"{disk_size_prefix}{constants.VM_ROOT_DISK_SIZE}"
    replace_content_in_file(
        machine_file,
        current_vm_root_disk_size,
        vm_root_disk_size
    )


def sync_time_with_host(machine_file, enable=False):
    """
    Syncs the guest time with host

    Args:
         machine_file (str): machine file to sync the guest time with host
         enable (bool): True to sync guest time with host

    """
    # terraform will support only lowercase bool
    enable = str(enable).lower()
    to_change = 'enable_disk_uuid = "true"'
    sync_time = f"{to_change}\n sync_time_with_host = \"{enable}\""

    replace_content_in_file(
        machine_file,
        to_change,
        sync_time
    )


def clone_openshift_installer():
    """
    Clone the openshift installer repo
    """
    # git clone repo from openshift installer
    # installer ( https://github.com/openshift/installer ) master and
    # other branches (greater than release-4.3) structure has been
    # changed. Use appropriate branch when ocs-ci is ready
    # with the changes.
    # Note: Currently use release-4.3 branch for the ocp versions
    # which is greater than 4.3
    upi_repo_path = os.path.join(
        constants.EXTERNAL_DIR,
        'installer'
    )
    ocp_version = get_ocp_version()
    # supporting folder structure from ocp4.5
    if Version.coerce(ocp_version) >= Version.coerce('4.5'):
        clone_repo(
            constants.VSPHERE_INSTALLER_REPO, upi_repo_path,
            f'release-{ocp_version}'
        )
    elif Version.coerce(ocp_version) == Version.coerce('4.4'):
        clone_repo(
            constants.VSPHERE_INSTALLER_REPO, upi_repo_path,
            constants.VSPHERE_INSTALLER_BRANCH
        )
    else:
        clone_repo(
            constants.VSPHERE_INSTALLER_REPO, upi_repo_path,
            f'release-{ocp_version}'
        )


def change_mem_and_cpu():
    """
    Increase CPUs and memory for nodes
    """
    worker_num_cpus = config.ENV_DATA.get('worker_num_cpus')
    master_num_cpus = config.ENV_DATA.get('master_num_cpus')
    worker_memory = config.ENV_DATA.get('compute_memory')
    master_memory = config.ENV_DATA.get('master_memory')
    if (
            worker_num_cpus
            or master_num_cpus
            or master_memory
            or worker_memory
    ):
        with open(constants.VSPHERE_MAIN, 'r') as fd:
            obj = hcl.load(fd)
            if worker_num_cpus:
                obj['module']['compute']['num_cpu'] = worker_num_cpus
            if master_num_cpus:
                obj['module']['control_plane']['num_cpu'] = master_num_cpus
            if worker_memory:
                obj['module']['compute']['memory'] = worker_memory
            if master_memory:
                obj['module']['control_plane']['memory'] = master_memory
        # Dump data to json file since hcl module
        # doesn't support dumping of data in HCL format
        dump_data_to_json(obj, f"{constants.VSPHERE_MAIN}.json")
        os.rename(constants.VSPHERE_MAIN, f"{constants.VSPHERE_MAIN}.backup")


def update_gw(str_to_replace, config_file):
    """
    Updates the gateway

    Args:
        str_to_replace (str): string to replace in config file
        config_file (str): file to replace the string

    """
    # update gateway
    if config.ENV_DATA.get('gateway'):
        replace_content_in_file(
            config_file,
            str_to_replace,
            f"{config.ENV_DATA.get('gateway')}"
        )


def update_dns():
    """
    Updates the DNS
    """
    # update DNS
    if config.ENV_DATA.get('dns'):
        replace_content_in_file(
            constants.INSTALLER_IGNITION,
            constants.INSTALLER_DEFAULT_DNS,
            f"{config.ENV_DATA.get('dns')}"
        )


def update_zone():
    """
    Updates the zone in constants.INSTALLER_ROUTE53
    """
    # update the zone in route
    if config.ENV_DATA.get('region'):
        def_zone = 'provider "aws" { region = "%s" } \n' % config.ENV_DATA.get('region')
        replace_content_in_file(constants.INSTALLER_ROUTE53, "xyz", def_zone)


def update_path():
    """
    Updates Path to var.folder in resource vsphere_folder
    """
    logger.debug(f"Updating path to var.folder in {constants.VSPHERE_MAIN}")
    replace_str = "path          = var.cluster_id"
    replace_content_in_file(
        constants.VSPHERE_MAIN,
        replace_str,
        "path          = var.folder"
    )


def add_var_folder():
    """
    Add folder variable to vsphere variables.tf
    """
    # read the variables.tf data to var_data
    with open(constants.VSPHERE_VAR, "r") as fd:
        var_data = fd.read()

    # backup the variables.tf
    os.rename(constants.VSPHERE_VAR, f"{constants.VSPHERE_VAR}.backup")

    # write var_data along with folder variable at the end of file
    with open(constants.VSPHERE_VAR, "w+") as fd:
        fd.write(var_data)
        fd.write('\nvariable "folder" {\n')
        fd.write('  type    = string\n')
        fd.write('}\n')


def update_machine_conf(folder_structure=True):
    """
    Updates the machine configurations

    Args:
        folder_structure (bool): True if folder structure installations.
            Currently True for OCP release greater than 4.4 versions

    """
    if not folder_structure:
        gw_string = "${cidrhost(var.machine_cidr,1)}"
        gw_conf_file = constants.INSTALLER_IGNITION
        disk_size_conf_file = constants.INSTALLER_MACHINE_CONF
        # update dns
        update_dns()

        # update the zone in route
        update_zone()

        # increase CPUs and memory
        change_mem_and_cpu()

    else:
        gw_string = "${cidrhost(machine_cidr, 1)}"
        gw_conf_file = constants.VM_IFCFG
        disk_size_conf_file = constants.VM_MAIN

        # change cluster ID to folder
        update_path()

        # Add variable folder to variables.tf
        add_var_folder()

    # update gateway
    update_gw(gw_string, gw_conf_file)

    # change root disk size
    change_vm_root_disk_size(disk_size_conf_file)


def generate_terraform_vars_and_update_machine_conf():
    """
    Generates the terraform.tfvars file
    """
    ocp_version = get_ocp_version()
    folder_structure = False
    if Version.coerce(ocp_version) >= Version.coerce('4.5'):

        folder_structure = True
        # export AWS_REGION
        set_aws_region()

        # generate terraform variable file
        generate_terraform_vars_with_folder()

        # update the machine configurations
        update_machine_conf(folder_structure)

        if Version.coerce(ocp_version) >= Version.coerce('4.5'):
            modify_haproxyservice()
    else:
        # generate terraform variable file
        generate_terraform_vars_with_out_folder()

        # update the machine configurations
        update_machine_conf(folder_structure)


def generate_terraform_vars_with_folder():
    """
    Generates the terraform.tfvars file which includes folder structure
    """
    # generate terraform variables from template
    logger.info("Generating terraform variables with folder structure")
    cluster_domain = (
        f"{config.ENV_DATA.get('cluster_name')}."
        f"{config.ENV_DATA.get('base_domain')}"
    )
    config.ENV_DATA['cluster_domain'] = cluster_domain

    # Form the ignition paths
    bootstrap_ignition_path = os.path.join(
        config.ENV_DATA['cluster_path'],
        constants.BOOTSTRAP_IGN
    )
    control_plane_ignition_path = os.path.join(
        config.ENV_DATA['cluster_path'],
        constants.MASTER_IGN
    )
    compute_ignition_path = os.path.join(
        config.ENV_DATA['cluster_path'],
        constants.WORKER_IGN
    )

    # Update ignition paths to ENV_DATA
    config.ENV_DATA['bootstrap_ignition_path'] = bootstrap_ignition_path
    config.ENV_DATA['control_plane_ignition_path'] = (
        control_plane_ignition_path
    )
    config.ENV_DATA['compute_ignition_path'] = compute_ignition_path

    # Copy DNS address to vm_dns_addresses
    config.ENV_DATA['vm_dns_addresses'] = config.ENV_DATA['dns']

    # Get the infra ID from metadata.json and update in ENV_DATA
    metadata_path = os.path.join(
        config.ENV_DATA['cluster_path'],
        "metadata.json"
    )
    metadata_dct = json_to_dict(metadata_path)
    config.ENV_DATA['folder'] = metadata_dct['infraID']

    # expand ssh_public_key_path and update in ENV_DATA
    ssh_public_key_path = os.path.expanduser(
        config.DEPLOYMENT['ssh_key']
    )
    config.ENV_DATA['ssh_public_key_path'] = ssh_public_key_path

    create_terraform_var_file("terraform_4_5.tfvars.j2")


def create_terraform_var_file(terraform_var_template):
    """
    Creates the terraform variable file from jinja template

    Args:
        terraform_var_template (str): terraform template in jinja format

    """
    _templating = Templating()
    terraform_var_template_path = os.path.join(
        "ocp-deployment", terraform_var_template
    )
    terraform_config_str = _templating.render_template(
        terraform_var_template_path, config.ENV_DATA
    )

    terraform_var_yaml = os.path.join(
        config.ENV_DATA['cluster_path'],
        constants.TERRAFORM_DATA_DIR,
        "terraform.tfvars.yaml"
    )
    with open(terraform_var_yaml, "w") as f:
        f.write(terraform_config_str)

    convert_yaml2tfvars(terraform_var_yaml)


def generate_terraform_vars_with_out_folder():
    """
    Generates the normal ( old structure ) terraform.tfvars file
    """
    logger.info("Generating terraform variables without folder structure")

    # upload bootstrap ignition to public access server
    bootstrap_path = os.path.join(
        config.ENV_DATA.get('cluster_path'),
        constants.BOOTSTRAP_IGN
    )
    remote_path = os.path.join(
        config.ENV_DATA.get('path_to_upload'),
        f"{config.RUN.get('run_id')}_{constants.BOOTSTRAP_IGN}"
    )
    upload_file(
        config.ENV_DATA.get('httpd_server'),
        bootstrap_path,
        remote_path,
        config.ENV_DATA.get('httpd_server_user'),
        config.ENV_DATA.get('httpd_server_password')
    )

    # generate bootstrap ignition url
    path_to_bootstrap_on_remote = remote_path.replace("/var/www/html/", "")
    bootstrap_ignition_url = (
        f"http://{config.ENV_DATA.get('httpd_server')}/"
        f"{path_to_bootstrap_on_remote}"
    )
    logger.info(f"bootstrap_ignition_url: {bootstrap_ignition_url}")
    config.ENV_DATA['bootstrap_ignition_url'] = bootstrap_ignition_url

    # load master and worker ignitions to variables
    master_ignition_path = os.path.join(
        config.ENV_DATA.get('cluster_path'),
        constants.MASTER_IGN
    )
    master_ignition = read_file_as_str(f"{master_ignition_path}")
    config.ENV_DATA['control_plane_ignition'] = master_ignition

    worker_ignition_path = os.path.join(
        config.ENV_DATA.get('cluster_path'),
        constants.WORKER_IGN
    )
    worker_ignition = read_file_as_str(f"{worker_ignition_path}")
    config.ENV_DATA['compute_ignition'] = worker_ignition

    cluster_domain = (
        f"{config.ENV_DATA.get('cluster_name')}."
        f"{config.ENV_DATA.get('base_domain')}"
    )
    config.ENV_DATA['cluster_domain'] = cluster_domain

    # generate terraform variables from template
    create_terraform_var_file("terraform.tfvars.j2")


def comment_bootstrap_in_lb_module():
    """
    Commenting the bootstrap module in vsphere main.tf
    """
    logger.debug(f"Commenting bootstrap module in {constants.VSPHERE_MAIN}")
    replace_str = "module.ipam_bootstrap.ip_addresses[0]"
    replace_content_in_file(
        constants.VSPHERE_MAIN,
        replace_str,
        f"//{replace_str}"
    )


def modify_haproxyservice():
    """
    Add ExecStop in haproxy service
    """
    to_change = 'TimeoutStartSec=0'
    execstop = f"{to_change}\nExecStop=/bin/podman rm -f haproxy"

    replace_content_in_file(
        constants.TERRAFORM_HAPROXY_SERVICE,
        to_change,
        execstop
    )
