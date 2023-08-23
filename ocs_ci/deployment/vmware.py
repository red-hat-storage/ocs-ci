"""
This module contains platform specific methods and classes for deployment
on vSphere platform
"""
import glob
import json
import logging
import os
from shutil import rmtree, copyfile
from subprocess import TimeoutExpired
import time
import requests
import base64
from ping3 import ping
import tempfile
import hcl2
import yaml
import re
import shutil

from ocs_ci.deployment.helpers.vsphere_helpers import VSPHEREHELPERS
from ocs_ci.deployment.helpers.prechecks import VSpherePreChecks
from ocs_ci.deployment.helpers.external_cluster_helpers import (
    ExternalCluster,
    get_external_cluster_client,
    remove_csi_users,
)
from ocs_ci.deployment.install_ocp_on_rhel import OCPINSTALLRHEL
from ocs_ci.deployment.ocp import OCPDeployment as BaseOCPDeployment
from ocs_ci.deployment.terraform import Terraform
from ocs_ci.framework import config
from ocs_ci.ocs import constants, defaults, exceptions
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    RDMDiskNotFound,
    PassThroughEnabledDeviceNotFound,
)
from ocs_ci.ocs.node import (
    get_node_ips,
    get_typed_worker_nodes,
    remove_nodes,
    wait_for_nodes_status,
)
from ocs_ci.utility import templating, version
from ocs_ci.ocs.openshift_ops import OCP
from ocs_ci.ocs.resources.pod import (
    get_mon_pods,
    get_deployment_name,
    get_osd_pods,
    get_osd_prepare_pods,
    delete_pods,
)
from ocs_ci.ocs.resources.pv import get_all_pvs
from ocs_ci.ocs.resources.pvc import (
    delete_pvcs,
    get_all_pvc_objs,
)
from ocs_ci.utility.aws import AWS
from ocs_ci.utility.bootstrap import gather_bootstrap
from ocs_ci.utility.csr import approve_pending_csr, wait_for_all_nodes_csr_and_approve
from ocs_ci.utility.ipam import IPAM
from ocs_ci.utility.load_balancer import LoadBalancer
from ocs_ci.utility.retry import retry
from ocs_ci.utility.templating import (
    dump_data_to_json,
    Templating,
    json_to_dict,
)
from ocs_ci.utility.utils import (
    clone_repo,
    convert_yaml2tfvars,
    create_directory_path,
    read_file_as_str,
    replace_content_in_file,
    run_cmd,
    upload_file,
    wait_for_co,
    get_infra_id,
    get_ocp_version,
    get_openshift_installer,
    get_terraform,
    set_aws_region,
    get_terraform_ignition_provider,
    get_ocp_upgrade_history,
    add_chrony_to_ocp_deployment,
)
from ocs_ci.utility.vsphere import VSPHERE as VSPHEREUtil
from semantic_version import Version
from .deployment import Deployment
from .flexy import FlexyVSPHEREUPI
from ocs_ci.utility.vsphere import VSPHERE
from ocs_ci.utility.connection import Connection
from ocs_ci.ocs.exceptions import ConnectivityFail

logger = logging.getLogger(__name__)


# As of now only UPI
__all__ = ["VSPHEREUPI", "VSPHEREIPI"]


class VSPHEREBASE(Deployment):

    # default storage class for StorageCluster CRD on VmWare platform
    if version.get_semantic_ocp_version_from_config() >= version.VERSION_4_13:
        if config.ENV_DATA.get("use_custom_sc_in_deployment"):
            CUSTOM_STORAGE_CLASS_PATH = os.path.join(
                constants.TEMPLATE_DEPLOYMENT_DIR, "storageclass_thin-csi-odf.yaml"
            )
        else:
            DEFAULT_STORAGECLASS = "thin-csi"
    else:
        DEFAULT_STORAGECLASS = "thin"

    def __init__(self):
        """
        This would be base for both IPI and UPI deployment
        """
        super(VSPHEREBASE, self).__init__()
        self.region = config.ENV_DATA["region"]
        self.server = config.ENV_DATA["vsphere_server"]
        self.user = config.ENV_DATA["vsphere_user"]
        self.password = config.ENV_DATA["vsphere_password"]
        self.cluster = config.ENV_DATA["vsphere_cluster"]
        self.datacenter = config.ENV_DATA["vsphere_datacenter"]
        self.datastore = config.ENV_DATA["vsphere_datastore"]
        self.vsphere = VSPHEREUtil(self.server, self.user, self.password)
        self.upi_repo_path = os.path.join(constants.EXTERNAL_DIR, "installer")
        self.upi_scale_up_repo_path = os.path.join(
            constants.EXTERNAL_DIR, "openshift-misc"
        )
        self.cluster_launcer_repo_path = os.path.join(
            constants.EXTERNAL_DIR, "v4-scaleup"
        )
        os.environ["TF_LOG"] = config.ENV_DATA.get("TF_LOG_LEVEL", "TRACE")
        os.environ["TF_LOG_PATH"] = os.path.join(
            config.ENV_DATA.get("cluster_path"), config.ENV_DATA.get("TF_LOG_FILE")
        )

        # pre-checks for the vSphere environment
        # skip pre-checks for destroying cluster
        teardown = config.RUN["cli_params"].get("teardown")
        if not teardown:
            vsphere_prechecks = VSpherePreChecks()
            vsphere_prechecks.get_all_checks()

        self.ocp_version = get_ocp_version()
        config.ENV_DATA["ocp_version"] = self.ocp_version
        config.ENV_DATA[
            "ocp_version_object"
        ] = version.get_semantic_ocp_version_from_config()
        config.ENV_DATA["version_4_9_object"] = version.VERSION_4_9

        self.wait_time = 90

    def attach_disk(self, size=100, disk_type=constants.VM_DISK_TYPE):
        """
        Add a new disk to all the workers nodes

        Args:
            size (int): Size of disk in GB (default: 100)

        """
        vms = self.vsphere.get_all_vms_in_pool(
            config.ENV_DATA.get("cluster_name"), self.datacenter, self.cluster
        )
        # Add disks to all worker nodes
        for vm in vms:
            if "compute" in vm.name:
                self.vsphere.add_disks(
                    config.ENV_DATA.get("extra_disks", 1), vm, size, disk_type
                )

    def add_nodes(self):
        """
        Add new nodes to the cluster
        """
        # create separate directory for scale-up terraform data
        scaleup_terraform_data_dir = os.path.join(
            self.cluster_path,
            constants.TERRAFORM_DATA_DIR,
            constants.SCALEUP_TERRAFORM_DATA_DIR,
        )
        create_directory_path(scaleup_terraform_data_dir)
        logger.info(f"scale-up terraform data directory: {scaleup_terraform_data_dir}")

        # git clone repo from openshift-misc
        clone_repo(constants.VSPHERE_SCALEUP_REPO, self.upi_scale_up_repo_path)

        # git clone repo from v4-scaleup
        clone_repo(constants.VSPHERE_CLUSTER_LAUNCHER, self.cluster_launcer_repo_path)

        helpers = VSPHEREHELPERS()
        helpers.modify_scaleup_repo()

        config.ENV_DATA["vsphere_resource_pool"] = config.ENV_DATA.get("cluster_name")

        # sync guest time with host
        sync_time_with_host_file = constants.SCALEUP_VSPHERE_MACHINE_CONF
        if config.ENV_DATA["folder_structure"]:
            sync_time_with_host_file = os.path.join(
                constants.CLUSTER_LAUNCHER_VSPHERE_DIR,
                f"aos-{get_ocp_version(seperator='_')}",
                constants.CLUSTER_LAUNCHER_MACHINE_CONF,
            )
        if config.ENV_DATA.get("sync_time_with_host"):
            sync_time_with_host(sync_time_with_host_file, True)

        # get the RHCOS worker list
        rhcos_ips = get_node_ips()
        logger.info(f"RHCOS IP's: {json.dumps(rhcos_ips)}")

        # generate terraform variable for scaling nodes
        self.scale_up_terraform_var = helpers.generate_terraform_vars_for_scaleup(
            rhcos_ips
        )

        # choose the vsphere_dir based on OCP version
        # generate cluster_info and config yaml files
        # for OCP version greater than 4.4
        vsphere_dir = constants.SCALEUP_VSPHERE_DIR
        rhel_module = "rhel-worker"
        if Version.coerce(self.ocp_version) >= Version.coerce("4.5"):
            vsphere_dir = os.path.join(
                constants.CLUSTER_LAUNCHER_VSPHERE_DIR,
                f"aos-{get_ocp_version('_')}",
                "vsphere",
            )
            helpers.generate_cluster_info()
            helpers.generate_config_yaml()
            rhel_module = "RHEL_WORKER_LIST"

        # Add nodes using terraform
        scaleup_terraform_tfstate = os.path.join(
            scaleup_terraform_data_dir, "terraform.tfstate"
        )
        scaleup_terraform = Terraform(
            vsphere_dir, state_file_path=scaleup_terraform_tfstate
        )
        previous_dir = os.getcwd()
        os.chdir(scaleup_terraform_data_dir)
        scaleup_terraform.initialize()
        scaleup_terraform.apply(self.scale_up_terraform_var)

        out = scaleup_terraform.output(scaleup_terraform_tfstate, rhel_module)
        if config.ENV_DATA["folder_structure"]:
            rhel_worker_nodes = out.strip().replace('"', "").split(",")
        else:
            rhel_worker_nodes = json.loads(out)["value"]

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
        Delete the extra disks from all the worker nodes and is sno delete it from sno nodes which is compute also
        """
        if config.ENV_DATA["sno"]:
            vms = self.get_vms_by_string(self.datacenter, self.cluster, "sno")
        else:
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
            config.ENV_DATA["cluster_name"], self.datacenter, self.cluster
        ):
            vms = self.vsphere.get_all_vms_in_pool(
                config.ENV_DATA.get("cluster_name"), dc, cluster
            )
            return [vm for vm in vms if "compute" in vm.name or "rhel" in vm.name]

    def get_vms_by_string(self, dc, cluster, vm_string_to_match):
        """
        Gets the sno VM's from resource pool

        Args:
            dc (str): Datacenter name
            cluster (str): Cluster name
            vm_string_to_match (str): string to match the VM's like "compute", "sno" etc

        Returns:
            list: VM instance

        """
        if self.vsphere.is_resource_pool_exist(
            config.ENV_DATA["cluster_name"], self.datacenter, self.cluster
        ):
            vms = self.vsphere.get_all_vms_in_pool(
                config.ENV_DATA.get("cluster_name"), dc, cluster
            )
            return [vm for vm in vms if vm_string_to_match in vm.name]

    def add_rdm_disks(self):
        """
        Attaches RDM disk to the compute nodes

        Raises:
            RDMDiskNotFound: In case there is no disks found on host

        """
        logger.info("Adding RDM disk to all compute nodes")
        datastore_type = self.vsphere.get_datastore_type_by_name(
            self.datastore, self.datacenter
        )

        compute_vms = self.get_compute_vms(self.datacenter, self.cluster)
        for vm in compute_vms:
            host = self.vsphere.get_host(vm)
            logger.info(f"{vm.name} belongs to host {host.name}")
            devices_available = self.vsphere.available_storage_devices(
                host, datastore_type=datastore_type
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

    def add_pci_devices(self):
        """
        Attach PCI devices to compute nodes

        Raises:
            PassThroughEnabledDeviceNotFound: In case there is no passthrough enabled device
                not found on host

        """
        logger.info("Adding PCI devices to all compute nodes")
        compute_vms = self.get_compute_vms(self.datacenter, self.cluster)
        for vm in compute_vms:
            passthrough_enabled_device = self.vsphere.get_passthrough_enabled_devices(
                vm
            )[0]
            if not passthrough_enabled_device:
                raise PassThroughEnabledDeviceNotFound

            # power off the VM before adding PCI device
            self.vsphere.poweroff_vms([vm])

            # add PCI device
            self.vsphere.add_pci_device(vm, passthrough_enabled_device)

            # power on the VM
            self.vsphere.poweron_vms([vm])

    def post_destroy_checks(self):
        """
        Post destroy checks on cluster
        """
        pool = config.ENV_DATA["cluster_name"]
        if self.vsphere.is_resource_pool_exist(pool, self.datacenter, self.cluster):
            logger.warning(f"Resource pool {pool} exists even after destroying cluster")
            self.vsphere.destroy_pool(pool, self.datacenter, self.cluster)
        else:
            logger.info(
                f"Resource pool {pool} does not exist in " f"cluster {self.cluster}"
            )

        # destroy the folder in templates
        template_folder = get_infra_id(self.cluster_path)
        self.vsphere.destroy_folder(template_folder, self.cluster, self.datacenter)

        # remove .terraform directory ( this is only to reclaim space )
        terraform_plugins_dir = os.path.join(
            config.ENV_DATA["cluster_path"],
            constants.TERRAFORM_DATA_DIR,
            constants.TERRAFORM_PLUGINS_DIR,
        )
        rmtree(terraform_plugins_dir, ignore_errors=True)

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
            logger.error(
                f"Resource pool with the prefix of {cluster_name_prefix} was found"
            )
            return True
        else:
            return False


class VSPHEREUPI(VSPHEREBASE):
    """
    A class to handle vSphere UPI specific deployment
    """

    def __init__(self):
        super(VSPHEREUPI, self).__init__()
        self.ipam = config.ENV_DATA.get("ipam")
        self.token = config.ENV_DATA.get("ipam_token")
        self.cidr = config.ENV_DATA.get("machine_cidr")
        self.vm_network = config.ENV_DATA.get("vm_network")

    class OCPDeployment(BaseOCPDeployment):
        def __init__(self):
            super(VSPHEREUPI.OCPDeployment, self).__init__()
            self.public_key = {}
            self.upi_repo_path = os.path.join(constants.EXTERNAL_DIR, "installer")
            self.previous_dir = os.getcwd()

            # get OCP version
            ocp_version = get_ocp_version()

            # create terraform_data directory
            self.terraform_data_dir = os.path.join(
                self.cluster_path, constants.TERRAFORM_DATA_DIR
            )
            create_directory_path(self.terraform_data_dir)

            # Download terraform binary based on ocp version and
            # update the installer path in ENV_DATA
            # use "0.11.14" for releases below OCP 4.5
            terraform_version = config.DEPLOYMENT["terraform_version"]
            terraform_installer = get_terraform(version=terraform_version)
            config.ENV_DATA["terraform_installer"] = terraform_installer

            # Download terraform ignition provider
            # ignition provider dependancy from OCP 4.6
            if Version.coerce(ocp_version) >= Version.coerce("4.6"):
                get_terraform_ignition_provider(
                    self.terraform_data_dir, version=get_ignition_provider_version()
                )

            # Initialize Terraform
            self.terraform_work_dir = constants.VSPHERE_DIR
            self.terraform = Terraform(self.terraform_work_dir)

            self.folder_structure = False
            if Version.coerce(ocp_version) >= Version.coerce("4.5"):
                self.folder_structure = True
                config.ENV_DATA["folder_structure"] = self.folder_structure

        def deploy_prereq(self):
            """
            Pre-Requisites for vSphere UPI Deployment
            """
            super(VSPHEREUPI.OCPDeployment, self).deploy_prereq()
            # generate manifests
            if not self.sno:
                self.generate_manifests()

            # create chrony resource
            if (
                Version.coerce(get_ocp_version()) >= Version.coerce("4.5")
                and not self.sno
            ):
                add_chrony_to_ocp_deployment()

            # create ignitions
            self.create_ignitions()
            self.kubeconfig = os.path.join(
                self.cluster_path, config.RUN.get("kubeconfig_location")
            )
            self.terraform_var = os.path.join(
                config.ENV_DATA["cluster_path"],
                constants.TERRAFORM_DATA_DIR,
                "terraform.tfvars",
            )

            # git clone repo from openshift installer
            clone_openshift_installer()

            # comment sensitive variable as current terraform version doesn't support
            if version.get_semantic_ocp_version_from_config() >= version.VERSION_4_11:
                comment_sensitive_var()

            # generate terraform variable file
            generate_terraform_vars_and_update_machine_conf()

            # Add shutdown_wait_timeout to VM's
            add_shutdown_wait_timeout()

            # sync guest time with host
            vm_file = (
                constants.VM_MAIN
                if self.folder_structure
                else constants.INSTALLER_MACHINE_CONF
            )
            if config.ENV_DATA.get("sync_time_with_host"):
                sync_time_with_host(vm_file, True)

        def create_sno_iso_and_upload(self):
            """
            Creating iso file with values for SNO deployment

            """
            # Getting ip from ipam
            ipam = IPAM(
                "address", config.ENV_DATA["ipam"], config.ENV_DATA["ipam_token"]
            )
            subnet = config.ENV_DATA["machine_cidr"].split("/")[0]
            config.ENV_DATA["vm_ip_address"] = ipam.assign_ip(
                f"{constants.SNO_NODE_NAME}.{config.ENV_DATA['cluster_name']}", subnet
            )
            logger.info(f"IP from ipam: {config.ENV_DATA['vm_ip_address']}")
            self.change_ignition_ip_and_hostname(config.ENV_DATA["vm_ip_address"])
            os.chdir(self.cluster_path)
            iso_url_data = run_cmd(f"{self.installer} coreos print-stream-json")
            resp = json.loads(iso_url_data)
            iso_url = resp["architectures"]["x86_64"]["artifacts"]["metal"]["formats"][
                "iso"
            ]["disk"]["location"]
            sno_image_name = f"{self.cluster_name}.iso"
            logger.info(f"Downloading rh-cos is {iso_url}")
            run_cmd(f"curl -L -o {sno_image_name} {iso_url}")

            # Installing and compiling coreos-installer with prereq
            run_cmd(f"curl -o {self.cluster_path}/rustup.sh {constants.RUST_URL} -sSf")
            os.chmod(f"{self.cluster_path}/rustup.sh", 448)
            run_cmd(f"{self.cluster_path}/rustup.sh -y")
            os.environ["PATH"] += os.pathsep + f"{os.path.expanduser('~')}/.cargo/bin"
            os.chdir(f"{constants.EXTERNAL_DIR}")
            coreos_installer_repo_path = os.path.join(
                constants.EXTERNAL_DIR,
                "coreos-installer",
            )
            clone_repo(
                url=constants.COREOS_INSTALLER_REPO,
                location=coreos_installer_repo_path,
                branch="main",
            )
            if os.path.isdir(f"{constants.EXTERNAL_DIR}/coreos-install"):
                shutil.rmtree(f"{constants.EXTERNAL_DIR}/coreos-install")
            os.mkdir(f"{constants.EXTERNAL_DIR}/coreos-install")
            os.chdir(f"{constants.EXTERNAL_DIR}/coreos-installer/")
            run_cmd("git checkout tags/v0.14.0")
            run_cmd("make")
            run_cmd(f"make install DESTDIR={constants.EXTERNAL_DIR}/coreos-install/")
            os.chdir(f"{self.cluster_path}")
            coreos_installer_exec = (
                f"{constants.EXTERNAL_DIR}/coreos-install/usr/bin/coreos-installer"
            )
            logger.info("coreos-installler installed successfully")
            run_cmd(
                f"{coreos_installer_exec} iso ignition embed -fi iso.ign {sno_image_name}"
            )

            # connecteing to vsanDataStore and uploading iso file
            vsphere = VSPHERE(
                config.ENV_DATA["vsphere_server"],
                config.ENV_DATA["vsphere_user"],
                config.ENV_DATA["vsphere_password"],
            )

            vsphere_content = vsphere.get_content
            client_cookie = vsphere_content.propertyCollector._stub.soapStub.cookie
            logger.info(f"this is the client cookie {client_cookie}")
            remote_file = sno_image_name
            resource = "/folder/ISO/" + remote_file
            params = {
                "dsName": config.ENV_DATA["vsphere_datastore"],
                "dcPath": config.ENV_DATA["vsphere_datacenter"],
            }
            http_url = (
                "https://" + config.ENV_DATA["vsphere_server"] + ":443" + resource
            )
            cookie_name = client_cookie.split("=", 1)[0]
            cookie_value = client_cookie.split("=", 1)[1].split(";", 1)[0]
            cookie_path = (
                client_cookie.split("=", 1)[1]
                .split(";", 1)[1]
                .split(";", 1)[0]
                .lstrip()
            )
            cookie_text = " " + cookie_value + "; $" + cookie_path
            # # Make a cookie
            cookie = dict()
            cookie[cookie_name] = cookie_text
            logger.info(cookie)
            verify_cert = False
            headers = {"Content-Type": "application/octet-stream"}
            with open(f"{self.cluster_path}/{sno_image_name}", "rb") as file_data:
                requests.put(
                    http_url,
                    params=params,
                    data=file_data,
                    headers=headers,
                    cookies=cookie,
                    verify=verify_cert,
                )
            logger.info(f"{sno_image_name} uploaded successfully to the VsanDataStore")
            logger.info(f"Removing iso {sno_image_name} from {self.cluster_path}")
            full_sno_image_path = os.path.join(self.cluster_path, sno_image_name)
            os.remove(full_sno_image_path)

        def change_ignition_ip_and_hostname(self, ip_address):
            """
            Embed into iso.ign ip address and hostname (sno-edge-0)
            args:
                ip_address (str): ip address we got from IPAM to embed inside iso"

            """
            logger.info(f"Changing {constants.SNO_BOOTSTRAP_IGN}")
            with open(f"{self.cluster_path}/{constants.SNO_BOOTSTRAP_IGN}", "r") as fn:
                ign_data = json.load(fn)
            gw = config.ENV_DATA["gateway"]
            dns = config.ENV_DATA["dns"]
            logger.info(f"adding {ip_address} and hostname into bootkube.sh")
            bootkube_source = []
            new_files = []
            for file in ign_data["storage"]["files"]:
                if file["path"] == "/usr/local/bin/bootkube.sh":
                    bootkube_source.append(
                        f'nmcli connection modify "Wired connection 1" '
                        f"ifname ens192 ipv4.method manual ipv4.addresses"
                        f" {ip_address}/23 gw4 {gw} ipv4.dns {dns}"
                        f' ipv4.dns-search "{self.cluster_name}.qe.rh-ocs.com"'
                    )
                    bootkube_source.append(
                        'nmcli con up "Wired connection 1" ifname ens192'
                    )
                    bootkube_source.append(
                        f"hostnamectl set-hostname {constants.SNO_NODE_NAME}"
                    )
                    pattern = re.compile(".*base64,(.*)")
                    result = pattern.search(file["contents"]["source"])
                    bootkube_base64_enc = result.group(1)
                    bootkube_base64_enc_encode_ascii = bootkube_base64_enc.encode(
                        "ascii"
                    )
                    bootkube_base64_decode = base64.b64decode(
                        bootkube_base64_enc_encode_ascii
                    )
                    bootkube_base64_enc_encode_clean = bootkube_base64_decode.decode(
                        "ascii"
                    )
                    bootkube_base64_script_array = (
                        bootkube_base64_enc_encode_clean.split("\n")
                    )
                    final_script_array = []
                    for script_line in bootkube_base64_script_array:
                        if "#!/usr/bin/env bash" in script_line:
                            final_script_array.append(script_line)
                            for nmcli_line in bootkube_source:
                                final_script_array.append(nmcli_line)
                            continue
                        final_script_array.append(script_line)
                    final_script = "\n".join(final_script_array)
                    final_script_ascii_enc = final_script.encode("ascii")
                    final_script_base64_ascii_encode = base64.b64encode(
                        final_script_ascii_enc
                    )
                    final_script_ready = final_script_base64_ascii_encode.decode(
                        "ascii"
                    )
                    source_new_content = (
                        f"data:text/plain;charset=utf-8;base64,{final_script_ready}"
                    )
                    file["contents"]["source"] = source_new_content
                    new_files.append(file)
                    continue
                new_files.append(file)
            ign_data["storage"]["files"] = new_files
            logger.info(f"adding {ip_address} to system network-manager")
            all_lines = []
            ens_file = open(
                f"{constants.TEMPLATE_DIR}/ocp-deployment/ens192.nmconnection", "r"
            )
            for line in ens_file:
                line = line.strip()
                line = line.replace("address=", f"address={ip_address}/23,{gw}")
                line = line.replace("dns=", f"dns={dns}")
                all_lines.append(line)
            one_line_ens = "\n".join(all_lines)
            one_line_ens_bytes = one_line_ens.encode("utf-8")
            base64ens = base64.b64encode(one_line_ens_bytes).decode("utf-8")

            ens_json_to_add = (
                f'{{"overwrite": True, "path": "'
                f'/etc/NetworkManager/system-connections/Wired connection 1.nmconnection"'
                f', "user": {{"name": "root"}}, "contents": {{"source": "data:text/plain;'
                f'charset=utf-8;base64,{base64ens}"}}, "mode": 420}}'
            )
            ens_dict = eval(ens_json_to_add)
            ign_data["storage"]["files"].append(ens_dict)
            hostname = constants.SNO_NODE_NAME
            logger.info(f"Adding hostname {hostname} to /etc/hostname")
            encode_hostname = hostname.encode("utf-8")
            encode64_hostname = base64.b64encode(encode_hostname).decode("utf-8")
            hostname_json_to_add = (
                f'{{"overwrite": True, "path": "/etc/hostname",'
                f' "user": {{"name": "root"}}, "contents": {{"source": "data'
                f':text/plain;charset=utf-8;base64,{encode64_hostname}"}}, "mode": 420}}'
            )
            hostname_dict = eval(hostname_json_to_add)

            ign_data["storage"]["files"].append(hostname_dict)

            with open(f"{self.cluster_path}/iso.ign", "w") as ign_new_file:
                json.dump(ign_data, ign_new_file)

        def wait_for_sno_second_boot_change_ip_and_hostname(self, ip_address):
            """
            After second boot ocp is booting with the right ip address but after a while the ip is changed to dhcp.
            We monitor the ip address and when it changed we ssh to the node and change back the ip address and hostname
            args:
                ip_address (str): The ip address given from the IPAM server
            raises:
                ConnectivityFail: Incase after the change we ping the ip_address. If it doesn't reply we raise.

            """

            # Connect to Vcenter
            vsphere = VSPHERE(
                config.ENV_DATA["vsphere_server"],
                config.ENV_DATA["vsphere_user"],
                config.ENV_DATA["vsphere_password"],
            )
            # Get the VM object
            vm = vsphere.get_vm_in_pool_by_name(
                name=constants.SNO_NODE_NAME,
                dc=config.ENV_DATA["vsphere_datacenter"],
                cluster=config.ENV_DATA["vsphere_cluster"],
                pool=self.cluster_name,
            )
            gw = config.ENV_DATA["gateway"]
            dns = config.ENV_DATA["dns"]
            # Check the ip if it changes and when, there are few stages. first boot, None when server reboots,
            # then second boot starts with defined ip but can be changed and we monitor that.
            # If the ip is not changed after 20 retries we just change the hostname.
            boot_got_ip_address = 0
            got_none = 0
            destination_boot_counter = 0
            for i in range(0, 250):
                ips = vsphere.get_vms_ips([vm])
                ip = ips[0]
                logger.info(f"try num {i} the ip is {ip}")
                if ip == ip_address:
                    boot_got_ip_address = 1

                if ip is None and boot_got_ip_address == 1:
                    got_none = 1
                if got_none == 1 and ip == ip_address:
                    destination_boot_counter += 1
                if destination_boot_counter > 20:
                    logger.info(
                        f"{constants.SNO_NODE_NAME} stayed with same ip address {ip_address}- changing hostname"
                    )
                    node_ssh = Connection(
                        host=ip,
                        user="core",
                        private_key=f"{os.path.expanduser('~')}/.ssh/openshift-dev.pem",
                    )
                    node_ssh.exec_cmd('echo "sleep 3" > changenet.sh')
                    node_ssh.exec_cmd(
                        f'echo "hostnamectl set-hostname {constants.SNO_NODE_NAME}" >> changenet.sh'
                    )
                    node_ssh.exec_cmd("chmod 700 changenet.sh")
                    node_ssh.exec_cmd(
                        "sudo nohup ./changenet.sh </dev/null &>/dev/null &"
                    )
                    break
                if ip != ip_address and ip is not None and boot_got_ip_address == 1:
                    node_ssh = Connection(
                        host=ip,
                        user="core",
                        private_key=f"{os.path.expanduser('~')}/.ssh/openshift-dev.pem",
                    )
                    node_ssh.exec_cmd('echo "sleep 3" > changenet.sh')
                    node_ssh.exec_cmd(
                        f"echo \"nmcli connection modify 'Wired connection 1' "
                        f"ifname ens192 ipv4.method manual ipv4.addresses"
                        f" {ip_address}/23 gw4 {gw} ipv4.dns {dns}"
                        f" ipv4.dns-search '{self.cluster_name}.qe.rh-ocs.com'\" >> changenet.sh"
                    )
                    node_ssh.exec_cmd(
                        "echo \"nmcli con up 'Wired connection 1' ifname ens192\" >> changenet.sh"
                    )

                    node_ssh.exec_cmd(
                        f'echo "hostnamectl set-hostname {constants.SNO_NODE_NAME}" >> changenet.sh'
                    )
                    node_ssh.exec_cmd("chmod 700 changenet.sh")
                    node_ssh.exec_cmd(
                        "sudo nohup ./changenet.sh </dev/null &>/dev/null &"
                    )
                    break
                time.sleep(1)

            for i in range(0, 20):
                result = ping(ip_address)
                if result is not None:
                    logger.info(f"ip is changed to {ip_address}")
                    break
                time.sleep(1)
                if i == 20:
                    raise ConnectivityFail(f"ip {ip_address} is not reachable")

        def create_config(self):
            """
            Creates the OCP deploy config for the vSphere
            """
            # Generate install-config from template
            _templating = Templating()

            if self.sno:
                ocp_install_template = (
                    f"install-config-{self.deployment_platform}-"
                    f"{self.deployment_type}-sno.yaml.j2"
                )
            else:
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
            if (
                version.get_semantic_ocp_version_from_config() >= version.VERSION_4_10
                and not self.sno
            ):
                install_config_obj["platform"]["vsphere"]["network"] = config.ENV_DATA[
                    "vm_network"
                ]
            install_config_obj["pullSecret"] = self.get_pull_secret()
            install_config_obj["sshKey"] = self.get_ssh_key()
            install_config_str = yaml.safe_dump(install_config_obj)
            install_config = os.path.join(self.cluster_path, "install-config.yaml")
            with open(install_config, "w") as f:
                f.write(install_config_str)

        def generate_manifests(self):
            """
            Generates manifest files
            """
            logger.info("creating manifest files for the cluster")
            run_cmd(f"{self.installer} create manifests --dir {self.cluster_path}")

            # remove machines and machinesets
            # Some of the manifests produced are for creating machinesets
            # and machine objects. We should remove these, because we don't
            # want to involve the machine-API operator and
            # machine-api-operator during install.
            manifest_files_path = os.path.join(self.cluster_path, "openshift")
            files_to_remove = glob.glob(
                f"{manifest_files_path}/99_openshift-cluster-api_"
                f"master-machines-*.yaml"
            )
            files_to_remove.extend(
                glob.glob(
                    f"{manifest_files_path}/99_openshift-cluster-api_"
                    f"worker-machineset-*.yaml"
                )
            )
            logger.debug(f"Removing machines and machineset files: {files_to_remove}")
            for each_file in files_to_remove:
                os.remove(each_file)

        def create_ignitions(self):
            """
            Creates the ignition files
            """
            logger.info("creating ignition files for the cluster")
            if not self.sno:
                run_cmd(
                    f"{self.installer} create ignition-configs "
                    f"--dir {self.cluster_path} "
                )
            else:
                copyfile(
                    f"{self.cluster_path}/install-config.yaml",
                    f"{self.cluster_path}/install-config.yaml.bck",
                )
                run_cmd(
                    f"{self.installer} create single-node-ignition-config "
                    f"--dir {self.cluster_path} "
                )
                copyfile(
                    f"{self.cluster_path}/install-config.yaml.bck",
                    f"{self.cluster_path}/install-config.yaml",
                )
                self.create_sno_iso_and_upload()

        @retry(exceptions.CommandFailed, tries=10, delay=30, backoff=1)
        def configure_storage_for_image_registry(self, kubeconfig):
            """
            Configures storage for the image registry
            """
            logger.info("configuring storage for image registry")
            patch = ' \'{"spec":{"storage":{"emptyDir":{}}}}\' '
            run_cmd(
                f"oc --kubeconfig {kubeconfig} patch "
                f"configs.imageregistry.operator.openshift.io "
                f"cluster --type merge --patch {patch}"
            )

        def deploy(self, log_cli_level="DEBUG"):
            """
            Deployment specific to OCP cluster on this platform

            Args:
                log_cli_level (str): openshift installer's log level
                    (default: "DEBUG")

            """
            logger.info("Deploying OCP cluster for vSphere platform")
            logger.info(f"Openshift-installer will be using loglevel:{log_cli_level}")
            os.chdir(self.terraform_data_dir)
            self.terraform.initialize()
            self.terraform.apply(self.terraform_var)
            if config.ENV_DATA["sno"]:
                self.wait_for_sno_second_boot_change_ip_and_hostname(
                    config.ENV_DATA["vm_ip_address"]
                )
                run_cmd(
                    f"{self.installer} wait-for install-complete"
                    f" --dir {self.cluster_path} "
                    f"--log-level {log_cli_level}",
                    timeout=3600,
                )
                vsphere = VSPHERE(
                    config.ENV_DATA["vsphere_server"],
                    config.ENV_DATA["vsphere_user"],
                    config.ENV_DATA["vsphere_password"],
                )
                vm = vsphere.get_vm_in_pool_by_name(
                    name=constants.SNO_NODE_NAME,
                    dc=config.ENV_DATA["vsphere_datacenter"],
                    cluster=config.ENV_DATA["vsphere_cluster"],
                    pool=config.ENV_DATA["cluster_name"],
                )

                vsphere.add_disks(
                    config.DEPLOYMENT["lvmo_disks"],
                    vm,
                    config.DEPLOYMENT["lvmo_disks_size"],
                )

            os.chdir(self.previous_dir)
            if not self.sno:
                logger.info("waiting for bootstrap to complete")
                try:
                    run_cmd(
                        f"{self.installer} wait-for bootstrap-complete "
                        f"--dir {self.cluster_path} "
                        f"--log-level {log_cli_level}",
                        timeout=3600,
                    )
                except (CommandFailed, TimeoutExpired) as e:
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
                    lb.rename_haproxy_conf_and_reload()
                    lb.remove_boostrap_in_proxy()
                    lb.restart_haproxy()

                # remove bootstrap node
                if not config.DEPLOYMENT["preserve_bootstrap_node"]:
                    logger.info("removing bootstrap node")
                    bootstrap_module_to_remove = constants.BOOTSTRAP_MODULE_413
                    if (
                        version.get_semantic_ocp_version_from_config()
                        < version.VERSION_4_13
                    ):
                        bootstrap_module_to_remove = constants.BOOTSTRAP_MODULE
                    os.chdir(self.terraform_data_dir)
                    if self.folder_structure:
                        self.terraform.destroy_module(
                            self.terraform_var, bootstrap_module_to_remove
                        )
                    else:
                        self.terraform.apply(
                            self.terraform_var, bootstrap_complete=True
                        )
                    os.chdir(self.previous_dir)

            OCP.set_kubeconfig(self.kubeconfig)
            if not config.ENV_DATA["sno"]:
                timeout = 1800
                # wait for all nodes to generate CSR
                # From OCP version 4.4 and above, we have to approve CSR manually
                # for all the nodes
                ocp_version = get_ocp_version()
                if Version.coerce(ocp_version) >= Version.coerce("4.4"):
                    wait_for_all_nodes_csr_and_approve(timeout=1500, sleep=10)

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
                    timeout=timeout,
                )

                # Approving CSRs here in-case if any exists
                approve_pending_csr()

            self.test_cluster()

    def deploy_ocp(self, log_cli_level="DEBUG"):
        """
        Deployment specific to OCP cluster on vSphere platform

        Args:
            log_cli_level (str): openshift installer's log level
                (default: "DEBUG")

        """
        cluster_name_parts = config.ENV_DATA.get("cluster_name").split("-")
        prefix = cluster_name_parts[0]
        if not (
            prefix.startswith(tuple(constants.PRODUCTION_JOBS_PREFIX))
            or config.DEPLOYMENT.get("force_deploy_multiple_clusters")
        ):
            if self.check_cluster_existence(prefix):
                raise exceptions.SameNamePrefixClusterAlreadyExistsException(
                    f"Cluster with name prefix {prefix} already exists. "
                    f"Please destroy the existing cluster for a new cluster "
                    f"deployment"
                )
        super(VSPHEREUPI, self).deploy_ocp(log_cli_level)
        if config.ENV_DATA.get("scale_up"):
            logger.info("Adding extra nodes to cluster")
            self.add_nodes()

        # remove RHCOS compute nodes
        if config.ENV_DATA.get("scale_up") and not config.ENV_DATA.get("mixed_cluster"):
            rhcos_nodes = get_typed_worker_nodes()
            logger.info(
                f"RHCOS compute nodes to delete: "
                f"{[node.name for node in rhcos_nodes]}"
            )
            logger.info("Removing RHCOS compute nodes from a cluster")
            remove_nodes(rhcos_nodes)

            # remove ingress-router for RHCOS compute nodes on load balancer
            # set the tfstate file
            config.ENV_DATA["terraform_state_file"] = os.path.join(
                config.ENV_DATA["cluster_path"], "terraform_data", "terraform.tfstate"
            )
            lb = LoadBalancer()
            lb.remove_compute_node_in_proxy()
            lb.restart_haproxy()

            # sleep for few seconds after restarting haproxy
            time.sleep(self.wait_time)

        if config.DEPLOYMENT.get("thick_sc"):
            sc_data = templating.load_yaml(constants.VSPHERE_THICK_STORAGECLASS_YAML)
            sc_data_yaml = tempfile.NamedTemporaryFile(
                mode="w+", prefix="storageclass", delete=False
            )
            if config.DEPLOYMENT.get("eager_zeroed_thick_sc"):
                sc_data["parameters"]["diskformat"] = "eagerzeroedthick"
            else:
                sc_data["parameters"]["diskformat"] = "zeroedthick"
            templating.dump_data_to_temp_yaml(sc_data, sc_data_yaml.name)
            run_cmd(f"oc create -f {sc_data_yaml.name}")
            self.DEFAULT_STORAGECLASS = "thick"

    def destroy_cluster(self, log_level="DEBUG"):
        """
        Destroy OCP cluster specific to vSphere UPI

        Args:
            log_level (str): log level openshift-installer (default: DEBUG)

        """
        previous_dir = os.getcwd()

        # Download terraform binary based on terraform version
        # in terraform.log

        terraform_version = Terraform.get_terraform_version()

        terraform_installer = get_terraform(version=terraform_version)
        config.ENV_DATA["terraform_installer"] = terraform_installer

        # getting OCP version here since we run destroy job as
        # separate job in jenkins
        ocp_version = get_ocp_version()
        self.folder_structure = False
        if Version.coerce(ocp_version) >= Version.coerce("4.5"):
            set_aws_region()
            self.folder_structure = True
            config.ENV_DATA["folder_structure"] = self.folder_structure

        # removing mon and osd pods and also removing PVC's to avoid stale CNS volumes
        try:
            self.scale_down_pods_and_remove_pvcs()
        except Exception as err:
            logger.warning(
                f"Failed to scale down mon/osd pods or failed to remove PVC's. Error: {err}"
            )

        # delete the extra disks
        self.delete_disks()

        # check whether cluster has scale-up nodes
        scale_up_terraform_data_dir = os.path.join(
            self.cluster_path,
            constants.TERRAFORM_DATA_DIR,
            constants.SCALEUP_TERRAFORM_DATA_DIR,
        )
        scale_up_terraform_var = os.path.join(
            scale_up_terraform_data_dir, "scale_up_terraform.tfvars"
        )
        if os.path.exists(scale_up_terraform_var):
            os.chdir(scale_up_terraform_data_dir)
            self.destroy_scaleup_nodes(
                scale_up_terraform_data_dir, scale_up_terraform_var
            )
            os.chdir(previous_dir)

        terraform_data_dir = os.path.join(
            self.cluster_path, constants.TERRAFORM_DATA_DIR
        )
        upi_repo_path = os.path.join(
            constants.EXTERNAL_DIR,
            "installer",
        )
        tfvars = os.path.join(
            config.ENV_DATA.get("cluster_path"),
            constants.TERRAFORM_DATA_DIR,
            constants.TERRAFORM_VARS,
        )

        clone_openshift_installer()
        if config.ENV_DATA["sno"]:
            add_var_folder()

        # comment sensitive variable as current terraform version doesn't support
        if version.get_semantic_ocp_version_from_config() >= version.VERSION_4_11:
            comment_sensitive_var()

        rename_files = [constants.VSPHERE_MAIN, constants.VM_MAIN]
        for each_file in rename_files:
            if os.path.exists(f"{each_file}.backup") and os.path.exists(
                f"{each_file}.json"
            ):
                os.rename(
                    f"{each_file}.json",
                    f"{each_file}.json.backup",
                )

        # change the keep_on_remove state to false
        terraform_tfstate = os.path.join(terraform_data_dir, "terraform.tfstate")
        str_to_modify = '"keep_on_remove": true,'
        target_str = '"keep_on_remove": false,'
        logger.debug(f"changing state from {str_to_modify} to {target_str}")
        replace_content_in_file(terraform_tfstate, str_to_modify, target_str)

        # remove csi users in case of external deployment
        if config.DEPLOYMENT["external_mode"]:
            logger.debug("deleting csi users")
            # In some cases where deployment of external cluster is failed, external tool box doesn't exist
            try:
                # remove csi users
                remove_csi_users()

                # get all PV's
                pvs = get_all_pvs()
                pvs_to_delete = [
                    each_pv["spec"]["csi"]["volumeAttributes"]["imageName"]
                    for each_pv in pvs["items"]
                    if each_pv["spec"]["csi"]["volumeAttributes"]["pool"] == "rbd"
                ]

            except exceptions.CephToolBoxNotFoundException:
                logger.warning(
                    "Failed to setup the Ceph toolbox pod. Probably due to installation was not successful"
                )
            except CommandFailed:
                logger.warning(
                    "Failed to remove CSI users. Probably ceph toolbox is not in running state due to "
                    "installation was not successful or it is not configured correctly"
                )

        # terraform initialization and destroy cluster
        terraform = Terraform(os.path.join(upi_repo_path, "upi/vsphere/"))
        os.chdir(terraform_data_dir)
        if Version.coerce(ocp_version) >= Version.coerce("4.6"):
            # Download terraform ignition provider. For OCP upgrade clusters,
            # ignition provider doesn't exist, so downloading in destroy job
            # as well
            terraform_provider_ignition_version = None
            terraform_plugins_path = ".terraform/plugins/linux_amd64/"
            if version.get_semantic_ocp_version_from_config() >= version.VERSION_4_11:
                terraform_provider_ignition_file = "terraform-provider-ignition_v2.1.2"
            else:
                terraform_provider_ignition_file = "terraform-provider-ignition"

            # check the upgrade history of cluster and checkout to the
            # original installer release. This is due to the issue of not
            # supporting terraform state of OCP 4.5 in installer
            # release of 4.6 branch. More details in
            # https://github.com/red-hat-storage/ocs-ci/issues/2941
            try:
                upgrade_history = get_ocp_upgrade_history()
                if len(upgrade_history) > 1:
                    original_installed_ocp_version = upgrade_history[-1]
                    original_installed_ocp_version_major_minor_obj = (
                        version.get_semantic_version(
                            original_installed_ocp_version, only_major_minor=True
                        )
                    )
                    original_installed_ocp_version_major_minor = str(
                        original_installed_ocp_version_major_minor_obj
                    )
                    installer_release_branch = (
                        f"release-{original_installed_ocp_version_major_minor}"
                    )
                    clone_repo(
                        url=constants.VSPHERE_INSTALLER_REPO,
                        location=upi_repo_path,
                        branch=installer_release_branch,
                        force_checkout=True,
                    )

                    # comment sensitive variable as current terraform version doesn't support
                    if (
                        version.get_semantic_ocp_version_from_config()
                        >= version.VERSION_4_11
                    ):
                        comment_sensitive_var()

                    if (
                        original_installed_ocp_version_major_minor_obj
                        == version.VERSION_4_10
                    ):
                        config.ENV_DATA[
                            "original_installed_ocp_version_major_minor_obj"
                        ] = version.VERSION_4_10
                        terraform_provider_ignition_version = (
                            constants.TERRAFORM_IGNITION_PROVIDER_VERSION
                        )
                        terraform_provider_ignition_file = "terraform-provider-ignition"
            except Exception as ex:
                logger.error(ex)

            terraform_ignition_provider_path = os.path.join(
                terraform_data_dir,
                terraform_plugins_path,
                terraform_provider_ignition_file,
            )
            if not os.path.exists(terraform_ignition_provider_path):
                get_terraform_ignition_provider(
                    terraform_data_dir,
                    version=terraform_provider_ignition_version
                    or get_ignition_provider_version(),
                )
            terraform.initialize()
        else:
            terraform.initialize(upgrade=True)
        terraform.destroy(tfvars, refresh=(not self.folder_structure))
        os.chdir(previous_dir)

        if config.DEPLOYMENT["external_mode"]:
            try:
                rbd_name = config.ENV_DATA.get("rbd_name") or defaults.RBD_NAME
                # get external cluster details
                host, user, password, ssh_key = get_external_cluster_client()
                external_cluster = ExternalCluster(host, user, password, ssh_key)
                external_cluster.remove_rbd_images(pvs_to_delete, rbd_name)
            except Exception as ex:
                logger.warning(
                    f"Failed to remove rbd images, probably due to installation was not successful. Error: {ex}"
                )

        # release IPAM ip from sno
        if config.ENV_DATA["sno"]:
            ipam = IPAM(appiapp="address")
            hosts = [f"{constants.SNO_NODE_NAME}.{config.ENV_DATA['cluster_name']}"]
            ipam.release_ips(hosts)

        # post destroy checks
        self.post_destroy_checks()

    def scale_down_pods_and_remove_pvcs(self):
        """
        Removes the mon and osd pods and also removes PVC's
        """
        # scale down mon pods
        namespace = config.ENV_DATA["cluster_namespace"]
        mon_pod_obj_list = get_mon_pods()
        for mon_pod_obj in mon_pod_obj_list:
            mon_deployment_name = get_deployment_name(mon_pod_obj.name)
            run_cmd(
                f"oc scale deployment {mon_deployment_name} --replicas=0 -n {namespace}"
            )

        # scale down osd pods
        osd_pod_obj_list = get_osd_pods()
        for osd_pod_obj in osd_pod_obj_list:
            osd_deployment_name = get_deployment_name(osd_pod_obj.name)
            run_cmd(
                f"oc scale deployment {osd_deployment_name} --replicas=0 -n {namespace}"
            )

        # delete osd-prepare pods
        osd_prepare_pod_obj_list = get_osd_prepare_pods()
        delete_pods(osd_prepare_pod_obj_list)

        # delete PVC's
        pvcs_objs = get_all_pvc_objs(namespace=namespace)
        for pvc_obj in pvcs_objs:
            if pvc_obj.backed_sc == "thin-csi" or pvc_obj.backed_sc == "thin-csi-odf":
                pvc_name = pvc_obj.name
                pv_name = pvc_obj.backed_pv

                # set finalizers to null for both pvc and pv
                pvc_patch_cmd = (
                    f"oc patch pvc {pvc_name} -n {namespace} -p "
                    '\'{"metadata":{"finalizers":null}}\''
                )
                run_cmd(pvc_patch_cmd)
                pv_patch_cmd = (
                    f"oc patch pv {pv_name} -n {namespace} -p "
                    '\'{"metadata":{"finalizers":null}}\''
                )
                run_cmd(pv_patch_cmd)

                time.sleep(10)
                delete_pvcs([pvc_obj])

    def destroy_scaleup_nodes(
        self, scale_up_terraform_data_dir, scale_up_terraform_var
    ):
        """
        Destroy the scale-up nodes

        Args:
            scale_up_terraform_data_dir (str): Path to scale-up terraform
                data directory
            scale_up_terraform_var (str): Path to scale-up
                terraform.tfvars file

        """
        clone_repo(constants.VSPHERE_SCALEUP_REPO, self.upi_scale_up_repo_path)
        # git clone repo from v4-scaleup
        clone_repo(constants.VSPHERE_CLUSTER_LAUNCHER, self.cluster_launcer_repo_path)

        # modify scale-up repo
        helpers = VSPHEREHELPERS()
        helpers.modify_scaleup_repo()

        vsphere_dir = constants.SCALEUP_VSPHERE_DIR
        if Version.coerce(self.ocp_version) >= Version.coerce("4.5"):
            vsphere_dir = os.path.join(
                constants.CLUSTER_LAUNCHER_VSPHERE_DIR,
                f"aos-{get_ocp_version('_')}",
                "vsphere",
            )

        scaleup_terraform_tfstate = os.path.join(
            scale_up_terraform_data_dir, "terraform.tfstate"
        )
        terraform_scale_up = Terraform(
            vsphere_dir, state_file_path=scaleup_terraform_tfstate
        )
        os.chdir(scale_up_terraform_data_dir)
        terraform_scale_up.initialize(upgrade=True)
        terraform_scale_up.destroy(scale_up_terraform_var)


class VSPHEREIPI(VSPHEREBASE):
    """
    A class to handle vSphere IPI specific deployment
    """

    def __init__(self):
        super(VSPHEREIPI, self).__init__()

    class OCPDeployment(BaseOCPDeployment):
        def __init__(self):
            super(VSPHEREIPI.OCPDeployment, self).__init__()

        def deploy_prereq(self):
            """
            Overriding deploy_prereq from parent. Perform all necessary
            prerequisites for VSPHEREIPI here.
            """
            #  Assign IPs from IPAM server
            ips = assign_ips(constants.NUM_OF_VIPS)
            config.ENV_DATA["vips"] = ips

            super(VSPHEREIPI.OCPDeployment, self).deploy_prereq()

            # create DNS records
            create_dns_records(ips)

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
            install_config_obj = yaml.safe_load(install_config_str)
            install_config_obj["pullSecret"] = self.get_pull_secret()
            install_config_obj["sshKey"] = self.get_ssh_key()
            install_config_obj["platform"]["vsphere"]["apiVIP"] = config.ENV_DATA[
                "vips"
            ][0]
            install_config_obj["platform"]["vsphere"]["ingressVIP"] = config.ENV_DATA[
                "vips"
            ][1]
            install_config_obj["metadata"]["name"] = config.ENV_DATA.get("cluster_name")
            install_config_obj["baseDomain"] = config.ENV_DATA.get("base_domain")
            install_config_str = yaml.safe_dump(install_config_obj)
            install_config = os.path.join(self.cluster_path, "install-config.yaml")

            with open(install_config, "w") as f:
                f.write(install_config_str)

        def deploy(self, log_cli_level="DEBUG"):
            """
            Deployment specific to OCP cluster on this platform

            Args:
                log_cli_level (str): openshift installer's log level
                    (default: "DEBUG")
            """

            logger.info("Deploying OCP cluster")
            logger.info(f"Openshift-installer will be using loglevel:{log_cli_level}")
            try:
                run_cmd(
                    f"{self.installer} create cluster "
                    f"--dir {self.cluster_path} "
                    f"--log-level {log_cli_level}",
                    timeout=7200,
                )
            except (CommandFailed, TimeoutExpired) as e:
                if constants.GATHER_BOOTSTRAP_PATTERN in str(e):
                    try:
                        gather_bootstrap()
                    except Exception as ex:
                        logger.error(ex)
                    raise e
                if "Waiting up to" in str(e):
                    run_cmd(
                        f"{self.installer} wait-for install-complete "
                        f"--dir {self.cluster_path} "
                        f"--log-level {log_cli_level}",
                        timeout=3600,
                    )
            self.test_cluster()

    def deploy_ocp(self, log_cli_level="DEBUG"):
        """
        Deployment specific to OCP cluster on this platform

        Args:
            log_cli_level (str): openshift installer's log level
                (default: "DEBUG")
        """
        super(VSPHEREIPI, self).deploy_ocp(log_cli_level)

    def destroy_cluster(self, log_level="DEBUG"):
        """
        Destroy OCP cluster specific to vSphere IPI

        Args:
            log_level (str): log level openshift-installer (default: DEBUG)
        """
        force_download = config.DEPLOYMENT["force_download_installer"]
        installer = get_openshift_installer(
            config.DEPLOYMENT["installer_version"], force_download=force_download
        )
        try:
            run_cmd(
                f"{installer} destroy cluster "
                f"--dir {self.cluster_path} "
                f"--log-level {log_level}",
                timeout=3600,
            )
        except CommandFailed as e:
            logger.error(e)

        # Delete DNS records
        delete_dns_records()

        # release the IP's
        ipam = IPAM(appiapp="address")
        hosts = [
            f"{config.ENV_DATA.get('cluster_name')}-{i}"
            for i in range(constants.NUM_OF_VIPS)
        ]
        ipam.release_ips(hosts)


class VSPHEREUPIFlexy(VSPHEREBASE):
    """
    A class to handle vSphere UPI Flexy deployment
    """

    def __init__(self):
        super().__init__()

    class OCPDeployment(BaseOCPDeployment):
        def __init__(self):
            self.flexy_deployment = True
            super().__init__()
            self.flexy_instance = FlexyVSPHEREUPI()

            # create terraform_data directory (used for compatibility with rest
            # of the ocs-ci)
            self.terraform_data_dir = os.path.join(
                self.cluster_path, constants.TERRAFORM_DATA_DIR
            )
            create_directory_path(self.terraform_data_dir)

        def deploy_prereq(self):
            """
            Instantiate proper flexy class here

            """
            super().deploy_prereq()
            self.flexy_instance.deploy_prereq()

        def deploy(self, log_level=""):
            """
            Deployment specific to OCP cluster on this platform

            Args:
                log_cli_level (str): openshift installer's log level
                    (default: "DEBUG")

            """
            self.flexy_instance.deploy(log_level)
            self.test_cluster()

        def destroy(self, log_level=""):
            """
            Destroy cluster using Flexy
            """
            self.flexy_instance.destroy()

    def destroy_cluster(self, log_level="DEBUG"):
        """
        Destroy OCP cluster specific to vSphere UPI Flexy

        Args:
            log_level (str): log level openshift-installer (default: DEBUG)

        """
        super().destroy_cluster(log_level)


def change_vm_root_disk_size(machine_file):
    """
    Change the root disk size of VM from constants.CURRENT_VM_ROOT_DISK_SIZE
    to constants.VM_ROOT_DISK_SIZE

    Args:
         machine_file (str): machine file to change the disk size
    """
    disk_size_prefix = "size             = "
    current_vm_root_disk_size = (
        f"{disk_size_prefix}{constants.CURRENT_VM_ROOT_DISK_SIZE}"
    )
    vm_root_disk_size = f"{disk_size_prefix}{constants.VM_ROOT_DISK_SIZE}"
    replace_content_in_file(machine_file, current_vm_root_disk_size, vm_root_disk_size)


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
    sync_time = f'{to_change}\n sync_time_with_host = "{enable}"'

    replace_content_in_file(machine_file, to_change, sync_time)


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
    upi_repo_path = os.path.join(constants.EXTERNAL_DIR, "installer")
    ocp_version = get_ocp_version()
    # supporting folder structure from ocp4.5
    if Version.coerce(ocp_version) >= Version.coerce("4.5"):
        if config.ENV_DATA["sno"]:
            constants.VSPHERE_INSTALLER_REPO = (
                "https://gitlab.cee.redhat.com/srozen/installer.git"
            )
            clone_repo(
                url=constants.VSPHERE_INSTALLER_REPO,
                location=upi_repo_path,
                branch="master",
                clone_type="normal",
            )
        else:
            # due to failure domain changes in 4.13, use 4.12 branch till
            # we incorporate changes
            # Once https://github.com/openshift/installer/issues/6810 issue is fixed,
            # we need to revert the changes
            if version.get_semantic_version(
                ocp_version
            ) >= version.get_semantic_version("4.13"):
                clone_repo(
                    url=constants.VSPHERE_INSTALLER_REPO,
                    location=upi_repo_path,
                    branch="release-4.12",
                )
            else:
                clone_repo(
                    url=constants.VSPHERE_INSTALLER_REPO,
                    location=upi_repo_path,
                    branch=f"release-{ocp_version}",
                )
    elif Version.coerce(ocp_version) == Version.coerce("4.4"):
        clone_repo(
            url=constants.VSPHERE_INSTALLER_REPO,
            location=upi_repo_path,
            branch=constants.VSPHERE_INSTALLER_BRANCH,
        )
    else:
        clone_repo(
            url=constants.VSPHERE_INSTALLER_REPO,
            location=upi_repo_path,
            branch=f"release-{ocp_version}",
        )


def change_mem_and_cpu():
    """
    Increase CPUs and memory for nodes
    """
    worker_num_cpus = config.ENV_DATA.get("worker_num_cpus")
    master_num_cpus = config.ENV_DATA.get("master_num_cpus")
    worker_memory = config.ENV_DATA.get("compute_memory")
    master_memory = config.ENV_DATA.get("master_memory")
    if worker_num_cpus or master_num_cpus or master_memory or worker_memory:
        with open(constants.VSPHERE_MAIN, "r") as fd:
            obj = hcl2.load(fd)
            if worker_num_cpus:
                obj["module"]["compute"]["num_cpu"] = worker_num_cpus
            if master_num_cpus:
                obj["module"]["control_plane"]["num_cpu"] = master_num_cpus
            if worker_memory:
                obj["module"]["compute"]["memory"] = worker_memory
            if master_memory:
                obj["module"]["control_plane"]["memory"] = master_memory
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
    if config.ENV_DATA.get("gateway"):
        replace_content_in_file(
            config_file, str_to_replace, f"{config.ENV_DATA.get('gateway')}"
        )


def add_shutdown_wait_timeout():
    """
    Add shutdown_wait_timeout to VM's

    shutdown_wait_timeout is the amount of time, in minutes, to wait for a graceful guest shutdown
    when making necessary updates to the virtual machine. If force_power_off is set to true, the VM will be
    force powered-off after this timeout, otherwise an error is returned. Default: 3 minutes.

    """
    with open(constants.VM_MAIN, "r") as fd:
        obj = hcl2.load(fd)
        obj["resource"][0]["vsphere_virtual_machine"]["vm"][
            "shutdown_wait_timeout"
        ] = 10
    dump_data_to_json(obj, f"{constants.VM_MAIN}.json")
    os.rename(constants.VM_MAIN, f"{constants.VM_MAIN}.backup")


def update_dns():
    """
    Updates the DNS
    """
    # update DNS
    if config.ENV_DATA.get("dns"):
        replace_content_in_file(
            constants.INSTALLER_IGNITION,
            constants.INSTALLER_DEFAULT_DNS,
            f"{config.ENV_DATA.get('dns')}",
        )


def update_zone():
    """
    Updates the zone in constants.INSTALLER_ROUTE53
    """
    # update the zone in route
    if config.ENV_DATA.get("region"):
        def_zone = 'provider "aws" { region = "%s" } \n' % config.ENV_DATA.get("region")
        replace_content_in_file(constants.INSTALLER_ROUTE53, "xyz", def_zone)


def update_path():
    """
    Updates Path to var.folder in resource vsphere_folder
    """
    logger.debug(f"Updating path to var.folder in {constants.VSPHERE_MAIN}")
    replace_str = "path          = var.cluster_id"
    replace_content_in_file(
        constants.VSPHERE_MAIN, replace_str, "path          = var.folder"
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
        fd.write("  type    = string\n")
        fd.write("}\n")


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
        if Version.coerce(get_ocp_version()) >= Version.coerce("4.6"):
            gw_string = "${cidrhost(var.machine_cidr, 1)}"
            gw_conf_file = constants.VM_MAIN
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
    if Version.coerce(ocp_version) >= Version.coerce("4.5"):

        folder_structure = True
        # export AWS_REGION
        set_aws_region()

        # generate terraform variable file
        generate_terraform_vars_with_folder()

        # update the machine configurations
        update_machine_conf(folder_structure)

        if (
            Version.coerce(ocp_version) >= Version.coerce("4.5")
            and not config.ENV_DATA["sno"]
        ):
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
    config.ENV_DATA["cluster_domain"] = cluster_domain

    if not config.ENV_DATA["sno"]:
        # Form the ignition paths
        bootstrap_ignition_path = os.path.join(
            config.ENV_DATA["cluster_path"], constants.BOOTSTRAP_IGN
        )
        control_plane_ignition_path = os.path.join(
            config.ENV_DATA["cluster_path"], constants.MASTER_IGN
        )
        compute_ignition_path = os.path.join(
            config.ENV_DATA["cluster_path"], constants.WORKER_IGN
        )

        # Update ignition paths to ENV_DATA
        config.ENV_DATA["bootstrap_ignition_path"] = bootstrap_ignition_path
        config.ENV_DATA["control_plane_ignition_path"] = control_plane_ignition_path
        config.ENV_DATA["compute_ignition_path"] = compute_ignition_path

    # Copy DNS address to vm_dns_addresses
    config.ENV_DATA["vm_dns_addresses"] = config.ENV_DATA["dns"]

    # Get the infra ID from metadata.json and update in ENV_DATA
    metadata_path = os.path.join(config.ENV_DATA["cluster_path"], "metadata.json")
    metadata_dct = json_to_dict(metadata_path)
    config.ENV_DATA["folder"] = metadata_dct["infraID"]

    # expand ssh_public_key_path and update in ENV_DATA
    ssh_public_key_path = os.path.expanduser(config.DEPLOYMENT["ssh_key"])
    config.ENV_DATA["ssh_public_key_path"] = ssh_public_key_path

    # overwrite RHCOS template
    # This use-case is mainly used for early RHCOS testing
    if config.ENV_DATA.get("vm_template_overwrite"):
        config.ENV_DATA["vm_template"] = config.ENV_DATA["vm_template_overwrite"]
    if config.ENV_DATA["sno"]:
        config.ENV_DATA["iso_file"] = f"ISO/{config.ENV_DATA['cluster_name']}.iso"
        config.ENV_DATA["vm_template"] = "sno_template1"

        create_terraform_var_file("terraform_4_5_sno.tfvars.j2")
    else:
        create_terraform_var_file("terraform_4_5.tfvars.j2")


def create_terraform_var_file(terraform_var_template):
    """
    Creates the terraform variable file from jinja template

    Args:
        terraform_var_template (str): terraform template in jinja format

    """
    _templating = Templating()
    terraform_var_template_path = os.path.join("ocp-deployment", terraform_var_template)
    if "\\" in config.ENV_DATA["vsphere_user"]:
        vsphere_user = config.ENV_DATA["vsphere_user"]
        config.ENV_DATA["vsphere_user"] = vsphere_user.replace("\\", "\\\\")
    terraform_config_str = _templating.render_template(
        terraform_var_template_path, config.ENV_DATA
    )

    terraform_var_yaml = os.path.join(
        config.ENV_DATA["cluster_path"],
        constants.TERRAFORM_DATA_DIR,
        "terraform.tfvars.yaml",
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
        config.ENV_DATA.get("cluster_path"), constants.BOOTSTRAP_IGN
    )
    remote_path = os.path.join(
        config.ENV_DATA.get("path_to_upload"),
        f"{config.RUN.get('run_id')}_{constants.BOOTSTRAP_IGN}",
    )
    upload_file(
        config.ENV_DATA.get("httpd_server"),
        bootstrap_path,
        remote_path,
        config.ENV_DATA.get("httpd_server_user"),
        config.ENV_DATA.get("httpd_server_password"),
    )

    # generate bootstrap ignition url
    path_to_bootstrap_on_remote = remote_path.replace("/var/www/html/", "")
    bootstrap_ignition_url = (
        f"http://{config.ENV_DATA.get('httpd_server')}/"
        f"{path_to_bootstrap_on_remote}"
    )
    logger.info(f"bootstrap_ignition_url: {bootstrap_ignition_url}")
    config.ENV_DATA["bootstrap_ignition_url"] = bootstrap_ignition_url

    # load master and worker ignitions to variables
    master_ignition_path = os.path.join(
        config.ENV_DATA.get("cluster_path"), constants.MASTER_IGN
    )
    master_ignition = read_file_as_str(f"{master_ignition_path}")
    config.ENV_DATA["control_plane_ignition"] = master_ignition

    worker_ignition_path = os.path.join(
        config.ENV_DATA.get("cluster_path"), constants.WORKER_IGN
    )
    worker_ignition = read_file_as_str(f"{worker_ignition_path}")
    config.ENV_DATA["compute_ignition"] = worker_ignition

    cluster_domain = (
        f"{config.ENV_DATA.get('cluster_name')}."
        f"{config.ENV_DATA.get('base_domain')}"
    )
    config.ENV_DATA["cluster_domain"] = cluster_domain

    # generate terraform variables from template
    create_terraform_var_file("terraform.tfvars.j2")


def comment_bootstrap_in_lb_module():
    """
    Commenting the bootstrap module in vsphere main.tf
    """
    logger.debug(f"Commenting bootstrap module in {constants.VSPHERE_MAIN}")
    replace_str = "module.ipam_bootstrap.ip_addresses[0]"
    replace_content_in_file(constants.VSPHERE_MAIN, replace_str, f"//{replace_str}")


def modify_haproxyservice():
    """
    Add ExecStop in haproxy service
    """
    to_change = "TimeoutStartSec=0"
    execstop = f"{to_change}\nExecStop=/bin/podman rm -f haproxy"

    replace_content_in_file(constants.TERRAFORM_HAPROXY_SERVICE, to_change, execstop)


def assign_ips(num_of_vips):
    """
    Assign IPs to hosts

    Args:
        num_of_vips (int): Number of IPs to assign

    """
    ipam = IPAM(appiapp="address")
    subnet = config.ENV_DATA["machine_cidr"].split("/")[0]
    hosts = [f"{config.ENV_DATA.get('cluster_name')}-{i}" for i in range(num_of_vips)]
    ips = ipam.assign_ips(hosts, subnet)
    logger.debug(f"IPs reserved for hosts {hosts} are {ips}")
    return ips


def create_dns_records(ips):
    """
    Create DNS records

    Args:
        ips (list): List if IPs for creating DNS records

    """
    logger.info("creating DNS records")
    aws = AWS()
    dns_record_names = [
        f"api.{config.ENV_DATA.get('cluster_name')}",
        f"*.apps.{config.ENV_DATA.get('cluster_name')}",
    ]
    responses = []
    dns_record_mapping = {}
    for index, record in enumerate(dns_record_names):
        dns_record_mapping[record] = ips[index]
    logger.debug(f"dns_record_mapping: {dns_record_mapping}")
    zone_id = aws.get_hosted_zone_id_for_domain(config.ENV_DATA["base_domain"])

    for record in dns_record_names:
        responses.append(
            aws.update_hosted_zone_record(
                zone_id=zone_id,
                record_name=record,
                data=dns_record_mapping[record],
                type="A",
                operation_type="Add",
            )
        )

    # wait for records to create
    logger.info("Waiting for record response")
    aws.wait_for_record_set(response_list=responses)
    logger.info("Records created successfully")


def delete_dns_records():
    """
    Deletes DNS records
    """
    logger.info("Deleting DNS records")
    aws = AWS()
    cluster_domain = (
        f"{config.ENV_DATA.get('cluster_name')}."
        f"{config.ENV_DATA.get('base_domain')}"
    )
    # get the record sets
    record_sets = aws.get_record_sets()

    # form the record sets to delete
    records_to_delete = [
        f"api.{cluster_domain}.",
        f"\\052.apps.{cluster_domain}.",
    ]

    # delete the records
    hosted_zone_id = aws.get_hosted_zone_id_for_domain()
    logger.info(f"hosted zone id: {hosted_zone_id}")
    for record in record_sets:
        if record["Name"] in records_to_delete:
            aws.delete_record(record, hosted_zone_id)


def get_ignition_provider_version():
    """
    Gets the ignition provider version based on OCP version

    Returns:
        str: ignition provider version

    """
    if version.get_semantic_ocp_version_from_config() >= version.VERSION_4_11:
        return "v2.1.2"
    else:
        return "v2.1.0"


def comment_sensitive_var():
    """
    Comment out sensitive var in vm/variables.tf
    """
    str_to_modify = "sensitive = true"
    target_str = "//sensitive = true"
    logger.debug(
        f"commenting out {str_to_modify} in {constants.VM_VAR} as current terraform version doesn't support"
    )
    replace_content_in_file(constants.VM_VAR, str_to_modify, target_str)
