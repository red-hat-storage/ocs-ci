import json
import os
import logging
import tempfile
from datetime import datetime
from time import sleep

import yaml
import requests
from semantic_version import Version

from .flexy import FlexyBaremetalPSI
from ocs_ci.utility import psiutils, aws, version

from ocs_ci.deployment.deployment import Deployment
from ocs_ci.framework import config
from ocs_ci.deployment.ocp import OCPDeployment as BaseOCPDeployment
from ocs_ci.ocs import constants, ocp, exceptions
from ocs_ci.ocs.exceptions import CommandFailed, RhcosImageNotFound
from ocs_ci.ocs.node import get_nodes
from ocs_ci.ocs.openshift_ops import OCP
from ocs_ci.utility.bootstrap import gather_bootstrap
from ocs_ci.utility.connection import Connection
from ocs_ci.utility.csr import wait_for_all_nodes_csr_and_approve, approve_pending_csr
from ocs_ci.utility.templating import Templating
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import (
    run_cmd,
    upload_file,
    get_ocp_version,
    wait_for_co,
    check_for_rhcos_images,
    get_infra_id,
    TimeoutSampler,
    add_chrony_to_ocp_deployment,
)

logger = logging.getLogger(__name__)


class BAREMETALUPI(Deployment):
    """
    A class to handle Bare metal UPI specific deployment
    """

    def __init__(self):
        logger.info("BAREMETAL UPI")
        super().__init__()

    class OCPDeployment(BaseOCPDeployment):
        def __init__(self):
            super().__init__()
            self.bm_config = config.ENV_DATA["baremetal"]
            self.srv_details = config.ENV_DATA["baremetal"]["servers"]
            self.aws = aws.AWS()

        def deploy_prereq(self):
            """
            Pre-Requisites for Bare Metal UPI Deployment
            """
            super(BAREMETALUPI.OCPDeployment, self).deploy_prereq()
            # check for BM status
            logger.info("Checking BM Status")
            status = self.check_bm_status_exist()
            assert (
                status == constants.BM_STATUS_ABSENT
            ), f"BM Cluster still present and locked by {self.get_locked_username()}"
            # update BM status
            logger.info("Updating BM Status")
            result = self.update_bm_status(constants.BM_STATUS_PRESENT)
            assert (
                result == constants.BM_STATUS_RESPONSE_UPDATED
            ), "Failed to update request"
            # create manifest
            self.create_manifest()
            # create chrony resource
            add_chrony_to_ocp_deployment()
            # create ignitions
            self.create_ignitions()
            self.kubeconfig = os.path.join(
                self.cluster_path, config.RUN.get("kubeconfig_location")
            )
            bootstrap_path = os.path.join(
                config.ENV_DATA.get("cluster_path"), constants.BOOTSTRAP_IGN
            )
            master_path = os.path.join(
                config.ENV_DATA.get("cluster_path"), constants.MASTER_IGN
            )
            worker_path = os.path.join(
                config.ENV_DATA.get("cluster_path"), constants.WORKER_IGN
            )

            self.host = self.bm_config["bm_httpd_server"]
            self.user = self.bm_config["bm_httpd_server_user"]
            self.private_key = os.path.expanduser(config.DEPLOYMENT["ssh_key_private"])

            self.helper_node_handler = Connection(
                self.host, self.user, self.private_key
            )
            cmd = f"rm -rf {self.bm_config['bm_path_to_upload']}"
            logger.info(self.helper_node_handler.exec_cmd(cmd=cmd))
            cmd = f"mkdir -m 755 {self.bm_config['bm_path_to_upload']}"
            assert self.helper_node_handler.exec_cmd(
                cmd=cmd
            ), "Failed to create required folder"
            # Upload ignition to public access server
            upload_dict = {
                bootstrap_path: constants.BOOTSTRAP_IGN,
                master_path: constants.MASTER_IGN,
                worker_path: constants.WORKER_IGN,
            }

            for key, val in zip(upload_dict.keys(), upload_dict.values()):
                upload_file(
                    self.host,
                    key,
                    os.path.join(self.bm_config["bm_path_to_upload"], f"{val}"),
                    self.user,
                    key_file=self.private_key,
                )

            # Perform Cleanup for stale entry's
            cmd = f"rm -rf {self.bm_config['bm_tftp_base_dir']}"
            assert self.helper_node_handler.exec_cmd(cmd=cmd), "Failed to Delete folder"

            # Installing Required packages
            cmd = "yum install dnsmasq -y"
            assert self.helper_node_handler.exec_cmd(
                cmd=cmd
            ), "Failed to install required package"

            # Enable dnsmasq service on boot
            cmd = "systemctl enable dnsmasq"
            assert self.helper_node_handler.exec_cmd(
                cmd=cmd
            ), "Failed to Enable dnsmasq service"

            # Starting dnsmasq service
            cmd = "systemctl start dnsmasq"
            assert self.helper_node_handler.exec_cmd(
                cmd=cmd
            ), "Failed to Start dnsmasq service"

            cmd = f"mkdir -m 755 -p {self.bm_config['bm_tftp_base_dir']}"
            assert self.helper_node_handler.exec_cmd(
                cmd=cmd
            ), "Failed to create required folder"

            cmd = f"mkdir -m 755 -p {self.bm_config['bm_tftp_base_dir']}ocs4qe"
            assert self.helper_node_handler.exec_cmd(
                cmd=cmd
            ), "Failed to create required folder"

            cmd = (
                f"mkdir -m 755 -p {self.bm_config['bm_tftp_base_dir']}ocs4qe/baremetal"
            )
            assert self.helper_node_handler.exec_cmd(
                cmd=cmd
            ), "Failed to create required folder"

            # Install syslinux
            cmd = "yum install syslinux -y"
            assert self.helper_node_handler.exec_cmd(
                cmd=cmd
            ), "Failed to install required package"

            # Copy syslinux files to the tftp path
            cmd = f"cp -ar /usr/share/syslinux/* {self.bm_config['bm_tftp_dir']}"
            assert self.helper_node_handler.exec_cmd(
                cmd=cmd
            ), "Failed to Copy required files"

            # Restarting dnsmasq service
            cmd = "systemctl restart dnsmasq"
            assert self.helper_node_handler.exec_cmd(
                cmd=cmd
            ), "Failed to restart dnsmasq service"
            with open(constants.RHCOS_IMAGES_FILE) as file_stream:
                rhcos_images_file = yaml.safe_load(file_stream)
            ocp_version = get_ocp_version()
            logger.info(rhcos_images_file)

            # Download installer_initramfs

            if version.get_semantic_ocp_version_from_config() >= version.VERSION_4_12:
                out = run_cmd(f"{self.installer} coreos print-stream-json")
                coreos_print_stream_json = json.loads(out)
            else:
                image_data = rhcos_images_file[ocp_version]

            if version.get_semantic_ocp_version_from_config() >= version.VERSION_4_12:
                initramfs_image_path = coreos_print_stream_json["architectures"][
                    "x86_64"
                ]["artifacts"]["metal"]["formats"]["pxe"]["initramfs"]["location"]
            else:
                initramfs_image_path = (
                    constants.coreos_url_prefix + image_data["installer_initramfs_url"]
                )
            if check_for_rhcos_images(initramfs_image_path):
                cmd = (
                    "wget -O "
                    f"{self.bm_config['bm_tftp_dir']}"
                    "/rhcos-installer-initramfs.x86_64.img "
                    f"{initramfs_image_path}"
                )
                assert self.helper_node_handler.exec_cmd(
                    cmd=cmd
                ), "Failed to Download required File"
            else:
                raise RhcosImageNotFound
            # Download installer_kernel
            if version.get_semantic_ocp_version_from_config() >= version.VERSION_4_12:
                kernel_image_path = coreos_print_stream_json["architectures"]["x86_64"][
                    "artifacts"
                ]["metal"]["formats"]["pxe"]["kernel"]["location"]
            else:
                kernel_image_path = (
                    constants.coreos_url_prefix + image_data["installer_kernel_url"]
                )
            if check_for_rhcos_images(kernel_image_path):
                cmd = (
                    "wget -O "
                    f"{self.bm_config['bm_tftp_dir']}"
                    "/rhcos-installer-kernel-x86_64 "
                    f"{kernel_image_path}"
                )
                assert self.helper_node_handler.exec_cmd(
                    cmd=cmd
                ), "Failed to Download required File"
            else:
                raise RhcosImageNotFound
            # Download metal_bios
            if Version.coerce(ocp_version) <= Version.coerce("4.6"):
                metal_image_path = (
                    constants.coreos_url_prefix + image_data["metal_bios_url"]
                )
                if check_for_rhcos_images(metal_image_path):
                    cmd = (
                        "wget -O "
                        f"{self.bm_config['bm_path_to_upload']}"
                        f"/{constants.BM_METAL_IMAGE} "
                        f"{metal_image_path}"
                    )
                    assert self.helper_node_handler.exec_cmd(
                        cmd=cmd
                    ), "Failed to Download required File"
                else:
                    raise RhcosImageNotFound

            if Version.coerce(ocp_version) >= Version.coerce("4.6"):
                # Download rootfs
                if (
                    version.get_semantic_ocp_version_from_config()
                    >= version.VERSION_4_12
                ):
                    rootfs_image_path = coreos_print_stream_json["architectures"][
                        "x86_64"
                    ]["artifacts"]["metal"]["formats"]["pxe"]["rootfs"]["location"]
                else:
                    rootfs_image_path = (
                        constants.coreos_url_prefix + image_data["live_rootfs_url"]
                    )
                if check_for_rhcos_images(rootfs_image_path):
                    cmd = (
                        "wget -O "
                        f"{self.bm_config['bm_path_to_upload']}"
                        "/rhcos-live-rootfs.x86_64.img "
                        f"{rootfs_image_path}"
                    )
                    assert self.helper_node_handler.exec_cmd(
                        cmd=cmd
                    ), "Failed to Download required File"
                else:
                    raise RhcosImageNotFound

            # Create pxelinux.cfg directory
            cmd = f"mkdir -m 755 {self.bm_config['bm_tftp_dir']}/pxelinux.cfg"
            assert self.helper_node_handler.exec_cmd(
                cmd=cmd
            ), "Failed to create required folder"

        def deploy(self, log_cli_level="DEBUG"):
            """
            Deploy
            """
            # Uploading pxe files
            master_count = 0
            worker_count = 0
            logger.info("Deploying OCP cluster for Bare Metal platform")
            logger.info(f"Openshift-installer will be using log level:{log_cli_level}")
            ocp_version = get_ocp_version()
            for machine in self.srv_details:
                if self.srv_details[machine].get("cluster_name") or self.srv_details[
                    machine
                ].get("extra_node"):
                    pxe_file_path = self.create_pxe_files(
                        ocp_version=ocp_version,
                        role=self.srv_details[machine].get("role"),
                        disk_path=self.srv_details[machine].get("root_disk_id"),
                    )
                    upload_file(
                        server=self.host,
                        localpath=pxe_file_path,
                        remotepath=f"{self.bm_config['bm_tftp_dir']}"
                        f"/pxelinux.cfg/01-{self.srv_details[machine]['private_mac'].replace(':', '-')}",
                        user=self.user,
                        key_file=self.private_key,
                    )
            # Applying Permission
            cmd = f"chmod 755 -R {self.bm_config['bm_tftp_dir']}"
            self.helper_node_handler.exec_cmd(cmd=cmd)

            # Applying Permission
            cmd = f"chmod 755 -R {self.bm_config['bm_path_to_upload']}"
            self.helper_node_handler.exec_cmd(cmd=cmd)

            # Restarting dnsmasq service
            cmd = "systemctl restart dnsmasq"
            assert self.helper_node_handler.exec_cmd(
                cmd=cmd
            ), "Failed to restart dnsmasq service"
            # Rebooting Machine with pxe boot
            api_record_ip_list = []
            apps_record_ip_list = []
            response_list = []
            cluster_name = config.ENV_DATA.get("cluster_name")
            self.aws.delete_hosted_zone(
                cluster_name=cluster_name,
                delete_from_base_domain=True,
            )
            for machine in self.srv_details:
                if (
                    self.srv_details[machine].get("cluster_name")
                    == constants.BM_DEFAULT_CLUSTER_NAME
                ):
                    if self.srv_details[machine]["role"] == constants.BOOTSTRAP_MACHINE:
                        self.set_pxe_boot_and_reboot(machine)
                        bootstrap_ip = self.srv_details[machine]["ip"]
                        api_record_ip_list.append(self.srv_details[machine]["ip"])

                    elif (
                        self.srv_details[machine]["role"] == constants.MASTER_MACHINE
                        and master_count < config.ENV_DATA["master_replicas"]
                    ):
                        self.set_pxe_boot_and_reboot(machine)
                        api_record_ip_list.append(self.srv_details[machine]["ip"])
                        master_count += 1

                    elif (
                        self.srv_details[machine]["role"] == constants.WORKER_MACHINE
                        and worker_count < config.ENV_DATA["worker_replicas"]
                    ):
                        self.set_pxe_boot_and_reboot(machine)
                        apps_record_ip_list.append(self.srv_details[machine]["ip"])
                        worker_count += 1

            logger.info("Configuring DNS records")
            zone_id = self.aws.create_hosted_zone(cluster_name=cluster_name)

            if config.ENV_DATA["worker_replicas"] == 0:
                apps_record_ip_list = api_record_ip_list
            for ip in api_record_ip_list:
                response_list.append(
                    self.aws.update_hosted_zone_record(
                        zone_id=zone_id,
                        record_name=f"api-int.{cluster_name}",
                        data=ip,
                        type="A",
                        operation_type="Add",
                    )
                )
                response_list.append(
                    self.aws.update_hosted_zone_record(
                        zone_id=zone_id,
                        record_name=f"api.{cluster_name}",
                        data=ip,
                        type="A",
                        operation_type="Add",
                    )
                )
            for ip in apps_record_ip_list:
                response_list.append(
                    self.aws.update_hosted_zone_record(
                        zone_id=zone_id,
                        record_name=f"*.apps.{cluster_name}",
                        data=ip,
                        type="A",
                        operation_type="Add",
                    )
                )

            ns_list = self.aws.get_ns_for_hosted_zone(zone_id)
            dns_data_dict = {}
            ns_list_values = []
            for value in ns_list:
                dns_data_dict["Value"] = value
                ns_list_values.append(dns_data_dict.copy())
            base_domain_zone_id = self.aws.get_hosted_zone_id_for_domain(
                domain=config.ENV_DATA["base_domain"]
            )
            response_list.append(
                self.aws.update_hosted_zone_record(
                    zone_id=base_domain_zone_id,
                    record_name=f"{cluster_name}",
                    data=ns_list_values,
                    type="NS",
                    operation_type="Add",
                    ttl=300,
                    raw_data=True,
                )
            )
            logger.info("Waiting for Record Response")
            self.aws.wait_for_record_set(response_list=response_list)
            logger.info("Records Created Successfully")
            logger.info("waiting for bootstrap to complete")
            try:
                run_cmd(
                    f"{self.installer} wait-for bootstrap-complete "
                    f"--dir {self.cluster_path} "
                    f"--log-level {log_cli_level}",
                    timeout=3600,
                )
            except CommandFailed as e:
                if constants.GATHER_BOOTSTRAP_PATTERN in str(e):
                    try:
                        gather_bootstrap()
                    except Exception as ex:
                        logger.error(ex)
                raise e

            OCP.set_kubeconfig(self.kubeconfig)
            wait_for_all_nodes_csr_and_approve()
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
                timeout=1800,
            )
            logger.info("Removing Bootstrap Ip for DNS Records")
            self.aws.update_hosted_zone_record(
                zone_id=zone_id,
                record_name=f"api-int.{cluster_name}",
                data=bootstrap_ip,
                type="A",
                operation_type="Delete",
            )
            self.aws.update_hosted_zone_record(
                zone_id=zone_id,
                record_name=f"api.{cluster_name}",
                data=bootstrap_ip,
                type="A",
                operation_type="Delete",
            )
            # Approving CSRs here in-case if any exists
            approve_pending_csr()

            self.test_cluster()
            logger.info("Performing Disk cleanup")
            clean_disk()

        def create_config(self):
            """
            Creates the OCP deploy config for the Bare Metal
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
            install_config_obj["metadata"]["name"] = config.ENV_DATA.get("cluster_name")
            install_config_str = yaml.safe_dump(install_config_obj)
            install_config = os.path.join(self.cluster_path, "install-config.yaml")
            install_config_backup = os.path.join(
                self.cluster_path, "install-config.yaml.backup"
            )
            with open(install_config, "w") as f:
                f.write(install_config_str)
            with open(install_config_backup, "w") as f:
                f.write(install_config_str)

        def create_manifest(self):
            """
            Creates the Manifest files
            """
            logger.info("creating manifest files for the cluster")
            run_cmd(f"{self.installer} create manifests " f"--dir {self.cluster_path} ")

        def create_ignitions(self):
            """
            Creates the ignition files
            """
            logger.info("creating ignition files for the cluster")
            run_cmd(
                f"{self.installer} create ignition-configs "
                f"--dir {self.cluster_path} "
            )

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

        def destroy(self, log_level=""):
            """
            Destroy OCP cluster specific to BM UPI
            """
            self.aws.delete_hosted_zone(
                cluster_name=config.ENV_DATA.get("cluster_name"),
                delete_from_base_domain=True,
            )

            logger.info("Updating BM status")
            result = self.update_bm_status(constants.BM_STATUS_ABSENT)
            assert (
                result == constants.BM_STATUS_RESPONSE_UPDATED
            ), "Failed to update request"

        def check_bm_status_exist(self):
            """
            Check if BM Cluster already exist

            Returns:
                str: response status
            """
            headers = {"content-type": "application/json"}
            response = requests.get(
                url=self.bm_config["bm_status_check"], headers=headers
            )
            return response.json()[0]["status"]

        def get_locked_username(self):
            """
            Get name of user who has locked baremetal resource

            Returns:
                str: username
            """
            headers = {"content-type": "application/json"}
            response = requests.get(
                url=self.bm_config["bm_status_check"], headers=headers
            )
            return response.json()[0]["user"]

        def update_bm_status(self, bm_status):
            """
            Update BM status when cluster is deployed/teardown

            Args:
                bm_status (str): Status to be updated

            Returns:
                str: response message
            """
            if bm_status == constants.BM_STATUS_PRESENT:
                now = datetime.today().strftime("%Y-%m-%d")
                payload = {
                    "status": bm_status,
                    "cluster_name": config.ENV_DATA["cluster_name"],
                    "creation_date": now,
                }
            else:
                payload = {
                    "status": bm_status,
                    "cluster_name": "null",
                    "creation_date": "null",
                }
            headers = {"content-type": "application/json"}
            response = requests.put(
                url=self.bm_config["bm_status_check"],
                json=payload,
                headers=headers,
            )
            return response.json()["message"]

        def create_pxe_files(self, ocp_version, role, disk_path):
            """
            Create pxe file for giver role

            Args:
                ocp_version (float): OCP version
                role (str): Role of node eg:- bootstrap,master,worker

            Returns:
                str: temp file path

            """
            extra_data = ""
            bm_install_files_loc = self.bm_config["bm_install_files"]
            extra_data_pxe = "rhcos-live-rootfs.x86_64.img coreos.inst.insecure"
            if Version.coerce(ocp_version) <= Version.coerce("4.6"):
                bm_metal_loc = f"coreos.inst.image_url={bm_install_files_loc}{constants.BM_METAL_IMAGE}"
            else:
                bm_metal_loc = ""
            if Version.coerce(ocp_version) >= Version.coerce("4.6"):
                extra_data = (
                    f"coreos.live.rootfs_url={bm_install_files_loc}{extra_data_pxe}"
                )
            default_pxe_file = f"""DEFAULT menu.c32
TIMEOUT 20
PROMPT 0
LABEL pxeboot
    MENU LABEL PXE Boot
    MENU DEFAULT
    KERNEL rhcos-installer-kernel-x86_64
    APPEND ip=enp1s0f0:dhcp ip=enp1s0f1:dhcp rd.neednet=1 initrd=rhcos-installer-initramfs.x86_64.img console=ttyS0 \
console=tty0 coreos.inst.install_dev=/dev/disk/by-id/{disk_path} {bm_metal_loc} \
coreos.inst.ignition_url={bm_install_files_loc}{role}.ign \
{extra_data}
LABEL disk0
  MENU LABEL Boot disk (0x80)
  LOCALBOOT 0"""
            temp_file = tempfile.NamedTemporaryFile(
                mode="w+", prefix=f"pxe_file_{role}", delete=False
            )
            with open(temp_file.name, "w") as t_file:
                t_file.writelines(default_pxe_file)
            return temp_file.name

        def set_pxe_boot_and_reboot(self, machine):
            """
            Ipmi Set Pxe boot and Restart the machine

            Args:
                machine (str): Machine Name

            """
            secrets = [
                self.srv_details[machine]["mgmt_username"],
                self.srv_details[machine]["mgmt_password"],
            ]
            # Changes boot prioriy to pxe
            cmd = (
                f"ipmitool -I lanplus -U {self.srv_details[machine]['mgmt_username']} "
                f"-P {self.srv_details[machine]['mgmt_password']} "
                f"-H {self.srv_details[machine]['mgmt_console']} chassis bootdev pxe"
            )
            run_cmd(cmd=cmd, secrets=secrets)
            logger.info(
                "Sleeping for 2 Sec to make sure bootdev pxe is set properly using ipmitool cmd"
            )
            sleep(2)
            # Power On Machine
            cmd = (
                f"ipmitool -I lanplus -U {self.srv_details[machine]['mgmt_username']} "
                f"-P {self.srv_details[machine]['mgmt_password']} "
                f"-H {self.srv_details[machine]['mgmt_console']} chassis power cycle || "
                f"ipmitool -I lanplus -U {self.srv_details[machine]['mgmt_username']} "
                f"-P {self.srv_details[machine]['mgmt_password']} "
                f"-H {self.srv_details[machine]['mgmt_console']} chassis power on"
            )
            run_cmd(cmd=cmd, secrets=secrets)


@retry(exceptions.CommandFailed, tries=10, delay=30, backoff=1)
def clean_disk():
    """
    Perform disk cleanup
    """
    lvm_to_clean = []
    workers = get_nodes(node_type="worker")

    ocp_obj = ocp.OCP()
    policy = constants.PSA_BASELINE
    if version.get_semantic_ocp_version_from_config() >= version.VERSION_4_12:
        policy = constants.PSA_PRIVILEGED
    ocp_obj.new_project(project_name=constants.BM_DEBUG_NODE_NS, policy=policy)

    for worker in workers:
        out = ocp_obj.exec_oc_debug_cmd(
            node=worker.name,
            cmd_list=["lsblk -nd -e252,7 --output NAME --json"],
            namespace=constants.BM_DEBUG_NODE_NS,
        )
        logger.info(out)
        lsblk_output = json.loads(str(out))
        lsblk_devices = lsblk_output["blockdevices"]
        for lsblk_device in lsblk_devices:
            base_cmd = (
                """pvs --config "devices{filter = [ 'a|/dev/%s.*|', 'r|.*|' ] }" --reportformat json"""
                % lsblk_device["name"]
            )

            cmd = (
                f"debug nodes/{worker.name} --to-namespace={constants.BM_DEBUG_NODE_NS} "
                f"-- chroot /host {base_cmd}"
            )
            out = ocp_obj.exec_oc_cmd(
                command=cmd,
                out_yaml_format=False,
            )
            logger.info(out)
            pvs_output = json.loads(str(out))
            pvs_list = pvs_output["report"]
            for pvs in pvs_list:
                pv_list = pvs["pv"]
                for pv in pv_list:
                    logger.debug(pv)
                    device_dict = {
                        "hostname": f"{worker.name}",
                        "pv_name": f"{pv['pv_name']}",
                    }
                    lvm_to_clean.append(device_dict)
            base_cmd = (
                """vgs --config "devices{filter = [ 'a|/dev/%s.*|', 'r|.*|' ] }" --reportformat json"""
                % lsblk_device["name"]
            )

            cmd = (
                f"debug nodes/{worker.name} --to-namespace={constants.BM_DEBUG_NODE_NS} "
                f"-- chroot /host {base_cmd}"
            )
            out = ocp_obj.exec_oc_cmd(
                command=cmd,
                out_yaml_format=False,
            )
            logger.info(out)
            vgs_output = json.loads(str(out))
            vgs_list = vgs_output["report"]
            for vgs in vgs_list:
                vg_list = vgs["vg"]
                for vg in vg_list:
                    logger.debug(vg)
                    device_dict = {
                        "hostname": f"{worker.name}",
                        "vg_name": f"{vg['vg_name']}",
                    }
                    lvm_to_clean.append(device_dict)
    for devices in lvm_to_clean:
        if devices.get("vg_name"):
            cmd = (
                f"debug nodes/{devices['hostname']} --to-namespace={constants.BM_DEBUG_NODE_NS} "
                f"-- chroot /host timeout 120 vgremove {devices['vg_name']} -y -f"
            )
            logger.info("Removing vg")
            out = ocp_obj.exec_oc_cmd(
                command=cmd,
                out_yaml_format=False,
            )
            logger.info(out)
    for devices in lvm_to_clean:
        if devices.get("pv_name"):
            out = ocp_obj.exec_oc_debug_cmd(
                node=devices["hostname"],
                cmd_list=[f"pvremove {devices['pv_name']} -y"],
                namespace=constants.BM_DEBUG_NODE_NS,
            )
            logger.info(out)

    for worker in workers:
        cmd = """lsblk --all --noheadings --output "KNAME,PKNAME,TYPE,MOUNTPOINT" --json"""
        out = ocp_obj.exec_oc_debug_cmd(
            node=worker.name, cmd_list=[cmd], namespace=constants.BM_DEBUG_NODE_NS
        )
        disk_to_ignore_cleanup_raw = json.loads(str(out))
        disk_to_ignore_cleanup_json = disk_to_ignore_cleanup_raw["blockdevices"]
        for disk_to_ignore_cleanup in disk_to_ignore_cleanup_json:
            if disk_to_ignore_cleanup["mountpoint"] == "/boot":
                logger.info(
                    f"Ignorning disk {disk_to_ignore_cleanup['pkname']} for cleanup because it's a root disk "
                )
                selected_disk_to_ignore_cleanup = disk_to_ignore_cleanup["pkname"]
                # Adding break when root disk is found
                break
        out = ocp_obj.exec_oc_debug_cmd(
            node=worker.name,
            cmd_list=["lsblk -nd -e252,7 --output NAME --json"],
            namespace=constants.BM_DEBUG_NODE_NS,
        )
        lsblk_output = json.loads(str(out))
        lsblk_devices = lsblk_output["blockdevices"]
        for lsblk_device in lsblk_devices:
            out = ocp_obj.exec_oc_debug_cmd(
                node=worker.name,
                cmd_list=[f"lsblk -b /dev/{lsblk_device['name']} --output NAME --json"],
                namespace=constants.BM_DEBUG_NODE_NS,
            )
            lsblk_output = json.loads(str(out))
            lsblk_devices_to_clean = lsblk_output["blockdevices"]
            for device_to_clean in lsblk_devices_to_clean:
                if device_to_clean["name"] == str(selected_disk_to_ignore_cleanup):
                    logger.info(
                        f"Skipping disk cleanup for {device_to_clean['name']} because it's a root disk"
                    )
                else:
                    out = ocp_obj.exec_oc_debug_cmd(
                        node=worker.name,
                        cmd_list=[f"wipefs -a -f /dev/{device_to_clean['name']}"],
                        namespace=constants.BM_DEBUG_NODE_NS,
                    )
                    logger.info(out)
                    out = ocp_obj.exec_oc_debug_cmd(
                        node=worker.name,
                        cmd_list=[f"sgdisk --zap-all /dev/{device_to_clean['name']}"],
                        namespace=constants.BM_DEBUG_NODE_NS,
                    )
                    logger.info(out)

    ocp_obj.delete_project(project_name=constants.BM_DEBUG_NODE_NS)


class BaremetalPSIUPI(Deployment):
    """
    All the functionalities related to BaremetalPSI- UPI deployment
    lives here
    """

    def __init__(self):
        self.cluster_name = config.ENV_DATA["cluster_name"]
        super().__init__()

    class OCPDeployment(BaseOCPDeployment):
        def __init__(self):
            self.flexy_deployment = True
            super().__init__()
            self.flexy_instance = FlexyBaremetalPSI()
            self.psi_conf = config.AUTH["psi"]
            self.utils = psiutils.PSIUtils(self.psi_conf)

        def deploy_prereq(self):
            """
            Instantiate proper flexy class here

            """
            super().deploy_prereq()
            self.flexy_instance.deploy_prereq()

        def deploy(self, log_level=""):
            self.flexy_instance.deploy(log_level)
            self.test_cluster()
            # add disks to instances
            # Get all instances and for each instance add
            # one disk
            pattern = "-".join(
                [get_infra_id(config.ENV_DATA["cluster_path"]), "compute"]
            )
            for instance in self.utils.get_instances_with_pattern(pattern):
                vol = self.utils.create_volume(
                    name=f"{pattern}-disk0-{instance.name[-1]}",
                    size=config.FLEXY["volume_size"],
                )
                # wait till volume is available
                sample = TimeoutSampler(
                    300, 10, self.utils.check_expected_vol_status, vol, "available"
                )
                if not sample.wait_for_func_status(True):
                    logger.info("Volume failed to reach 'available'")
                    raise exceptions.PSIVolumeNotInExpectedState
                # attach the volume
                self.utils.attach_volume(vol, instance.id)

        def destroy(self, log_level=""):
            """
            Destroy volumes attached if any and then the cluster
            """
            # Get all the additional volumes and detach,delete.
            volumes = self.utils.get_volumes_with_tag(
                {"cluster_name": config.ENV_DATA["cluster_name"]}
            )
            self.flexy_instance.destroy()
            self.utils.detach_and_delete_vols(volumes)
