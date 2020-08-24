import json
import os
import logging
from time import sleep

import yaml
import requests

from .flexy import FlexyBaremetalPSI
from ocs_ci.utility import psiutils

from ocs_ci.deployment.deployment import Deployment
from ocs_ci.framework import config
from ocs_ci.deployment.ocp import OCPDeployment as BaseOCPDeployment
from ocs_ci.ocs import constants, ocp, exceptions
from ocs_ci.ocs.exceptions import CommandFailed, RhcosImageNotFound
from ocs_ci.ocs.node import get_typed_nodes
from ocs_ci.ocs.openshift_ops import OCP
from ocs_ci.utility.bootstrap import gather_bootstrap
from ocs_ci.utility.connection import Connection
from ocs_ci.utility.csr import wait_for_all_nodes_csr_and_approve, approve_pending_csr
from ocs_ci.utility.templating import Templating
from ocs_ci.utility.utils import (
    run_cmd, upload_file, get_ocp_version, load_auth_config,
    wait_for_co, configure_chrony_and_wait_for_machineconfig_status, check_for_rhcos_images,
    get_infra_id, TimeoutSampler
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
            self.helper_node_details = load_auth_config()['baremetal']
            self.mgmt_details = load_auth_config()['ipmi']

        def deploy_prereq(self):
            """
             Pre-Requisites for Bare Metal UPI Deployment
            """
            super(BAREMETALUPI.OCPDeployment, self).deploy_prereq()
            # check for BM status
            logger.info("Checking BM Status")
            status = self.check_bm_status_exist()
            assert status == constants.BM_STATUS_ABSENT, "BM Cluster still present"
            # update BM status
            logger.info("Updating BM Status")
            result = self.update_bm_status(constants.BM_STATUS_PRESENT)
            assert result == constants.BM_STATUS_RESPONSE_UPDATED, "Failed to update request"
            # create manifest
            self.create_manifest()
            # create ignitions
            self.create_ignitions()
            self.kubeconfig = os.path.join(self.cluster_path, config.RUN.get('kubeconfig_location'))
            bootstrap_path = os.path.join(config.ENV_DATA.get('cluster_path'), constants.BOOTSTRAP_IGN)
            master_path = os.path.join(config.ENV_DATA.get('cluster_path'), constants.MASTER_IGN)
            worker_path = os.path.join(config.ENV_DATA.get('cluster_path'), constants.WORKER_IGN)

            self.host = self.helper_node_details['bm_httpd_server']
            self.user = self.helper_node_details['bm_httpd_server_user']
            self.private_key = os.path.expanduser(
                config.DEPLOYMENT['ssh_key_private']
            )

            self.helper_node_handler = Connection(self.host, self.user, self.private_key)
            cmd = f"rm -rf {self.helper_node_details['bm_path_to_upload']}"
            logger.info(self.helper_node_handler.exec_cmd(cmd=cmd))
            cmd = f"mkdir -m 755 {self.helper_node_details['bm_path_to_upload']}"
            assert self.helper_node_handler.exec_cmd(cmd=cmd), ("Failed to create required folder")
            # Upload ignition to public access server
            upload_dict = {
                bootstrap_path: constants.BOOTSTRAP_IGN,
                master_path: constants.MASTER_IGN,
                worker_path: constants.WORKER_IGN
            }

            for key, val in zip(upload_dict.keys(), upload_dict.values()):
                upload_file(
                    self.host,
                    key,
                    os.path.join(
                        self.helper_node_details['bm_path_to_upload'], f"{val}"
                    ),
                    self.user,
                    key_file=self.private_key
                )

            # Perform Cleanup for stale entry's
            cmd = f"rm -rf {self.helper_node_details['bm_tftp_base_dir']}"
            assert self.helper_node_handler.exec_cmd(cmd=cmd), "Failed to Delete folder"

            # Installing Required packages
            cmd = "yum install dnsmasq -y"
            assert self.helper_node_handler.exec_cmd(cmd=cmd), "Failed to install required package"

            # Enable dnsmasq service on boot
            cmd = "systemctl enable dnsmasq"
            assert self.helper_node_handler.exec_cmd(cmd=cmd), "Failed to Enable dnsmasq service"

            # Starting dnsmasq service
            cmd = "systemctl start dnsmasq"
            assert self.helper_node_handler.exec_cmd(cmd=cmd), "Failed to Start dnsmasq service"

            cmd = f"mkdir -m 755 -p {self.helper_node_details['bm_tftp_base_dir']}"
            assert self.helper_node_handler.exec_cmd(cmd=cmd), "Failed to create required folder"

            cmd = f"mkdir -m 755 -p {self.helper_node_details['bm_tftp_base_dir']}ocs4qe"
            assert self.helper_node_handler.exec_cmd(cmd=cmd), "Failed to create required folder"

            cmd = f"mkdir -m 755 -p {self.helper_node_details['bm_tftp_base_dir']}ocs4qe/baremetal"
            assert self.helper_node_handler.exec_cmd(cmd=cmd), "Failed to create required folder"

            cmd = f"rm -rf {self.helper_node_details['bm_dnsmasq_dir']}*"
            assert self.helper_node_handler.exec_cmd(cmd=cmd), "Failed to Delete dir"

            # Install syslinux
            cmd = "yum install syslinux -y"
            assert self.helper_node_handler.exec_cmd(cmd=cmd), "Failed to install required package"

            # Copy syslinux files to the tftp path
            cmd = f"cp -ar /usr/share/syslinux/* {self.helper_node_details['bm_tftp_dir']}"
            assert self.helper_node_handler.exec_cmd(cmd=cmd), "Failed to Copy required files"

            upload_dict = {
                constants.PXE_CONF_FILE: "dnsmasq.pxe.conf",
                constants.COMMON_CONF_FILE: "dnsmasq.common.conf"
            }
            for key, val in zip(upload_dict.keys(), upload_dict.values()):
                upload_file(
                    self.host,
                    key,
                    os.path.join(
                        self.helper_node_details['bm_dnsmasq_dir'],
                        val
                    ),
                    self.user,
                    key_file=self.private_key
                )
            # Restarting dnsmasq service
            cmd = "systemctl restart dnsmasq"
            assert self.helper_node_handler.exec_cmd(cmd=cmd), "Failed to restart dnsmasq service"
            with open(constants.RHCOS_IMAGES_FILE) as file_stream:
                rhcos_images_file = yaml.safe_load(file_stream)
            ocp_version = get_ocp_version()
            logger.info(rhcos_images_file)
            image_data = rhcos_images_file[ocp_version]
            # Download installer_initramfs
            initramfs_image_path = constants.coreos_url_prefix + image_data['installer_initramfs_url']
            if check_for_rhcos_images(initramfs_image_path):
                cmd = (
                    "wget -O "
                    f"{self.helper_node_details['bm_tftp_dir']}"
                    "/rhcos-installer-initramfs.x86_64.img "
                    f"{initramfs_image_path}"
                )
                assert self.helper_node_handler.exec_cmd(cmd=cmd), "Failed to Download required File"
            else:
                raise RhcosImageNotFound
            # Download installer_kernel
            kernel_image_path = constants.coreos_url_prefix + image_data['installer_kernel_url']
            if check_for_rhcos_images(kernel_image_path):
                cmd = (
                    "wget -O "
                    f"{self.helper_node_details['bm_tftp_dir']}"
                    "/rhcos-installer-kernel-x86_64 "
                    f"{kernel_image_path}"
                )
                assert self.helper_node_handler.exec_cmd(cmd=cmd), "Failed to Download required File"
            else:
                raise RhcosImageNotFound
            # Download metal_bios
            metal_image_path = constants.coreos_url_prefix + image_data['metal_bios_url']
            if check_for_rhcos_images(metal_image_path):
                cmd = (
                    "wget -O "
                    f"{self.helper_node_details['bm_path_to_upload']}"
                    "/rhcos-metal.x86_64.raw.gz "
                    f"{metal_image_path}"
                )
                assert self.helper_node_handler.exec_cmd(cmd=cmd), "Failed to Download required File"
            else:
                raise RhcosImageNotFound
            # Create pxelinux.cfg directory
            cmd = f"mkdir -m 755 {self.helper_node_details['bm_tftp_dir']}/pxelinux.cfg"
            assert self.helper_node_handler.exec_cmd(cmd=cmd), "Failed to create required folder"

        def deploy(self, log_cli_level='DEBUG'):
            """
            Deploy
            """
            # Uploading pxe files
            logger.info("Deploying OCP cluster for Bare Metal platform")
            logger.info(
                f"Openshift-installer will be using log level:{log_cli_level}"
            )
            upload_file(
                self.host,
                constants.COMMON_CONF_FILE,
                os.path.join(
                    self.helper_node_details['bm_dnsmasq_dir'],
                    "dnsmasq.common.conf"
                ),
                self.user,
                key_file=self.private_key
            )
            logger.info("Uploading PXE files")
            for machine in self.mgmt_details:
                if self.mgmt_details[machine].get('role') == "bootstrap":
                    upload_file(
                        server=self.host,
                        localpath=constants.BOOTSTRAP_PXE_FILE,
                        remotepath=f"{self.helper_node_details['bm_tftp_dir']}"
                                   f"/pxelinux.cfg/01-{self.mgmt_details[machine]['mac'].replace(':', '-')}",
                        user=self.user,
                        key_file=self.private_key
                    )
                elif self.mgmt_details[machine].get('role') == "master":
                    upload_file(
                        server=self.host,
                        localpath=constants.MASTER_PXE_FILE,
                        remotepath=f"{self.helper_node_details['bm_tftp_dir']}"
                                   f"/pxelinux.cfg/01-{self.mgmt_details[machine]['mac'].replace(':', '-')}",
                        user=self.user,
                        key_file=self.private_key
                    )
                elif self.mgmt_details[machine].get('role') == "worker":
                    upload_file(
                        server=self.host,
                        localpath=constants.WORKER_PXE_FILE,
                        remotepath=f"{self.helper_node_details['bm_tftp_dir']}"
                                   f"/pxelinux.cfg/01-{self.mgmt_details[machine]['mac'].replace(':', '-')}",
                        user=self.user,
                        key_file=self.private_key
                    )
            # Applying Permission
            cmd = f"chmod 755 -R {self.helper_node_details['bm_tftp_dir']}"
            self.helper_node_handler.exec_cmd(cmd=cmd)

            # Applying Permission
            cmd = f"chmod 755 -R {self.helper_node_details['bm_path_to_upload']}"
            self.helper_node_handler.exec_cmd(cmd=cmd)

            # Restarting dnsmasq service
            cmd = "systemctl restart dnsmasq"
            assert self.helper_node_handler.exec_cmd(cmd=cmd), "Failed to restart dnsmasq service"
            # Rebooting Machine with pxe boot

            for machine in self.mgmt_details:
                if self.mgmt_details[machine].get('cluster_name') == constants.BM_DEFAULT_CLUSTER_NAME:
                    secrets = [
                        self.mgmt_details[machine]['mgmt_username'],
                        self.mgmt_details[machine]['mgmt_password']
                    ]
                    # Changes boot prioriy to pxe
                    cmd = (
                        f"ipmitool -I lanplus -U {self.mgmt_details[machine]['mgmt_username']} "
                        f"-P {self.mgmt_details[machine]['mgmt_password']} "
                        f"-H {self.mgmt_details[machine]['mgmt_console']} chassis bootdev pxe"
                    )
                    run_cmd(cmd=cmd, secrets=secrets)
                    sleep(2)
                    # Power On Machine
                    cmd = (
                        f"ipmitool -I lanplus -U {self.mgmt_details[machine]['mgmt_username']} "
                        f"-P {self.mgmt_details[machine]['mgmt_password']} "
                        f"-H {self.mgmt_details[machine]['mgmt_console']} chassis power cycle || "
                        f"ipmitool -I lanplus -U {self.mgmt_details[machine]['mgmt_username']} "
                        f"-P {self.mgmt_details[machine]['mgmt_password']} "
                        f"-H {self.mgmt_details[machine]['mgmt_console']} chassis power on"
                    )
                    run_cmd(cmd=cmd, secrets=secrets)
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
                timeout=1800
            )

            # Approving CSRs here in-case if any exists
            approve_pending_csr()

            self.test_cluster()
            logger.info("Performing Disk cleanup")
            clean_disk()
            # We need NTP for OCS cluster to become clean
            configure_chrony_and_wait_for_machineconfig_status(node_type="all")

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
            install_config_obj['pullSecret'] = self.get_pull_secret()
            install_config_obj['sshKey'] = self.get_ssh_key()
            install_config_obj['metadata']['name'] = constants.BM_DEFAULT_CLUSTER_NAME
            install_config_str = yaml.safe_dump(install_config_obj)
            install_config = os.path.join(self.cluster_path, "install-config.yaml")
            install_config_backup = os.path.join(self.cluster_path, "install-config.yaml.backup")
            with open(install_config, "w") as f:
                f.write(install_config_str)
            with open(install_config_backup, "w") as f:
                f.write(install_config_str)

        def create_manifest(self):
            """
            Creates the Manifest files
            """
            logger.info("creating manifest files for the cluster")
            run_cmd(
                f"{self.installer} create manifests "
                f"--dir {self.cluster_path} "
            )

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

        def destroy(self, log_level=''):
            """
            Destroy OCP cluster specific to BM UPI
            """
            logger.info("Updating BM status")
            result = self.update_bm_status(constants.BM_STATUS_ABSENT)
            assert result == constants.BM_STATUS_RESPONSE_UPDATED, "Failed to update request"

        def check_bm_status_exist(self):
            """
            Check if BM Cluster already exist

            Returns:
                str: response status
            """
            headers = {'content-type': "application/json"}
            response = requests.get(
                url=self.helper_node_details['bm_status_check'],
                headers=headers
            )
            return response.json()[0]['status']

        def update_bm_status(self, bm_status):
            """
            Update BM status when cluster is deployed/teardown

            Args:
                bm_status (str): Status to be updated

            Returns:
                str: response message
            """
            payload = {'status': bm_status}
            headers = {'content-type': "application/json"}
            response = requests.put(
                url=self.helper_node_details['bm_status_check'],
                json=payload,
                headers=headers
            )
            return response.json()['message']


def clean_disk():
    """
    Perform disk cleanup
    """
    lvm_to_clean = []
    workers = get_typed_nodes(node_type='worker')
    ocp_obj = ocp.OCP()
    for worker in workers:
        out = ocp_obj.exec_oc_debug_cmd(
            node=worker.name, cmd_list=["lsblk -nd -e252,7 --output NAME --json"]
        )
        logger.info(out)
        lsblk_output = json.loads(str(out))
        lsblk_devices = lsblk_output['blockdevices']
        for lsblk_device in lsblk_devices:
            base_cmd = (
                """pvs --config "devices{filter = [ 'a|/dev/%s.*|', 'r|.*|' ] }" --reportformat json"""
                % lsblk_device['name']
            )

            cmd = (
                f"debug nodes/{worker.name} "
                f"-- chroot /host {base_cmd}"
            )
            out = ocp_obj.exec_oc_cmd(
                command=cmd, out_yaml_format=False,
            )
            logger.info(out)
            pvs_output = json.loads(str(out))
            pvs_list = pvs_output['report']
            for pvs in pvs_list:
                pv_list = pvs['pv']
                for pv in pv_list:
                    logger.debug(pv)
                    device_dict = {
                        'hostname': f"{worker.name}", 'pv_name': f"{pv['pv_name']}"
                    }
                    lvm_to_clean.append(device_dict)
            base_cmd = (
                """vgs --config "devices{filter = [ 'a|/dev/%s.*|', 'r|.*|' ] }" --reportformat json"""
                % lsblk_device['name']
            )

            cmd = (
                f"debug nodes/{worker.name} "
                f"-- chroot /host {base_cmd}"
            )
            out = ocp_obj.exec_oc_cmd(
                command=cmd, out_yaml_format=False,
            )
            logger.info(out)
            vgs_output = json.loads(str(out))
            vgs_list = vgs_output['report']
            for vgs in vgs_list:
                vg_list = vgs['vg']
                for vg in vg_list:
                    logger.debug(vg)
                    device_dict = {
                        'hostname': f"{worker.name}", 'vg_name': f"{vg['vg_name']}"
                    }
                    lvm_to_clean.append(device_dict)
    for devices in lvm_to_clean:
        if devices.get('vg_name'):
            cmd = (
                f"debug nodes/{devices['hostname']} "
                f"-- chroot /host timeout 120 vgremove {devices['vg_name']} -y -f"
            )
            logger.info("Removing vg")
            out = ocp_obj.exec_oc_cmd(
                command=cmd, out_yaml_format=False,
            )
            logger.info(out)
    for devices in lvm_to_clean:
        if devices.get('pv_name'):
            out = ocp_obj.exec_oc_debug_cmd(
                node=devices['hostname'], cmd_list=[f"pvremove {devices['pv_name']} -y"]
            )
            logger.info(out)

    for worker in workers:
        out = ocp_obj.exec_oc_debug_cmd(
            node=worker.name, cmd_list=["lsblk -nd -e252,7 --output NAME --json"]
        )
        lsblk_output = json.loads(str(out))
        lsblk_devices = lsblk_output['blockdevices']
        for lsblk_device in lsblk_devices:
            out = ocp_obj.exec_oc_debug_cmd(
                node=worker.name, cmd_list=[f"lsblk -b /dev/{lsblk_device['name']} --output NAME --json"]
            )
            lsblk_output = json.loads(str(out))
            lsblk_devices_to_clean = lsblk_output['blockdevices']
            for device_to_clean in lsblk_devices_to_clean:
                if not device_to_clean.get('children'):
                    logger.info("Cleaning Disk")
                    out = ocp_obj.exec_oc_debug_cmd(
                        node=worker.name, cmd_list=[f"wipefs -a -f /dev/{device_to_clean['name']}"]
                    )
                    logger.info(out)
                    out = ocp_obj.exec_oc_debug_cmd(
                        node=worker.name, cmd_list=[f"sgdisk --zap-all /dev/{device_to_clean['name']}"]
                    )
                    logger.info(out)


class BaremetalPSIUPI(Deployment):
    """
    All the functionalities related to BaremetalPSI- UPI deployment
    lives here
    """
    def __init__(self):
        self.cluster_name = config.ENV_DATA['cluster_name']
        super().__init__()

    class OCPDeployment(BaseOCPDeployment):
        def __init__(self):
            self.flexy_deployment = True
            super().__init__()
            self.flexy_instance = FlexyBaremetalPSI()
            self.psi_conf = load_auth_config()['psi']
            self.utils = psiutils.PSIUtils(self.psi_conf)

        def deploy_prereq(self):
            """
            Instantiate proper flexy class here

            """
            super().deploy_prereq()
            self.flexy_instance.deploy_prereq()

        def deploy(self, log_level=''):
            self.flexy_instance.deploy(log_level)
            self.test_cluster()
            # add disks to instances
            # Get all instances and for each instance add
            # one disk
            pattern = "-".join(
                [get_infra_id(config.ENV_DATA['cluster_path']), "compute"]
            )
            for instance in self.utils.get_instances_with_pattern(pattern):
                vol = self.utils.create_volume(
                    name=f'{pattern}-disk0-{instance.name[-1]}',
                    size=config.FLEXY['volume_size'],
                )
                # wait till volume is available
                sample = TimeoutSampler(
                    300, 10,
                    self.utils.check_expected_vol_status,
                    vol,
                    'available'
                )
                if not sample.wait_for_func_status(True):
                    logger.info("Volume failed to reach 'available'")
                    raise exceptions.PSIVolumeNotInExpectedState
                # attach the volume
                self.utils.attach_volume(vol, instance.id)

        def destroy(self, log_level=''):
            """
            Destroy volumes attached if any and then the cluster
            """
            # Get all the additional volumes and detach,delete.
            volumes = self.utils.get_volumes_with_tag(
                {'cluster_name': config.ENV_DATA['cluster_name']}
            )
            self.flexy_instance.destroy()
            self.utils.detach_and_delete_vols(volumes)
