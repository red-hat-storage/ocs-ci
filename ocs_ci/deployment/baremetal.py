import json
import logging
import os

import yaml

from ocs_ci.deployment.deployment import Deployment
from ocs_ci.framework import config
from ocs_ci.deployment.ocp import OCPDeployment as BaseOCPDeployment
from ocs_ci.ocs import constants, ocp, exceptions
from ocs_ci.ocs.node import get_typed_nodes
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility.bootstrap import gather_bootstrap
from ocs_ci.utility.connection import Connection
from ocs_ci.utility.csr import wait_for_all_nodes_csr_and_approve, approve_pending_csr
from ocs_ci.utility.templating import Templating
from ocs_ci.utility.utils import run_cmd, upload_file, get_ocp_version, load_auth_config, wait_for_co

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

        def deploy_prereq(self):
            """
             Pre-Requisites for Bare Metal UPI Deployment
            """
            super(BAREMETALUPI.OCPDeployment, self).deploy_prereq()
            # create manifest
            self.create_manifest()
            # create ignitions
            self.create_ignitions()
            self.kubeconfig = os.path.join(self.cluster_path, config.RUN.get('kubeconfig_location'))
            bootstrap_path = os.path.join(config.ENV_DATA.get('cluster_path'), constants.BOOTSTRAP_IGN)
            master_path = os.path.join(config.ENV_DATA.get('cluster_path'), constants.MASTER_IGN)
            worker_path = os.path.join(config.ENV_DATA.get('cluster_path'), constants.WORKER_IGN)

            self.host = constants.bm_httpd_server
            self.user = constants.bm_httpd_server_user
            self.private_key = os.path.expanduser(
                config.DEPLOYMENT['ssh_key_private']
            )

            self.helper_node_handler = Connection(self.host, self.user, self.private_key)
            cmd = f"rm -rf {constants.bm_path_to_upload}"
            logger.info(self.helper_node_handler.exec_cmd(cmd=cmd))
            cmd = f"mkdir -m 755 {constants.bm_path_to_upload}"
            assert self.helper_node_handler.exec_cmd(cmd=cmd), ("Failed to create required folder")

            # Upload bootstrap ignition to public access server
            upload_file(
                self.host,
                bootstrap_path,
                os.path.join(
                    constants.bm_path_to_upload,
                    f"{constants.BOOTSTRAP_IGN}"
                ),
                self.user,
                key_file=self.private_key
            )
            # Upload Master ignition to public access server
            upload_file(
                self.host,
                master_path,
                os.path.join(
                    constants.bm_path_to_upload,
                    f"{constants.MASTER_IGN}"
                ),
                self.user,
                key_file=self.private_key
            )
            # Upload Worker ignition to public access server
            upload_file(
                self.host,
                worker_path,
                os.path.join(
                    constants.bm_path_to_upload,
                    f"{constants.WORKER_IGN}"
                ),
                self.user,
                key_file=self.private_key
            )
            # Applying Permission
            cmd = f"chmod 755 -R {constants.bm_path_to_upload}"
            self.helper_node_handler.exec_cmd(cmd=cmd)

            # Perform Cleanup for stale entry's
            cmd = f"rm -rf {constants.bm_tftp_base_dir}"
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

            cmd = f"mkdir -m 755 -p {constants.bm_tftp_base_dir}"
            assert self.helper_node_handler.exec_cmd(cmd=cmd), ("Failed to create required folder")

            cmd = f"mkdir -m 755 -p {constants.bm_tftp_base_dir}/ocs4qe"
            assert self.helper_node_handler.exec_cmd(cmd=cmd), ("Failed to create required folder")

            cmd = f"mkdir -m 755 -p {constants.bm_tftp_base_dir}/ocs4qe/baremetal"
            assert self.helper_node_handler.exec_cmd(cmd=cmd), ("Failed to create required folder")

            cmd = f"rm -rf {constants.bm_dnsmasq_dir}*"
            assert self.helper_node_handler.exec_cmd(cmd=cmd), ("Failed to Delete dir")

            # Install syslinux
            cmd = "yum install syslinux -y"
            assert self.helper_node_handler.exec_cmd(cmd=cmd), "Failed to install required package"

            # Copy syslinux files to the tftp path
            cmd = f"cp -ar /usr/share/syslinux/* {constants.bm_tftp_dir}"
            assert self.helper_node_handler.exec_cmd(cmd=cmd), ("Failed to Copy required files")

            upload_file(
                self.host,
                constants.PXE_CONF_FILE,
                os.path.join(
                    constants.bm_dnsmasq_dir,
                    "dnsmasq.pxe.conf"
                ),
                self.user,
                key_file=self.private_key
            )
            upload_file(
                self.host,
                constants.COMMON_CONF_FILE,
                os.path.join(
                    constants.bm_dnsmasq_dir,
                    "dnsmasq.common.conf"
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
            cmd = f"wget -O {constants.bm_tftp_dir}/rhcos-installer-initramfs.x86_64.img {constants.coreos_url_prefix}" \
                  f"{image_data['installer_initramfs_url']}"
            assert self.helper_node_handler.exec_cmd(cmd=cmd), "Failed to Download required File"
            # Download installer_kernel
            cmd = f"wget -O {constants.bm_tftp_dir}/rhcos-installer-kernel-x86_64 {constants.coreos_url_prefix}" \
                  f"{image_data['installer_kernel_url']}"
            assert self.helper_node_handler.exec_cmd(cmd=cmd), "Failed to Download required File"
            # Download metal_bios
            cmd = f"wget -O {constants.bm_path_to_upload}/rhcos-metal.x86_64.raw.gz {constants.coreos_url_prefix}" \
                  f"{image_data['metal_bios_url']}"
            assert self.helper_node_handler.exec_cmd(cmd=cmd), "Failed to Download required File"
            # Create pxelinux.cfg directory
            cmd = f"mkdir -m 755 {constants.bm_tftp_dir}/pxelinux.cfg"
            assert self.helper_node_handler.exec_cmd(cmd=cmd), "Failed to create required folder"

        def deploy(self, log_cli_level='DEBUG'):
            """
            Deploy
            """
            # Uploading pxe files
            logger.info("Deploying OCP cluster for Bare Metal platform")
            logger.info(
                f"Openshift-installer will be using loglevel:{log_cli_level}"
            )
            upload_file(
                self.host,
                constants.COMMON_CONF_FILE,
                os.path.join(
                    constants.bm_dnsmasq_dir,
                    "dnsmasq.common.conf"
                ),
                self.user,
                key_file=self.private_key
            )
            file_list = os.listdir(constants.PXE_FILE)
            for files in file_list:
                upload_file(
                    self.host,
                    f"{constants.PXE_FILE}/{files}",
                    os.path.join(
                        f"{constants.bm_tftp_dir}/pxelinux.cfg",
                        f"{files}"
                    ),
                    self.user,
                    key_file=self.private_key
                )
            # Restarting dnsmasq service
            cmd = "systemctl restart dnsmasq"
            assert self.helper_node_handler.exec_cmd(cmd=cmd), "Failed to restart dnsmasq service"

            # Rebooting Machine with pxe boot
            self.mgmt_details = load_auth_config()['ipmi']
            for machine_id in range(1, 6):
                machine_name = f"argo00{1}.ceph.redhat.com"
                if self.mgmt_details[machine_name]:
                    # Power off machine
                    cmd = f"ipmitool -I lanplus -U {self.mgmt_details[machine_name]['mgmt_username']} " \
                          f"-P {self.mgmt_details[machine_name]['mgmt_password']} " \
                          f"-H {self.mgmt_details[machine_name]['mgmt_console']} chassis power off"
                    secrets = [
                        {self.mgmt_details[machine_name]['mgmt_username']},
                        {self.mgmt_details[machine_name]['mgmt_password']},
                    ]
                    run_cmd(cmd=cmd, secrets=secrets)
                    # Changes boot prioriy to pxe
                    cmd = f"ipmitool -I lanplus -U {self.mgmt_details[machine_name]['mgmt_username']} " \
                          f"-P {self.mgmt_details[machine_name]['mgmt_password']} " \
                          f"-H {self.mgmt_details[machine_name]['mgmt_console']} chassis bootdev pxe"
                    run_cmd(cmd=cmd, secrets=secrets)
                    # Power On Machine
                    cmd = f"ipmitool -I lanplus -U {self.mgmt_details[machine_name]['mgmt_username']} " \
                          f"-P {self.mgmt_details[machine_name]['mgmt_password']} " \
                          f"-H {self.mgmt_details[machine_name]['mgmt_console']} chassis power on"
                    run_cmd(cmd=cmd, secrets=secrets)

            try:
                run_cmd(
                    f"{self.installer} create cluster "
                    f"--dir {self.cluster_path} "
                    f"--log-level {log_cli_level}",
                    timeout=3600
                )
            except exceptions.CommandFailed as e:
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
            # We need NTP for OCS cluster to become clean
            logger.info("creating ntp chrony")
            run_cmd(f"oc create -f {constants.NTP_CHRONY_CONF}")

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
                f"{self.installer} create ignition-configs "
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


def clean_disk():
    """
    Perform disk cleanup
    """
    device_to_clean_list = []
    workers = get_typed_nodes(node_type='worker')
    ocp_obj = ocp.OCP()
    for worker in workers:
        cmd = (
            f"debug nodes/{worker.name} "
            f"-- chroot /host lsblk -nd -e252,7 --output NAME --json"
        )
        out = ocp_obj.exec_oc_cmd(
            command=cmd, out_yaml_format=False,
        )
        lsblk_output = json.loads(str(out))
        lsblk_devices = lsblk_output['blockdevices']
        for lsblk_device in lsblk_devices:
            base_cmd = """pvs --config "devices{filter = [ 'a|/dev/%s.*|', 'r|.*|' ] }" --reportformat json""" \
                       % lsblk_device['name']
            cmd = (
                f"debug nodes/{worker.name} "
                f"-- chroot /host {base_cmd}"
            )
            out = ocp_obj.exec_oc_cmd(
                command=cmd, out_yaml_format=False,
            )
            pvs_output = json.loads(str(out))
            pvs_list = pvs_output['report']
            for pvs in pvs_list:
                pv_list = pvs['pv']
                for pv in pv_list:
                    logger.debug(pv)
                    device_dict = {
                        'hostname': f"{worker.name}", 'pv_name': f"{pv['pv_name']}",
                        'vg_name': f"{pv['vg_name']}"
                    }
                    device_to_clean_list.append(device_dict)

    for devices in device_to_clean_list:
        cmd = (
            f"debug nodes/{devices['hostname']} "
            f"-- chroot /host vgremove {devices['vg_name']} -y"
        )
        logger.info("Removing vg")
        out = ocp_obj.exec_oc_cmd(
            command=cmd, out_yaml_format=False,
        )
        logger.info(out)

    for devices in device_to_clean_list:
        cmd = (
            f"debug nodes/{devices['hostname']} "
            f"-- chroot /host pvremove {devices['pv_name']} -y"
        )
        logger.info("Removing pv")
        out = ocp_obj.exec_oc_cmd(
            command=cmd, out_yaml_format=False,
        )
        logger.info(out)

    for devices in device_to_clean_list:
        cmd = (
            f"debug nodes/{devices['hostname']} "
            f"-- chroot /host wipefs -a -f {devices['pv_name']}"
        )
        logger.info("Removing pv")
        out = ocp_obj.exec_oc_cmd(
            command=cmd, out_yaml_format=False,
        )
        logger.info(out)
