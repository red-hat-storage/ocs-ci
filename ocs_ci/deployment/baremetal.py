import json
import os
import pytest
import logging
import re
import tempfile
import time
from datetime import datetime
from time import sleep

import yaml
import requests
from semantic_version import Version
import socket

from .flexy import FlexyBaremetalPSI
from ocs_ci.utility import psiutils, aws, version

from ocs_ci.deployment.deployment import Deployment
from ocs_ci.framework import config
from ocs_ci.deployment.ocp import OCPDeployment as BaseOCPDeployment
from ocs_ci.deployment import assisted_installer
from ocs_ci.ocs import constants, ocp, exceptions
from ocs_ci.ocs.exceptions import CommandFailed, RhcosImageNotFound
from ocs_ci.ocs.node import get_nodes
from ocs_ci.ocs.openshift_ops import OCP
from ocs_ci.utility import ibmcloud_bm
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
    replace_content_in_file,
)

logger = logging.getLogger(__name__)


class BAREMETALBASE(Deployment):
    """
    A common class for Bare metal deployments
    """

    def __init__(self):
        super().__init__()
        self.cluster_name = config.ENV_DATA["cluster_name"]


class BMBaseOCPDeployment(BaseOCPDeployment):
    def __init__(self):
        super().__init__()
        self.bm_config = config.ENV_DATA["baremetal"]
        self.srv_details = config.ENV_DATA["baremetal"]["servers"]
        self.aws = aws.AWS()
        self.__helper_node_handler = None

    def deploy_prereq(self):
        """
        Pre-Requisites for Bare Metal deployments
        """
        super(BMBaseOCPDeployment, self).deploy_prereq()
        if self.bm_config.get("bm_status_check"):
            # check for BM status
            logger.info("Checking BM Status")
            status = self.check_bm_status_exist()
            if status == constants.BM_STATUS_PRESENT:
                pytest.fail(
                    f"BM Cluster still present and locked by {self.get_locked_username()}"
                )

            # update BM status
            logger.info("Updating BM Status")
            result = self.update_bm_status(constants.BM_STATUS_PRESENT)
            assert (
                result == constants.BM_STATUS_RESPONSE_UPDATED
            ), "Failed to update request"

    # the VM hosting the httpd, tftp and dhcp services might be just started and it might take some time to
    # propagate the DDNS name, if used, so re-trying this function for 20 minutes
    @property
    @retry((TimeoutError, socket.gaierror), tries=10, delay=120, backoff=1)
    def helper_node_handler(self):
        """
        Create connection to helper node hosting httpd, tftp and dhcp services for PXE boot
        """
        if not self.__helper_node_handler:
            self.host = self.bm_config["bm_httpd_server"]
            self.user = self.bm_config["bm_httpd_server_user"]
            self.private_key = os.path.expanduser(config.DEPLOYMENT["ssh_key_private"])

            # wait till the server is up and running
            self.__helper_node_handler = Connection(
                self.host, self.user, self.private_key
            )
        return self.__helper_node_handler

    def check_bm_status_exist(self):
        """
        Check if BM Cluster already exist

        Returns:
            str: response status
        """
        headers = {"content-type": "application/json"}
        response = requests.get(url=self.bm_config["bm_status_check"], headers=headers)
        return response.json()[0]["status"]

    def get_locked_username(self):
        """
        Get name of user who has locked baremetal resource

        Returns:
            str: username
        """
        headers = {"content-type": "application/json"}
        response = requests.get(url=self.bm_config["bm_status_check"], headers=headers)
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

    def destroy(self, log_level=""):
        """
        Destroy OCP cluster
        """
        logger.info("Updating BM status")
        result = self.update_bm_status(constants.BM_STATUS_ABSENT)
        assert (
            result == constants.BM_STATUS_RESPONSE_UPDATED
        ), "Failed to update request"

    def configure_dnsmasq_on_helper_vm(self):
        """
        Install and configure dnsmasq and other required packages
        for DHCP and PXE boot server on helper VM
        """
        # Install Required packages
        cmd = "yum install dnsmasq syslinux-tftpboot -y"
        assert (
            self.helper_node_handler.exec_cmd(cmd=cmd)[0] == 0
        ), "Failed to install required packages"

        # Enable dnsmasq service on boot
        cmd = "systemctl enable dnsmasq"
        assert (
            self.helper_node_handler.exec_cmd(cmd=cmd)[0] == 0
        ), "Failed to Enable dnsmasq service"

        # Create pxelinux.cfg directory
        cmd = f"mkdir -m 755 -p {self.bm_config['bm_tftp_base_dir']}/pxelinux.cfg"
        assert (
            self.helper_node_handler.exec_cmd(cmd=cmd)[0] == 0
        ), "Failed to create required folder"

        if self.bm_config.get("bm_dnsmasq_common_config"):
            self.configure_dnsmasq_common_config()

        if self.bm_config.get("bm_dnsmasq_pxe_config"):
            self.configure_dnsmasq_pxe_config()

        if self.bm_config.get("bm_dnsmasq_hosts_config"):
            self.configure_dnsmasq_hosts_config()

        self.restart_dnsmasq_service_on_helper_vm()

    def configure_dnsmasq_common_config(self):
        """
        Prepare common configuration for dnsmasq
        """
        # create dnsmasq common configuration
        _templating = Templating()
        template_data = {
            "interface": self.bm_config["bm_dnsmasq_interface"],
            "dhcp_range": self.bm_config["bm_dnsmasq_dhcp_range"],
            "dhcp_options": self.bm_config["bm_dnsmasq_dhcp_options"],
        }
        common_config = _templating.render_template(
            constants.DNSMASQ_COMMON_CONF_FILE_TEMPLATE,
            template_data,
        )
        common_config_temp_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="dnsmasq.common", suffix=".conf", delete=False
        )
        with open(common_config_temp_file.name, "w") as t_file:
            t_file.writelines(common_config)
        self.helper_node_handler.upload_file(
            common_config_temp_file.name,
            "/etc/dnsmasq.d/dnsmasq.common.conf",
        )

    def configure_dnsmasq_pxe_config(self):
        """
        Prepare PXE configuration for dnsmasq
        """
        # create dnsmasq PXE configuration
        _templating = Templating()
        template_data = {"tftp_root": self.bm_config["bm_tftp_base_dir"]}
        pxe_config = _templating.render_template(
            constants.DNSMASQ_PXE_CONF_FILE_TEMPLATE,
            template_data,
        )
        pxe_config_temp_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="dnsmasq.pxe", suffix=".conf", delete=False
        )
        with open(pxe_config_temp_file.name, "w") as t_file:
            t_file.writelines(pxe_config)
        self.helper_node_handler.upload_file(
            pxe_config_temp_file.name,
            "/etc/dnsmasq.d/dnsmasq.pxe.conf",
        )

    def configure_dnsmasq_hosts_config(self):
        """
        prepare hosts configuration for dnsmasq dhcp
        """
        hosts_config = ""
        for machine in self.srv_details:
            # which network is used for provisioning (public|private)
            provisioning_network = self.bm_config["bm_provisioning_network"]
            mac = self.srv_details[machine][f"{provisioning_network}_mac"]
            ip = self.srv_details[machine][f"{provisioning_network}_ip"]
            hostname = machine.split(".")[0]
            hosts_config += f"dhcp-host={mac},{ip},{hostname},1h\n"
        temp_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="dnsmasq.hosts", suffix=".conf", delete=False
        )
        with open(temp_file.name, "w") as t_file:
            t_file.writelines(hosts_config)
        self.helper_node_handler.upload_file(
            temp_file.name,
            f"/etc/dnsmasq.d/dnsmasq.hosts.{self.bm_config['env_name']}.conf",
        )

    def start_dnsmasq_service_on_helper_vm(self):
        """
        Start dnsmasq service providing DHCP and TFTP services for UPI deployment
        """
        # Starting dnsmasq service
        cmd = "systemctl start dnsmasq"
        assert (
            self.helper_node_handler.exec_cmd(cmd=cmd)[0] == 0
        ), "Failed to Start dnsmasq service"

    def stop_dnsmasq_service_on_helper_vm(self):
        """
        Stop dnsmasq service providing DHCP and TFTP services for UPI deployment
        """
        # Stopping dnsmasq service
        cmd = "systemctl stop dnsmasq"
        assert (
            self.helper_node_handler.exec_cmd(cmd=cmd)[0] == 0
        ), "Failed to Stop dnsmasq service"

    def restart_dnsmasq_service_on_helper_vm(self):
        """
        Restart dnsmasq service providing DHCP and TFTP services for UPI deployment
        """
        # Restarting dnsmasq service
        cmd = "systemctl restart dnsmasq"
        assert (
            self.helper_node_handler.exec_cmd(cmd=cmd)[0] == 0
        ), "Failed to restart dnsmasq service"


class BAREMETALUPI(BAREMETALBASE):
    """
    A class to handle Bare metal UPI specific deployment
    """

    def __init__(self):
        logger.info("BAREMETAL UPI")
        super().__init__()

    class OCPDeployment(BMBaseOCPDeployment):
        def __init__(self):
            super().__init__()

        def deploy_prereq(self):
            """
            Pre-Requisites for Bare Metal UPI Deployment
            """
            super(BAREMETALUPI.OCPDeployment, self).deploy_prereq()
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

            self.configure_dnsmasq_on_helper_vm()

            # Perform Cleanup for stale entry's
            cmd = f"rm -rf {self.bm_config['bm_tftp_base_dir']}/upi"
            assert self.helper_node_handler.exec_cmd(cmd=cmd), "Failed to Delete folder"

            # prepare pxe boot directory for UPI deployment
            cmd = f"mkdir -m 755 -p {self.bm_config['bm_tftp_base_dir']}/upi"
            assert self.helper_node_handler.exec_cmd(
                cmd=cmd
            ), "Failed to create required folder"

            self.restart_dnsmasq_service_on_helper_vm()

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
                    f"{self.bm_config['bm_tftp_base_dir']}/upi"
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
                    f"{self.bm_config['bm_tftp_base_dir']}/upi"
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
                        remotepath=f"{self.bm_config['bm_tftp_base_dir']}"
                        f"/pxelinux.cfg/01-{self.srv_details[machine]['private_mac'].replace(':', '-')}",
                        user=self.user,
                        key_file=self.private_key,
                    )
            # Applying Permission
            cmd = f"chmod 755 -R {self.bm_config['bm_tftp_base_dir']}"
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
            ocp_obj = ocp.OCP()
            policy = constants.PSA_BASELINE
            if version.get_semantic_ocp_version_from_config() >= version.VERSION_4_12:
                policy = constants.PSA_PRIVILEGED
            ocp_obj.new_project(project_name=constants.BM_DEBUG_NODE_NS, policy=policy)
            time.sleep(10)
            workers = get_nodes(node_type="worker")
            for worker in workers:
                clean_disk(worker)
            ocp_obj.delete_project(project_name=constants.BM_DEBUG_NODE_NS)

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

            super().destroy(log_level=log_level)

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
    KERNEL upi/rhcos-installer-kernel-x86_64
    APPEND ip=enp1s0f0:dhcp ip=enp1s0f1:dhcp rd.neednet=1 initrd=upi/rhcos-installer-initramfs.x86_64.img \
console=ttyS0 console=tty0 coreos.inst.install_dev=/dev/disk/by-id/{disk_path} {bm_metal_loc} \
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
            # Changes boot priority to pxe
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


class BAREMETALAI(BAREMETALBASE):
    """
    A class to handle Bare metal Assisted Installer specific deployment
    """

    def __init__(self):
        logger.info("BAREMETAL AI")
        super(BAREMETALAI, self).__init__()

    class OCPDeployment(BMBaseOCPDeployment):
        def __init__(self):
            super(BAREMETALAI.OCPDeployment, self).__init__()

        def deploy_prereq(self):
            """
            Pre-Requisites for Bare Metal AI Deployment
            """
            super().deploy_prereq()

            # create initial metadata.json file in cluster dir, to ensure, that
            # destroy job will be properly triggered even when the deployment fails
            # and metadata.json file will not be created
            with open(
                os.path.join(self.cluster_path, "metadata.json"), "w"
            ) as metadata_file:
                json.dump(
                    {"clusterName": self.cluster_name, "infraID": self.cluster_name},
                    metadata_file,
                )
            # load API and Ingress IPs from config
            self.api_vip = config.ENV_DATA["api_vip"]
            self.ingress_vip = config.ENV_DATA["ingress_vip"]

            # prepare required dnsmasq configuration (for PXE boot)
            self.configure_dnsmasq_on_helper_vm()

            # prepare ipxe directory in web document root
            cmd = f"mkdir -m 755 -p {self.bm_config['bm_httpd_document_root']}/ipxe"
            logger.info(self.helper_node_handler.exec_cmd(cmd=cmd))

            # cleanup leftover files on httpd server from previous deployment
            cmd = f"rm -rf {self.bm_config['bm_httpd_document_root']}/ipxe/{self.bm_config['env_name']}"
            logger.info(self.helper_node_handler.exec_cmd(cmd=cmd))
            # create cluster/environment specific folder on httpd server
            cmd = f"mkdir -m 755 {self.bm_config['bm_httpd_document_root']}/ipxe/{self.bm_config['env_name']}"
            assert (
                self.helper_node_handler.exec_cmd(cmd=cmd)[0] == 0
            ), "Failed to create required folder"

            self.configure_ipxe_on_helper()

        def create_config(self):
            """
            Create the OCP deploy config.
            """
            logger.debug(
                "create_config() is not required for Assisted installer deployment"
            )

        def deploy(self, log_cli_level="DEBUG"):
            """
            Deployment specific to OCP cluster on this platform

            Args:
                log_cli_level (str): not used for Assisted Installer deployment

            """
            logger.info(
                "Deploying OCP cluster on Bare Metal platform via Assisted Installer"
            )

            # prepare hosts configuration
            master_count = 0
            master_nodes = []
            worker_count = 0
            worker_nodes = []
            # MAC addresses to node name mapping
            mac_name_mapping = {}
            # MAC addresses to node role mapping
            mac_role_mapping = {}
            static_network_config = []
            # which network is used for provisioning (public|private)
            provisioning_network = self.bm_config["bm_provisioning_network"]
            for machine in self.srv_details:
                if (
                    self.srv_details[machine]["role"] == constants.MASTER_MACHINE
                    and master_count < config.ENV_DATA["master_replicas"]
                ):
                    master_nodes.append(machine)
                    mac_name_mapping[
                        self.srv_details[machine][f"{provisioning_network}_mac"]
                    ] = machine
                    mac_role_mapping[
                        self.srv_details[machine][f"{provisioning_network}_mac"]
                    ] = "master"
                    master_count += 1
                elif (
                    self.srv_details[machine]["role"] == constants.WORKER_MACHINE
                    and worker_count < config.ENV_DATA["worker_replicas"]
                ):
                    worker_nodes.append(machine)
                    mac_name_mapping[
                        self.srv_details[machine][f"{provisioning_network}_mac"]
                    ] = machine
                    mac_role_mapping[
                        self.srv_details[machine][f"{provisioning_network}_mac"]
                    ] = "worker"
                    worker_count += 1

            # check number of available master and worker nodes (in configuration)
            assert len(master_nodes) == config.ENV_DATA["master_replicas"], (
                f"Number of available master_nodes ({len(master_nodes)}: {master_nodes}) in configuration "
                f"doesn't match configured master_replicas ({config.ENV_DATA['master_replicas']}."
            )
            assert len(worker_nodes) == config.ENV_DATA["worker_replicas"], (
                f"Number of available worker_nodes ({len(worker_nodes)}: {worker_nodes}) in configuration "
                f"doesn't match configured worker_replicas ({config.ENV_DATA['worker_replicas']}."
            )

            # use static network configuration, instead of configuration from DHCP
            # TODO: this part (static network configuration) is not fully implemented
            if self.bm_config.get("network_config") == "static":
                _templating = Templating()
                for machine in master_nodes + worker_nodes:
                    network_yaml_str = _templating.render_template(
                        constants.AI_NETWORK_CONFIG_TEMPLATE, self.srv_details[machine]
                    )
                    network_yaml = yaml.safe_load(network_yaml_str)
                    static_network_config.append(
                        {
                            "mac_interface_map": [
                                {
                                    "logical_nic_name": "eth0",
                                    "mac_address": self.srv_details[machine][
                                        "private_mac"
                                    ],
                                },
                            ],
                            "network_yaml": yaml.safe_dump(network_yaml),
                        }
                    )

            # initialize AssistedInstallerCluster object
            self.ai_cluster = assisted_installer.AssistedInstallerCluster(
                name=self.cluster_name,
                cluster_path=self.cluster_path,
                openshift_version=str(version.get_semantic_ocp_version_from_config()),
                base_dns_domain=config.ENV_DATA["base_domain"],
                api_vip=self.api_vip,
                ingress_vip=self.ingress_vip,
                ssh_public_key=self.get_ssh_key(),
                pull_secret=self.get_pull_secret(),
                static_network_config=static_network_config,
            )

            # create (register) cluster in Assisted Installer console
            self.ai_cluster.create_cluster()

            # create Infrastructure Environment in Assisted Installer console
            self.ai_cluster.create_infrastructure_environment()

            # configure DNS records for API and Ingress
            self.create_dns_records()

            # download discovery ipxe config
            ipxe_config_file = self.ai_cluster.download_ipxe_config(self.cluster_path)
            # parse ipxe_config_file for initrd, kernel and rootfs urls
            with open(ipxe_config_file) as ipxe_config_content:
                content = ipxe_config_content.read()
            initrd_url = re.search(r"\ninitrd --name initrd (.*)\n", content).group(1)
            kernel_url, rootfs_url = re.search(
                r"\nkernel ([^ ]*) initrd=initrd coreos.live.rootfs_url=([^ ]*)",
                content,
            ).groups()

            # download initrd, kernel and rootfs to httpd server
            dest_dir = f"{self.bm_config['bm_httpd_document_root']}/ipxe/{self.bm_config['env_name']}"
            cmd = f"wget --no-verbose -O {dest_dir}/initrd '{initrd_url}'"
            assert (
                self.helper_node_handler.exec_cmd(cmd=cmd)[0] == 0
            ), "Failed to download initrd"
            cmd = f"wget --no-verbose -O {dest_dir}/kernel '{kernel_url}'"
            assert (
                self.helper_node_handler.exec_cmd(cmd=cmd)[0] == 0
            ), "Failed to download kernel"
            cmd = f"wget --no-verbose -O {dest_dir}/rootfs '{rootfs_url}'"
            assert (
                self.helper_node_handler.exec_cmd(cmd=cmd)[0] == 0
            ), "Failed to download rootfs"

            # update ipxe_config_file to point initrd, kernel and rootfs to the downloaded files
            base_dest_url = (
                f"http://{self.bm_config['bm_httpd_provision_server']}/ipxe/"
                f"{self.bm_config['env_name']}"
            )
            replace_content_in_file(
                ipxe_config_file, initrd_url, f"{base_dest_url}/initrd"
            )
            replace_content_in_file(
                ipxe_config_file, kernel_url, f"{base_dest_url}/kernel"
            )
            replace_content_in_file(
                ipxe_config_file, rootfs_url, f"{base_dest_url}/rootfs"
            )

            # upload ipxe config to httpd server (helper_node)
            self.helper_node_handler.upload_file(
                ipxe_config_file,
                (
                    f"{self.bm_config['bm_httpd_document_root']}/ipxe/"
                    f"{self.bm_config['env_name']}/discovery.ipxe"
                ),
            )
            ipxe_file_url = (
                f"http://{self.bm_config['bm_httpd_provision_server']}/ipxe/"
                f"{self.bm_config['env_name']}/discovery.ipxe"
            )

            # configure pxelinux.cfg files for each server (named based on MAC address)
            # to boot from the ipxe configuration file
            pxelinux_cfg_file = self.create_pxe_file(ipxe_file_url=ipxe_file_url)
            for machine in master_nodes + worker_nodes:
                mac = self.srv_details[machine][f"{provisioning_network}_mac"]
                dest_cfg_file_name = f"01-{mac.replace(':', '-')}"
                self.helper_node_handler.upload_file(
                    pxelinux_cfg_file,
                    f"{self.bm_config['bm_tftp_base_dir']}/pxelinux.cfg/{dest_cfg_file_name}",
                )

            # reboot all servers and boot them from PXE
            for machine in master_nodes + worker_nodes:
                self.set_pxe_boot_and_reboot(machine)

            # wait for discovering all nodes
            expected_node_num = (
                config.ENV_DATA["master_replicas"] + config.ENV_DATA["worker_replicas"]
            )
            self.ai_cluster.wait_for_discovered_nodes(expected_node_num)

            # verify validations info
            self.ai_cluster.verify_validations_info_for_discovered_nodes()

            # configure pxelinux.cfg files for each server (named based on MAC address)
            # to boot from the first disk (without this change, if the servers are configured to boot from PXE, they
            # will be stuck in boot loop
            pxelinux_cfg_file = self.create_pxe_file(
                template=constants.PXELINUX_CFG_DISK0_TEMPLATE
            )
            for machine in master_nodes + worker_nodes:
                mac = self.srv_details[machine][f"{provisioning_network}_mac"]
                dest_cfg_file_name = f"01-{mac.replace(':', '-')}"
                self.helper_node_handler.upload_file(
                    pxelinux_cfg_file,
                    f"{self.bm_config['bm_tftp_base_dir']}/pxelinux.cfg/{dest_cfg_file_name}",
                )

            # update discovered hosts (configure hostname and role)
            self.ai_cluster.update_hosts_config(
                mac_name_mapping=mac_name_mapping, mac_role_mapping=mac_role_mapping
            )

            # install the OCP cluster
            self.ai_cluster.install_cluster()

        def create_dns_records(self):
            """
            Configure DNS records for api and ingress
            """
            response_list = []
            zone_id = self.aws.get_hosted_zone_id_for_domain()
            response_list.append(
                self.aws.update_hosted_zone_record(
                    zone_id=zone_id,
                    record_name=f"api.{self.cluster_name}",
                    data=self.api_vip,
                    type="A",
                    operation_type="Add",
                )
            )
            response_list.append(
                self.aws.update_hosted_zone_record(
                    zone_id=zone_id,
                    record_name=f"*.apps.{self.cluster_name}",
                    data=self.ingress_vip,
                    type="A",
                    operation_type="Add",
                )
            )
            logger.info("Waiting for Record Response")
            self.aws.wait_for_record_set(response_list=response_list)
            logger.info("Records Created Successfully")

        def configure_ipxe_on_helper(self):
            """
            Configure iPXE on helper node
            """
            cmd = f"mkdir -m 755 -p {self.bm_config['bm_tftp_base_dir']}/ipxe"
            assert (
                self.helper_node_handler.exec_cmd(cmd=cmd)[0] == 0
            ), "Failed to create required folder"

            cmd = (
                f"wget -O {self.bm_config['bm_tftp_base_dir']}/ipxe/ipxe.lkrn "
                f"http://boot.ipxe.org/ipxe.lkrn"
            )
            assert (
                self.helper_node_handler.exec_cmd(cmd=cmd)[0] == 0
            ), "Failed to download ipxe.lkrn"

        def create_pxe_file(
            self, template=constants.PXELINUX_CFG_IPXE_TEMPLATE, **kwargs
        ):
            """
            Prepare content of PXE file for chain loading to ipxe
            """
            _templating = Templating()
            template_data = kwargs
            pxe_config_content = _templating.render_template(
                template,
                template_data,
            )
            temp_file = tempfile.NamedTemporaryFile(
                mode="w+", prefix=f"ipxe.{self.bm_config['env_name']}", delete=False
            )
            with open(temp_file.name, "w") as t_file:
                t_file.writelines(pxe_config_content)
            return temp_file.name

        def set_pxe_boot_and_reboot(self, machine):
            """
            Ipmi Set Pxe boot and Restart the machine

            Args:
                machine (str): Machine Name

            """
            if self.srv_details[machine].get("mgmt_provider", "ipmitool") == "ipmitool":
                secrets = [
                    self.srv_details[machine]["mgmt_username"],
                    self.srv_details[machine]["mgmt_password"],
                ]
                # Changes boot priority to pxe
                cmd = (
                    f"ipmitool -I lanplus -U {self.srv_details[machine]['mgmt_username']} "
                    f"-P {self.srv_details[machine]['mgmt_password']} "
                    f"-H {self.srv_details[machine]['mgmt_console']} chassis bootdev pxe"
                )
                self.helper_node_handler.exec_cmd(cmd=cmd, secrets=secrets)
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
                self.helper_node_handler.exec_cmdrun_cmd(cmd=cmd, secrets=secrets)

            elif (
                self.srv_details[machine].get("mgmt_provider", "ipmitool") == "ibmcloud"
            ):
                ibmcloud = ibmcloud_bm.IBMCloudBM()
                m = ibmcloud.get_machines_by_names([machine])
                ibmcloud.stop_machines(m)
                time.sleep(5)
                ibmcloud.start_machines(m)
                # run the power-on command second time to make sure the host is powered on
                time.sleep(5)
                ibmcloud.start_machines(m)

        def destroy(self):
            """
            Cleanup cluster related resources.
            """
            # delete cluster definition from Assisted Installer console
            try:
                ai_cluster = assisted_installer.AssistedInstallerCluster(
                    name=self.cluster_name,
                    cluster_path=self.cluster_path,
                    existing_cluster=True,
                )
                ai_cluster.delete_cluster()
                ai_cluster.delete_infrastructure_environment()
            except (
                exceptions.OpenShiftAPIResponseException,
                exceptions.ClusterNotFoundException,
            ) as err:
                logger.warning(
                    f"Failed to delete cluster in Assisted Installer Console: {err}\n"
                    "(ignoring the failure and continuing the destroy process to remove other resources)"
                )

            # delete DNS records for API and Ingress
            # get the record sets
            record_sets = self.aws.get_record_sets()
            # form the record sets to delete
            cluster_domain = (
                f"{config.ENV_DATA.get('cluster_name')}."
                f"{config.ENV_DATA.get('base_domain')}"
            )
            records_to_delete = [
                f"api.{cluster_domain}.",
                f"\\052.apps.{cluster_domain}.",
            ]
            # delete the records
            hosted_zone_id = self.aws.get_hosted_zone_id_for_domain()
            logger.debug(f"hosted zone id: {hosted_zone_id}")
            for record in record_sets:
                if record["Name"] in records_to_delete:
                    logger.info(f"Deleting DNS record: {record}")
                    self.aws.delete_record(record, hosted_zone_id)

            # cleanup ipxe provisioning files
            cmd = f"rm -rf {self.bm_config['bm_httpd_document_root']}/ipxe/{self.bm_config['env_name']}"
            logger.info(self.helper_node_handler.exec_cmd(cmd=cmd))

    def destroy_cluster(self, log_level="DEBUG"):
        """
        Destroy OCP cluster specific to Baremetal - Assisted installer deployment

        Args:
            log_level (str): this parameter is not used here

        """
        self.ocp_deployment = self.OCPDeployment()
        self.ocp_deployment.destroy()


@retry(exceptions.CommandFailed, tries=10, delay=30, backoff=1)
def disks_available_to_cleanup(worker, namespace=constants.DEFAULT_NAMESPACE):
    """
    disks available for cleanup

    Args:
        worker (object): worker node object
        namespace (str): namespace where the oc_debug command will be executed

    Returns:
        disk_names_available_for_cleanup (list): The disk names available for cleanup on a node

    """
    ocp_obj = ocp.OCP()
    cmd = """lsblk --all --noheadings --output "KNAME,PKNAME,TYPE,MOUNTPOINT" --json"""
    out = ocp_obj.exec_oc_debug_cmd(
        node=worker.name, cmd_list=[cmd], namespace=namespace
    )
    disk_to_ignore_cleanup_raw = json.loads(str(out))
    disks_available = disk_to_ignore_cleanup_raw["blockdevices"]
    boot_disks = set()
    disks_available_for_cleanup = []
    for disk in disks_available:
        # First pass: identify boot disks and filter out ROM disks
        if disk["type"] == "rom":
            continue
        if "nbd" in disk["kname"]:
            continue
        if disk["type"] == "part" and disk["mountpoint"] == "/boot":
            boot_disks.add(disk["pkname"])
        if disk["type"] == "disk":
            disks_available_for_cleanup.append(disk)

    # Second pass: filter out boot disks
    disks_available_for_cleanup = [
        disk for disk in disks_available_for_cleanup if disk["kname"] not in boot_disks
    ]
    disks_names_available_for_cleanup = [
        disk["kname"] for disk in disks_available_for_cleanup
    ]

    return disks_names_available_for_cleanup


@retry(exceptions.CommandFailed, tries=10, delay=30, backoff=1)
def clean_disk(worker, namespace=constants.DEFAULT_NAMESPACE):
    """
    Perform disk cleanup

    Args:
        worker (object): worker node object
        namespace (str): namespace where the oc_debug command will be executed

    """
    ocp_obj = ocp.OCP()
    disks_available_on_worker_nodes_for_cleanup = disks_available_to_cleanup(worker)

    out = ocp_obj.exec_oc_debug_cmd(
        node=worker.name,
        cmd_list=["lsblk -nd -e252,7 --output NAME --json"],
        namespace=namespace,
    )
    lsblk_output = json.loads(str(out))
    lsblk_devices = lsblk_output["blockdevices"]

    for lsblk_device in lsblk_devices:
        if lsblk_device["name"] not in disks_available_on_worker_nodes_for_cleanup:
            logger.info(f'the disk cleanup is ignored for, {lsblk_device["name"]}')
            pass
        else:
            logger.info(f"Cleaning up {lsblk_device['name']}")
            out = ocp_obj.exec_oc_debug_cmd(
                node=worker.name,
                cmd_list=[f"wipefs -a -f /dev/{lsblk_device['name']}"],
                namespace=namespace,
            )
            logger.info(out)
            out = ocp_obj.exec_oc_debug_cmd(
                node=worker.name,
                cmd_list=[f"sgdisk --zap-all /dev/{lsblk_device['name']}"],
                namespace=namespace,
            )
            logger.info(out)


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
