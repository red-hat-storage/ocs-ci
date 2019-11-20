"""
This module will install OCP on RHEL nodes
"""
import logging
import os

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import upload
from ocs_ci.utility.templating import Templating
from ocs_ci.utility.utils import create_rhelpod, download_file


logger = logging.getLogger(__name__)


class OCPINSTALLRHEL(object):
    """
    Class to install OCP on RHEL nodes
    """
    def __init__(self, rhel_worker_nodes):
        """
        Initializes the required variables

        Args:
            rhel_worker_nodes (list): list of RHEL nodes
        """
        self.rhel_worker_nodes = rhel_worker_nodes
        ssh_key = config.DEPLOYMENT['ssh_key']
        self.ssh_key_pem = f"{os.path.splitext(ssh_key)[0]}.pem"
        self.pod_ssh_key_pem = os.path.join(
            constants.POD_UPLOADPATH,
            self.ssh_key_pem.split("/")[-1]
        )
        self.ops_mirror_pem = os.path.join(
            f"{constants.DATA_DIR}",
            constants.OCP_PEM
        )
        self.cluster_path = config.ENV_DATA['cluster_path']
        self.kubeconfig = os.path.join(
            config.ENV_DATA['cluster_path'],
            config.RUN.get('kubeconfig_location')
        )
        self.pod_name = "rhelpod"
        self.pull_secret_path = os.path.join(
            constants.TOP_DIR,
            "data",
            "pull-secret"
        )
        self.pod_pull_secret_path = os.path.join(
            constants.POD_UPLOADPATH,
            "pull-secret"
        )
        self.pod_kubeconfig_path = os.path.join(
            constants.POD_UPLOADPATH,
            config.RUN.get('kubeconfig_location').split("/")[-1]
        )
        self.rhelpod = create_rhelpod(
            constants.DEFAULT_NAMESPACE,
            self.pod_name
        )

    def upload_helpers(self):
        """
        Upload helper files to pod for OCP installation on RHEL
        Helper Files:
            - ssh_key pem
            - ocp repo
            - ocp pem
            - kubeconfig
            - pull secret
            - inventory yaml
        """
        upload(self.pod_name, self.ssh_key_pem, constants.POD_UPLOADPATH)
        upload(self.pod_name, constants.OCP4_2_REPO, constants.YUM_REPOS_PATH)
        download_file(
            config.ENV_DATA['ops_mirror_pem_file_location'],
            self.ops_mirror_pem
        )
        upload(self.pod_name, self.ops_mirror_pem, constants.PEM_PATH)
        upload(self.pod_name, self.kubeconfig, constants.POD_UPLOADPATH)
        upload(self.pod_name, self.pull_secret_path, constants.POD_UPLOADPATH)
        self.inventory_yaml = self.create_inventory()
        upload(self.pod_name, self.inventory_yaml, constants.POD_UPLOADPATH)

    def create_inventory(self):
        """
        Creates the inventory file

        Returns:
            str: Path to inventory file
        """
        inventory_data = {}
        inventory_data['pod_kubeconfig'] = self.pod_kubeconfig_path
        inventory_data['pod_pull_secret'] = self.pod_pull_secret_path
        inventory_data['rhel_worker_nodes'] = self.rhel_worker_nodes

        logger.info("Generating inventory file")
        _templating = Templating()
        inventory_template_path = os.path.join(
            "ocp-deployment", constants.INVENTORY_TEMPLATE
        )
        inventory_config_str = _templating.render_template(
            inventory_template_path, inventory_data
        )
        inventory_yaml = os.path.join(
            self.cluster_path,
            constants.TERRAFORM_DATA_DIR,
            constants.INVENTORY_FILE
        )
        logger.info(f"inventory_config_str: {inventory_config_str}")
        logger.info(f"inventory_yaml: {inventory_yaml}")
        with open(inventory_yaml, "w") as f:
            f.write(inventory_config_str)

        return inventory_yaml

    def install_packages_in_pod(self, packages):
        """
        Install packages in pod

        Args:
             packages (list): List of packages
        """
        self.rhelpod.install_packages(packages)

    def prepare_rhel_nodes(self):
        """
        Prepare RHEL nodes for OCP installation
        """
        for node in self.rhel_worker_nodes:
            # set the hostname
            cmd = f"sudo hostnamectl set-hostname {node}"
            self.rhelpod.exec_cmd_on_node(
                node,
                self.pod_ssh_key_pem,
                cmd,
                user=constants.VM_RHEL_USER
            )

            # upload ocp repo to node
            # considering normal user doesn't have permissions
            # to /etc/yum.repos.d/, upload to /tmp/ and then move to
            # /etc/yum.repos.d/ with sudo
            self.rhelpod.copy_to_server(
                node,
                self.pod_ssh_key_pem,
                os.path.join(
                    constants.YUM_REPOS_PATH,
                    f"{constants.OCP4_2_REPO}".split("/")[-1]
                ),
                constants.RHEL_TMP_PATH,
                user=constants.VM_RHEL_USER
            )
            ocp_repo_path_in_rhel = os.path.join(
                constants.RHEL_TMP_PATH,
                f"{constants.OCP4_2_REPO}".split("/")[-1]
            )
            cmd = f"sudo cp {ocp_repo_path_in_rhel} {constants.YUM_REPOS_PATH}"
            self.rhelpod.exec_cmd_on_node(
                node,
                self.pod_ssh_key_pem,
                cmd,
                user=constants.VM_RHEL_USER
            )

            # copy ops-mirror.pem
            self.rhelpod.copy_to_server(
                node,
                self.pod_ssh_key_pem,
                os.path.join(constants.PEM_PATH, constants.OCP_PEM),
                constants.RHEL_TMP_PATH,
                user=constants.VM_RHEL_USER
            )
            pem_path_in_rhel = os.path.join(
                constants.RHEL_TMP_PATH,
                constants.OCP_PEM
            )
            cmd = f"sudo cp {pem_path_in_rhel} {constants.PEM_PATH}"
            self.rhelpod.exec_cmd_on_node(
                node,
                self.pod_ssh_key_pem,
                cmd,
                user=constants.VM_RHEL_USER
            )

    def execute_ansible_playbook(self):
        """
        Run ansible-playbook on pod
        """
        cmd = (
            f"ansible-playbook -i {os.path.join(constants.POD_UPLOADPATH, constants.INVENTORY_FILE)}"
            f" {constants.SCALEUP_ANSIBLE_PLAYBOOK}"
            f" --private-key={self.pod_ssh_key_pem} -v"
        )
        self.rhelpod.exec_cmd_on_pod(cmd, out_yaml_format=False)
