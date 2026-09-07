"""
Module that contains operations related to vSphere nodes in a cluster
This module directly interacts with VM nodes
"""
import errno
import json
import logging
import os

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import get_node_ips
from ocs_ci.utility.connection import Connection
from ocs_ci.utility.utils import get_module_ip

logger = logging.getLogger(__name__)


class VSPHERENode(object):
    """
    A class that handles operations related to VM node
    """

    def __init__(self, host, user=None, private_key=None):
        """
        Initialize all required variables

        Args:
            host (str): Host name or IP address
            user (str): User name to connect
            private_key (str): Private key  to connect to node

        """
        self.host = host
        self.user = user or constants.VSPHERE_NODE_USER
        self.private_key = private_key or os.path.expanduser(
            config.DEPLOYMENT["ssh_key_private"]
        )
        self.vmnode = Connection(self.host, self.user, self.private_key)

    def replace_ntp_server_in_chrony(self, server=None):
        """
        Replace default NTP server to given server

        Args:
            server (str): NTP server

        """
        default_str = "pool 2.rhel.pool.ntp.org"
        ntp_server_str = f"server {server}"

        # backup the conf file
        cmd = f"sudo cp {constants.CHRONY_CONF}" f" {constants.CHRONY_CONF}_backup"
        self.vmnode.exec_cmd(cmd)

        # replace default NTP server
        cmd = (
            f"sudo sed -i 's/{default_str}/{ntp_server_str}/'"
            f" {constants.CHRONY_CONF}"
        )
        self.vmnode.exec_cmd(cmd)

    def restart_service(self, service_name):
        """
        Restarts the given service
        """
        cmd = f"sudo systemctl restart {service_name}"
        return self.vmnode.exec_cmd(cmd)

    def restart_chrony(self):
        """
        Restarts chrony service

        Returns:
            bool: True if successful restarts of chrony, False otherwise

        """
        _rc = False
        logger.info(f"Restarting chronyd service on {self.host}")
        retcode, _, _ = self.restart_service("chronyd")
        if retcode:
            logger.info("Successfully restarted chronyd service")
            _rc = True
        return _rc

    def set_host_name(self, host_name):
        """
        Sets the host name

        Args:
            host_name (str): Name to set as host name

        Returns:
            tuple: tuple which contains command return code, output and error

        """
        cmd = f"sudo hostnamectl set-hostname {host_name}"
        return self.vmnode.exec_cmd(cmd)

    def is_kubelet_active(self):
        """
        Check if kubelet service is active on the node

        Returns:
            bool: True if kubelet is active, False otherwise

        """
        retcode, stdout, _ = self.vmnode.exec_cmd("sudo systemctl is-active kubelet")
        is_active = retcode == 0 and stdout.strip() == "active"
        logger.info(f"Kubelet on {self.host}: {'active' if is_active else 'inactive'}")
        return is_active

    def get_active_ostree_image(self):
        """
        Get the active ostree image reference from rpm-ostree status

        Returns:
            str: The active deployment's container image reference, or None

        """
        retcode, stdout, _ = self.vmnode.exec_cmd("sudo rpm-ostree status --json")
        if retcode != 0:
            logger.warning(f"Failed to get rpm-ostree status on {self.host}")
            return None
        try:
            status = json.loads(stdout)
            for deployment in status.get("deployments", []):
                if deployment.get("booted"):
                    return deployment.get("container-image-reference", "")
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Failed to parse rpm-ostree status on {self.host}: {e}")
        return None

    def rpm_ostree_rebase(self, image_ref):
        """
        Rebase the node to a new ostree image

        Args:
            image_ref (str): The target image reference
                e.g. quay.io/openshift-release-dev/ocp-v4.0-art-dev@sha256:...

        Returns:
            tuple: tuple which contains command return code, output and error

        """
        self.vmnode.exec_cmd(
            "sudo cp /var/lib/kubelet/config.json /etc/ostree/auth.json"
        )
        cmd = f"sudo rpm-ostree rebase ostree-unverified-registry:{image_ref}"
        return self.vmnode.exec_cmd(cmd)

    def reboot(self):
        """
        Reboots the node

        Returns:
            tuple: tuple which contains command return code, output and error

        """
        cmd = "sudo reboot"
        return self.vmnode.exec_cmd(cmd)


def get_node_ips_from_module(module):
    """
    Fetches the node IP's in cluster from terraform state file

    Args:
        module (str): Module name in terraform.tfstate file
            e.g: constants.COMPUTE_MODULE

    Returns:
        list: List of module node IP's

    """
    terraform_state_file = os.path.join(
        config.ENV_DATA["cluster_path"], "terraform_data", "terraform.tfstate"
    )

    if not os.path.isfile(terraform_state_file):
        raise FileNotFoundError(
            errno.ENOENT, os.strerror(errno.ENOENT), terraform_state_file
        )
    ip_address = get_module_ip(terraform_state_file, module)
    return ip_address


def update_ntp_and_restart_chrony(node, server=None):
    """
    Update the NTP in the node and restarts chronyd service

    Args:
        node (str): Hostname/IP of node
        server (str): NTP server to update in chrony config

    """
    server = server or constants.RH_NTP_CLOCK
    vmnode = VSPHERENode(node)
    vmnode.replace_ntp_server_in_chrony(server)
    vmnode.restart_chrony()


def update_ntp_compute_nodes():
    """
    Updates NTP server on all compute nodes
    """
    if config.ENV_DATA["deployment_type"] == "upi":
        compute_nodes = get_node_ips_from_module(constants.COMPUTE_MODULE)
    else:
        compute_nodes = get_node_ips()
    for compute in compute_nodes:
        update_ntp_and_restart_chrony(compute)
