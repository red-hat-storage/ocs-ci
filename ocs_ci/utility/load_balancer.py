"""
Module that contains all operations related to load balancer in a cluster
"""

import errno
import logging
import os

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.utility.connection import Connection
from ocs_ci.utility.utils import get_module_ip

logger = logging.getLogger(__name__)


class LoadBalancer(object):
    """
    A class that handles all operations related to load balancer
    """

    def __init__(self, host=None, user=None, private_key=None):
        """
        Initialize all required variables

        Args:
            host (str): Host name or IP address
            user (str): User name to connect
            private_key (str): Private key  to connect to load balancer

        """
        self.haproxy_conf_file = "/etc/haproxy/haproxy.conf"
        self.host = host or self._get_host()
        self.user = user or constants.VSPHERE_NODE_USER
        self.private_key = private_key or os.path.expanduser(
            config.DEPLOYMENT["ssh_key_private"]
        )
        jump_host = (
            config.DEPLOYMENT.get("ssh_jump_host")
            if (config.DEPLOYMENT.get("disconnected") or config.DEPLOYMENT.get("proxy"))
            else None
        )
        if jump_host:
            jump_host["private_key"] = self.private_key
        self.lb = Connection(
            self.host, self.user, self.private_key, jump_host=jump_host
        )

    def _get_host(self):
        """
        Fetches the Host/IP address from terraform.tfstate file

        Returns:
             str: IP Address of load balancer

        """
        self.terraform_state_file = config.ENV_DATA["terraform_state_file"]

        if not os.path.isfile(self.terraform_state_file):
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), self.terraform_state_file
            )
        ip_address = get_module_ip(
            self.terraform_state_file, constants.LOAD_BALANCER_MODULE
        )
        return ip_address[0]

    def restart_service(self, service_name):
        """
        Restarts the given service
        """
        cmd = f"sudo systemctl restart {service_name}"
        return self.lb.exec_cmd(cmd)

    def restart_haproxy(self):
        """
        Restarts haproxy service

        Returns:
            bool: True if successful restarts of haproxy, False otherwise

        """
        _rc = False
        logger.info("Restarting haproxy service on load balancer")
        retcode, _, _ = self.restart_service("haproxy.service")
        if retcode:
            logger.info("Successfully restarted haproxy service on load balancer")
            _rc = True
        return _rc

    def remove_boostrap_in_proxy(self):
        """
        Removes bootstrap IP from haproxy.conf
        """
        bootstrap_ip = get_module_ip(
            self.terraform_state_file, constants.BOOTSTRAP_MODULE
        )[0]
        # backup the conf file
        cmd = (
            f"sudo cp {constants.HAPROXY_LOCATION}"
            f" {constants.HAPROXY_LOCATION}_backup"
        )
        self.lb.exec_cmd(cmd)

        # remove bootstrap IP
        cmd = f"sudo sed -i '/{bootstrap_ip}/d' {constants.HAPROXY_LOCATION}"
        self.lb.exec_cmd(cmd)

    def remove_compute_node_in_proxy(self):
        """
        Removes compute node IP's from haproxy.conf
        """
        compute_ips = get_module_ip(self.terraform_state_file, constants.COMPUTE_MODULE)
        # backup the conf file
        cmd = (
            f"sudo cp {constants.HAPROXY_LOCATION}"
            f" {constants.HAPROXY_LOCATION}_backup"
        )
        self.lb.exec_cmd(cmd)

        # remove compute IPs
        logger.debug(f"removing {compute_ips} from {constants.HAPROXY_LOCATION}")
        for each_compute_ip in compute_ips:
            cmd = f"sudo sed -i '/{each_compute_ip}/d' {constants.HAPROXY_LOCATION}"
            self.lb.exec_cmd(cmd)

    def update_haproxy_with_nodes(self, nodes):
        """
        Args:
            nodes (list): List of nodes to update in haproxy

        """
        ports = ["80", "443"]
        for port in ports:
            for node in nodes:
                cmd = (
                    f"sudo sed -i '0,/.*:{port} check$/s/.*:{port} check$/        server "
                    f"{node} {node}:{port} check\\n&/' {constants.HAPROXY_LOCATION}"
                )
                self.lb.exec_cmd(cmd)

    def rename_haproxy(self):
        """
        Rename haproxy configuration file from haproxy.conf to haproxy.cfg
        """
        cmd = f"sudo cp {self.haproxy_conf_file} {constants.HAPROXY_LOCATION}"
        self.lb.exec_cmd(cmd)

    def modify_haproxy_service(self):
        """
        Modify haproxy service
        """
        cmd = (
            f"sudo sed -i 's/haproxy\\.conf/haproxy.cfg/g' {constants.HAPROXY_SERVICE}"
        )
        self.lb.exec_cmd(cmd)

    def reload_daemon(self):
        """
        Reload daemon-reload
        """
        cmd = "sudo systemctl daemon-reload"
        self.lb.exec_cmd(cmd)

    def rename_haproxy_conf_and_reload(self):
        """
        Rename haproxy config and restart haproxy service
        """
        self.rename_haproxy()
        self.modify_haproxy_service()
        self.reload_daemon()
        self.restart_haproxy()
