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
        self.host = host or self._get_host()
        self.user = user or constants.VSPHERE_NODE_USER
        self.private_key = private_key or os.path.expanduser(
            config.DEPLOYMENT['ssh_key_private']
        )
        self.lb = Connection(self.host, self.user, self.private_key)

    def _get_host(self):
        """
        Fetches the Host/IP address from terraform.tfstate file

        Returns:
             str: IP Address of load balancer

        """
        self.terraform_state_file = os.path.join(
            config.ENV_DATA['cluster_path'],
            "terraform_data",
            "terraform.tfstate"
        )

        if not os.path.isfile(self.terraform_state_file):
            raise FileNotFoundError(
                errno.ENOENT,
                os.strerror(errno.ENOENT),
                self.terraform_state_file
            )
        ip_address = get_module_ip(
            self.terraform_state_file,
            constants.LOAD_BALANCER_MODULE
        )
        return ip_address

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
            logger.info(
                "Successfully restarted haproxy service on load balancer"
            )
            _rc = True
        return _rc

    def remove_boostrap_in_proxy(self):
        """
        Removes bootstrap IP from haproxy.conf
        """
        bootstrap_ip = get_module_ip(
            self.terraform_state_file,
            constants.BOOTSTRAP_MODULE
        )
        # backup the conf file
        cmd = (
            f"sudo cp {constants.HAPROXY_LOCATION}"
            f" {constants.HAPROXY_LOCATION}_backup"
        )
        self.lb.exec_cmd(cmd)

        # remove bootstrap IP
        cmd = f"sudo sed -i '/{bootstrap_ip}/d' {constants.HAPROXY_LOCATION}"
        self.lb.exec_cmd(cmd)
