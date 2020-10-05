"""
Module that connects to remote server and execute operations on remote server
"""

import logging

from paramiko import SSHClient, AutoAddPolicy
from paramiko.auth_handler import AuthenticationException, SSHException

logger = logging.getLogger(__name__)


class Connection(object):
    """
    A class that connects to remote server
    """
    def __init__(self, host, user=None, private_key=None):
        """
        Initialize all required variables

        Args:
            host (str):
            user (str): User name to connect
            private_key (str): Private key  to connect to load balancer

        """
        self.host = host
        self.user = user
        self.private_key = private_key
        self.client = self._connect()

    def _connect(self):
        """
        Get connection to load balancer

        Returns:
            paramiko.client: Paramiko SSH client connection to load balancer

        Raises:
            FileNotFoundError: In-case terraform.tfstate file not found
                in terraform_data directory

        """
        try:
            client = SSHClient()
            client.set_missing_host_key_policy(AutoAddPolicy())
            client.connect(
                self.host,
                username=self.user,
                key_filename=self.private_key
            )
        except AuthenticationException as authException:
            logger.error(f"Authentication failed: {authException}")
            raise authException
        except SSHException as sshException:
            logger.error(f"SSH connection failed: {sshException}")
            raise sshException

        return client

    def exec_cmd(self, cmd):
        """
        Executes command on server

        Args:
            cmd (str): Command to run on server

        Returns:
            tuple: tuple which contains command return code, output and error

        """
        logger.info(f"Executing cmd: {cmd} on {self.host}")
        _, out, err = self.client.exec_command(cmd)
        retcode = out.channel.recv_exit_status()
        stdout = out.read().decode('ascii').strip("\n")
        stderr = err.read().decode('ascii').strip("\n")
        logger.debug(f"retcode: {retcode}")
        logger.debug(f"stdout: {stdout}")
        logger.debug(f"stderr: {stderr}")
        return (retcode, stdout, stderr)
