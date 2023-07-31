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

    def __init__(self, host, user=None, private_key=None, password=None, stdout=False):
        """
        Initialize all required variables

        Args:
            host (str): Hostname or IP to connect
            user (str): User name to connect
            private_key (str): Private key  to connect to load balancer
            password (password): Password for host
            stdout (bool): output stdout to console

        """
        self.host = host
        self.user = user
        self.private_key = private_key
        self.password = password
        self.stdout = stdout
        self.client = self._connect()

    def _connect(self):
        """
        Get connection to load balancer

        Returns:
            paramiko.client: Paramiko SSH client connection to load balancer

        Raises:
            authException: In-case of authentication failed
            sshException: In-case of ssh connection failed

        """
        try:
            client = SSHClient()
            client.set_missing_host_key_policy(AutoAddPolicy())
            if self.private_key:
                client.connect(
                    self.host, username=self.user, key_filename=self.private_key
                )
            elif self.password:
                client.connect(self.host, username=self.user, password=self.password)
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
        stdout = out.read().decode("ascii").strip("\n")
        try:
            stderr = err.read().decode("ascii").strip("\n")
        except UnicodeDecodeError:
            stderr = err.read()
        logger.debug(f"retcode: {retcode}")
        logger.info(f"stdout: {stdout}") if self.stdout else logger.debug(
            f"stdout: {stdout}"
        )
        logger.debug(f"stderr: {stderr}")
        return (retcode, stdout, stderr)
