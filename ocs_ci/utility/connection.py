"""
Module that connects to remote server and execute operations on remote server
"""

import logging

from paramiko import SSHClient, AutoAddPolicy
from paramiko.client import SSH_PORT
from paramiko.auth_handler import AuthenticationException, SSHException

logger = logging.getLogger(__name__)


class Connection(object):
    """
    A class that connects to remote server
    """

    def __init__(
        self,
        host,
        user=None,
        private_key=None,
        password=None,
        stdout=False,
        jump_host=None,
    ):
        """
        Initialize all required variables

        Args:
            host (str): Hostname or IP to connect
            user (str): User name to connect
            private_key (str): Private key  to connect to load balancer
            password (password): Password for host
            stdout (bool): output stdout to console
            jump_host (dict): configuration of jump host, if required or None
                the dict could contain following keys: host, user, private_key, password

        """
        self.host = host
        self.user = user
        self.private_key = private_key
        self.password = password
        self.stdout = stdout
        self.jump_channel = None
        if jump_host:
            self.jump_channel = self._jump_channel(jump_host)
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
                    self.host,
                    username=self.user,
                    key_filename=self.private_key,
                    sock=self.jump_channel,
                )
            elif self.password:
                client.connect(
                    self.host,
                    username=self.user,
                    password=self.password,
                    sock=self.jump_channel,
                )
        except AuthenticationException as authException:
            logger.error(f"Authentication failed: {authException}")
            raise authException
        except SSHException as sshException:
            logger.error(f"SSH connection failed: {sshException}")
            raise sshException

        return client

    def _jump_channel(self, jump_host):
        """
        Configure and return jump host channel of None.

        Args:
            jump_host (dict): jump host configuration or None

        """
        logger.debug(f"SSH Connection: using jump_host: {jump_host}")
        jump_tr = Connection(**jump_host).client.get_transport()
        jump_channel = jump_tr.open_channel(
            "direct-tcpip",
            (self.host, SSH_PORT),
            (jump_host["host"], SSH_PORT),
        )
        logger.debug("SSH Jump host channel were created.")
        return jump_channel

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
