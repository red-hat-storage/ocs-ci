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

    def exec_cmd(self, cmd, secrets=None):
        """
        Executes command on server

        Args:
            cmd (str): Command to run on server
            secrets (list): A list of secrets to be masked with asterisks
                This kwarg is popped in order to not interfere with
                subprocess.run(``**kwargs``)

        Returns:
            tuple: tuple which contains command return code, output and error

        """
        # importing it here to avoid circular import issue
        from ocs_ci.utility.utils import mask_secrets

        masked_cmd = mask_secrets(cmd, secrets)
        logger.info(f"Executing cmd: {masked_cmd} on {self.host}")
        _, out, err = self.client.exec_command(cmd)
        retcode = out.channel.recv_exit_status()
        stdout = out.read().decode("utf-8").strip("\n")
        try:
            stderr = err.read().decode("utf-8").strip("\n")
        except UnicodeDecodeError:
            stderr = err.read()
        logger.debug(f"retcode: {retcode}")
        masked_stdout = mask_secrets(stdout, secrets)
        (
            logger.info(f"stdout: {masked_stdout}")
            if self.stdout
            else logger.debug(f"stdout: {masked_stdout}")
        )
        masked_stderr = mask_secrets(stderr, secrets)
        logger.debug(f"stderr: {masked_stderr}")
        return (retcode, stdout, stderr)

    def upload_file(self, localpath, remotepath):
        """
        Upload a file to remote server

        Args:
            localpath (str): Local file to upload
            remotepath (str): Target path on the remote server. Filename should be included

        """
        sftp = self.client.open_sftp()
        logger.info(f"uploading {localpath} to {self.user}@{self.host}:{remotepath}")
        sftp.put(localpath, remotepath)
        sftp.close()
