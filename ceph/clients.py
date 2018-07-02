import logging
import paramiko
from time import sleep

log = logging


class TimeoutException(Exception):
    pass


class CommandFailed(Exception):
    pass


class WinNode(object):

    def __init__(self, **kw):
        self.login = "Administrator"
        self.password = "CephUser123"
        self.ip_address = kw['ip_address']
        self.private_ip = kw['private_ip']

    def win_exec(self, ps_command):
        log.info("Running powershell`s command `{}`".format(ps_command))
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(self.ip_address, username=self.login, password=self.password)
        command = 'powershell -Command "& {{{}}}"'.format(ps_command)
        chan_ssh = client.get_transport().open_session()
        chan_ssh.exec_command(command)
        for i in range(0, 180):
            sleep(1)
            if chan_ssh.exit_status_ready():
                break
        else:
            raise TimeoutException("Timeout")
        output = dict()
        output["exit_code"] = chan_ssh.recv_exit_status()
        output["stdout"] = chan_ssh.recv(-1)
        output["stderr"] = chan_ssh.recv_stderr(-1)
        if not bool(output["stderr"]) and output["exit_code"] == 0:
            return output
        else:
            raise CommandFailed(output["stderr"])

    def start_iscsi_initiator(self):
        self.win_exec("Start-Service msiscsi")
        self.win_exec("Set-Service msiscsi -startuptype 'automatic'")

    def get_iscsi_initiator_name(self):
        output = self.win_exec("(Get-InitiatorPort).NodeAddress")
        stdout = output["stdout"].strip()
        return stdout

    def create_new_target(self, ip, port=3260):
        command = "New-IscsiTargetPortal -TargetPortalAddress {} -TargetPortalPortNumber {}".format(ip, port)
        self.win_exec(command)

    def connect_to_target(self, ip, username, password):
        command = "Connect-IscsiTarget -NodeAddress iqn.2003-01.com.redhat.iscsi-gw:ceph-igw"\
            " -IsMultipathEnabled \$True -TargetPortalAddress {}  -AuthenticationType ONEWAYCHAP"\
            " -ChapUsername {} -ChapSecret {}".format(ip, username, password)
        self.win_exec(command)

    def create_disk(self):
        self.win_exec("Initialize-Disk -Number 1 -PartitionStyle MBR")
        self.win_exec("New-Partition -DiskNumber 1 -UseMaximumSize -DriveLetter D")
        self.win_exec("Get-Volume -DriveLetter D")
        self.win_exec("Format-Volume -DriveLetter D -FileSystem NTFS")
