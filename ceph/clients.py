import logging
import paramiko
import string
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

    def win_exec(self, ps_command, timeout=180):
        log.info("Running powershell`s command `{}`".format(ps_command))
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(self.ip_address, username=self.login, password=self.password)
        command = 'powershell -Command "& {{{}}}"'.format(ps_command)
        chan_ssh = client.get_transport().open_session()
        chan_ssh.exec_command(command)
        for i in range(0, timeout):
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

    def delete_target(self):
        pass

    def connect_to_target(self, ip, username, password):
        command = "Connect-IscsiTarget -NodeAddress iqn.2003-01.com.redhat.iscsi-gw:ceph-igw"\
            r" -IsMultipathEnabled \$True -TargetPortalAddress {}  -AuthenticationType ONEWAYCHAP"\
            " -ChapUsername {} -ChapSecret {}".format(ip, username, password)
        self.win_exec(command)

    def disconnect_from_target(self,):
        command = "Disconnect-IscsiTarget -NodeAddress "\
            "iqn.2003-01.com.redhat.iscsi-gw:ceph-igw -Confirm:$false"
        self.win_exec(command)

    def create_disk(self, number):
        letters = list(string.ascii_uppercase)[3:3 + number]
        for disk, part in zip(letters, list(range(1, 1 + number))):
            self.win_exec("Initialize-Disk -Number {} -PartitionStyle MBR".format(part))
            self.win_exec("New-Partition -DiskNumber {0} -UseMaximumSize -DriveLetter {1}".format(part, disk))
            self.win_exec("Get-Volume -DriveLetter {}".format(disk))
            self.win_exec("Format-Volume -DriveLetter {} -FileSystem NTFS".format(disk))

    def check_disk(self, number):
        command = "Get-Disk -Number {}".format(number)
        self.win_exec(command)

    def create_fio_job_options(self, job_options):
        command = 'Set-Content -Value "{}" -Path \'C:\\Program Files\\fio\\test.fio\''.format(job_options)
        self.win_exec(command)

    def run_fio_test(self):
        log.info("starting fio test")
        try:
            output = self.win_exec(
                "cd 'C:\\Program Files\\fio\\'; .\\fio.exe .\\test.fio",
                timeout=4800)
        except CommandFailed:
            log.exception("fio test filed")
            return 1
        else:
            log.info(output["stdout"])
            return 0
