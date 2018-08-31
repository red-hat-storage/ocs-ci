import logging
import traceback
from select import select
from time import sleep

import datetime
import paramiko
from paramiko.ssh_exception import SSHException

logger = logging.getLogger(__name__)


class Ceph(object):
    """
    higher level ceph cluster object
    still in development
     - keep track of ceph nodes in cluster
     - exec at once on all nodes of same ceph role
     -
    """

    def __init__(self, **kw):
        self.nodes = kw['nodes']
        self.osd_nodes = kw['osd_nodes']
        self.mon_nodes = kw['mon_nodes']
        self.mds_nodes = kw['mds_nodes']
        self.clients = kw['clients']
        self.roles = kw['roles']


class CommandFailed(Exception):
    pass


class RolesContainer(object):
    """
    Container for single or multiple node roles.
    Can be used as iterable or with equality '==' operator to check if role is present for the node.
    Note that '==' operator will behave the same way as 'in' operator i.e. check that value is present in the role list.
    """

    def __init__(self, role='pool'):
        if hasattr(role, '__iter__'):
            self.role_list = role
        else:
            self.role_list = [str(role)]

    def __eq__(self, role):
        if hasattr(role, '__iter__'):
            if all(atomic_role in role for atomic_role in self.role_list):
                return True
            else:
                return False
        else:
            if role in self.role_list:
                return True
            else:
                return False

    def __ne__(self, role):
        return not self.__eq__(role)

    def equals(self, other):
        if getattr(other, 'role_list') == self.role_list:
            return True
        else:
            return False

    def __len__(self):
        return len(self.role_list)

    def __getitem__(self, key):
        return self.role_list[key]

    def __setitem__(self, key, value):
        self.role_list[key] = value

    def __delitem__(self, key):
        del self.role_list[key]

    def __iter__(self):
        return iter(self.role_list)

    def remove(self, object):
        self.role_list.remove(object)

    def append(self, object):
        self.role_list.append(object)

    def extend(self, iterable):
        self.role_list.extend(iterable)
        self.role_list = list(set(self.role_list))

    def update_role(self, roles_list):
        if 'pool' in self.role_list:
            self.role_list.remove('pool')
        self.extend(roles_list)

    def clear(self):
        self.role_list = ['pool']


class NodeVolume(object):
    FREE = 'free'
    ALLOCATED = 'allocated'

    def __init__(self, status):
        self.status = status


class CephNode(object):

    def __init__(self, **kw):
        """
        Initialize a CephNode in a libcloud environment
        eg CephNode(username='cephuser', password='cephpasswd',
                    root_password='passwd', ip_address='ip_address',
                    hostname='hostname', role='mon|osd|client',
                    no_of_volumes=3, ceph_vmnode='ref_to_libcloudvm')

        """
        self.username = kw['username']
        self.password = kw['password']
        self.root_passwd = kw['root_password']
        self.root_login = kw['root_login']
        self.private_ip = kw['private_ip']
        self.ip_address = kw['ip_address']
        self.vmname = kw['hostname']
        vmshortname = self.vmname.split('.')
        self.vmshortname = vmshortname[0]
        self.role = RolesContainer(kw['role'])
        self.voulume_list = []
        if kw['no_of_volumes']:
            self.voulume_list = [NodeVolume(NodeVolume.FREE) for vol_id in xrange(kw['no_of_volumes'])]
        if self.role == 'osd':
            for volume in self.voulume_list:
                volume.status = NodeVolume.ALLOCATED
        if kw.get('ceph_vmnode'):
            self.vm_node = kw['ceph_vmnode']
        self.run_once = False

    def get_free_volumes(self):
        return [volume for volume in self.voulume_list if volume.status == NodeVolume.FREE]

    def get_allocated_volumes(self):
        return [volume for volume in self.voulume_list if volume.status == NodeVolume.ALLOCATED]

    def connect(self, timeout=300):
        """
        connect to ceph instance using paramiko ssh protocol
        eg: self.connect()
        - setup tcp keepalive to max retries for active connection
        - set up hostname and shortname as attributes for tests to query
        """
        logger.info('Connecting {host_name} / {ip_address}'.format(host_name=self.vmname, ip_address=self.ip_address))
        if self.run_once is True:
            return
        self.rssh = paramiko.SSHClient()
        self.rssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        tout = datetime.timedelta(seconds=timeout)
        starttime = datetime.datetime.now()
        while True:
            try:
                self.rssh.connect(self.vmname,
                                  username='root',
                                  password=self.root_passwd,
                                  look_for_keys=False)
                self.rssh_transport = self.rssh.get_transport()
            except Exception:
                if datetime.datetime.now() - starttime > tout:
                    raise RuntimeError("{traceback} \nFailed to connect in {timeout} on {ip_address}"
                                       .format(timeout=tout, ip_address=self.ip_address,
                                               traceback=traceback.format_exc()))
                sleep(10)
                continue
            if not self.rssh_transport.is_active():
                if datetime.datetime.now() - starttime <= tout:
                    logger.warn("Transport status check failed, Reconnecting...")
                    sleep(10)
                    continue
                else:
                    raise RuntimeError(
                        'Failed to establish active trasport on {ip_address}'.format(ip_address=self.ip_address))
            break
        stdin, stdout, stderr = self.rssh.exec_command("dmesg")
        self.rssh_transport.set_keepalive(15)
        changepwd = 'echo ' + "'" + self.username + ":" + self.password + "'" \
                    + "|" + "chpasswd"
        logger.info("Running command %s", changepwd)
        stdin, stdout, stderr = self.rssh.exec_command(changepwd)
        logger.info(stdout.readlines())
        self.rssh.exec_command(
            "echo 120 > /proc/sys/net/ipv4/tcp_keepalive_time")
        self.rssh.exec_command(
            "echo 60 > /proc/sys/net/ipv4/tcp_keepalive_intvl")
        self.rssh.exec_command(
            "echo 20 > /proc/sys/net/ipv4/tcp_keepalive_probes")
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        tout = datetime.timedelta(seconds=timeout)
        starttime = datetime.datetime.now()
        while True:
            try:
                self.ssh.connect(self.vmname,
                                 password=self.password,
                                 username=self.username,
                                 look_for_keys=False)
                self.ssh_transport = self.ssh.get_transport()
            except Exception:
                if datetime.datetime.now() - starttime > tout:
                    raise RuntimeError("{traceback} \nFailed to connect in {timeout} on {ip_address}"
                                       .format(timeout=tout, ip_address=self.ip_address,
                                               traceback=traceback.format_exc()))
                    raise
                sleep(10)
                continue
            if not self.ssh_transport.is_active():
                if datetime.datetime.now() - starttime <= tout:
                    logger.warn("Transport status check failed, Reconnecting...")
                    sleep(10)
                    continue
                else:
                    raise RuntimeError(
                        'Failed to establish active trasport on {ip_address}'.format(ip_address=self.ip_address))
            break
        self.exec_command(cmd="ls / ; uptime ; date")
        self.ssh_transport.set_keepalive(15)
        out, err = self.exec_command(cmd="hostname")
        self.hostname = out.read().strip()
        shortname = self.hostname.split('.')
        self.shortname = shortname[0]
        logger.info("hostname and shortname set to %s and %s", self.hostname,
                    self.shortname)
        self.set_internal_ip()
        self.exec_command(cmd="echo 'TMOUT=600' >> ~/.bashrc")
        self.exec_command(cmd='[ -f /etc/redhat-release ]', check_ec=False)
        if self.exit_status == 0:
            self.pkg_type = 'rpm'
        else:
            self.pkg_type = 'deb'
        logger.info("finished connect")
        self.run_once = True

    def set_internal_ip(self):
        """
        set the internal ip of the vm which differs from floating ip
        """
        out, _ = self.exec_command(
            cmd="/sbin/ifconfig eth0 | grep 'inet ' | awk '{ print $2}'")
        self.internal_ip = out.read().strip()

    def set_eth_interface(self, eth_interface):
        """
        set the eth interface
        """
        self.eth_interface = eth_interface

    def generate_id_rsa(self):
        """
        generate id_rsa key files for the new vm node
        """
        # remove any old files
        self.exec_command(cmd="test -f ~/.ssh/id_rsa.pub && rm -f ~/.ssh/id*",
                          check_ec=False)
        self.exec_command(
            cmd="ssh-keygen -b 2048 -f ~/.ssh/id_rsa -t rsa -q -N ''")
        out1, _ = self.exec_command(cmd="cat ~/.ssh/id_rsa.pub")
        self.id_rsa_pub = out1.read()

    def exec_command(self, **kw):
        """
        execute a command on the vm
        eg: self.exec_cmd(cmd='uptime')
            or
            self.exec_cmd(cmd='background_cmd', check_ec=False)

        Attributes:
        check_ec: False will run the command and not wait for exit code

        """
        if not (self.ssh.get_transport().is_active() and self.rssh.get_transport().is_active()):
            self.reconnect()

        if kw.get('sudo'):
            ssh = self.rssh
        else:
            ssh = self.ssh

        if kw.get('timeout'):
            timeout = kw['timeout']
        else:
            timeout = 120
        logger.info("Running command %s on %s", kw['cmd'], self.ip_address)
        stdin = None
        stdout = None
        stderr = None
        if self.run_once:
            self.ssh_transport.set_keepalive(15)
            self.rssh_transport.set_keepalive(15)
        if kw.get('long_running'):
            logger.info("long running command --")
            channel = ssh.get_transport().open_session()
            channel.exec_command(kw['cmd'])
            read = ''
            while True:
                if channel.exit_status_ready():
                    ec = channel.recv_exit_status()
                    break
                rl, wl, xl = select([channel], [], [channel], 4200)
                if len(rl) > 0:
                    data = channel.recv(1024)
                    read = read + data
                    logger.info(data)
                if len(xl) > 0:
                    data = channel.recv(1024)
                    read = read + data
                    logger.info(data)
            return read, ec
        try:
            stdin, stdout, stderr = ssh.exec_command(
                kw['cmd'], timeout=timeout)
        except SSHException as e:
            logger.error("Exception during cmd %s", str(e))
            if 'Timeout openning channel' in str(e):
                logger.error("channel reset error")
        exit_status = stdout.channel.recv_exit_status()
        self.exit_status = exit_status
        if kw.get('check_ec', True):
            if exit_status == 0:
                logger.info("Command completed successfully")
            else:
                logger.error("Error during cmd %s, timeout %d", exit_status, timeout)
                raise CommandFailed(kw['cmd'] + " Error:  " + str(stderr.read()) + ' ' + str(self.ip_address))
            return stdout, stderr
        else:
            return stdout, stderr

    def write_file(self, **kw):
        if kw.get('sudo'):
            self.client = self.rssh
        else:
            self.client = self.ssh
        file_name = kw['file_name']
        file_mode = kw['file_mode']
        self.ftp = self.client.open_sftp()
        remote_file = self.ftp.file(file_name, file_mode, -1)
        return remote_file

    def _keep_alive(self):
        while True:
            self.exec_command(cmd='uptime', check_ec=False)
            sleep(60)

    def reconnect(self):
        self.rssh = paramiko.SSHClient()
        self.rssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        count = 0
        while True:
            self.rssh.connect(self.vmname,
                              username='root',
                              password=self.root_passwd,
                              look_for_keys=False)
            self.rssh_transport = self.rssh.get_transport()
            if not self.rssh_transport.is_active() and count <= 3:
                logger.info("Connect failed, Retrying...")
                sleep(10)
                count += 1
            else:
                break
        while True:
            self.ssh.connect(self.vmname,
                             password=self.password,
                             username=self.username,
                             look_for_keys=False)
            self.ssh_transport = self.ssh.get_transport()
            if not self.ssh_transport.is_active() and count <= 3:
                logger.info("Connect failed, Retrying...")
                sleep(10)
                count += 1
            else:
                break

    def __getstate__(self):
        d = dict(self.__dict__)
        del d['vm_node']
        del d['rssh']
        del d['ssh']
        del d['rssh_transport']
        del d['ssh_transport']
        return d
