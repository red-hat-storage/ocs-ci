import logging
from select import select
from time import sleep

import datetime
import paramiko
from paramiko.ssh_exception import SSHException

logger = logging.getLogger(__name__)


class Ceph(object):
    def __init__(self, name, node_list=None):
        """
        Ceph cluster representation. Contains list of cluster nodes.
        Args:
            name (str): cluster name
            node_list (ceph.utils.CephVMNode): CephVMNode list
        """
        self.name = name
        self.node_list = list(node_list)

    def __eq__(self, ceph_cluster):
        if hasattr(ceph_cluster, 'node_list'):
            if all(atomic_node in ceph_cluster for atomic_node in self.node_list):
                return True
            else:
                return False
        else:
            return False

    def __ne__(self, ceph_cluster):
        return not self.__eq__(ceph_cluster)

    def __len__(self):
        return len(self.node_list)

    def __getitem__(self, key):
        return self.node_list[key]

    def __setitem__(self, key, value):
        self.node_list[key] = value

    def __delitem__(self, key):
        del self.node_list[key]

    def __iter__(self):
        return iter(self.node_list)

    def get_nodes(self, role=None):
        """
        Get node(s) by role. Return all nodes if role is not defined
        Args:
            role (str): node's role. Can be RoleContainer or str

        Returns:
            list: nodes
        """
        return [node for node in self.node_list if node.role == role] if role else list(self.node_list)

    def get_ceph_objects(self, role=None):
        """
        Get Ceph Object by role. Returns all objects if role is not defined. Ceph object can be Ceph demon, client,
        installer or generic entity. Pool role is never assigned to Ceph object and means that node has no Ceph objects
        Args:
            role (str): Ceph object's role as str

        Returns:
            list: ceph objects
        """
        node_list = self.get_nodes(role)
        ceph_object_list = []
        for node in node_list:
            ceph_object_list.extend(node.get_ceph_objects(role))
        return ceph_object_list


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
            self.role_list = role if len(role) > 0 else ['pool']
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


class SSHConnectionManager(object):
    def __init__(self, vmname, username, password, look_for_keys=False, outage_timeout=300):
        self.vmname = vmname
        self.username = username
        self.password = password
        self.look_for_keys = look_for_keys
        self.__client = paramiko.SSHClient()
        self.__client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.__transport = None
        self.__outage_start_time = None
        self.outage_timeout = datetime.timedelta(seconds=outage_timeout)

    @property
    def client(self):
        return self.get_client()

    def get_client(self):
        if not (self.__transport and self.__transport.is_active()):
            self.__connect()
            self.__transport = self.__client.get_transport()

        return self.__client

    def __connect(self):
        while True:
            try:
                self.__client.connect(self.vmname,
                                      username=self.username,
                                      password=self.password,
                                      look_for_keys=self.look_for_keys)
                break
            except Exception as e:
                logger.warn('Connection outage: \n{error}'.format(error=e))
                if not self.__outage_start_time:
                    self.__outage_start_time = datetime.datetime.now()
                if datetime.datetime.now() - self.__outage_start_time > self.outage_timeout:
                    raise e
                sleep(10)
        self.__outage_start_time = None

    @property
    def transport(self):
        return self.get_transport()

    def get_transport(self):
        self.__transport = self.client.get_transport()
        return self.__transport

    def __getstate__(self):
        pickle_dict = self.__dict__.copy()
        del pickle_dict['_SSHConnectionManager__transport']
        del pickle_dict['_SSHConnectionManager__client']
        return pickle_dict


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
        self.ceph_object_list = [CephObjectFactory(self).create_ceph_object(role) for role in kw['role'] if
                                 role != 'pool']
        self.voulume_list = []
        if kw['no_of_volumes']:
            self.voulume_list = [NodeVolume(NodeVolume.FREE) for vol_id in xrange(kw['no_of_volumes'])]
        if self.role == 'osd':
            for volume in self.voulume_list:
                volume.status = NodeVolume.ALLOCATED
        if kw.get('ceph_vmnode'):
            self.vm_node = kw['ceph_vmnode']
        self.root_connection = SSHConnectionManager(self.vmname, 'root', self.root_passwd)
        self.connection = SSHConnectionManager(self.vmname, self.username, self.password)
        self.rssh = self.root_connection.get_client
        self.rssh_transport = self.root_connection.get_transport
        self.ssh = self.connection.get_client
        self.ssh_transport = self.connection.get_transport
        self.run_once = False

    @property
    def role(self):
        return RolesContainer([ceph_demon.role for ceph_demon in self.ceph_object_list if ceph_demon])

    def get_free_volumes(self):
        return [volume for volume in self.voulume_list if volume.status == NodeVolume.FREE]

    def get_allocated_volumes(self):
        return [volume for volume in self.voulume_list if volume.status == NodeVolume.ALLOCATED]

    def get_ceph_demon(self, role=None):
        return [ceph_demon for ceph_demon in self.ceph_object_list if ceph_demon.role == role] if role else list()

    def connect(self):
        """
        connect to ceph instance using paramiko ssh protocol
        eg: self.connect()
        - setup tcp keepalive to max retries for active connection
        - set up hostname and shortname as attributes for tests to query
        """
        logger.info('Connecting {host_name} / {ip_address}'.format(host_name=self.vmname, ip_address=self.ip_address))

        stdin, stdout, stderr = self.rssh().exec_command("dmesg")
        self.rssh_transport().set_keepalive(15)
        changepwd = 'echo ' + "'" + self.username + ":" + self.password + "'" \
                    + "|" + "chpasswd"
        logger.info("Running command %s", changepwd)
        stdin, stdout, stderr = self.rssh().exec_command(changepwd)
        logger.info(stdout.readlines())
        self.rssh().exec_command(
            "echo 120 > /proc/sys/net/ipv4/tcp_keepalive_time")
        self.rssh().exec_command(
            "echo 60 > /proc/sys/net/ipv4/tcp_keepalive_intvl")
        self.rssh().exec_command(
            "echo 20 > /proc/sys/net/ipv4/tcp_keepalive_probes")
        self.exec_command(cmd="ls / ; uptime ; date")
        self.ssh_transport().set_keepalive(15)
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
            self.ssh_transport().set_keepalive(15)
            self.rssh_transport().set_keepalive(15)
        if kw.get('long_running'):
            logger.info("long running command --")
            channel = ssh().get_transport().open_session()
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
            stdin, stdout, stderr = ssh().exec_command(
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
        self.ftp = self.client().open_sftp()
        remote_file = self.ftp.file(file_name, file_mode, -1)
        return remote_file

    def _keep_alive(self):
        while True:
            self.exec_command(cmd='uptime', check_ec=False)
            sleep(60)

    def reconnect(self):
        # TODO: Deprecated. Left for compatibility with exisitng tests. Should be removed on refactoring.
        pass

    def __getstate__(self):
        d = dict(self.__dict__)
        del d['vm_node']
        del d['rssh']
        del d['ssh']
        del d['rssh_transport']
        del d['ssh_transport']
        del d['root_connection']
        del d['connection']
        return d

    def __setstate__(self, pickle_dict):
        self.__dict__.update(pickle_dict)
        self.root_connection = SSHConnectionManager(self.vmname, 'root', self.root_passwd)
        self.connection = SSHConnectionManager(self.vmname, self.username, self.password)
        self.rssh = self.root_connection.get_client
        self.ssh = self.connection.get_client
        self.rssh_transport = self.root_connection.get_transport
        self.ssh_transport = self.connection.get_transport

    def get_ceph_objects(self, role=None):
        """
        Get Ceph objects list on the node
        Args:
            role(str): Ceph object role

        Returns:
            list: ceph objects

        """
        return [ceph_demon for ceph_demon in self.ceph_object_list if ceph_demon.role == role or not role]

    def create_ceph_object(self, role):
        """
        Create ceph object on the node
        Args:
            role(str): ceph object role
        """
        self.ceph_object_list.append(CephObjectFactory(self).create_ceph_object(role))

    def remove_ceph_object(self, ceph_object):
        """
        Removes ceph object form the node
        Args:
            ceph_object(CephObject): ceph object to remove
        """
        self.ceph_object_list.remove(ceph_object)

    def open_firewall_port(self, port, protocol):
        """
        Opens firewall port on the node
        Args:
            port(str): port, can be range
            protocol(str): protcol
        """
        if self.pkg_type == 'rpm':
            try:
                self.exec_command(sudo=True, cmd="rpm -qa | grep firewalld")
            except CommandFailed:
                self.exec_command(sudo=True, cmd="yum install -y firewalld", long_running=True)
            self.exec_command(sudo=True, cmd="systemctl enable firewalld")
            self.exec_command(sudo=True, cmd="systemctl start firewalld")
            self.exec_command(sudo=True, cmd="systemctl status firewalld")
            self.exec_command(sudo=True, cmd="firewall-cmd --zone=public --add-port={port}/{protocol}"
                              .format(port=port, protocol=protocol))
            self.exec_command(sudo=True, cmd="firewall-cmd --zone=public --add-port={port}/{protocol} --permanent"
                              .format(port=port, protocol=protocol))
        if self.pkg_type == 'deb':
            # Ubuntu section stub
            pass
            # ceph_node.exec_command(sudo=True, cmd="ufw --force enable")
            # ceph_node.exec_command(sudo=True, cmd="ufw status")
            # ceph_node.exec_command(sudo=True, cmd="iptables -I INPUT -p {protocol} --dport {port} -j ACCEPT"
            #                        .format(port=str(port).replace('-',':'), protocol=protocol))
            # ceph_node.exec_command(sudo=True, cmd="update-locale LC_ALL=en_US.UTF-8"
            #                        .format(port=str(port).replace('-', ':'), protocol=protocol))
            # ceph_node.exec_command(cmd="sudo DEBIAN_FRONTEND=noninteractive apt-get -yq install iptables-persistent",
            #                        long_running=True)

    def search_ethernet_interface(self, ceph_node_list):
        """
        Search interface on the given node node which allows every node in the cluster accesible by it's shortname.

        Args:
            ceph_node_list (list): lsit of CephNode

        Returns:
            eth_interface (str): retturns None if no suitable interface found

        """
        logger.info('Searching suitable ethernet interface on {node}'.format(node=self.ip_address))
        out, err = self.exec_command(cmd='sudo ls /sys/class/net | grep -v lo')
        eth_interface_list = out.read().strip().split('\n')
        for eth_interface in eth_interface_list:
            try:
                for ceph_node in ceph_node_list:
                    self.exec_command(
                        cmd='sudo ping -I {interface} -c 3 {ceph_node}'.format(interface=eth_interface,
                                                                               ceph_node=ceph_node.shortname))
                logger.info(
                    'Suitable ethernet interface {eth_interface} found on {node}'.format(eth_interface=eth_interface,
                                                                                         node=ceph_node.ip_address))
                return eth_interface
            except Exception:
                continue
        logger.info('No suitable ethernet interface found on {node}'.format(node=ceph_node.ip_address))
        return None

    def write_docker_daemon_json(self, json_text):
        """
        Write given string to /etc/docker/daemon/daemon
        Args:
            json_text (str): json as string
        """
        self.exec_command(cmd='sudo mkdir -p /etc/docker/ && sudo chown $USER /etc/docker && chmod 755 /etc/docker')
        docker_daemon = self.write_file(file_name='/etc/docker/daemon.json', file_mode='w')
        docker_daemon.write(json_text)
        docker_daemon.flush()
        docker_daemon.close()

    def setup_deb_cdn_repos(self, build):
        """
        Setup cdn repositories for deb systems
        Args:
            build(str): rhcs version
        """
        user = 'redhat'
        passwd = 'OgYZNpkj6jZAIF20XFZW0gnnwYBjYcmt7PeY76bLHec9'
        num = build.split('.')[0]
        cmd = 'umask 0077; echo deb https://{user}:{passwd}@rhcs.download.redhat.com/{num}-updates/Tools ' \
              '$(lsb_release -sc) main | tee /etc/apt/sources.list.d/Tools.list'.format(user=user, passwd=passwd,
                                                                                        num=num)
        self.exec_command(sudo=True, cmd=cmd)
        self.exec_command(sudo=True, cmd='wget -O - https://www.redhat.com/security/fd431d51.txt | apt-key add -')
        self.exec_command(sudo=True, cmd='apt-get update')

    def setup_rhel_cdn_repos(self, build):
        """
        Setup cdn repositories for rhel systems
        Args:
            build(str): rhcs version
        """
        repos_13x = ['rhel-7-server-rhceph-1.3-mon-rpms',
                     'rhel-7-server-rhceph-1.3-osd-rpms',
                     'rhel-7-server-rhceph-1.3-calamari-rpms',
                     'rhel-7-server-rhceph-1.3-installer-rpms',
                     'rhel-7-server-rhceph-1.3-tools-rpms']

        repos_20 = ['rhel-7-server-rhceph-2-mon-rpms',
                    'rhel-7-server-rhceph-2-osd-rpms',
                    'rhel-7-server-rhceph-2-tools-rpms',
                    'rhel-7-server-rhscon-2-agent-rpms',
                    'rhel-7-server-rhscon-2-installer-rpms',
                    'rhel-7-server-rhscon-2-main-rpms']

        repos_30 = ['rhel-7-server-rhceph-3-mon-rpms',
                    'rhel-7-server-rhceph-3-osd-rpms',
                    'rhel-7-server-rhceph-3-tools-rpms',
                    'rhel-7-server-extras-rpms']

        repos = None
        if build.startswith('1'):
            repos = repos_13x
        elif build.startswith('2'):
            repos = repos_20
        elif build.startswith('3'):
            repos = repos_30
        for repo in repos:
            self.exec_command(
                sudo=True, cmd='subscription-manager repos --enable={r}'.format(r=repo))

    def setup_deb_repos(self, deb_repo):
        """
        Setup repositories for deb system
        Args:
            deb_repo(str): deb (Ubuntu) repository link
        """
        self.exec_command(cmd='sudo rm -f /etc/apt/sources.list.d/*')
        repos = ['MON', 'OSD', 'Tools']
        for repo in repos:
            cmd = 'sudo echo deb ' + deb_repo + '/{0}'.format(repo) + \
                  ' $(lsb_release -sc) main'
            self.exec_command(cmd=cmd + ' > ' + "/tmp/{0}.list".format(repo))
            self.exec_command(cmd='sudo cp /tmp/{0}.list'.format(repo) +
                                  ' /etc/apt/sources.list.d/')
        ds_keys = ['https://www.redhat.com/security/897da07a.txt',
                   'https://www.redhat.com/security/f21541eb.txt',
                   # 'https://prodsec.redhat.com/keys/00da75f2.txt',
                   # TODO: replace file file.rdu.redhat.com/~kdreyer with prodsec.redhat.com when it's back
                   'http://file.rdu.redhat.com/~kdreyer/keys/00da75f2.txt',
                   'https://www.redhat.com/security/data/fd431d51.txt']

        for key in ds_keys:
            wget_cmd = 'sudo wget -O - ' + key + ' | sudo apt-key add -'
            self.exec_command(cmd=wget_cmd)
            self.exec_command(cmd='sudo apt-get update')

    def setup_rhel_repos(self, base_url, installer_url=None):
        """
        Setup repositories for rhel
        Args:
            base_url (str): compose url for rhel
            installer_url (str): installer repos url
        """
        repos = ['MON', 'OSD', 'Tools', 'Calamari', 'Installer']
        base_repo = Ceph.generate_repository_file(base_url, repos)
        base_file = self.write_file(
            sudo=True,
            file_name='/etc/yum.repos.d/rh_ceph.repo',
            file_mode='w')
        base_file.write(base_repo)
        base_file.flush()
        if installer_url is not None:
            installer_repos = ['Agent', 'Main', 'Installer']
            inst_repo = Ceph.generate_repository_file(installer_url, installer_repos)
            logger.info("Setting up repo on %s", self.hostname)
            inst_file = self.write_file(
                sudo=True,
                file_name='/etc/yum.repos.d/rh_ceph_inst.repo',
                file_mode='w')
            inst_file.write(inst_repo)
            inst_file.flush()

    def obtain_root_permissions(self, path):
        """
        Transfer ownership of root to current user for the path given. Recursive.
        Args:
            path(str): file path
        """
        self.exec_command(cmd='sudo chown -R $USER:$USER {path}'.format(path=path))


class CephObject(object):
    def __init__(self, role, node):
        """
        Generic Ceph object, works as proxy to exec_command method
        Args:
            role (str): role string
            node (CephNode): node object
        """
        self.role = role
        self.node = node

    @property
    def pkg_type(self):
        return self.node.pkg_type

    def exec_command(self, cmd, **kw):
        """
        Proxy to node's exec_command
        Args:
            cmd(str): command to execute
            **kw: options

        Returns:
        node's exec_command resut
        """
        return self.node.exec_command(cmd=cmd, **kw)

    def write_file(self, **kw):
        """
        Proxy to node's write file
        Args:
            **kw: options

        Returns:
            node's write_file resut
        """
        return self.node.write_file(**kw)


class CephDemon(CephObject):
    def __init__(self, role, node):
        """
        Ceph demon representation. Can be containerized.
        Args:
            role(str): Ceph demon type
            node(CephNode): node object
        """
        super(CephDemon, self).__init__(role, node)
        self.containerized = None

    @property
    def container_name(self):
        return 'ceph-{role}-{host}'.format(role=self.role, host=self.node.hostmname) if self.containerized else ''

    @property
    def container_prefix(self):
        return 'sudo docker {container_name} exec'.format(
            container_name=self.container_name) if self.containerized else ''

    def exec_command(self, cmd, **kw):
        """
        Proxy to node's exec_command with wrapper to run commands inside the container for containerized demons
        Args:
            cmd(str): command to execute
            **kw: options

        Returns:
        node's exec_command resut
        """
        return self.node.exec_command(cmd=' '.join([self.container_prefix, cmd]),
                                      **kw) if self.containerized else self.node.exec_command(cmd=cmd, **kw)


class CephClient(CephObject):
    def __init__(self, role, node):
        """
        Ceph client representation, works as proxy to exec_command method.
        Args:
            role(str): role string
            node(CephNode): node object
        """
        super(CephClient, self).__init__(role, node)


class CephInstaller(CephObject):
    def __init__(self, role, node):
        """
        Ceph client representation, works as proxy to exec_command method
        Args:
            role(str): role string
            node(CephNode): node object
        """
        super(CephInstaller, self).__init__(role, node)


class CephObjectFactory(object):
    DEMON_ROLES = ['mon', 'osd', 'mgr', 'rgw', 'mds']
    CLIENT_ROLES = ['client']

    def __init__(self, node):
        """
        Factory for Ceph objects.
        Args:
            node: node object
        """
        self.node = node

    def create_ceph_object(self, role):
        """
        Create an appropriate Ceph object by role
        Args:
            role: role string

        Returns:
        Ceph object based on role
        """
        if role == 'installer':
            return CephInstaller(role, self.node)
        if role == self.CLIENT_ROLES:
            return CephClient(role, self.node)
        if role in self.DEMON_ROLES:
            return CephDemon(role, self.node)
        if role != 'pool':
            return CephObject(role, self.node)
