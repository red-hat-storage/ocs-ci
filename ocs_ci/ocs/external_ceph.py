import logging
import json
from ocs_ci.ocs.exceptions import CommandFailed
from select import select
from time import sleep

import datetime
import paramiko
from paramiko.ssh_exception import SSHException


logger = logging.getLogger(__name__)


class Ceph(object):

    def __init__(self, name='ceph', node_list=None):
        """
        Ceph cluster representation. Contains list of cluster nodes.
        Args:
            name (str): cluster name
            node_list (ceph.utils.CephNode): CephNode list
        """
        self.name = name
        self.node_list = node_list

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

    def get_nodes(self, role=None, ignore=None):
        """
        Get node(s) by role. Return all nodes if role is not defined
        Args:
            role (str, RolesContainer): node's role. Takes precedence over ignore
            ignore (str, RolesContainer): node's role to ignore from the list

        Returns:
            list: nodes
        """
        if role:
            return [node for node in self.node_list if node.role == role]
        elif ignore:
            return [node for node in self.node_list if node.role != ignore]
        else:
            return list(self.node_list)

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

    def get_ceph_object(self, role, order_id=0):
        """
        Returns single ceph object. If order id is provided returns that occurrence from results list, otherwise returns
        first occurrence
        Args:
            role(str): Ceph object's role
            order_id(int): order number of the ceph object

        Returns:
            CephObject: ceph object

        """
        try:
            return self.get_ceph_objects(role)[order_id]
        except IndexError:
            return None

    def get_ceph_demons(self, role=None):
        """
        Get Ceph demons list
        Returns:
            list: list of CephDemon

        """
        node_list = self.get_nodes(role)
        ceph_demon_list = []
        for node in node_list:  # type: CephNode
            ceph_demon_list.extend(node.get_ceph_demons(role))
        return ceph_demon_list

    @property
    def ceph_demon_stat(self):
        """
        Retrieves expected numbers for demons of each role
        Returns:
            dict: Ceph demon stats
        """
        ceph_demon_counter = {}
        for demon in self.get_ceph_demons():
            if demon.role == 'mgr':
                continue
            increment = 1  # len(self.get_osd_devices(demon.node)) if demon.role == 'osd' else 1
            ceph_demon_counter[demon.role] = ceph_demon_counter[demon.role] + increment if ceph_demon_counter.get(
                demon.role) else increment
        return ceph_demon_counter

    def get_metadata_list(self, role, client=None):
        """
        Returns metadata for demons of specified role
        Args:
            role(str): ceph demon role
            client(CephObject): Client with keyring and ceph-common

        Returns:
            list: metadata as json object representation
        """
        if not client:
            client = self.get_ceph_object('client') if self.get_ceph_object('client') else self.get_ceph_object('mon')

        out, err = client.exec_command('sudo ceph {role} metadata -f json-pretty'.format(role=role))

        return json.loads(out.read().decode())

    def get_osd_metadata(self, osd_id, client=None):
        """
        Retruns metadata for osd by given id
        Args:
            osd_id(int): osd id
            client(CephObject): Client with keyring and ceph-common

        Returns:
            dict: osd metadata like::

                 {
                    "id": 8,
                    "arch": "x86_64",
                    "back_addr": "172.16.115.29:6801/1672",
                    "back_iface": "eth0",
                    "backend_filestore_dev_node": "vdd",
                    "backend_filestore_partition_path": "/dev/vdd1",
                    "ceph_version": "ceph version 12.2.5-42.el7cp (82d52d7efa6edec70f6a0fc306f40b89265535fb) luminous
                            (stable)",
                    "cpu": "Intel(R) Xeon(R) CPU E5-2690 v3 @ 2.60GHz",
                    "default_device_class": "hdd",
                    "distro": "rhel",
                    "distro_description": "Red Hat Enterprise Linux",
                    "distro_version": "7.5",
                    "filestore_backend": "xfs",
                    "filestore_f_type": "0x58465342",
                    "front_addr": "172.16.115.29:6800/1672",
                    "front_iface": "eth0",
                    "hb_back_addr": "172.16.115.29:6802/1672",
                    "hb_front_addr": "172.16.115.29:6803/1672",
                    "hostname": "ceph-shmohan-1537910194970-node2-osd",
                    "journal_rotational": "1",
                    "kernel_description": "#1 SMP Wed Mar 21 18:14:51 EDT 2018",
                    "kernel_version": "3.10.0-862.el7.x86_64",
                    "mem_swap_kb": "0",
                    "mem_total_kb": "3880928",
                    "os": "Linux",
                    "osd_data": "/var/lib/ceph/osd/ceph-8",
                    "osd_journal": "/var/lib/ceph/osd/ceph-8/journal",
                    "osd_objectstore": "filestore",
                    "rotational": "1"
                 }

        """
        metadata_list = self.get_metadata_list('osd', client)
        for metadata in metadata_list:
            if metadata.get('id') == osd_id:
                return metadata
        return None

    def get_osd_container_name_by_id(self, osd_id, client=None):
        """
        Args:
            osd_id (int): osd id
            client (CephObject): Client with keyring and ceph-common:

        """
        return self.get_osd_by_id(osd_id, client).container_name

    def get_osd_by_id(self, osd_id, client=None):
        """

        Args:
            osd_id (int): osd id
            client (CephObject): Client with keyring and ceph-common:


        Returns:
            CephDemon: daemon object

        """
        hostname = self.get_osd_metadata(osd_id).get('hostname')
        node = self.get_node_by_hostname(hostname)
        osd_device = self.get_osd_device(osd_id)
        osd_demon_list = [osd_demon for osd_demon in node.get_ceph_objects('osd') if osd_demon.device == osd_device]
        return osd_demon_list[0] if len(osd_demon_list) > 0 else None

    def get_osd_service_name(self, osd_id, client=None):
        """

        Args:
            osd_id (int): osd id
            client (CephObject): Client with keyring and ceph-common:

        Returns:
            str : service name

        """
        osd_demon = self.get_osd_by_id(osd_id, client)
        if osd_demon is None:
            raise RuntimeError('Unable to locate osd@{id} demon'.format(id=osd_id))
        if not osd_demon.containerized:
            osd_service_id = osd_id
        else:
            osd_service_id = self.get_osd_device(osd_id)
        osd_service_name = 'ceph-osd@{id}'.format(id=osd_service_id)
        return osd_service_name

    def get_osd_device(self, osd_id, client=None):
        """

        Args:
            osd_id (int): osd id
            client (CephObject): Client with keyring and ceph-common:

        Returns:
            str : osd device

        """
        osd_metadata = self.get_osd_metadata(osd_id, client)
        if osd_metadata.get('osd_objectstore') == 'filestore':
            osd_device = osd_metadata.get('backend_filestore_dev_node')
        elif osd_metadata.get('osd_objectstore') == 'bluestore':
            osd_device = osd_metadata.get('bluefs_db_dev_node')
        else:
            raise RuntimeError('Unable to detect filestore type for osd #{osd_id}'.format(osd_id=osd_id))
        return osd_device

    def get_node_by_hostname(self, hostname):
        """
        Returns Ceph node by it's hostname

        Args:
            hostname (str): hostname

        Returns:
            CephNode : ceph node object

        """
        node_list = [node for node in self.get_nodes() if node.hostname == hostname]
        return node_list[0] if len(node_list) > 0 else None

    def get_osd_data_partition_path(self, osd_id, client=None):
        """
        Returns data partition path by given osd id

        Args:
            osd_id (int): osd id
            client (CephObject): Client with keyring and ceph-common:

        Returns:
            str: data partition path

        """
        osd_metadata = self.get_osd_metadata(osd_id, client)
        osd_data = osd_metadata.get('osd_data')
        osd_object = self.get_osd_by_id(osd_id, client)
        out, err = osd_object.exec_command('ceph-volume simple scan {osd_data} --stdout'.format(osd_data=osd_data),
                                           check_ec=False)
        simple_scan = out.read().decode()
        simple_scan = json.loads(simple_scan[simple_scan.index('{')::])
        return simple_scan.get('data').get('path')

    def get_osd_data_partition(self, osd_id, client=None):
        """
        Returns data partition by given osd id

        Args:
            osd_id (int): osd id
            client (CephObject): client, optional

        Returns:
            str: data path

        """
        osd_partition_path = self.get_osd_data_partition_path(osd_id, client)
        return osd_partition_path[osd_partition_path.rfind('/') + 1::]

    def role_to_node_mapping(self):
        """
        Given a role we should be able to get the corresponding
        CephNode object

        """
        self.role_to_node = {}


class RolesContainer(object):
    """
    Container for single or multiple node roles.
    Can be used as iterable or with equality '==' operator to check if role is present for the node.
    Note that '==' operator will behave the same way as 'in' operator i.e. check that value is present in the role list.
    """

    def __init__(self, role='pool'):
        if isinstance(role, str):
            self.role_list = [str(role)]
        else:
            self.role_list = role if len(role) > 0 else ['pool']

    def __eq__(self, role):
        if isinstance(role, str):
            return role in self.role_list
        else:
            return all(atomic_role in role for atomic_role in self.role_list)

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
    def __init__(self, ip_address, username, password, look_for_keys=False, outage_timeout=300):
        self.ip_address = ip_address
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
                self.__client.connect(self.ip_address,
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
        self.root_passwd = self.password
        self.root_login = self.username
        self.ip_address = kw['ip_address']
        self.vmname = kw['hostname']
        vmshortname = self.vmname.split('.')
        self.vmshortname = vmshortname[0]
        self.volume_list = []
        if kw.get('no_of_volumes', 3):
            self.volume_list = [NodeVolume(NodeVolume.FREE) for vol_id in range(kw['no_of_volumes'])]

        self.ceph_object_list = [CephObjectFactory(self).create_ceph_object(role) for role in kw['role'] if
                                 role != 'pool']
        while len(self.get_ceph_objects('osd')) > 0 and len(self.get_free_volumes()) > 0:
            self.ceph_object_list.append(CephObjectFactory(self).create_ceph_object('osd'))

        if kw.get('ceph_vmnode'):
            self.vm_node = kw['ceph_vmnode']
        self.root_connection = SSHConnectionManager(self.ip_address, 'root', self.root_passwd)
        self.connection = SSHConnectionManager(self.ip_address, self.username, self.password)
        self.rssh = self.root_connection.get_client
        self.rssh_transport = self.root_connection.get_transport
        self.ssh = self.connection.get_client
        self.ssh_transport = self.connection.get_transport
        self.run_once = False

    @property
    def role(self):
        return RolesContainer([ceph_demon.role for ceph_demon in self.ceph_object_list if ceph_demon])

    def get_free_volumes(self):
        return [volume for volume in self.volume_list if volume.status == NodeVolume.FREE]

    def get_allocated_volumes(self):
        return [volume for volume in self.volume_list if volume.status == NodeVolume.ALLOCATED]

    def get_ceph_demons(self, role=None):
        """
         Get Ceph demons list. Only active (those which will be part of the cluster) demons are shown.
         Returns:
             list: list of CephDemon

         """
        return [ceph_demon for ceph_demon in self.get_ceph_objects(role) if
                isinstance(ceph_demon, CephDemon) and ceph_demon.is_active]

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
        self.hostname = out.read().strip().decode()
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
        self.internal_ip = out.read().strip().decode()

    def set_eth_interface(self, eth_interface):
        """
        set the eth interface
        """
        self.eth_interface = eth_interface

    def exec_command(self, **kw):
        """
        execute a command on the vm
        eg: self.exec_cmd(cmd='uptime')
        or
        self.exec_cmd(cmd='background_cmd', check_ec=False)

        Attributes:
            check_ec (bool): False will run the command and not wait for exit
                code

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
                    read += data.decode()
                    logger.info(data.decode())
                if len(xl) > 0:
                    data = channel.recv(1024)
                    read += data.decode()
                    logger.info(data.decode())
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
                raise CommandFailed(kw['cmd'] + " Error:  " + str(stderr.read().decode()) + ' ' + str(self.ip_address))
            return stdout, stderr
        else:
            return stdout, stderr

    def write_file(self, **kw):
        if kw.get('sudo'):
            client = self.rssh
        else:
            client = self.ssh
        file_name = kw['file_name']
        file_mode = kw['file_mode']
        ftp = client().open_sftp()
        remote_file = ftp.file(file_name, file_mode, -1)
        return remote_file

    def _keep_alive(self):
        while True:
            self.exec_command(cmd='uptime', check_ec=False)
            sleep(60)

    def reconnect(self):
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
        self.root_connection = SSHConnectionManager(self.ip_address, 'root', self.root_passwd)
        self.connection = SSHConnectionManager(self.ip_address, self.username, self.password)
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

        Returns:
            CephObject|CephDemon: created ceph object

        """
        ceph_object = CephObjectFactory(self).create_ceph_object(role)
        self.ceph_object_list.append(ceph_object)
        return ceph_object

    def remove_ceph_object(self, ceph_object):
        """
        Removes ceph object form the node
        Args:
            ceph_object(CephObject): ceph object to remove

        """
        self.ceph_object_list.remove(ceph_object)
        if ceph_object.role == 'osd':
            self.get_allocated_volumes()[0].status = NodeVolume.FREE

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
        eth_interface_list = out.read().strip().decode().split('\n')
        for eth_interface in eth_interface_list:
            try:
                for ceph_node in ceph_node_list:
                    if self.vmname == ceph_node.vmname:
                        logger.info("Skipping ping check on localhost")
                        continue
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
            node's exec_command result

        """
        return self.node.exec_command(cmd=cmd, **kw)

    def write_file(self, **kw):
        """
        Proxy to node's write file
        Args:
            **kw: options

        Returns:
            node's write_file result

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
        self.__custom_container_name = None
        self.is_active = True

    @property
    def container_name(self):
        return ('ceph-{role}-{host}'.format(role=self.role, host=self.node.hostname)
                if not self.__custom_container_name else self.__custom_container_name) if self.containerized else ''

    @container_name.setter
    def container_name(self, name):
        self.__custom_container_name = name

    @property
    def container_prefix(self):
        return 'sudo docker exec {container_name}'.format(
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
        return self.node.exec_command(cmd=cmd, **kw)

    def ceph_demon_by_container_name(self, container_name):
        self.exec_command(cmd='sudo docker info')


class CephOsd(CephDemon):
    def __init__(self, node, device=None):
        """
        Represents single osd instance associated with a device.
        Args:
            node (CephNode): ceph node
            device (str): device, can be left unset but must be set during inventory file configuration

        """
        super(CephOsd, self).__init__('osd', node)
        self.device = device

    @property
    def container_name(self):
        return 'ceph-{role}-{device}'.format(role=self.role, device=self.device) if self.containerized else ''

    @property
    def is_active(self):
        return True if self.device else False

    @is_active.setter
    def is_active(self, value):
        pass


class CephClient(CephObject):
    def __init__(self, role, node):
        """
        Ceph client representation, works as proxy to exec_command method.
        Args:
            role(str): role string
            node(CephNode): node object

        """
        super(CephClient, self).__init__(role, node)


class CephObjectFactory(object):
    DEMON_ROLES = ['mon', 'osd', 'mgr', 'rgw', 'mds', 'nfs']
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
        if role == self.CLIENT_ROLES:
            return CephClient(role, self.node)
        if role == 'osd':
            free_volume_list = self.node.get_free_volumes()
            if len(free_volume_list) > 0:
                free_volume_list[0].status = NodeVolume.ALLOCATED
            else:
                raise RuntimeError('Insufficient of free volumes')
            return CephOsd(self.node)
        if role in self.DEMON_ROLES:
            return CephDemon(role, self.node)
        if role != 'pool':
            return CephObject(role, self.node)
