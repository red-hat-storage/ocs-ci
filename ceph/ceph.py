import logging
import json
import re
from distutils.version import LooseVersion
from select import select
from time import sleep

import datetime
import requests
import paramiko
import yaml
from paramiko.ssh_exception import SSHException

from utility.utils import custom_ceph_config

logger = logging.getLogger(__name__)


class Ceph(object):
    DEFAULT_RHCS_VERSION = '3.2'

    def __init__(self, name, node_list=None):
        """
        Ceph cluster representation. Contains list of cluster nodes.
        Args:
            name (str): cluster name
            node_list (ceph.utils.CephVMNode): CephVMNode list
        """
        self.name = name
        self.node_list = list(node_list)
        self.use_cdn = False
        self.custom_config_file = None
        self.custom_config = None
        self.allow_custom_ansible_config = True
        self.__rhcs_version = None

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

    @property
    def rhcs_version(self):
        """
        Get rhcs version, will return DEFAULT_RHCS_VERSION if not set
        Returns:
            LooseVersion: rhcs version of given cluster

        """
        return LooseVersion(str(self.__rhcs_version if self.__rhcs_version else self.DEFAULT_RHCS_VERSION))

    @rhcs_version.setter
    def rhcs_version(self, version):
        self.__rhcs_version = version

    def get_nodes(self, role=None, ignore=None):
        """
        Get node(s) by role. Return all nodes if role is not defined
        Args:
            role (str): node's role. Can be RoleContainer or str. Takes precedence over ignore
            ignore (str): node's role to ignore from the list. Can be RoleContainer or str

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
        Returns single ceph object. If order id is provided returns that occurence from results list, otherwise returns
        first occurence
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

    def setup_ceph_firewall(self):
        """
        Open required ports on nodes based on relevant ceph demons types
        """
        for node in self.get_nodes():
            if node.role == 'mon':
                node.open_firewall_port(port='6789', protocol='tcp')
                # for upgrades from 2.5 to 3.x, we convert mon to mgr
                # so lets open ports from 6800 to 6820
                node.open_firewall_port(port='6800-6820', protocol='tcp')
            if node.role == 'osd':
                node.open_firewall_port(port='6800-7300', protocol='tcp')
            if node.role == 'mgr':
                node.open_firewall_port(port='6800-6820', protocol='tcp')
            if node.role == 'mds':
                node.open_firewall_port(port='6800', protocol='tcp')
            if node.role == 'iscsi-gw':
                node.open_firewall_port(port='3260', protocol='tcp')
                node.open_firewall_port(port='5000-5001', protocol='tcp')

    def setup_ssh_keys(self):
        """
        Generate and distribute ssh keys within cluster
        """
        keys = ''
        hosts = ''
        hostkeycheck = 'Host *\n\tStrictHostKeyChecking no\n\tServerAliveInterval 2400\n'
        for ceph in self.get_nodes():
            ceph.generate_id_rsa()
            keys = keys + ceph.id_rsa_pub
            hosts = hosts + ceph.ip_address + "\t" + ceph.hostname + "\t" + ceph.shortname + "\n"
        for ceph in self.get_nodes():
            keys_file = ceph.write_file(
                file_name='.ssh/authorized_keys', file_mode='a')
            hosts_file = ceph.write_file(
                sudo=True, file_name='/etc/hosts', file_mode='a')
            ceph.exec_command(
                cmd='[ -f ~/.ssh/config ] && chmod 700 ~/.ssh/config',
                check_ec=False)
            ssh_config = ceph.write_file(file_name='.ssh/config', file_mode='a')
            keys_file.write(keys)
            hosts_file.write(hosts)
            ssh_config.write(hostkeycheck)
            keys_file.flush()
            hosts_file.flush()
            ssh_config.flush()
            ceph.exec_command(cmd='chmod 600 ~/.ssh/authorized_keys')
            ceph.exec_command(cmd='chmod 400 ~/.ssh/config')

    def generate_ansible_inventory(self, bluestore=False):
        """
        Generate ansible inventory file content for given cluster
        Args:
            bluestore(bool): True for bluestore usage, dafault False

        Returns:
            str: inventory

        """
        mon_hosts = []
        osd_hosts = []
        rgw_hosts = []
        mds_hosts = []
        mgr_hosts = []
        nfs_hosts = []
        client_hosts = []
        iscsi_gw_hosts = []
        for node in self:
            eth_interface = node.search_ethernet_interface(self)
            if eth_interface is None:
                err = 'Network test failed: No suitable interface is found on {node}.'.format(node=node.ip_address)
                logger.error(err)
                raise RuntimeError(err)
            node.set_eth_interface(eth_interface)
            mon_interface = ' monitor_interface=' + node.eth_interface + ' '
            if node.role == 'mon':
                mon_host = node.shortname + ' monitor_interface=' + node.eth_interface
                mon_hosts.append(mon_host)
                # num_mons += 1
            if node.role == 'mgr' and self.rhcs_version >= '3':
                mgr_host = node.shortname + ' monitor_interface=' + node.eth_interface
                mgr_hosts.append(mgr_host)
            if node.role == 'osd':
                devs = self.get_osd_devices(node)
                # num_osds = num_osds + len(devs)
                auto_discovery = self.ansible_config.get('osd_auto_discovery', False)
                objectstore = ''
                if bluestore:
                    objectstore = 'osd_objectstore="bluestore"'
                osd_host = node.shortname + mon_interface + (
                    " devices='" + json.dumps(devs) + "'" if not auto_discovery else '') + ' ' + objectstore
                osd_hosts.append(osd_host)
            if node.role == 'mds':
                mds_host = node.shortname + ' monitor_interface=' + node.eth_interface
                mds_hosts.append(mds_host)
            if node.role == 'nfs' and self.rhcs_version >= '3':
                nfs_host = node.shortname + ' monitor_interface=' + node.eth_interface
                nfs_hosts.append(nfs_host)
            if node.role == 'rgw':
                rgw_host = node.shortname + ' radosgw_interface=' + node.eth_interface
                rgw_hosts.append(rgw_host)
            if node.role == 'client':
                client_host = node.shortname + ' client_interface=' + node.eth_interface
                client_hosts.append(client_host)
            if node.role == 'iscsi-gw':
                iscsi_gw_host = node.shortname
                iscsi_gw_hosts.append(iscsi_gw_host)
        hosts_file = ''
        if mon_hosts:
            mon = '[mons]\n' + '\n'.join(mon_hosts)
            hosts_file += mon + '\n'
        if mgr_hosts:
            mgr = '[mgrs]\n' + '\n'.join(mgr_hosts)
            hosts_file += mgr + '\n'
        if osd_hosts:
            osd = '[osds]\n' + '\n'.join(osd_hosts)
            hosts_file += osd + '\n'
        if mds_hosts:
            mds = '[mdss]\n' + '\n'.join(mds_hosts)
            hosts_file += mds + '\n'
        if nfs_hosts:
            nfs = '[nfss]\n' + '\n'.join(nfs_hosts)
            hosts_file += nfs + '\n'
        if rgw_hosts:
            rgw = '[rgws]\n' + '\n'.join(rgw_hosts)
            hosts_file += rgw + '\n'
        if client_hosts:
            client = '[clients]\n' + '\n'.join(client_hosts)
            hosts_file += client + '\n'
        if iscsi_gw_hosts:
            iscsi_gw = '[iscsigws]\n' + '\n'.join(iscsi_gw_hosts)
            hosts_file += iscsi_gw + '\n'
        logger.info('Generated hosts file: \n{file}'.format(file=hosts_file))
        return hosts_file

    def get_osd_devices(self, node):
        """
        Get osd devices list
        Args:
            node(CephNode): Ceph node with osd demon

        Returns:
            list: devices

        """
        devices = len(node.get_allocated_volumes())
        devchar = 98
        devs = []
        for vol in range(0, devices):
            dev = '/dev/vd' + chr(devchar)
            devs.append(dev)
            devchar += 1
        reserved_devs = []
        collocated = self.ansible_config.get('osd_scenario') == 'collocated'
        if not collocated:
            reserved_devs = \
                [raw_journal_device for raw_journal_device in set(self.ansible_config.get('dedicated_devices'))]
        devs = [_dev for _dev in devs if _dev not in reserved_devs]
        return devs

    def get_ceph_demons(self):
        """
        Get Ceph demons list
        Returns:
            list: list of CephDemon

        """
        return [ceph_demon for ceph_demon in self.get_ceph_objects() if type(ceph_demon) is CephDemon]

    def set_ansible_config(self, ansible_config):
        """
        Set ansible config for all.yml
        Args:
            ansible_config(dict): Ceph Ansible all.yml config
        """
        if self.allow_custom_ansible_config:
            ceph_conf_overrides = ansible_config.get('ceph_conf_overrides')
            custom_config = self.custom_config
            custom_config_file = self.custom_config_file
            ansible_config['ceph_conf_overrides'] = custom_ceph_config(
                ceph_conf_overrides, custom_config, custom_config_file)
            logger.info("ceph_conf_overrides: \n{}".format(
                yaml.dump(ansible_config.get('ceph_conf_overrides'), default_flow_style=False)))
        self.__ansible_config = ansible_config
        self.containerized = self.ansible_config.get('containerized_deployment', False)
        for ceph_demon in self.get_ceph_demons():
            ceph_demon.containerized = True if self.containerized else False
        if self.ansible_config.get('fetch_directory') is None:
            # default fetch directory is not writeable, lets use local one if not set
            self.__ansible_config['fetch_directory'] = '~/fetch/'

    def get_ansible_config(self):
        """
        Get Ansible config settings for all.yml
        Returns:
            dict: Ansible config

        """
        try:
            self.__ansible_config
        except AttributeError:
            raise RuntimeError('Ceph ansible config is not set')
        return self.__ansible_config

    @property
    def ansible_config(self):
        return self.get_ansible_config()

    @ansible_config.setter
    def ansible_config(self, ansible_config):
        self.set_ansible_config(ansible_config)

    def setup_insecure_registry(self):
        """
        Update all ceph demons nodes to allow insecure registry use
        """
        if self.containerized and self.ansible_config.get('ceph_docker_registry'):
            insecure_registry = '{{"insecure-registries" : ["{registry}"]}}'.format(
                registry=self.ansible_config.get('ceph_docker_registry'))
            logger.warn('Adding insecure registry:\n{registry}'.format(registry=insecure_registry))
            for node in self.get_nodes():
                node.write_docker_daemon_json(insecure_registry)

    @property
    def ceph_demon_stat(self):
        """
        Retrieves expected numbers for demons of each role
        Returns:
            dict: Ceph demon stats
        """
        ceph_demon_counter = {}
        for demon in self.get_ceph_demons():
            if demon.role == 'mgr' and self.rhcs_version < '3':
                continue
            increment = len(self.get_osd_devices(demon.node)) if demon.role == 'osd' else 1
            ceph_demon_counter[demon.role] = ceph_demon_counter[demon.role] + increment if ceph_demon_counter.get(
                demon.role) else increment
        return ceph_demon_counter

    @property
    def ceph_stable_release(self):
        """
        Retrieve ceph stable realease based on ansible config (jewel, luminous, etc.)
        Returns:
            str: Ceph stable release
        """
        return self.ansible_config['ceph_stable_release']

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

        return json.loads(out.read())

    def get_osd_metadata(self, osd_id, client=None):
        """
        Retruns metadata for osd by given id
        Args:
            osd_id(int): osd id
            client(CephObject): Client with keyring and ceph-common

        Returns:
            dict: osd metadata like:
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

    def check_health(self, client=None, timeout=300):
        """
        Check if ceph is in healthy state

        Args:
           client(CephObject): ceph object with ceph-common and ceph-keyring
           timeout (int): max time to check if cluster is not healthy within timeout period - return 1
        Returns:
           int: return 0 when ceph is in healthy state, else 1
        """

        if not client:
            client = self.get_ceph_object('client') if self.get_ceph_object('client') else self.get_ceph_object('mon')

        timeout = datetime.timedelta(seconds=timeout)
        starttime = datetime.datetime.now()
        lines = None
        pending_states = ['peering', 'activating', 'creating']
        valid_states = ['active+clean']

        while datetime.datetime.now() - starttime <= timeout:
            out, err = client.exec_command(cmd='sudo ceph -s')
            lines = out.read()

            if not any(state in lines for state in pending_states):
                if all(state in lines for state in valid_states):
                    break
            sleep(5)
        logger.info(lines)
        if not all(state in lines for state in valid_states):
            logger.error("Valid States are not found in the health check")
            return 1
        match = re.search(r"(\d+)\s+osds:\s+(\d+)\s+up,\s+(\d+)\s+in", lines)
        all_osds = int(match.group(1))
        up_osds = int(match.group(2))
        in_osds = int(match.group(3))
        if self.ceph_demon_stat['osd'] != all_osds:
            logger.error("Not all osd's are up. Actual: %s / Expected: %s" % (all_osds, self.ceph_demon_stat['osd']))
            return 1
        if up_osds != in_osds:
            logger.error("Not all osd's are in. Actual: %s / Expected: %s" % (up_osds, all_osds))
            return 1

        # attempt luminous pattern first, if it returns none attempt jewel pattern
        match = re.search(r"(\d+) daemons, quorum", lines)
        if not match:
            match = re.search(r"(\d+) mons at", lines)
        all_mons = int(match.group(1))
        if all_mons != self.ceph_demon_stat['mon']:
            logger.error("Not all monitors are in cluster")
            return 1
        if "HEALTH_ERR" in lines:
            logger.error("HEALTH in ERROR STATE")
            return 1
        return 0

    def distribute_all_yml(self):
        """
        Distributes ansible all.yml config across all installers
        """
        gvar = yaml.dump(self.ansible_config, default_flow_style=False)
        for installer in self.get_ceph_objects('installer'):
            installer.append_to_all_yml(gvar)
        logger.info("updated all.yml: \n" + gvar)

    def refresh_ansible_config_from_all_yml(self, installer=None):
        """
        Refreshes ansible config based on installer all.yml content
        Args:
            installer(CephInstaller): Ceph installer. Will use first available installer if omitted
        """
        if not installer:
            installer = self.get_ceph_object('installer')
        self.ansible_config = installer.get_all_yml()

    def setup_packages(self, base_url, hotfix_repo, installer_url, ubuntu_repo, build=None):
        """
        Setup packages required for ceph-ansible istallation
        Args:
            base_url (str): rhel compose url
            hotfix_repo (str): hotfix repo to use with priority
            installer_url (str): installer url
            ubuntu_repo (str): deb repo url
            build (str): ceph-ansible build as numeric
        """
        if not build:
            build = self.rhcs_version
        for node in self.get_nodes():
            if self.use_cdn:
                if node.pkg_type == 'deb':
                    if node.role == 'installer':
                        logger.info("Enabling tools repository")
                        node.setup_deb_cdn_repo(build)
                else:
                    logger.info("Using the cdn repo for the test")
                    node.setup_rhel_cdn_repos(build)
            else:
                if self.ansible_config.get('ceph_repository_type') != 'iso' or \
                        self.ansible_config.get('ceph_repository_type') == 'iso' and \
                        (node.role == 'installer'):
                    if node.pkg_type == 'deb':
                        node.setup_deb_repos(ubuntu_repo)
                        sleep(15)
                        # install python2 on xenial
                        node.exec_command(sudo=True, cmd='sudo apt-get install -y python')
                        node.exec_command(sudo=True, cmd='apt-get install -y python-pip')
                        node.exec_command(sudo=True, cmd='apt-get install -y ntp')
                        node.exec_command(sudo=True, cmd='apt-get install -y chrony')
                        node.exec_command(sudo=True, cmd='pip install nose')
                    else:
                        if hotfix_repo:
                            node.exec_command(sudo=True,
                                              cmd='wget -O /etc/yum.repos.d/rh_repo.repo {repo}'.format(
                                                  repo=hotfix_repo))
                        else:
                            node.setup_rhel_repos(base_url, installer_url)
                if self.ansible_config.get('ceph_repository_type') == 'iso' and node.role == 'installer':
                    iso_file_url = self.get_iso_file_url(base_url)
                    node.exec_command(sudo=True, cmd='mkdir -p {}/iso'.format(node.ansible_dir))
                    node.exec_command(sudo=True,
                                      cmd='wget -O {}/iso/ceph.iso {}'.format(node.ansible_dir, iso_file_url))
            if node.pkg_type == 'rpm':
                logger.info("Updating metadata")
                node.exec_command(sudo=True, cmd='yum update metadata')
            sleep(15)

    def create_rbd_pool(self, k_and_m):
        """
        Generate pools for later testing use
        Args:
            k_and_m(bool): ec-pool-k-m settings
        """
        ceph_mon = self.get_ceph_object('mon')
        if self.rhcs_version >= '3':
            if k_and_m:
                pool_name = 'rbd'
                ceph_mon.exec_command(
                    cmd='sudo ceph osd erasure-code-profile set %s k=%s m=%s' %
                        ('ec_profile', k_and_m[0], k_and_m[2]))
                ceph_mon.exec_command(
                    cmd='sudo ceph osd pool create %s 64 64 erasure ec_profile' %
                        pool_name)
                ceph_mon.exec_command(
                    cmd='sudo ceph osd pool set %s allow_ec_overwrites true' %
                        (pool_name))
                ceph_mon.exec_command(
                    sudo=True,
                    cmd='ceph osd pool application enable %s rbd --yes-i-really-mean-it' %
                        pool_name)
            else:
                ceph_mon.exec_command(
                    sudo=True, cmd='ceph osd pool create rbd 64 64 ')
                ceph_mon.exec_command(
                    sudo=True,
                    cmd='ceph osd pool application enable rbd rbd --yes-i-really-mean-it')

    @staticmethod
    def get_iso_file_url(base_url):
        """
        Retrurns iso url for given compose link
        Args:
            base_url(str): rhel compose

        Returns:
            str:  iso file url
        """
        iso_file_path = base_url + "compose/Tools/x86_64/iso/"
        iso_dir_html = requests.get(iso_file_path, timeout=10).content
        match = re.search('<a href="(.*?)">(.*?)-x86_64-dvd.iso</a>', iso_dir_html)
        iso_file_name = match.group(1)
        logger.info('Using {}'.format(iso_file_name))
        iso_file = iso_file_path + iso_file_name
        return iso_file

    @staticmethod
    def generate_repository_file(base_url, repos):
        """
        Generate rhel repository file for given repos
        Args:
            base_url(str): rhel compose url
            repos(list): repos behind compose/ to process

        Returns:
            str: repository file content
        """
        repo_file = ''
        for repo in repos:
            repo_to_use = base_url + "compose/" + repo + "/x86_64/os/"
            r = requests.get(repo_to_use, timeout=10)
            logger.info("Checking %s", repo_to_use)
            if r.status_code == 200:
                logger.info("Using %s", repo_to_use)
                header = "[ceph-" + repo + "]" + "\n"
                name = "name=ceph-" + repo + "\n"
                baseurl = "baseurl=" + repo_to_use + "\n"
                gpgcheck = "gpgcheck=0\n"
                enabled = "enabled=1\n\n"
                repo_file = repo_file + header + name + baseurl + gpgcheck + enabled
        return repo_file


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
            build(str|LooseVersion): rhcs version
        """
        user = 'redhat'
        passwd = 'OgYZNpkj6jZAIF20XFZW0gnnwYBjYcmt7PeY76bLHec9'
        num = str(build).split('.')[0]
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
            build(str|LooseVersion): rhcs version
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
        if '2' > build >= '1':
            repos = repos_13x
        elif '3' > build >= '2':
            repos = repos_20
        elif '4' > build >= '3':
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
        return 'ceph-{role}-{host}'.format(role=self.role, host=self.node.hostname) if self.containerized else ''

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
        self.ansible_dir = '/usr/share/ceph-ansible'

    def append_to_all_yml(self, content):
        """
        Adds content to all.yml
        Args:
            content(str): all.yml config as yml string
        """
        all_yml_file = self.write_file(
            sudo=True, file_name='{}/group_vars/all.yml'.format(self.ansible_dir), file_mode='a')
        all_yml_file.write(content)
        all_yml_file.flush()

    def get_all_yml(self):
        """
        Returns all.yml content
        Returns:
            dict: all.yml content

        """
        out, err = self.exec_command(sudo=True,
                                     cmd='cat {ansible_dir}/group_vars/all.yml'.format(
                                         ansible_dir=self.ansible_dir))
        return yaml.safe_load(out.read())

    def get_installed_ceph_versions(self):
        """
        Returns installed ceph versions
        Returns:
            str: ceph vsersions

        """
        if self.pkg_type == 'rpm':
            out, rc = self.exec_command(cmd='rpm -qa | grep ceph')
        else:
            out, rc = self.exec_command(sudo=True, cmd='apt-cache search ceph')
        return out.read()

    def write_inventory_file(self, inventory_config):
        """
        Write inventory to hosts file for ansible use. Old file will be overwritten
        Args:
            inventory_config(str):invnetory config compatible with ceph-ansible
        """
        host_file = self.write_file(
            sudo=True, file_name='{}/hosts'.format(self.ansible_dir), file_mode='w')
        host_file.write(inventory_config)
        host_file.flush()

    def setup_ansible_site_yml(self, containerized):
        """
        Create proper site.yml from sample for containerized or non-containerized deployment
        Args:
            containerized(bool): use site-docker.yml.sample if True else site.yml.sample
        """
        if containerized:
            self.exec_command(
                sudo=True,
                cmd='cp -R {ansible_dir}/site-docker.yml.sample {ansible_dir}/site.yml'.format(
                    ansible_dir=self.ansible_dir))
        else:
            self.exec_command(
                sudo=True, cmd='cp -R {ansible_dir}/site.yml.sample {ansible_dir}/site.yml'.format(
                    ansible_dir=self.ansible_dir))

    def install_ceph_ansible(self, rhbuild, **kw):
        """
        Installs ceph-ansible
        """
        if self.pkg_type == 'deb':
            self.exec_command(sudo=True, cmd='apt-get install -y ceph-ansible')
        else:
            self.exec_command(
                cmd='sudo subscription-manager repos --disable=rhel-7-server-ansible-*-rpms',
                long_running=True)

            if rhbuild == "3.2":
                self.exec_command(
                    cmd='sudo subscription-manager repos --enable=rhel-7-server-ansible-2.6-rpms',
                    long_running=True)
            else:
                self.exec_command(
                    cmd='sudo subscription-manager repos --enable=rhel-7-server-ansible-2.4-rpms',
                    long_running=True)

            if kw.get('upgrade'):
                self.exec_command(sudo=True, cmd='yum update metadata')
                self.exec_command(sudo=True, cmd='yum update -y ceph-ansible')
            else:
                self.exec_command(sudo=True, cmd='yum install -y ceph-ansible')

    def add_iscsi_settings(self, test_data):
        """
        Add iscsi config to iscsigws.yml
        Args:
            test_data: test data dict
        """
        iscsi_file = self.write_file(
            sudo=True, file_name='{}/group_vars/iscsigws.yml'.format(self.ansible_dir), file_mode='a')
        iscsi_file.write(test_data["luns_setting"])
        iscsi_file.write(test_data["initiator_setting"])
        iscsi_file.write(test_data["gw_ip_list"])
        iscsi_file.flush()


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
        if role == 'installer':
            return CephInstaller(role, self.node)
        if role == self.CLIENT_ROLES:
            return CephClient(role, self.node)
        if role in self.DEMON_ROLES:
            return CephDemon(role, self.node)
        if role != 'pool':
            return CephObject(role, self.node)
