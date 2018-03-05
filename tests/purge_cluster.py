import logging
from time import sleep

import datetime
import re

from ceph.parallel import parallel

log = logging.getLogger(__name__)


def run(**kw):
    """

    :param kw:
       - ceph_nodes: ceph node list representing a cluster
       - config: (optional)
         - ansible-dir: path to nsible working directory, default is /usr/share/ceph-ansible
         - inventory: ansible inventory file, default is hosts
         - playbook-command: ansible playbook command string,
            default is purge-cluster.yml --extra-vars 'ireallymeanit=yes'


    :return: 0 on sucess, non-zero for failures
    """
    log.info("Running test")
    ceph_nodes = kw.get('ceph_nodes')
    iscsi_clients_node = None
    installer_node = None
    ansible_dir = '/usr/share/ceph-ansible'
    inventory = 'hosts'
    playbook_command = "purge-cluster.yml --extra-vars 'ireallymeanit=yes'"
    config = kw.get('config')
    if config:
        ansible_dir = config.get('ansible-dir', ansible_dir)
        inventory = config.get('inventory', inventory)
        playbook_command = config.get('playbook-command', playbook_command)

    for ceph in ceph_nodes:
        if ceph.role == "installer":
            installer_node = ceph
            break
    for ceph in ceph_nodes:
        if ceph.role == "iscsi-clients":
            iscsi_clients_node = ceph
            break

    log.info("Purge Ceph cluster")

    if iscsi_clients_node:
        iscsi_clients_node.exec_command(sudo=True, cmd='yum remove -y iscsi-initiator-utils')
        iscsi_clients_node.exec_command(sudo=True, cmd='yum remove -y device-mapper-multipath')

    playbook_regex = re.search('(purge-.*?\\.yml)(.*)', playbook_command)
    playbook = playbook_regex.group(1)
    playbook_options = playbook_regex.group(2)
    installer_node.exec_command(sudo=True,
                                cmd='cd {ansible_dir}; cp {ansible_dir}/infrastructure-playbooks/{playbook} .'
                                .format(ansible_dir=ansible_dir, playbook=playbook))
    out, err = installer_node.exec_command(
        cmd="cd {ansible_dir} ; ansible-playbook -i {inventory} {playbook} {playbook_options}"
            .format(ansible_dir=ansible_dir, playbook=playbook.strip(), playbook_options=playbook_options.strip(),
                    inventory=inventory.strip()),
        long_running=True)

    if err == 0:
        log.info("ansible-playbook purge cluster successful")
        installer_node.exec_command(sudo=True, cmd="rm -rf {ansible_dir}".format(ansible_dir=ansible_dir))
        if installer_node.pkg_type == 'deb':
            installer_node.exec_command(
                sudo=True, cmd='apt-get remove -y ceph-ansible')
            installer_node.exec_command(
                sudo=True, cmd='apt-get install -y ceph-ansible')
        else:
            installer_node.exec_command(
                sudo=True, cmd='yum remove -y ceph-ansible')
            installer_node.exec_command(
                sudo=True, cmd='yum install -y ceph-ansible')
        with parallel() as p:
            for cnode in ceph_nodes:
                if cnode.role != "installer":
                    p.spawn(reboot_node, cnode)
        return 0
    else:
        log.info("ansible-playbook failed to purge cluster")
        return 1


def reboot_node(ceph_node, timeout=300):
    ceph_node.exec_command(sudo=True, cmd='reboot', check_ec=False)
    timeout = datetime.timedelta(seconds=timeout)
    starttime = datetime.datetime.now()
    while True:
        try:
            ceph_node.reconnect()
            break
        except BaseException:
            if datetime.datetime.now() - starttime > timeout:
                log.error('Failed to reconnect to the node {node} after reboot '.format(node=ceph_node.ip_address))
                sleep(5)
                raise RuntimeError(
                    'Failed to reconnect to the node {node} after reboot '.format(node=ceph_node.ip_address))
