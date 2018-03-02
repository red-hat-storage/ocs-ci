import logging
from time import sleep

from ceph.parallel import parallel

log = logging.getLogger(__name__)


def run(**kw):
    log.info("Running test")
    ceph_nodes = kw.get('ceph_nodes')
    iscsi_clients_node = None
    installer_node = None
    ansible_dir = '/usr/share/ceph-ansible'

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
    installer_node.exec_command(sudo=True,
                                cmd='cd {ansible_dir}; cp {ansible_dir}/infrastructure-playbooks/purge-cluster.yml .'
                                .format(ansible_dir=ansible_dir))
    out, err = installer_node.exec_command(
        cmd="cd {ansible_dir} ; ansible-playbook -i hosts purge-cluster.yml --extra-vars 'ireallymeanit=yes'"
            .format(ansible_dir=ansible_dir),
        long_running=True)

    if err == 0:
        log.info("ansible-playbook purge cluster successful")
        installer_node.exec_command(sudo=True, cmd="rm -rf {ansible_dir}".format(ansible_dir=ansible_dir))
        with parallel() as p:
            for cnode in ceph_nodes:
                if cnode.role != "installer":
                    p.spawn(reboot_node, cnode)
        return 0
    else:
        log.info("ansible-playbook failed to purge cluster")
        return 1


def reboot_node(client):
    client.exec_command(sudo=True, cmd='reboot', check_ec=False)
    sleep(300)
    client.reconnect()
