import datetime
import yaml
import logging
import json
import re
import random
import install_iscsi_gwcli
from ceph.parallel import parallel
from ceph.utils import setup_deb_repos
from ceph.utils import setup_repos, create_ceph_conf
from time import sleep



logger = logging.getLogger(__name__)
log = logger



def run(**kw):
    log.info("Running test")
    ceph_nodes = kw.get('ceph_nodes')
    for ceph in ceph_nodes:
        if ceph.role=="installer":
            installer_node=ceph
            break
    for ceph in ceph_nodes:
        if ceph.role=="iscsi-clients":
            iscsi_clients_node=ceph
            break
    log.info("Purge Ceph cluster")
    iscsi_clients_node.exec_command(cmd='sudo yum remove -y iscsi-initiator-utils')
    iscsi_clients_node.exec_command(cmd='sudo yum remove -y device-mapper-multipath')
    installer_node.exec_command(
        cmd="cp ~/ceph-ansible/infrastructure-playbooks/purge-cluster.yml ~/ceph-ansible/")
    out,err=installer_node.exec_command(
        cmd="cd ~/ceph-ansible ; ansible-playbook -i hosts purge-cluster.yml --extra-vars 'ireallymeanit=yes'",
        long_running=True)
    output=err
    output=output
    if output==0:
        log.info("ansible-playbook purge cluster successfull")
        installer_node.exec_command(sudo=True,cmd="rm -rf ~/ceph-ansible")
        with parallel() as p:
            for cnode in ceph_nodes:
                if cnode.role != "installer":
                    p.spawn(reboot_node, cnode)
        return 0
    else:
        log.info("ansible-playbook failed to purge cluster")
        return 1

def reboot_node(client):
    client.exec_command(cmd='sudo reboot', check_ec=False)
    sleep(300)
    client.reconnect()