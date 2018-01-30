import yaml
import logging
import json
import re

from ceph.utils import setup_deb_repos
from ceph.utils import setup_repos, check_ceph_healthly

logger = logging.getLogger(__name__)
log = logger


def run(**kw):
    log.info("Running test")
    ceph_nodes = kw.get('ceph_nodes')
    log.info("Running ceph ansible test")
    config = kw.get('config')
    test_data = kw.get('test_data')
    if config.get('ubuntu_repo'):
        ubuntu_repo = config.get('ubuntu_repo')
    if config.get('base_url'):
        base_url = config.get('base_url')
    installer_url = None
    if config.get('installer_url'):
        installer_url = config.get('installer_url')
    if config.get('skip_setup') is True:
        log.info("Skipping setup of ceph cluster")
        return 0
    for node in ceph_nodes:
        if node.role == 'installer':
            ceph_installer = node
        elif node.role == 'mon':
            ceph_mon = node


    for ceph in ceph_nodes:
        # setup latest repo's
        if ceph.pkg_type == 'deb':
            setup_deb_repos(ceph, ubuntu_repo)
        else:
            setup_repos(ceph, base_url, installer_url)
            log.info("Using the cdn repo for the test")
        log.info("Updating metadata")
        if ceph.pkg_type == 'rpm':
            ceph.exec_command(sudo=True, cmd='yum update metadata')

    ceph_installer.exec_command(
        sudo=True, cmd='cd cd; yum install -y ceph-ansible ; sleep 4')
    ceph_installer.exec_command(
        cmd='cp -R /usr/share/ceph-ansible ~/ ; sleep 2')

    # copy rolling update from infrastructure playbook
    ceph_installer.exec_command(cmd='cd ceph-ansible ; cp infrastructure-playbooks/rolling_update.yml .')
    out, rc = ceph_installer.exec_command(
        cmd='cd ceph-ansible ; ansible-playbook -e ireallymeanit=yes -vv -i hosts rolling_update.yml', long_running=True)

    # check if all mon's and osd's are in correct state
    num_osds = test_data['ceph-ansible']['num-osds']
    num_mons = test_data['ceph-ansible']['num-osds']
    if (rc != 0):
        log.info("Failed during upgrade")
        return rc
    return check_ceph_healthly(ceph_mon, num_osds, num_mons)
