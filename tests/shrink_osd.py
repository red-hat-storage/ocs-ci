import json
import logging
import yaml

from ceph.ceph import NodeVolume
from ceph.utils import check_ceph_healthly, get_root_permissions

log = logging.getLogger(__name__)


def run(**kw):
    """
    Remove osd from cluster using shrink-osd.yml
    :param kw: config sample:
        config:
          osd-to-kill:
            - 2
    :return: non-zero on failure, zero on pass
    """
    log.info("Shrinking osd")
    ceph_nodes = kw.get('ceph_nodes')
    config = kw.get('config')
    ansible_dir = '/usr/share/ceph-ansible'
    osd_to_kill_list = config.get('osd-to-kill')
    osd_to_kill = ','.join((str(osd) for osd in osd_to_kill_list))

    for node in ceph_nodes:
        if node.role == 'client':
            log.info("Setting client node")
            ceph_client = node
            break

    for osd_id in osd_to_kill_list:
        out, err = ceph_client.exec_command(sudo=True, cmd='ceph osd find {osd_id}'.format(osd_id=osd_id))
        osd = json.loads(out.read())
        osd_host = osd.get('crush_location').get('host')
        for ceph_node in ceph_nodes:
            osd_volumes = ceph_node.get_allocated_volumes()
            if ceph_node.shortname == str(osd_host):
                osd_volumes.pop().status = NodeVolume.FREE
                if len(osd_volumes) < 1:
                    ceph_node.role.remove('osd')

    for node in ceph_nodes:
        if node.role == 'installer':
            log.info("Setting installer node")
            ceph_installer = node
            break

    out, err = ceph_installer.exec_command(sudo=True,
                                           cmd='cat {ansible_dir}/group_vars/all.yml'.format(ansible_dir=ansible_dir))
    all_yaml = yaml.safe_load(out.read())
    config['ansi_config'] = all_yaml

    ceph_installer.exec_command(sudo=True, cmd='cp -R {ansible_dir}/infrastructure-playbooks/shrink-osd.yml '
                                               '{ansible_dir}/shrink-osd.yml'.format(ansible_dir=ansible_dir))

    get_root_permissions(ceph_installer, '/var/log')

    out, err = ceph_installer.exec_command(cmd='export ANSIBLE_DEPRECATION_WARNINGS=False ; cd {ansible_dir} ; '
                                               'ansible-playbook -e ireallymeanit=yes shrink-osd.yml '
                                               '-e osd_to_kill={osd_to_kill} -i hosts'.format(ansible_dir=ansible_dir,
                                                                                              osd_to_kill=osd_to_kill),
                                           long_running=True)

    if err != 0:
        log.error("Failed during ansible playbook run")
        return err

    num_osds = 0
    num_mons = 0
    num_mgrs = 0
    for node in ceph_nodes:
        if node.role == 'mon':
            num_mons += 1
        if node.role == 'mgr':
            num_mgrs += 1
        if node.role == 'osd':
            devices = len(node.get_allocated_volumes())
            devchar = 98
            devs = []
            for vol in range(0, devices):
                dev = '/dev/vd' + chr(devchar)
                devs.append(dev)
                devchar += 1
            reserved_devs = []
            if config['ansi_config'].get('osd_scenario') == 'non-collocated':
                reserved_devs = \
                    [raw_journal_device for raw_journal_device in set(config['ansi_config'].get('dedicated_devices'))]
            devs = [_dev for _dev in devs if _dev not in reserved_devs]
            num_osds = num_osds + len(devs)

    if check_ceph_healthly(ceph_client, num_osds, num_mons, mon_container=None, timeout=120) != 0:
        return 1
    return 0
