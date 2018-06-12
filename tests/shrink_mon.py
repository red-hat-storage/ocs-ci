import logging

import re
import yaml

from ceph.utils import check_ceph_healthly, get_root_permissions

log = logging.getLogger(__name__)


def run(**kw):
    """
    Remove monitor from cluster using shrink-mon.yml
    :param kw: config sample:
        config:
           mon-to-kill:
            - .*node8.*
    :return: non-zero on failure, zero on pass
    """
    log.info("Shrinking monitor")
    ceph_nodes = kw.get('ceph_nodes')
    config = kw.get('config')
    ansible_dir = '/usr/share/ceph-ansible'
    mon_to_kill_list = config.get('mon-to-kill')
    mon_to_kill = None
    mon_short_name_list = [ceph_node.shortname for ceph_node in ceph_nodes if ceph_node.role == 'mon']
    for _mon_to_kill in mon_to_kill_list:
        matcher = re.compile(_mon_to_kill)
        matched_short_names = filter(matcher.match, mon_short_name_list)
        if len(matched_short_names) > 0:
            shrinked_nodes = [ceph_node for ceph_node in ceph_nodes if ceph_node.shortname in matched_short_names]
            for ceph_node in shrinked_nodes:
                ceph_node.role.remove('mon')
        else:
            raise RuntimeError('No match for {node_name}'.format(node_name=_mon_to_kill))
        mon_to_kill = ','.join([mon_to_kill, ','.join(matched_short_names)]) if mon_to_kill else ','.join(
            matched_short_names)

    for node in ceph_nodes:
        if node.role == 'installer':
            log.info("Setting installer node")
            ceph_installer = node
            break

    out, err = ceph_installer.exec_command(sudo=True,
                                           cmd='cat {ansible_dir}/group_vars/all.yml'.format(ansible_dir=ansible_dir))
    all_yaml = yaml.safe_load(out.read())
    config['ansi_config'] = all_yaml

    ceph_installer.exec_command(sudo=True, cmd='cp -R {ansible_dir}/infrastructure-playbooks/shrink-mon.yml '
                                               '{ansible_dir}/shrink-mon.yml'.format(ansible_dir=ansible_dir))

    get_root_permissions(ceph_installer, '/var/log')
    out, err = ceph_installer.exec_command(cmd='export ANSIBLE_DEPRECATION_WARNINGS=False ; cd {ansible_dir} ; '
                                               'ansible-playbook -e ireallymeanit=yes shrink-mon.yml '
                                               '-e mon_to_kill={mon_to_kill} -i hosts'.format(ansible_dir=ansible_dir,
                                                                                              mon_to_kill=mon_to_kill),
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

    for node in ceph_nodes:
        if node.role == 'client':
            log.info("Setting client node")
            ceph_client = node
            break

    if check_ceph_healthly(ceph_client, num_osds, num_mons, mon_container=None, timeout=120) != 0:
        return 1
    return 0
